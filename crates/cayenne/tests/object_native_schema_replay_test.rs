/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! The `schema` model family's traces (`cayenne-metastore-model`, `schema.rs`)
//! replayed through real Cayenne instances on one shared object-native
//! location: three instances, each with its own catalog handle, session and
//! request log, open the dataset as the writer or as a replica, widen its
//! schema, write, plan, run, follow and are killed as each trace says.
//!
//! The driver steps the model's design (`schema` mode, exported by the model
//! crate) beside the real calls, and checks after every action, against an
//! oracle of the rows it wrote:
//!
//! 1. every serving instance's fresh `SELECT *` returns exactly the oracle's
//!    rows in the schema the model says it serves, with that schema's column
//!    names and types;
//! 2. a kept plan run after a widening answers in its planned schema with exact
//!    values, or fails with a cast error for a value its type cannot hold;
//! 3. the stored schema, read by a fresh catalog handle, is the model's copy
//!    schema;
//! 4. a replica sends no write to the location, and opening one leaves every
//!    log head byte-for-byte as it was.
//!
//! The control files' FAIL witnesses are stepped through the control's own
//! model too, which must break the witness's property at its last step, while
//! the real code, which is the design, must keep every guarantee. See
//! `tests/traces/README.md` for which actions are driven and which are counted.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, SchemaEvolutionMode, VortexConfig};
use cayenne::metastore::object_native::{Access, NativeCatalogConfig, ObjectNativeCatalog};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::prelude::{DataFrame, SessionContext};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store_occ::LocalConditionalPut;
use object_store_sim::Op;
use object_store_sim::fault::FaultStore;
use serde::Deserialize;

const LOCATION: &str = "s3://bucket/state/cayenne/v1";
const TABLE: &str = "orders";

/// A copy of the design and controls of `cayenne-metastore-model`'s `schema`
/// family (its `State`, `actions` and `next_state`, line for line), which the
/// model crate does not export. The control replays check that it breaks each
/// control's property where the model's driver said it does.
/// The `schema` family itself, as the model crate exports it for this replay.
mod model {
    pub use cayenne_metastore_model::{
        SCHEMA_INSTANCES as INSTANCES, SchemaAction as Action, SchemaProtocol as Protocol,
        SchemaState as State, SchemaStatus as Status,
    };
}

use model::{Action, Protocol, State, Status};

#[derive(Debug, Deserialize)]
struct Trace {
    kind: String,
    run: String,
    actions: Vec<String>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    property: String,
}

fn traces(text: &str) -> Vec<Trace> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("driver JSON"))
        .collect()
}

/// The Arrow schema of the model's source schema `schema`. Schema 2, a change no
/// policy applies in place, has no counterpart: the design never stores it.
fn arrow_schema(schema: u8) -> Result<SchemaRef, String> {
    match schema {
        0 => Ok(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int32, false),
        ]))),
        1 => Ok(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, true),
        ]))),
        other => Err(format!("schema {other} has no Cayenne counterpart")),
    }
}

/// Column names and types, with view strings read as the stored strings they are.
fn shape(schema: &Schema) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = match field.data_type() {
                DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
                other => other.clone(),
            };
            (field.name().clone(), data_type)
        })
        .collect()
}

/// Which model schema the Arrow `schema` is, if any.
fn model_schema_of(schema: &Schema) -> Option<u8> {
    (0..=1).find(|candidate| {
        arrow_schema(*candidate).is_ok_and(|arrow| shape(&arrow) == shape(schema))
    })
}

/// One row the driver wrote: `extra` is `None` for a row written under schema 0.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    id: i64,
    v: i64,
    extra: Option<String>,
}

/// What a query answering in model schema `schema` must return for `oracle`:
/// its rows, or `Err` naming a value the schema's `v` cannot hold.
fn expected_answer(oracle: &[Row], schema: u8) -> Result<Vec<Row>, i64> {
    let mut rows: Vec<Row> = oracle.to_vec();
    rows.sort();
    if schema == 0 {
        for row in &mut rows {
            if i32::try_from(row.v).is_err() {
                return Err(row.v);
            }
            row.extra = None;
        }
    }
    Ok(rows)
}

/// The rows of `batches`, which answer in model schema `schema`.
fn rows_of(batches: &[RecordBatch], schema: u8) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| format!("the answer has no column '{name}'"))
        };
        let id = column("id")?;
        let id = id
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        let v = arrow::compute::cast(column("v")?, &DataType::Int64).map_err(|e| e.to_string())?;
        let v = v
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("v is not Int64")?;
        let extra = if schema == 1 {
            Some(
                arrow::compute::cast(column("extra")?, &DataType::Utf8)
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        let extra = extra
            .as_ref()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("extra is not a string")
            })
            .transpose()?;
        for row in 0..batch.num_rows() {
            if id.is_null(row) || v.is_null(row) {
                return Err(format!("row {row} has a NULL id or v"));
            }
            rows.push(Row {
                id: id.value(row),
                v: v.value(row),
                extra: extra
                    .and_then(|extra| (!extra.is_null(row)).then(|| extra.value(row).to_string())),
            });
        }
    }
    rows.sort();
    Ok(rows)
}

/// The value instance `i` writes into `v` for row `id` under model schema
/// `schema`: under the widened schema every other row needs the wider type.
fn written_value(id: i64, schema: u8) -> i64 {
    if schema == 1 && id % 2 == 0 {
        3_000_000_000 + id
    } else {
        id * 10
    }
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    InMemory,
    Local,
}

#[derive(Clone, Copy, Debug)]
enum Layout {
    /// Every write in data files.
    Files,
    /// Small writes inlined in the metastore.
    Inline,
}

/// One running instance's dataset.
struct Node {
    writer: bool,
    requests: Arc<FaultStore>,
    ctx: SessionContext,
    table: Arc<CayenneTableProvider>,
    /// The plan the instance cached.
    kept: Option<DataFrame>,
}

/// What the replay drove and what it only counted, across traces.
#[derive(Default)]
struct Tally {
    driven: BTreeMap<&'static str, usize>,
    counted: BTreeMap<&'static str, usize>,
    /// Kept plans re-made because the table's schema changed since they were
    /// planned (the runtime's plan cache check).
    replans: usize,
    /// Kept plans run on a table that took up a wider schema since they were
    /// planned: the scan was conformed to the planned schema.
    conformed: usize,
    /// Of those, runs that failed on a value the planned type cannot hold.
    cast_errors: usize,
    /// Replicas that took up the writer's wider schema without reopening.
    adoptions: usize,
    /// Checks of a replica whose next query would take up a committed schema
    /// the model has it not serve yet: the schema is checked, the rows are not.
    deferred: usize,
    /// Fresh queries whose rows were compared.
    answers: usize,
    traces: usize,
    steps: usize,
}

impl Tally {
    fn merge(&mut self, other: Tally) {
        for (name, n) in other.driven {
            *self.driven.entry(name).or_default() += n;
        }
        for (name, n) in other.counted {
            *self.counted.entry(name).or_default() += n;
        }
        self.replans += other.replans;
        self.conformed += other.conformed;
        self.cast_errors += other.cast_errors;
        self.adoptions += other.adoptions;
        self.deferred += other.deferred;
        self.answers += other.answers;
        self.traces += other.traces;
        self.steps += other.steps;
    }
}

fn name_of(action: Action) -> &'static str {
    match action {
        Action::SourceWidens => "SourceWidens",
        Action::SourceChangesIncompatibly => "SourceChangesIncompatibly",
        Action::SourceReverts => "SourceReverts",
        Action::Open(_) => "Open",
        Action::TakeLease(_) => "TakeLease",
        Action::CommitSchema(_) => "CommitSchema",
        Action::CommitCheckpoint(_) => "CommitCheckpoint",
        Action::Write(_) => "Write",
        Action::Follow(_) => "Follow",
        Action::Plan(_) => "Plan",
        Action::Run(_) => "Run",
        Action::Recreate(_) => "Recreate",
        Action::Retry(_) => "Retry",
        Action::Kill(_) => "Kill",
    }
}

/// The real instances on one location, and the oracle of the rows they wrote.
struct Real {
    objects: Arc<dyn ObjectStore>,
    layout: Layout,
    data_url: Option<String>,
    nodes: [Option<Node>; model::INSTANCES],
    /// Every instance's request log, the stopped ones' included.
    clients: Vec<(String, Arc<FaultStore>)>,
    oracle: Vec<Row>,
    next_id: i64,
    tally: Tally,
    _dir: Option<tempfile::TempDir>,
}

impl Real {
    fn new(backend: Backend, layout: Layout) -> Self {
        let (objects, dir): (Arc<dyn ObjectStore>, _) = match backend {
            Backend::InMemory => (Arc::new(InMemory::new()), None),
            Backend::Local => {
                let dir = tempfile::tempdir().expect("state directory");
                (
                    Arc::new(
                        LocalConditionalPut::new_durable(dir.path()).expect("durable local store"),
                    ),
                    Some(dir),
                )
            }
        };
        Self {
            objects,
            layout,
            data_url: None,
            nodes: [None, None, None],
            clients: Vec::new(),
            oracle: Vec::new(),
            next_id: 1,
            tally: Tally::default(),
            _dir: dir,
        }
    }

    /// Instance `i`'s Spicepod: instances 0 and 2 evolve, instance 1 does not.
    fn vortex_config(&self, i: usize) -> VortexConfig {
        let inline = matches!(self.layout, Layout::Inline);
        VortexConfig {
            inline_max_rows: if inline { 1000 } else { 0 },
            inline_max_bytes: if inline { 1_000_000 } else { 0 },
            schema_evolution: if Protocol::evolves_schema(i) {
                SchemaEvolutionMode::Widen
            } else {
                SchemaEvolutionMode::Disabled
            },
            ..VortexConfig::default()
        }
    }

    async fn catalog(
        &self,
        store: &Arc<dyn ObjectStore>,
        access: Access,
    ) -> Result<ObjectNativeCatalog, String> {
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(store),
            Path::from(crate::common::LOCATION_CATALOG),
            LOCATION,
            NativeCatalogConfig {
                access,
                ..NativeCatalogConfig::default()
            },
        )
        .await
        .map_err(|e| format!("open the catalog: {e}"))?;
        catalog
            .init()
            .await
            .map_err(|e| format!("init the catalog: {e}"))?;
        Ok(catalog)
    }

    /// Every log head on the location, with its bytes.
    async fn heads(&self) -> Result<BTreeMap<String, bytes::Bytes>, String> {
        let listed: Vec<_> = self
            .objects
            .list(None)
            .try_collect()
            .await
            .map_err(|e| format!("list the location: {e}"))?;
        let mut heads = BTreeMap::new();
        for object in listed {
            if object.location.filename() == Some("head") {
                let bytes = self
                    .objects
                    .get(&object.location)
                    .await
                    .map_err(|e| e.to_string())?
                    .bytes()
                    .await
                    .map_err(|e| e.to_string())?;
                heads.insert(object.location.to_string(), bytes);
            }
        }
        Ok(heads)
    }

    /// Start instance `i` with the dataset registered at model schema
    /// `registered`, as the writer or as a replica.
    async fn start(&mut self, i: usize, writer: bool, registered: u8) -> Result<(), String> {
        let requests = Arc::new(FaultStore::new(Arc::clone(&self.objects)));
        let store: Arc<dyn ObjectStore> = Arc::clone(&requests) as Arc<dyn ObjectStore>;
        self.clients.push((
            format!(
                "instance {i} as {}",
                if writer { "writer" } else { "replica" }
            ),
            Arc::clone(&requests),
        ));
        let access = if writer {
            Access::Writer
        } else {
            Access::Replica
        };
        let before = if writer {
            None
        } else {
            Some(self.heads().await?)
        };
        // Every instance's request log from here on, to name a writer if one did.
        let marks: Vec<usize> = self
            .clients
            .iter()
            .map(|(_, client)| client.log().len())
            .collect();
        let catalog = self.catalog(&store, access).await?;
        let data_url = self
            .data_url
            .get_or_insert_with(|| crate::common::location_data_url(&catalog, TABLE))
            .clone();
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
        let ctx = SessionContext::new();
        let table = crate::common::table_builder_on_store(
            Arc::clone(&catalog),
            ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse(&data_url).map_err(|e| e.to_string())?,
                store: Arc::clone(&store),
            },
        )
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: arrow_schema(registered)?,
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: self.vortex_config(i),
        })
        .await
        .map_err(|e| {
            format!(
                "instance {i} opens the dataset as {}: {e}",
                if writer { "the writer" } else { "a replica" }
            )
        })?;
        let table = Arc::new(table);
        ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)
            .map_err(|e| e.to_string())?;
        if let Some(before) = before {
            let after = self.heads().await?;
            let written: Vec<String> = self
                .clients
                .iter()
                .zip(&marks)
                .flat_map(|((label, client), from)| {
                    client.log()[*from..]
                        .iter()
                        .filter(|request| {
                            !matches!(
                                request.op,
                                Op::Get | Op::GetConditional | Op::Head | Op::List
                            )
                        })
                        .map(|request| format!("{label}: {:?} {}", request.op, request.path))
                        .collect::<Vec<_>>()
                })
                .collect();
            let changed: Vec<&String> = after
                .keys()
                .chain(before.keys())
                .filter(|key| before.get(*key) != after.get(*key))
                .collect();
            if !changed.is_empty() {
                let sent: Vec<String> = requests
                    .log()
                    .into_iter()
                    .filter(|request| {
                        !matches!(
                            request.op,
                            Op::Get | Op::GetConditional | Op::Head | Op::List
                        )
                    })
                    .map(|request| format!("{:?} {}", request.op, request.path))
                    .collect();
                return Err(format!(
                    "guarantee 4: opening replica {i} changed the location's log heads {changed:?}; the replica sent {sent:?}, the instances wrote {written:?}"
                ));
            }
        }
        self.nodes[i] = Some(Node {
            writer,
            requests,
            ctx,
            table,
            kept: None,
        });
        Ok(())
    }

    /// Wait out the writer's background work (statistics persisted after a
    /// scan, post-write maintenance): an in-process drop cannot stop a detached
    /// commit the way a process kill does, and a replica's open is checked
    /// against a location no one is writing to.
    async fn quiesce(&self) -> Result<(), String> {
        for node in self.nodes.iter().flatten().filter(|node| node.writer) {
            node.table.wait_for_scan_file_statistics_persistence().await;
            node.table
                .drain_in_flight_maintenance()
                .await
                .map_err(|e| format!("drain the writer's maintenance: {e}"))?;
        }
        Ok(())
    }

    fn node(&self, i: usize) -> Result<&Node, String> {
        self.nodes[i]
            .as_ref()
            .ok_or_else(|| format!("instance {i} is not running"))
    }

    async fn sql(&self, i: usize, sql: &str) -> Result<Vec<RecordBatch>, String> {
        self.node(i)?
            .ctx
            .sql(sql)
            .await
            .map_err(|e| format!("instance {i} plans {sql}: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("instance {i} runs {sql}: {e}"))
    }

    /// The model schema instance `i`'s table holds now.
    fn table_schema(&self, i: usize) -> Result<u8, String> {
        let schema = self.node(i)?.table.table_schema();
        model_schema_of(&schema)
            .ok_or_else(|| format!("instance {i} serves a schema of no model schema: {schema:?}"))
    }

    /// Drive `action`, which takes the design from `s` to `n`.
    async fn drive(&mut self, s: &State, n: &State, action: Action) -> Result<(), String> {
        let name = name_of(action);
        let mut counted = false;
        match action {
            // The source is the driver's: the copy only sees what a writer commits.
            Action::SourceWidens | Action::SourceChangesIncompatibly | Action::SourceReverts => {
                counted = true;
            }
            Action::Open(i) | Action::Retry(i) => {
                let copy = n.copy.as_ref().ok_or("the copy exists once opened")?;
                let writer = copy.writer == Some(i);
                let registered = match s.copy.as_ref() {
                    // The first instance creates the copy.
                    None => copy.schema,
                    // A writer registers at the runtime's checkpoint of the
                    // schema, which may lag what the copy's log committed.
                    Some(before) if writer => before.checkpoint,
                    // A replica registers at its own source's schema, wider or
                    // narrower than the stored one.
                    Some(_) if s.source <= 1 => s.source,
                    Some(before) => before.checkpoint,
                };
                self.start(i, writer, registered).await?;
            }
            Action::TakeLease(i) => {
                // The lease passes: the replica's dataset is reloaded as the
                // writer, registered at the narrowest schema it knows of.
                let copy = s.copy.as_ref().ok_or("a lease needs a copy")?;
                let registered = s.instances[i].bound.min(copy.checkpoint);
                let kept = self.nodes[i]
                    .as_ref()
                    .is_some_and(|node| node.kept.is_some());
                self.nodes[i] = None;
                self.start(i, true, registered).await?;
                if kept {
                    // A cached plan of the old registration is planned again.
                    let node = self.nodes[i].as_mut().ok_or("started")?;
                    node.kept = Some(
                        node.ctx
                            .sql(&format!("SELECT * FROM {TABLE}"))
                            .await
                            .map_err(|e| e.to_string())?,
                    );
                    self.tally.replans += 1;
                }
            }
            Action::CommitSchema(i) => {
                let SchemaEvolution::Widening(plan) = classify(
                    arrow_schema(0)?.as_ref(),
                    arrow_schema(1)?.as_ref(),
                    &EvolutionContext {
                        constraint_columns: &[],
                    },
                ) else {
                    return Err("schema 1 must widen schema 0".into());
                };
                let node = self.node(i)?;
                node.table
                    .evolve_schema_live(&plan)
                    .await
                    .map_err(|e| format!("instance {i} widens the schema: {e}"))?;
            }
            Action::CommitCheckpoint(_) | Action::Recreate(_) => {
                // The runtime's checkpoint, and the runtime's refusal to drop a
                // shared copy, have no Cayenne counterpart.
                counted = true;
                if let Action::Recreate(i) = action {
                    // The dataset failed on this instance.
                    self.nodes[i] = None;
                }
            }
            Action::Write(i) => {
                let schema = s.instances[i].bound;
                let id = self.next_id;
                self.next_id += 1;
                let v = written_value(id, schema);
                let (sql, extra) = if schema == 1 {
                    let extra = format!("e{id}");
                    (
                        format!("INSERT INTO {TABLE} VALUES ({id}, {v}, '{extra}')"),
                        Some(extra),
                    )
                } else {
                    (format!("INSERT INTO {TABLE} VALUES ({id}, {v})"), None)
                };
                self.sql(i, &sql).await?;
                self.node(i)?
                    .table
                    .flush_pending_maintenance()
                    .await
                    .map_err(|e| format!("drain instance {i}'s maintenance: {e}"))?;
                self.oracle.push(Row { id, v, extra });
            }
            Action::Follow(i) => {
                let before = self.table_schema(i)?;
                let batches = self
                    .sql(i, &format!("SELECT count(*) FROM {TABLE}"))
                    .await?;
                let count = batches
                    .first()
                    .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
                    .map(|count| count.value(0));
                let expected = i64::try_from(self.oracle.len()).map_err(|e| e.to_string())?;
                if count != Some(expected) {
                    return Err(format!(
                        "guarantee 1: replica {i} counts {count:?} rows, the oracle {expected}"
                    ));
                }
                if self.table_schema(i)? > before {
                    self.tally.adoptions += 1;
                }
            }
            Action::Plan(i) => {
                let node = self.nodes[i].as_mut().ok_or("not running")?;
                node.kept = Some(
                    node.ctx
                        .sql(&format!("SELECT * FROM {TABLE}"))
                        .await
                        .map_err(|e| format!("instance {i} plans: {e}"))?,
                );
            }
            Action::Run(i) => self.run_kept(i).await?,
            Action::Kill(i) => {
                self.nodes[i] = None;
            }
        }
        let entry = if counted {
            self.tally.counted.entry(name)
        } else {
            self.tally.driven.entry(name)
        };
        *entry.or_default() += 1;
        Ok(())
    }

    /// Run instance `i`'s kept plan: the plan cache check (the runtime's
    /// `plan_matches_scanned_tables`) plans it again when its table's schema
    /// changed since; a plan that outlives a change within the query (a replica
    /// taking up a widening as its scan confirms the head) answers in its
    /// planned schema, by name with checked casts.
    async fn run_kept(&mut self, i: usize) -> Result<(), String> {
        let served = self.table_schema(i)?;
        let node = self.nodes[i].as_mut().ok_or("not running")?;
        let mut kept = node.kept.clone().ok_or("no kept plan")?;
        let planned_schema = |kept: &DataFrame| {
            model_schema_of(kept.schema().as_arrow())
                .ok_or_else(|| format!("a plan of no model schema: {:?}", kept.schema()))
        };
        if planned_schema(&kept)? != served {
            kept = node
                .ctx
                .sql(&format!("SELECT * FROM {TABLE}"))
                .await
                .map_err(|e| e.to_string())?;
            node.kept = Some(kept.clone());
            self.tally.replans += 1;
        }
        let planned = planned_schema(&kept)?;
        let answer = kept.collect().await;
        let after = self.table_schema(i)?;
        if after > served {
            self.tally.adoptions += 1;
        }
        let conformed = after != planned;
        if conformed {
            self.tally.conformed += 1;
        }
        let expected = expected_answer(&self.oracle, planned);
        match (answer, expected) {
            (Ok(batches), Ok(rows)) => {
                for batch in &batches {
                    if shape(&batch.schema()) != shape(arrow_schema(planned)?.as_ref()) {
                        return Err(format!(
                            "guarantee 2: instance {i}'s plan of schema {planned} answered in {:?}",
                            batch.schema()
                        ));
                    }
                }
                let got = rows_of(&batches, planned)?;
                if got != rows {
                    return Err(format!(
                        "guarantee 2: instance {i}'s plan of schema {planned} returned {got:?}, expected {rows:?}"
                    ));
                }
            }
            (Err(error), Err(value)) => {
                let message = error.to_string();
                if !(message.contains("Cast error") && message.contains(&value.to_string())
                    || message.contains("Cast error") && message.contains("Int32"))
                {
                    return Err(format!(
                        "guarantee 2: instance {i}'s plan of schema {planned} failed other than on a cast: {message}"
                    ));
                }
                self.tally.cast_errors += 1;
            }
            (Ok(batches), Err(value)) => {
                return Err(format!(
                    "guarantee 2: instance {i}'s plan of schema {planned} answered {:?} where {value} does not fit its v",
                    rows_of(&batches, planned)
                ));
            }
            (Err(error), Ok(_)) => {
                return Err(format!(
                    "guarantee 2: instance {i}'s plan of schema {planned} failed: {error}"
                ));
            }
        }
        Ok(())
    }

    /// The guarantees, in state `n` of the design.
    async fn check(&mut self, n: &State) -> Result<(), String> {
        for i in 0..model::INSTANCES {
            let instance = &n.instances[i];
            if instance.status != Status::Serving {
                if self.nodes[i].is_some() {
                    return Err(format!("instance {i} runs where the model has it stopped"));
                }
                continue;
            }
            let copy = n.copy.as_ref().ok_or("a serving instance has a copy")?;
            let node = self.node(i)?;
            let writer = copy.writer == Some(i);
            if node.writer != writer {
                return Err(format!(
                    "instance {i} runs as writer={} where the model has writer={writer}",
                    node.writer
                ));
            }
            let held = self.table_schema(i)?;
            if !writer && copy.schema > instance.bound {
                // The replica's next query takes up the committed schema, which
                // the model has it not do yet; it may already serve it (a
                // replica opens at the stored schema).
                if held != instance.bound && held != copy.schema {
                    return Err(format!(
                        "guarantee 1: replica {i} holds schema {held}, the model has {} (copy {})",
                        instance.bound, copy.schema
                    ));
                }
                self.tally.deferred += 1;
                continue;
            }
            if held != instance.bound {
                return Err(format!(
                    "guarantee 1: instance {i} holds schema {held}, the model has it serve {}",
                    instance.bound
                ));
            }
            let kept_schema = self
                .node(i)?
                .ctx
                .sql(&format!("SELECT * FROM {TABLE}"))
                .await;
            let df = kept_schema.map_err(|e| format!("instance {i} plans SELECT *: {e}"))?;
            if shape(df.schema().as_arrow()) != shape(arrow_schema(instance.bound)?.as_ref()) {
                return Err(format!(
                    "guarantee 1: instance {i} plans SELECT * in {:?}, the model has it serve schema {}",
                    df.schema(),
                    instance.bound
                ));
            }
            let batches = df
                .collect()
                .await
                .map_err(|e| format!("guarantee 1: instance {i}'s SELECT * failed: {e}"))?;
            let expected = expected_answer(&self.oracle, instance.bound)
                .map_err(|v| format!("the oracle holds {v} under schema 0"))?;
            let got = rows_of(&batches, instance.bound)?;
            if got != expected {
                return Err(format!(
                    "guarantee 1: instance {i}'s SELECT * returned {got:?}, expected {expected:?}"
                ));
            }
            for batch in &batches {
                if shape(&batch.schema()) != shape(arrow_schema(instance.bound)?.as_ref()) {
                    return Err(format!(
                        "guarantee 1: instance {i} answered in {:?}",
                        batch.schema()
                    ));
                }
            }
            self.tally.answers += 1;
        }
        if let Some(copy) = n.copy.as_ref() {
            // A fresh handle reads the stored schema from the location alone.
            let observer = self.catalog(&self.objects, Access::Replica).await?;
            let stored = observer
                .get_table(TABLE)
                .await
                .map_err(|e| format!("read the stored table: {e}"))?;
            if shape(&stored.schema) != shape(arrow_schema(copy.schema)?.as_ref()) {
                return Err(format!(
                    "guarantee 3: the stored schema is {:?}, the model's copy has schema {}",
                    stored.schema, copy.schema
                ));
            }
        }
        for (i, node) in self.nodes.iter().enumerate() {
            let Some(node) = node.as_ref().filter(|node| !node.writer) else {
                continue;
            };
            let writes: Vec<String> = node
                .requests
                .log()
                .into_iter()
                .filter(|request| {
                    !matches!(
                        request.op,
                        Op::Get | Op::GetConditional | Op::Head | Op::List
                    )
                })
                .map(|request| format!("{:?} {}", request.op, request.path))
                .collect();
            if !writes.is_empty() {
                return Err(format!("guarantee 4: replica {i} wrote {writes:?}"));
            }
        }
        Ok(())
    }
}

/// Replay `trace` through the design and the real instances; with `control`,
/// also through that control's model, which must break the trace's property
/// at its last step.
async fn replay(
    trace: &Trace,
    control: Option<&str>,
    backend: Backend,
    layout: Layout,
) -> Result<Tally, String> {
    let design = Protocol::new("schema");
    let broken = control.map(Protocol::new);
    let mut s = State::default();
    let mut c = State::default();
    let mut real = Real::new(backend, layout);
    for (step, text) in trace.actions.iter().enumerate() {
        let context = |e: String| format!("step {step} {text}: {e}");
        let action = Action::parse(text).map_err(context)?;
        if !design.enabled(&s).contains(&action) {
            return Err(context("not enabled in the design".into()));
        }
        let n = design
            .step(&s, action)
            .ok_or_else(|| context("changes nothing in the design".into()))?;
        if let Some(broken) = &broken {
            c = broken
                .step(&c, action)
                .ok_or_else(|| context("changes nothing in the control".into()))?;
            let last = step + 1 == trace.actions.len();
            let fails = broken.violated(&c).contains(&trace.property.as_str());
            if fails != last {
                return Err(context(format!(
                    "the control breaks '{}' at this step: {fails}",
                    trace.property
                )));
            }
        }
        let broken_design = design.violated(&n);
        if !broken_design.is_empty() {
            return Err(context(format!("the design breaks {broken_design:?}")));
        }
        real.drive(&s, &n, action).await.map_err(context)?;
        real.check(&n).await.map_err(context)?;
        real.quiesce().await.map_err(context)?;
        real.tally.steps += 1;
        s = n;
    }
    real.tally.traces += 1;
    Ok(real.tally)
}

const BACKENDS: [Backend; 2] = [Backend::InMemory, Backend::Local];
const LAYOUTS: [Layout; 2] = [Layout::Files, Layout::Inline];

async fn replay_all(text: &str, control: Option<&str>) -> Tally {
    let traces = traces(text);
    let selected: Vec<&Trace> = traces
        .iter()
        .filter(|trace| control.is_none() || trace.status == "FAIL")
        .collect();
    assert!(!selected.is_empty(), "{control:?}: no traces to replay");
    let mut tally = Tally::default();
    for backend in BACKENDS {
        for layout in LAYOUTS {
            for (index, trace) in selected.iter().enumerate() {
                match replay(trace, control, backend, layout).await {
                    Ok(one) => tally.merge(one),
                    Err(error) => panic!(
                        "{} {} #{index} ({backend:?}, {layout:?}) {:?}: {error}",
                        trace.run, trace.kind, trace.actions
                    ),
                }
            }
        }
    }
    eprintln!(
        "{}: {} traces, {} steps; driven {:?}; counted {:?}; replans {}, conformed {}, cast errors {}, adoptions {}, deferred checks {}, compared answers {}",
        control.unwrap_or("schema"),
        tally.traces,
        tally.steps,
        tally.driven,
        tally.counted,
        tally.replans,
        tally.conformed,
        tally.cast_errors,
        tally.adoptions,
        tally.deferred,
        tally.answers
    );
    tally
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_design() {
    let tally = replay_all(include_str!("traces/schema.jsonl"), None).await;
    // The suite reaches the paths it is here for.
    assert!(tally.adoptions > 0, "a replica took up a wider schema");
    assert!(tally.conformed > 0, "a kept plan ran across a widening");
    // No trace runs a schema-0 plan over a row whose `v` needs the wider type
    // (the widening witnesses write before the replica plans), so the cast
    // error branch of guarantee 2 is checked when met but not reached here.
    assert!(tally.replans > 0, "a kept plan was planned again");
    assert!(tally.driven.contains_key("CommitSchema"));
    assert!(tally.driven.contains_key("TakeLease"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_positional() {
    let tally = replay_all(
        include_str!("traces/schema-positional.jsonl"),
        Some("schema-positional"),
    )
    .await;
    // A replica opens at the stored schema, so the witness's plan is made at
    // the schema its data was written under and the kept plan never outlives a
    // change: the breaking step does not arise. The design replay reaches it.
    assert_eq!(
        tally.conformed, 0,
        "the replica planned at the stored schema"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_no_conform() {
    let tally = replay_all(
        include_str!("traces/schema-no-conform.jsonl"),
        Some("schema-no-conform"),
    )
    .await;
    // A replica opens at the stored schema, so the witness's plan is made at
    // the schema its data was written under and the kept plan never outlives a
    // change: the breaking step does not arise. The design replay reaches it.
    assert_eq!(
        tally.conformed, 0,
        "the replica planned at the stored schema"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_no_adopt() {
    let tally = replay_all(
        include_str!("traces/schema-no-adopt.jsonl"),
        Some("schema-no-adopt"),
    )
    .await;
    assert!(tally.driven.contains_key("Follow"), "the replica followed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_replica_commits() {
    replay_all(
        include_str!("traces/schema-replica-commits.jsonl"),
        Some("schema-replica-commits"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_takeover_narrows() {
    replay_all(
        include_str!("traces/schema-takeover-narrows.jsonl"),
        Some("schema-takeover-narrows"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_takeover_trusts_checkpoint() {
    replay_all(
        include_str!("traces/schema-takeover-trusts-checkpoint.jsonl"),
        Some("schema-takeover-trusts-checkpoint"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_recreate_drops() {
    let tally = replay_all(
        include_str!("traces/schema-recreate-drops.jsonl"),
        Some("schema-recreate-drops"),
    )
    .await;
    assert!(tally.counted.contains_key("Recreate"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_native_schema_replay_control_stale_plan() {
    let tally = replay_all(
        include_str!("traces/schema-stale-plan.jsonl"),
        Some("schema-stale-plan"),
    )
    .await;
    assert!(tally.replans > 0, "the kept plan was planned again");
}
