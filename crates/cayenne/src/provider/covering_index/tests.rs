/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Datum, Decimal128Array, Int64Array, RecordBatch,
    RecordBatchOptions, StringArray, StringViewArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_common::DFSchema;
use datafusion_expr::{col, lit};
use datafusion_physical_plan::{ExecutionPlan, common::collect};

use super::super::lookup_index::KeySpec;
use super::super::memory_account::CayenneMemoryAccount;
use super::page_store::test_support::DelayedPageStore;
use super::{
    AllocationOwner, CayenneIndexScanExec, CoveredRowRef, CoveringIndexAccess,
    CoveringIndexCapability, CoveringPageStore, CoveringReadView, EncodedKey, Error, IndexCatalog,
    IndexDefinition, IndexRun, IndexedSource, KeyDirectory, KeyDirectoryEntry, KeyPage, KeyPageId,
    KeyPageLease, PageLease, PayloadPage, PayloadPageId, PayloadPageLease, PrimaryKeyLayout,
    ProbeRequest, ProbeStep, Result, RunId, SchemaIdentity, SourceId, SourceRole,
    VisibilityAdapter, build_source, build_sources, gather, prepare_literal_seek, probe_many,
    try_cover,
};
use crate::provider::delete::InsertRecordHandling;
use crate::provider::deletion_index::DeletionIndex;
use crate::provider::deletion_strategy::PositionDeletionVector;
use crate::provider::mem_tier::InMemTombstones;
use crate::provider::on_conflict::PkDeletionSnapshot;
use roaring::RoaringBitmap;

#[derive(Debug)]
struct TestPageStore {
    key_pages: BTreeMap<KeyPageId, KeyPageLease>,
    payload_pages: BTreeMap<PayloadPageId, PayloadPageLease>,
}

impl TestPageStore {
    fn new(key_pages: BTreeMap<KeyPageId, KeyPageLease>) -> Self {
        Self {
            key_pages,
            payload_pages: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl CoveringPageStore for TestPageStore {
    async fn load_key_pages(&self, ids: &[KeyPageId]) -> Result<Vec<KeyPageLease>> {
        ids.iter()
            .map(|id| {
                self.key_pages
                    .get(id)
                    .cloned()
                    .ok_or_else(|| Error::MissingPage {
                        page: format!("{id:?}"),
                    })
            })
            .collect()
    }

    async fn load_payload_pages(&self, ids: &[PayloadPageId]) -> Result<Vec<PayloadPageLease>> {
        ids.iter()
            .map(|id| {
                self.payload_pages
                    .get(id)
                    .cloned()
                    .ok_or_else(|| Error::MissingPage {
                        page: format!("{id:?}"),
                    })
            })
            .collect()
    }
}

fn schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

fn definition(schema: Arc<Schema>, columns: &[&str]) -> IndexDefinition {
    let spec = KeySpec::new(columns.iter().map(|column| (*column).to_string()).collect())
        .expect("nonempty test key");
    IndexDefinition::resolve(schema, &spec).expect("test key resolves")
}

fn token(bytes: usize) -> super::ReservationToken {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes.saturating_mul(2)));
    let account = Arc::new(CayenneMemoryAccount::new(
        "covering-index-contract-test",
        &pool,
    ));
    let reservation = account
        .try_reserve_lookup_index(bytes)
        .expect("test allocation is admitted");
    AllocationOwner::new(reservation).token()
}

fn row_ref(source: &SourceId, page: u32, row: usize) -> CoveredRowRef {
    CoveredRowRef::new(
        source.clone(),
        PayloadPageId::new(source.clone(), page),
        row,
        u64::try_from(row).expect("test row ordinal fits"),
    )
    .expect("same-source row reference")
}

fn build_account(bytes: usize) -> Arc<CayenneMemoryAccount> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    Arc::new(CayenneMemoryAccount::new(
        "covering-index-build-test",
        &pool,
    ))
}

fn key_value_batch(schema: Arc<Schema>, rows: usize, duplicate_key: bool) -> RecordBatch {
    let keys: ArrayRef = Arc::new(Int64Array::from_iter_values((0..rows).map(|row| {
        if duplicate_key {
            7
        } else {
            i64::try_from(row).expect("test key fits i64")
        }
    })));
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("value-{row}")),
    ));
    RecordBatch::try_new(schema, vec![keys, values]).expect("valid source batch")
}

fn into_view(
    definition: IndexDefinition,
    source_id: SourceId,
    built: super::BuiltCoveredSource,
) -> Arc<CoveringReadView> {
    let (source, runs, store) = built.into_parts();
    let query_schema = source.schema().clone();
    let page_store: Arc<dyn CoveringPageStore> = store;
    let catalog = Arc::new(
        IndexCatalog::new(definition, vec![source], runs.to_vec(), page_store)
            .expect("complete built source creates a catalog"),
    );
    Arc::new(CoveringReadView::new(
        catalog,
        vec![source_id],
        query_schema,
    ))
}

#[test]
fn key_resolution_preserves_order_and_rejects_ambiguous_or_float_columns() {
    let resolved_schema = schema(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("service", DataType::Int64, true),
    ]);
    let definition = definition(Arc::clone(&resolved_schema), &["service", "tenant"]);
    assert_eq!(
        definition
            .columns()
            .iter()
            .map(super::IndexColumn::name)
            .collect::<Vec<_>>(),
        vec!["service", "tenant"],
        "configured composite key order is part of the encoded identity"
    );
    assert_eq!(
        definition
            .columns()
            .iter()
            .map(super::IndexColumn::schema_index)
            .collect::<Vec<_>>(),
        vec![1, 0]
    );

    let ambiguous = schema(vec![
        Field::new("Tenant", DataType::Int64, false),
        Field::new("TENANT", DataType::Int64, false),
    ]);
    let ambiguous_spec = KeySpec::new(vec!["tenant".to_string()]).expect("nonempty key");
    IndexDefinition::resolve(ambiguous, &ambiguous_spec)
        .expect_err("case-insensitive ambiguity must not choose a different stored column");

    let floating = schema(vec![Field::new("score", DataType::Float64, false)]);
    let float_spec = KeySpec::new(vec!["score".to_string()]).expect("nonempty key");
    IndexDefinition::resolve(floating, &float_spec)
        .expect_err("covering key rules retain lookup index's floating-point rejection");
}

#[test]
fn encoded_integer_equality_and_order_match_arrow_kernels() {
    let schema = schema(vec![Field::new("key", DataType::Int64, true)]);
    let definition = definition(Arc::clone(&schema), &["key"]);
    let values = Arc::new(Int64Array::from(vec![
        Some(i64::MIN),
        Some(-1),
        Some(0),
        Some(i64::MAX),
        Some(-1),
        None,
    ])) as ArrayRef;
    let keys = (0..values.len())
        .map(|row| {
            definition
                .encode_probe_row(&[Arc::clone(&values)], row)
                .expect("integer probe encodes")
        })
        .collect::<Vec<_>>();
    assert!(keys[5].is_none(), "NULL keys do not form equality requests");

    for left in 0..values.len() {
        for right in 0..values.len() {
            if values.is_null(left) || values.is_null(right) {
                continue;
            }
            let left_value = values.slice(left, 1);
            let right_value = values.slice(right, 1);
            let left_datum: &dyn Datum = &left_value.as_ref();
            let right_datum: &dyn Datum = &right_value.as_ref();
            let arrow_equal = arrow::compute::kernels::cmp::eq(left_datum, right_datum)
                .expect("Arrow compares Int64 values")
                .value(0);
            assert_eq!(
                keys[left] == keys[right],
                arrow_equal,
                "covering key equality must match Arrow's physical equality class"
            );
        }
    }

    let arrow_order = arrow::compute::sort_to_indices(values.as_ref(), None, None)
        .expect("Arrow sorts Int64 values")
        .values()
        .iter()
        .map(|index| usize::try_from(*index).expect("u32 index fits usize"))
        .filter(|index| !values.is_null(*index))
        .collect::<Vec<_>>();
    let mut encoded_order = (0..values.len())
        .filter(|index| keys[*index].is_some())
        .collect::<Vec<_>>();
    encoded_order.sort_by(|left, right| {
        keys[*left]
            .as_ref()
            .expect("nonnull key")
            .cmp(keys[*right].as_ref().expect("nonnull key"))
    });
    assert_eq!(
        encoded_order, arrow_order,
        "encoded full-key order must agree with Arrow's ordered values"
    );
}

#[test]
fn composite_tuples_remain_correlated_and_null_tuples_are_skipped() {
    let schema = schema(vec![
        Field::new("first", DataType::Int64, true),
        Field::new("second", DataType::Int64, true),
    ]);
    let definition = definition(Arc::clone(&schema), &["first", "second"]);
    let first: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
    let second: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(10)]));
    let first_key = definition
        .encode_probe_row(&[Arc::clone(&first), Arc::clone(&second)], 0)
        .expect("first tuple encodes")
        .expect("nonnull tuple");
    let second_key = definition
        .encode_probe_row(&[Arc::clone(&first), Arc::clone(&second)], 1)
        .expect("second tuple encodes")
        .expect("nonnull tuple");
    assert_ne!(
        first_key, second_key,
        "separate tuple values must not collapse into scalar value sets"
    );
    assert!(
        definition
            .encode_probe_row(&[first, second], 2)
            .expect("NULL tuple is valid input")
            .is_none(),
        "any NULL component disables ordinary equality for exactly that tuple"
    );
}

#[test]
fn only_proven_string_and_binary_view_adaptations_are_admitted() {
    let utf8_schema = schema(vec![Field::new("key", DataType::Utf8, false)]);
    let utf8_definition = definition(utf8_schema, &["key"]);
    let string_view: ArrayRef = Arc::new(StringViewArray::from(vec!["embedded\0nul", "", "plain"]));
    let encoded = utf8_definition
        .encode_probe_row(&[Arc::clone(&string_view)], 0)
        .expect("Utf8View has a tested lossless adaptation")
        .expect("non-null string");
    let stored: ArrayRef = Arc::new(StringArray::from(vec!["embedded\0nul"]));
    let stored_encoded = utf8_definition
        .encode_probe_row(&[stored], 0)
        .expect("Utf8 encodes")
        .expect("non-null string");
    assert_eq!(encoded, stored_encoded);

    let binary_schema = schema(vec![Field::new("key", DataType::Binary, false)]);
    let binary_definition = definition(binary_schema, &["key"]);
    let binary_view: ArrayRef = Arc::new(BinaryViewArray::from(vec![b"\0a".as_slice()]));
    assert!(
        binary_definition
            .encode_probe_row(&[binary_view], 0)
            .expect("BinaryView has a tested lossless adaptation")
            .is_some()
    );

    let integer_schema = schema(vec![Field::new("key", DataType::Int64, false)]);
    let integer_definition = definition(integer_schema, &["key"]);
    let unsigned: ArrayRef = Arc::new(UInt64Array::from(vec![Some(u64::MAX)]));
    assert!(matches!(
        integer_definition.encode_probe_row(&[unsigned], 0),
        Err(Error::UnsupportedKeyAdaptation { .. })
    ));
}

#[test]
fn decimal_and_temporal_key_types_keep_their_stored_comparison_domain() {
    let decimal = Decimal128Array::from(vec![Some(-999), Some(0), Some(999)])
        .with_precision_and_scale(10, 2)
        .expect("valid Decimal128 test values");
    let decimal_schema = schema(vec![Field::new(
        "amount",
        decimal.data_type().clone(),
        false,
    )]);
    let decimal_definition = definition(decimal_schema, &["amount"]);
    assert_ne!(
        decimal_definition
            .encode_probe_row(&[Arc::new(decimal.clone())], 0)
            .expect("decimal encodes"),
        decimal_definition
            .encode_probe_row(&[Arc::new(decimal)], 2)
            .expect("decimal encodes")
    );

    let timestamp = TimestampMicrosecondArray::from(vec![Some(-1), Some(0), Some(1)])
        .with_timezone("Australia/Brisbane");
    let timestamp_schema = schema(vec![Field::new(
        "occurred_at",
        timestamp.data_type().clone(),
        false,
    )]);
    let timestamp_definition = definition(timestamp_schema, &["occurred_at"]);
    assert!(
        timestamp_definition
            .encode_probe_row(&[Arc::new(timestamp)], 1)
            .expect("timezone-bearing timestamp encodes")
            .is_some()
    );
}

#[test]
fn row_references_and_pages_cannot_cross_source_generations() {
    let left = SourceId::new("table", 1);
    let right = SourceId::new("table", 2);
    CoveredRowRef::new(left.clone(), PayloadPageId::new(right, 0), 0, 0)
        .expect_err("source and payload page source must match");

    let reference = row_ref(&left, 0, 0);
    let page = KeyPage::new(Arc::from(&b"a"[..]), vec![0, 1], vec![reference])
        .expect("valid single key page");
    page.key(1)
        .expect_err("out-of-range key offset is an error");
    page.row_ref(1)
        .expect_err("out-of-range row reference is an error");
}

#[test]
fn allocation_token_outlives_a_page_lease_without_a_reference_cycle() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256));
    let account = Arc::new(CayenneMemoryAccount::new(
        "covering-index-drop-order",
        &pool,
    ));
    let owner = AllocationOwner::new(
        account
            .try_reserve_lookup_index(128)
            .expect("test allocation is admitted"),
    );
    let source = SourceId::new("table", 1);
    let page = Arc::new(
        KeyPage::new(
            Arc::from(&b"k"[..]),
            vec![0, 1],
            vec![row_ref(&source, 0, 0)],
        )
        .expect("valid key page"),
    );
    let lease = PageLease::new(page, owner.token());
    let retained_output_token = lease.reservation_token();
    drop(lease);
    drop(owner);
    assert_eq!(
        account.reserved_bytes(),
        128,
        "a result retaining a source buffer must retain its source allocation charge"
    );
    drop(retained_output_token);
    assert_eq!(
        account.reserved_bytes(),
        0,
        "dropping the final output token releases the allocation without a page back-reference"
    );
}

#[tokio::test]
async fn page_store_preserves_repeated_ids_and_prepared_seek_is_compact() {
    let source = SourceId::new("table", 7);
    let key_page_id = KeyPageId::new(source.clone(), 0);
    let key_page = Arc::new(
        KeyPage::new(
            Arc::from(&b"abb"[..]),
            vec![0, 1, 2, 3],
            vec![
                row_ref(&source, 0, 0),
                row_ref(&source, 0, 1),
                row_ref(&source, 0, 2),
            ],
        )
        .expect("valid sorted key page"),
    );
    let mut pages = BTreeMap::new();
    pages.insert(key_page_id.clone(), PageLease::new(key_page, token(128)));
    let store: Arc<dyn CoveringPageStore> = Arc::new(TestPageStore::new(pages));
    let repeated = store
        .load_key_pages(&[key_page_id.clone(), key_page_id.clone()])
        .await
        .expect("repeated page ids preserve caller order");
    assert_eq!(repeated.len(), 2);
    assert!(Arc::ptr_eq(repeated[0].page(), repeated[1].page()));
    store
        .load_key_pages(&[KeyPageId::new(source.clone(), 99)])
        .await
        .expect_err("missing page id must be an error");

    let schema = schema(vec![Field::new("key", DataType::Utf8, false)]);
    let definition = definition(Arc::clone(&schema), &["key"]);
    let directory = KeyDirectory::new(
        source.clone(),
        vec![
            KeyDirectoryEntry::new(
                key_page_id,
                EncodedKey::from_page_bytes(b"a"),
                EncodedKey::from_page_bytes(b"b"),
                3,
            )
            .expect("directory bounds"),
        ],
    )
    .expect("directory source matches");
    let run = IndexRun::new(source.clone(), RunId::new(0), directory).expect("run source matches");
    let source_entry = IndexedSource::new(
        source.clone(),
        SchemaIdentity::identity(&schema).expect("schema identity"),
        3,
        vec![PayloadPageId::new(source.clone(), 0)],
    )
    .expect("source payload belongs to source");
    let catalog = Arc::new(
        IndexCatalog::new(definition.clone(), vec![source_entry], vec![run], store)
            .expect("catalog source is complete"),
    );
    let view = CoveringReadView::new(
        catalog,
        vec![source.clone()],
        SchemaIdentity::identity(&schema).expect("query schema identity"),
    );
    let prepared = prepare_literal_seek(&view, &EncodedKey::from_page_bytes(b"b"))
        .await
        .expect("prepared literal seek");
    assert_eq!(prepared.raw_entry_count(), 2);
    assert_eq!(prepared.spans().len(), 1);
    assert_eq!(prepared.spans()[0].first_offset, 1);
    assert_eq!(prepared.spans()[0].end_offset, 3);
    assert!(matches!(
        try_cover(Arc::new(view), &definition, &[0]).expect("coverage decision"),
        super::CoverageDecision::Complete(_)
    ));
}

fn page_store_fixture() -> (
    SourceId,
    BTreeMap<KeyPageId, KeyPageLease>,
    BTreeMap<PayloadPageId, PayloadPageLease>,
) {
    let source = SourceId::new("page-store-contract", 1);
    let first_key = KeyPageId::new(source.clone(), 0);
    let second_key = KeyPageId::new(source.clone(), 1);
    let first_payload = PayloadPageId::new(source.clone(), 0);
    let second_payload = PayloadPageId::new(source.clone(), 1);
    let key_pages = BTreeMap::from([
        (
            first_key,
            PageLease::new(
                Arc::new(
                    KeyPage::new(
                        Arc::from(&b"a"[..]),
                        vec![0, 1],
                        vec![row_ref(&source, 0, 0)],
                    )
                    .expect("first contract key page"),
                ),
                token(128),
            ),
        ),
        (
            second_key,
            PageLease::new(
                Arc::new(
                    KeyPage::new(
                        Arc::from(&b"b"[..]),
                        vec![0, 1],
                        vec![row_ref(&source, 1, 0)],
                    )
                    .expect("second contract key page"),
                ),
                token(128),
            ),
        ),
    ]);
    let payload_schema = schema(vec![Field::new("value", DataType::Utf8, false)]);
    let payload_pages = [first_payload, second_payload]
        .into_iter()
        .zip(["first", "second"])
        .map(|(id, value)| {
            let batch = RecordBatch::try_new(
                Arc::clone(&payload_schema),
                vec![Arc::new(StringArray::from(vec![value])) as ArrayRef],
            )
            .expect("contract payload batch");
            (
                id,
                PageLease::new(
                    Arc::new(
                        PayloadPage::new(
                            SchemaIdentity::identity(&payload_schema)
                                .expect("contract payload schema"),
                            batch,
                        )
                        .expect("contract payload page"),
                    ),
                    token(128),
                ),
            )
        })
        .collect();
    (source, key_pages, payload_pages)
}

async fn assert_resident_store_contract(store: Arc<dyn CoveringPageStore>, source: &SourceId) {
    let first_key = KeyPageId::new(source.clone(), 0);
    let second_key = KeyPageId::new(source.clone(), 1);
    let first_payload = PayloadPageId::new(source.clone(), 0);
    let second_payload = PayloadPageId::new(source.clone(), 1);

    let keys = store
        .load_key_pages(&[second_key.clone(), first_key.clone(), second_key.clone()])
        .await
        .expect("contract key pages load");
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0].page().key(0).expect("second key"), b"b");
    assert_eq!(keys[1].page().key(0).expect("first key"), b"a");
    assert!(Arc::ptr_eq(keys[0].page(), keys[2].page()));

    let payloads = store
        .load_payload_pages(&[
            second_payload.clone(),
            first_payload.clone(),
            second_payload,
        ])
        .await
        .expect("contract payload pages load");
    assert_eq!(payloads.len(), 3);
    assert_eq!(payloads[0].page().batch().num_rows(), 1);
    assert!(Arc::ptr_eq(payloads[0].page(), payloads[2].page()));

    assert!(
        store
            .load_key_pages(&[])
            .await
            .expect("zero key IDs")
            .is_empty()
    );
    assert!(
        store
            .load_payload_pages(&[])
            .await
            .expect("zero payload IDs")
            .is_empty()
    );
    assert!(matches!(
        store
            .load_key_pages(&[KeyPageId::new(SourceId::new("wrong-source", 1), 0)])
            .await,
        Err(Error::MissingPage { .. })
    ));
    assert!(matches!(
        store
            .load_payload_pages(&[PayloadPageId::new(SourceId::new("wrong-source", 1), 0)])
            .await,
        Err(Error::MissingPage { .. })
    ));
}

/// The resident and delayed stores have the identical public contract. The
/// delayed implementation intentionally completes the second physical request
/// first; the caller still receives IDs in its original order.
#[tokio::test]
async fn page_store_contract_conformance_covers_resident_and_delayed_stores() {
    let (source, keys, payloads) = page_store_fixture();
    let resident: Arc<dyn CoveringPageStore> = Arc::new(
        super::MemoryPageStore::new(keys.clone(), payloads.clone())
            .expect("resident contract store"),
    );
    assert_resident_store_contract(resident, &source).await;

    let delayed = Arc::new(DelayedPageStore::new(keys, payloads).expect("delayed contract store"));
    assert_resident_store_contract(Arc::clone(&delayed) as Arc<dyn CoveringPageStore>, &source)
        .await;

    let first = KeyPageId::new(source.clone(), 0);
    let second = KeyPageId::new(source.clone(), 1);
    delayed.block_key_page(first.clone());
    delayed.block_key_page(second.clone());
    let pending_store = Arc::clone(&delayed);
    let pending = tokio::spawn(async move {
        pending_store
            .load_key_pages(&[first, second])
            .await
            .expect("delayed key pages load")
    });
    tokio::task::yield_now().await;
    delayed.release_key_page(&KeyPageId::new(source.clone(), 1));
    tokio::task::yield_now().await;
    assert!(
        !pending.is_finished(),
        "the first requested page is still blocked even though the second completed"
    );
    delayed.release_key_page(&KeyPageId::new(source.clone(), 0));
    let pages = pending.await.expect("delayed task joins");
    assert_eq!(pages[0].page().key(0).expect("first key"), b"a");
    assert_eq!(pages[1].page().key(0).expect("second key"), b"b");

    let first_payload = PayloadPageId::new(source.clone(), 0);
    let second_payload = PayloadPageId::new(source, 1);
    delayed.block_payload_page(first_payload.clone());
    delayed.block_payload_page(second_payload.clone());
    let pending_store = Arc::clone(&delayed);
    let pending = tokio::spawn(async move {
        pending_store
            .load_payload_pages(&[first_payload, second_payload])
            .await
            .expect("delayed payload pages load")
    });
    tokio::task::yield_now().await;
    delayed.release_payload_page(&PayloadPageId::new(
        SourceId::new("page-store-contract", 1),
        1,
    ));
    tokio::task::yield_now().await;
    assert!(
        !pending.is_finished(),
        "the first requested payload page is still blocked even though the second completed"
    );
    delayed.release_payload_page(&PayloadPageId::new(
        SourceId::new("page-store-contract", 1),
        0,
    ));
    let pages = pending.await.expect("delayed payload task joins");
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].page().batch().num_rows(), 1);
    assert_eq!(pages[1].page().batch().num_rows(), 1);
}

/// Dropping a stalled or partly resolved request must release its temporary
/// leases. The resident store remains the only owner of the first page after
/// cancellation, so a later query can load it normally.
#[tokio::test]
async fn delayed_page_store_cancellation_and_faults_do_not_retain_leases() {
    let (source, keys, payloads) = page_store_fixture();
    let first_page = Arc::clone(
        keys.get(&KeyPageId::new(source.clone(), 0))
            .expect("first key lease")
            .page(),
    );
    let delayed =
        Arc::new(DelayedPageStore::new(keys, payloads).expect("delayed cancellation store"));
    let first = KeyPageId::new(source.clone(), 0);
    let second = KeyPageId::new(source.clone(), 1);
    delayed.block_key_page(second.clone());
    let pending_store = Arc::clone(&delayed);
    let pending = tokio::spawn(async move { pending_store.load_key_pages(&[first, second]).await });
    tokio::task::yield_now().await;
    pending.abort();
    assert!(
        pending
            .await
            .expect_err("cancelled page load task")
            .is_cancelled()
    );
    assert_eq!(
        Arc::strong_count(&first_page),
        2,
        "the fixture inspection clone and resident store are the only surviving key-page owners"
    );
    drop(first_page);

    let failed = KeyPageId::new(source.clone(), 0);
    delayed.fail_key_page(failed.clone(), "injected key read failure");
    assert!(matches!(
        delayed.load_key_pages(&[failed]).await,
        Err(Error::Unavailable { .. })
    ));
    let payload_failure = PayloadPageId::new(source, 0);
    delayed.fail_payload_page(payload_failure.clone(), "injected payload read failure");
    assert!(matches!(
        delayed.load_payload_pages(&[payload_failure]).await,
        Err(Error::Unavailable { .. })
    ));
}

#[tokio::test]
async fn index_scan_filters_before_limit_projects_and_resets() {
    let schema = schema(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let definition = definition(Arc::clone(&schema), &["key"]);
    let source = SourceId::new("point-scan", 1);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["first", "second", "other"])) as ArrayRef,
        ],
    )
    .expect("point-scan source batch");
    let built = build_source(
        source.clone(),
        definition.clone(),
        vec![batch],
        build_account(8 * 1024 * 1024),
    )
    .await
    .expect("point-scan source builds");
    let view = into_view(definition.clone(), source, built);
    let probe_array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let key = definition
        .encode_probe_row(&[probe_array], 0)
        .expect("point-scan key encodes")
        .expect("non-NULL point-scan key");
    let prepared = prepare_literal_seek(&view, &key)
        .await
        .expect("point-scan seek prepares");
    assert_eq!(prepared.raw_entry_count(), 2);

    let access = CoveringIndexAccess::new(Arc::clone(&view), definition);
    let capability = CoveringIndexCapability::new(
        vec![access.clone()],
        vec![1],
        Vec::new(),
        Some(prepared.raw_entry_count()),
    );
    let filter_schema = DFSchema::try_from(schema.as_ref().clone()).expect("filter schema");
    let filter = datafusion_physical_expr::create_physical_expr(
        &col("value").eq(lit("second")),
        &filter_schema,
        &datafusion_physical_expr::execution_props::ExecutionProps::new(),
    )
    .expect("residual filter plans");
    let exec = Arc::new(
        CayenneIndexScanExec::try_new(capability, access, prepared, vec![filter], vec![1], Some(1))
            .expect("point-scan exec builds"),
    );
    let context = SessionContext::new();
    let batches = collect(
        exec.execute(0, context.task_ctx())
            .expect("point-scan stream starts"),
    )
    .await
    .expect("point-scan stream succeeds");
    assert_eq!(batches.len(), 1);
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("projected value is Utf8");
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some("second")]);

    let reset = Arc::clone(&exec)
        .reset_state()
        .expect("point-scan reset succeeds");
    let reset_batches = collect(
        reset
            .execute(0, context.task_ctx())
            .expect("reset point-scan stream starts"),
    )
    .await
    .expect("reset point-scan stream succeeds");
    assert_eq!(
        reset_batches, batches,
        "reset must use a fresh probe cursor"
    );
}

#[tokio::test]
async fn index_scan_zero_column_projection_preserves_row_count() {
    let schema = schema(vec![Field::new("key", DataType::Int64, false)]);
    let definition = definition(Arc::clone(&schema), &["key"]);
    let source = SourceId::new("zero-column-point-scan", 1);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7, 7])) as ArrayRef],
    )
    .expect("zero-column point-scan source batch");
    let built = build_source(
        source.clone(),
        definition.clone(),
        vec![batch],
        build_account(8 * 1024 * 1024),
    )
    .await
    .expect("zero-column point-scan source builds");
    let view = into_view(definition.clone(), source, built);
    let probe_array = Arc::new(Int64Array::from(vec![7])) as ArrayRef;
    let key = definition
        .encode_probe_row(&[probe_array], 0)
        .expect("zero-column point-scan key encodes")
        .expect("non-NULL zero-column point-scan key");
    let prepared = prepare_literal_seek(&view, &key)
        .await
        .expect("zero-column point-scan seek prepares");
    let access = CoveringIndexAccess::new(Arc::clone(&view), definition);
    let capability = CoveringIndexCapability::new(
        vec![access.clone()],
        Vec::new(),
        Vec::new(),
        Some(prepared.raw_entry_count()),
    );
    let exec =
        CayenneIndexScanExec::try_new(capability, access, prepared, Vec::new(), Vec::new(), None)
            .expect("zero-column point-scan exec builds");
    let context = SessionContext::new();
    let batches = collect(
        exec.execute(0, context.task_ctx())
            .expect("zero-column point-scan stream starts"),
    )
    .await
    .expect("zero-column point-scan stream succeeds");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_columns(), 0);
    assert_eq!(batches[0].num_rows(), 2, "COUNT callers retain both rows");
}

#[test]
fn zero_column_payload_batches_retain_their_nonzero_row_count() {
    let schema = schema(vec![]);
    let identity = SchemaIdentity::identity(&schema).expect("empty schema identity");
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .expect("zero-column batch with rows");
    let page = PayloadPage::new(identity, batch).expect("payload page retains schema");
    assert_eq!(page.batch().num_rows(), 3);
}

#[tokio::test]
async fn builder_packs_exact_page_limits_and_probe_streams_duplicate_spans() {
    let schema = schema(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let definition = definition(Arc::clone(&schema), &["key"]);
    for (rows, expected_key_pages) in [(0, 0), (1, 1), (255, 1), (256, 1), (257, 2)] {
        let source = SourceId::new(format!("limit-{rows}"), 1);
        let built = build_source(
            source,
            definition.clone(),
            (rows > 0)
                .then(|| key_value_batch(Arc::clone(&schema), rows, false))
                .into_iter()
                .collect(),
            build_account(8 * 1024 * 1024),
        )
        .await
        .expect("admitted source builds");
        assert_eq!(
            built
                .runs()
                .iter()
                .map(|run| run.directory().entries().len())
                .sum::<usize>(),
            expected_key_pages,
            "{rows} rows use the configured key-page entry limit"
        );
    }

    let source = SourceId::new("duplicate-span", 1);
    let built = build_source(
        source.clone(),
        definition.clone(),
        vec![key_value_batch(Arc::clone(&schema), 513, true)],
        build_account(8 * 1024 * 1024),
    )
    .await
    .expect("duplicate source builds");
    assert_eq!(built.runs()[0].directory().entries().len(), 3);
    assert_eq!(built.runs()[0].max_duplicate_key_count(), 513);

    let key_column: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let key = definition
        .encode_probe_row(&[key_column], 0)
        .expect("probe key encodes")
        .expect("key is non-NULL");
    let view = into_view(definition, source, built);
    let empty = gather(&view, &[], &[1], 1, 1)
        .await
        .expect("empty gather retains its requested schema");
    assert_eq!(empty.batch().num_rows(), 0);
    assert_eq!(empty.batch().num_columns(), 1);
    let mut cursor = probe_many(
        Arc::clone(&view),
        vec![ProbeRequest::new(4, key, vec![0, 1])],
    );
    let mut matches = Vec::new();
    loop {
        match cursor
            .next_matches(200, 200 * std::mem::size_of::<super::ProbeMatch>())
            .await
            .expect("duplicate probe succeeds")
        {
            ProbeStep::Matches(chunk) => matches.extend(chunk),
            ProbeStep::Pending => {}
            ProbeStep::Exhausted => break,
        }
    }
    assert_eq!(matches.len(), 513);
    assert!(matches.iter().all(|matched| matched.request_ordinal() == 4));

    let refs = vec![
        matches[2].row_ref().clone(),
        matches[1].row_ref().clone(),
        matches[2].row_ref().clone(),
    ];
    let gathered = gather(&view, &refs, &[1], 3, 1024 * 1024)
        .await
        .expect("gather preserves repeated references");
    let values = gathered
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("gather retains its requested Utf8 column");
    assert_eq!(values.value(0), "value-2");
    assert_eq!(values.value(1), "value-1");
    assert_eq!(values.value(2), "value-2");
}

#[tokio::test]
async fn multiple_definitions_share_payload_pages_and_tight_admission_refuses_before_build() {
    let schema = schema(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let definitions = vec![
        definition(Arc::clone(&schema), &["key"]),
        definition(Arc::clone(&schema), &["value"]),
    ];
    let sources = build_sources(
        SourceId::new("shared-pages", 1),
        definitions,
        vec![key_value_batch(Arc::clone(&schema), 10, false)],
        build_account(8 * 1024 * 1024),
    )
    .await
    .expect("two index definitions build over one payload source");
    let first_id = sources[0].source().payload_pages()[0].clone();
    let first = sources[0]
        .page_store()
        .load_payload_pages(std::slice::from_ref(&first_id))
        .await
        .expect("first store payload page");
    let second = sources[1]
        .page_store()
        .load_payload_pages(std::slice::from_ref(&first_id))
        .await
        .expect("second store payload page");
    assert!(Arc::ptr_eq(first[0].page(), second[0].page()));

    let tight = build_source(
        SourceId::new("tight-admission", 1),
        definition(Arc::clone(&schema), &["key"]),
        vec![key_value_batch(schema, 1, false)],
        build_account(1),
    )
    .await;
    assert!(matches!(tight, Err(Error::Unavailable { .. })));
}

#[test]
fn file_visibility_preserves_apply_ignore_cutoff_and_physical_position_rules() {
    let schema = schema(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .expect("test primary-key batch");
    let deletions = PkDeletionSnapshot::Int64Pk {
        tombstones: Arc::new(DeletionIndex::from_maps(
            HashMap::from([(1, 10), (2, 20)]),
            HashMap::from([(1, 11)]),
        )),
    };
    let primary_key = PrimaryKeyLayout::new(vec![0], None);

    let warm = VisibilityAdapter::file(
        SourceRole::Warm,
        deletions.clone(),
        InsertRecordHandling::Apply,
        primary_key.clone(),
        None,
    )
    .expect("warm visibility adapter");
    assert_eq!(
        warm.select_visible_rows(&batch, &[0, 1, 2])
            .expect("warm visibility"),
        vec![0, 2],
        "a re-insert makes only the current warm row visible"
    );

    let cold = VisibilityAdapter::file(
        SourceRole::Cold,
        deletions.clone(),
        InsertRecordHandling::Ignore,
        primary_key.clone(),
        None,
    )
    .expect("cold visibility adapter");
    assert_eq!(
        cold.select_visible_rows(&batch, &[0, 1, 2])
            .expect("cold visibility"),
        vec![2],
        "old cold rows do not inherit re-insert visibility"
    );

    let protected = VisibilityAdapter::file(
        SourceRole::Protected {
            min_delete_sequence: 10,
        },
        deletions,
        InsertRecordHandling::Ignore,
        primary_key,
        None,
    )
    .expect("protected visibility adapter");
    assert_eq!(
        protected
            .select_visible_rows(&batch, &[0, 1, 2])
            .expect("protected visibility"),
        vec![0, 2],
        "the protected cutoff ignores delete sequence 10 but applies sequence 20"
    );

    let positions = PositionDeletionVector::new(RoaringBitmap::from_iter([0, 257]));
    let position_only = VisibilityAdapter::file(
        SourceRole::Warm,
        PkDeletionSnapshot::PositionBased,
        InsertRecordHandling::Apply,
        PrimaryKeyLayout::new(Vec::new(), None),
        Some(Arc::new(positions)),
    )
    .expect("position visibility adapter");
    assert_eq!(
        position_only
            .select_visible_rows(&batch, &[0, 257, 1])
            .expect("position visibility"),
        vec![2],
        "position vectors apply to original physical ordinals, not candidate indexes"
    );
}

#[test]
fn inline_and_memory_visibility_use_their_captured_sequence_tombstones() {
    let schema = schema(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
        .expect("test inline batch");
    let mut tombstones = InMemTombstones::default();
    tombstones.int64_pk.insert(1, 11);
    let primary_key = PrimaryKeyLayout::new(vec![0], None);

    let older_inline = VisibilityAdapter::inline(10, tombstones.clone(), primary_key.clone());
    assert_eq!(
        older_inline
            .select_visible_rows(&batch, &[0, 1])
            .expect("inline visibility"),
        vec![1],
        "a later tombstone hides the old inline payload before residual predicates"
    );

    let newer_memory = VisibilityAdapter::memory(12, tombstones, primary_key);
    assert_eq!(
        newer_memory
            .select_visible_rows(&batch, &[0, 1])
            .expect("memory visibility"),
        vec![0, 1],
        "a source inserted after its tombstone remains visible"
    );
}
