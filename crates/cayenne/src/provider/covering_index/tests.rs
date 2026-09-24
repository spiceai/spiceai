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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Datum, Decimal128Array, Int64Array, RecordBatch,
    RecordBatchOptions, StringArray, StringViewArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

use super::super::lookup_index::KeySpec;
use super::super::memory_account::CayenneMemoryAccount;
use super::{
    AllocationOwner, CoveredRowRef, CoveringPageStore, CoveringReadView, EncodedKey, Error,
    IndexCatalog, IndexDefinition, IndexRun, IndexedSource, KeyDirectory, KeyDirectoryEntry,
    KeyPage, KeyPageId, KeyPageLease, PageLease, PayloadPage, PayloadPageId, PayloadPageLease,
    Result, RunId, SchemaIdentity, SourceId, prepare_literal_seek, try_cover,
};

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
