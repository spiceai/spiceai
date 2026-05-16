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

// Criterion benchmarks for the truncation fast-paths and formatting helpers
// in arrow_tools.
//
// Run with: cargo bench -p arrow_tools --bench truncate
//
// The benchmarks deliberately separate "fast path" (no actual work needed,
// exercises the zero-copy `clone()` paths added in the audit) from
// "actual truncation" (exercises the slice+concat / collect paths).

use arrow::array::{
    FixedSizeListArray, Int32Array, LargeListViewArray, ListArray, ListViewArray, StringArray,
    StringViewBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_tools::record_batch::{truncate_numeric_column_length, truncate_string_columns};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;

// Creates a batch where *no* string needs truncation at the given limit.
// This exercises the new UTF8 fast-path (cheap any() + early Arc::clone).
fn make_all_short_string_batch(n: usize, _max_chars: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    // All strings are ASCII and well under the limit
    let short = "short".repeat(3); // ~15 chars
    let strings: Vec<Option<String>> = (0..n).map(|_| Some(short.clone())).collect();
    let arr = StringArray::from(strings);
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

// Creates a batch where a significant fraction of strings *do* need
// character truncation. Exercises the collect + truncation path.
fn make_many_long_string_batch(n: usize, max_chars: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let long = "x".repeat(max_chars + 20); // will need truncation
    let short = "s";
    let strings: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                Some(long.clone())
            } else {
                Some(short.to_string())
            }
        })
        .collect();
    let arr = StringArray::from(strings);
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

// StringViewArray versions of the above (exercises the specific fast-path
// arm for DataType::Utf8View that was added during the audit).
fn make_all_short_string_view_batch(n: usize, _max_chars: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "text",
        DataType::Utf8View,
        true,
    )]));
    let short = "short".repeat(3);
    let mut builder = StringViewBuilder::new();
    for _ in 0..n {
        builder.append_value(&short);
    }
    let arr = builder.finish();
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

fn make_many_long_string_view_batch(n: usize, max_chars: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "text",
        DataType::Utf8View,
        true,
    )]));
    let long = "x".repeat(max_chars + 20);
    let short = "s";
    let mut builder = StringViewBuilder::new();
    for i in 0..n {
        if i % 3 == 0 {
            builder.append_value(&long);
        } else {
            builder.append_value(short);
        }
    }
    let arr = builder.finish();
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

// Creates a List<i32> batch where no list exceeds the element limit.
// Exercises the list fast-path (try_fold + clone).
fn make_all_short_list_batch(n: usize, max_elems: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = max_elems.min(5);
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let offsets: Vec<i32> = (0..=n)
        .map(|i| i32::try_from(i * per_list).expect("offset fits in i32"))
        .collect();
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::<i32>::new(offsets.into()),
        Arc::new(values),
        None,
    );
    RecordBatch::try_new(schema, vec![Arc::new(list)]).expect("valid batch")
}

// Creates a List<i32> batch where truncation is required for every list.
fn make_long_list_batch(n: usize, truncate_to: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = truncate_to + 15; // will need truncation
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let offsets: Vec<i32> = (0..=n)
        .map(|i| i32::try_from(i * per_list).expect("offset fits in i32"))
        .collect();
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::<i32>::new(offsets.into()),
        Arc::new(values),
        None,
    );
    RecordBatch::try_new(schema, vec![Arc::new(list)]).expect("valid batch")
}

// ListView<i32> versions (exercises the ListView truncation path, which has
// more complex offset + size handling and a different fast-path decision
// based on the explicit sizes buffer).
fn make_all_short_list_view_batch(n: usize, max_elems: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = max_elems.min(5);
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let offsets: Vec<i32> = (0..n)
        .map(|i| i32::try_from(i * per_list).expect("offset fits in i32"))
        .collect();
    let per_list_i32 = i32::try_from(per_list).expect("per_list fits in i32");
    let sizes: Vec<i32> = (0..n).map(|_| per_list_i32).collect();
    let list_view = ListViewArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::<i32>::from(offsets),
        ScalarBuffer::<i32>::from(sizes),
        Arc::new(values),
        None,
    )
    .expect("ListViewArray construction for benchmark");
    RecordBatch::try_new(schema, vec![Arc::new(list_view)]).expect("valid batch")
}

fn make_long_list_view_batch(n: usize, truncate_to: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = truncate_to + 15;
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let offsets: Vec<i32> = (0..n)
        .map(|i| i32::try_from(i * per_list).expect("offset fits in i32"))
        .collect();
    let per_list_i32 = i32::try_from(per_list).expect("per_list fits in i32");
    let sizes: Vec<i32> = (0..n).map(|_| per_list_i32).collect();
    let list_view = ListViewArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::<i32>::from(offsets),
        ScalarBuffer::<i32>::from(sizes),
        Arc::new(values),
        None,
    )
    .expect("ListViewArray construction for benchmark");
    RecordBatch::try_new(schema, vec![Arc::new(list_view)]).expect("valid batch")
}

// FixedSizeList<i32> versions (exercises the FixedSizeList fast-path, which is
// the cheapest of all — just a uniform size comparison — plus the stride-based
// slicing + concat work path).
fn make_all_short_fixed_size_list_batch(n: usize, _max_elems: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 5),
        true,
    )]));
    let per_list = 5usize;
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let list = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        5,
        Arc::new(values),
        None,
    );
    RecordBatch::try_new(schema, vec![Arc::new(list)]).expect("valid batch")
}

fn make_long_fixed_size_list_batch(n: usize, _truncate_to: usize) -> RecordBatch {
    // For FixedSizeList the "long" case is when the fixed size > truncate_to.
    // We create lists of size 20 and truncate to 5.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 20),
        true,
    )]));
    let per_list = 20usize;
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let list = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        20,
        Arc::new(values),
        None,
    );
    RecordBatch::try_new(schema, vec![Arc::new(list)]).expect("valid batch")
}

// LargeListView<i32> versions (completes benchmark coverage for all five list
// variants that received full support during the type audit; i64 offsets/sizes
// + more complex offset rebuild in the work path).
fn make_all_short_large_list_view_batch(n: usize, max_elems: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = max_elems.min(5);
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let offsets: Vec<i64> = (0..n)
        .map(|i| {
            let offset = i.checked_mul(per_list).expect("offset fits in usize");
            i64::try_from(offset).expect("offset fits in i64")
        })
        .collect();
    let per_list_i64 = i64::try_from(per_list).expect("list size fits in i64");
    let sizes: Vec<i64> = std::iter::repeat_n(per_list_i64, n).collect();
    let list_view = LargeListViewArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::<i64>::from(offsets),
        ScalarBuffer::<i64>::from(sizes),
        Arc::new(values),
        None,
    )
    .expect("LargeListViewArray construction for benchmark");
    RecordBatch::try_new(schema, vec![Arc::new(list_view)]).expect("valid batch")
}

fn make_long_large_list_view_batch(n: usize, truncate_to: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = truncate_to + 15;
    let total_values = n * per_list;
    let values = Int32Array::from(
        (0..total_values)
            .map(|v| i32::try_from(v).expect("value fits in i32"))
            .collect::<Vec<i32>>(),
    );
    let offsets: Vec<i64> = (0..n)
        .map(|i| {
            let offset = i.checked_mul(per_list).expect("offset fits in usize");
            i64::try_from(offset).expect("offset fits in i64")
        })
        .collect();
    let per_list_i64 = i64::try_from(per_list).expect("list size fits in i64");
    let sizes: Vec<i64> = std::iter::repeat_n(per_list_i64, n).collect();
    let list_view = LargeListViewArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::<i64>::from(offsets),
        ScalarBuffer::<i64>::from(sizes),
        Arc::new(values),
        None,
    )
    .expect("LargeListViewArray construction for benchmark");
    RecordBatch::try_new(schema, vec![Arc::new(list_view)]).expect("valid batch")
}

fn bench_truncate(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_tools_truncate");

    // String (Utf8) fast path
    let short_strings = make_all_short_string_batch(2000, 50);
    group.bench_function("string_fast_path_2000_rows_all_short", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&short_strings), 50).expect("truncate short strings");
        });
    });

    // String (Utf8) actual work
    let long_strings = make_many_long_string_batch(2000, 50);
    group.bench_function("string_with_truncation_2000_rows_mixed", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&long_strings), 50).expect("truncate long strings");
        });
    });

    // StringView fast path (exercises the specific Utf8View arm + is_some_and decision)
    let short_views = make_all_short_string_view_batch(2000, 50);
    group.bench_function("stringview_fast_path_2000_rows_all_short", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&short_views), 50)
                .expect("truncate short string views");
        });
    });

    // StringView actual truncation
    let long_views = make_many_long_string_view_batch(2000, 50);
    group.bench_function("stringview_with_truncation_2000_rows_mixed", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&long_views), 50)
                .expect("truncate long string views");
        });
    });

    // List (regular List) fast path
    let short_lists = make_all_short_list_batch(800, 5);
    group.bench_function("list_fast_path_800_rows_all_short", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&short_lists), 5)
                .expect("truncate short lists");
        });
    });

    // List (regular List) actual truncation work
    let long_lists = make_long_list_batch(800, 5);
    group.bench_function("list_with_truncation_800_rows", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&long_lists), 5).expect("truncate long lists");
        });
    });

    // ListView fast path (exercises the sizes-based decision + clone)
    // Use iter_batched to isolate setup cost (creating the ListView batch
    // with non-contiguous offsets/sizes is non-trivial).
    group.bench_function("listview_fast_path_800_rows_all_short", |b| {
        b.iter_batched(
            || make_all_short_list_view_batch(800, 5),
            |batch| {
                truncate_numeric_column_length(black_box(&batch), 5)
                    .expect("truncate short list views");
            },
            BatchSize::SmallInput,
        );
    });

    // ListView actual truncation work (exercises the more complex offset/size rebuild)
    let long_list_views = make_long_list_view_batch(800, 5);
    group.bench_function("listview_with_truncation_800_rows", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&long_list_views), 5)
                .expect("truncate long list views");
        });
    });

    // FixedSizeList fast path (cheapest decision: uniform size comparison).
    // Use iter_batched for consistency with the view variants (even though
    // setup is simpler, it keeps the benchmark structure uniform).
    group.bench_function("fixed_size_list_fast_path_800_rows_all_short", |b| {
        b.iter_batched(
            || make_all_short_fixed_size_list_batch(800, 5),
            |batch| {
                truncate_numeric_column_length(black_box(&batch), 5)
                    .expect("truncate short fixed-size lists");
            },
            BatchSize::SmallInput,
        );
    });

    // FixedSizeList actual truncation work (stride-based slicing + concat)
    let long_fsl = make_long_fixed_size_list_batch(800, 5);
    group.bench_function("fixed_size_list_with_truncation_800_rows", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&long_fsl), 5)
                .expect("truncate long fixed-size lists");
        });
    });

    // LargeListView fast path (completes the five-variant benchmark coverage;
    // i64 sizes scan + zero-copy clone).
    // Use iter_batched to isolate setup cost (i64 ScalarBuffer creation).
    group.bench_function("large_listview_fast_path_800_rows_all_short", |b| {
        b.iter_batched(
            || make_all_short_large_list_view_batch(800, 5),
            |batch| {
                truncate_numeric_column_length(black_box(&batch), 5)
                    .expect("truncate short large list views");
            },
            BatchSize::SmallInput,
        );
    });

    // LargeListView actual truncation work (i64 offset/size rebuild path)
    let long_large_list_views = make_long_large_list_view_batch(800, 5);
    group.bench_function("large_listview_with_truncation_800_rows", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&long_large_list_views), 5)
                .expect("truncate long large list views");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_truncate);
criterion_main!(benches);
