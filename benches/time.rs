use criterion::{criterion_group, criterion_main, setup, BatchSize, Criterion};
use seahash_groove::{File, IndexHandle};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;

criterion_group!(
    name = benches;
    config = Criterion::default().noise_threshold(0.02).sample_size(10);
    targets = benchmark_enrichment_tables_file
);
criterion_main!(benches);

/*
fn lookup(data: Vec<Vec<String>>, cols: (String, String)) -> Option<Vec<String>> {
    let mut iter = data
        .into_iter()
        .filter(|row| row[0] == cols.0 && row[1] == cols.1);

    iter.next()
}

fn benchmark_seahash(c: &mut Criterion) {
    let mut group = c.benchmark_group("seahash");

    let setup_hash = |size| {
        let mut index =
            HashMap::with_capacity_and_hasher(size, hash_hasher::HashBuildHasher::default());

        for i in 0..size {
            let mut hash = seahash::SeaHasher::new();
            hash.write(format!("data-1-{}", i).as_bytes());
            hash.write_u8(0);
            hash.write(format!("data-9-{}", i).as_bytes());

            let num = hash.finish();
            index.insert(num, i);
        }

        let mut hash = seahash::SeaHasher::new();
        hash.write(format!("data-1-{}", size - 1).as_bytes());
        hash.write_u8(0);
        hash.write(format!("data-9-{}", size - 1).as_bytes());

        let lookup = hash.finish();

        (index, lookup)
    };

    let setup_vec = |size| {
        let nums = (0..size)
            .map(|i| vec![format!("data-1-{}", i), format!("data-9-{}", i)])
            .collect::<Vec<_>>();

        let lookup = (
            format!("data-1-{}", size - 1),
            format!("data-9-{}", size - 1),
        );

        (nums, lookup)
    };

    group.bench_function("enrichment_tables/hash_100_000", |b| {
        b.iter_batched(
            || setup_hash(100_000),
            |(nums, lookup)| {
                assert_eq!(Some(&99_999), nums.get(&lookup));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("enrichment_tables/vec_100_000", |b| {
        b.iter_batched(
            || setup_vec(100_000),
            |(nums, cols)| {
                assert_eq!(
                    Some(vec![cols.0.clone(), cols.1.clone()]),
                    lookup(nums, cols)
                );
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("enrichment_tables/hash_1_000_000", |b| {
        b.iter_batched(
            || setup_hash(1_000_000),
            |(nums, lookup)| {
                assert_eq!(Some(&999_999), nums.get(&lookup));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("enrichment_tables/vec_1_000_000", |b| {
        b.iter_batched(
            || setup_vec(1_000_000),
            |(nums, cols)| {
                assert_eq!(
                    Some(vec![cols.0.clone(), cols.1.clone()]),
                    lookup(nums, cols)
                );
            },
            BatchSize::SmallInput,
        )
    });
}
*/

fn benchmark_enrichment_tables_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("enrichment_tables_file");
    group.bench_function("enrichment_tables/file_noindex_10", |b| {
        b.iter_batched(
            || setup(10),
            |(file, _index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, None))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_hashindex_10", |b| {
        b.iter_batched(
            || setup(10),
            |(file, index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, Some(index)))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_noindex_1_000", |b| {
        b.iter_batched(
            || setup(1_000),
            |(file, _index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, None))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_hashindex_1_000", |b| {
        b.iter_batched(
            || setup(1_000),
            |(file, index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, Some(index)))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_noindex_10_000", |b| {
        b.iter_batched(
            || setup(10_000),
            |(file, _index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, None))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_hashindex_10_000", |b| {
        b.iter_batched(
            || setup(10_000),
            |(file, index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, Some(index)))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_noindex_1_000_000", |b| {
        b.iter_batched(
            || setup(1_000_000),
            |(file, _index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, None))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("enrichment_tables/file_hashindex_1_000_000", |b| {
        b.iter_batched(
            || setup(1_000_000),
            |(file, index, condition, expected)| {
                assert_eq!(Ok(expected), file.find_table_row(condition, Some(index)))
            },
            BatchSize::SmallInput,
        );
    });
}
