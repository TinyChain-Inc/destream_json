use bytes::Bytes;
use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use destream_json::{de, try_decode, Value};
use futures::stream;
use futures::StreamExt;
use std::hint::black_box;
static KB: usize = 1_000;

// a random json file 13.8 MB
const DATA: &[u8] = include_bytes!("generated.json");

fn iter_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_chunks");

    async fn test(chunk_size: usize) {
        let source = stream::iter(DATA.chunks(chunk_size))
            .map(Bytes::from_static)
            .map(Result::<Bytes, de::Error>::Ok);

        #[allow(unused_variables)]
        let result: Result<Value, de::Error> = black_box(try_decode((), source).await);
    }

    group.sample_size(10);
    for size in [10, 1 * KB, 10 * KB, 30 * KB, 50 * KB, 100 * KB].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(FuturesExecutor).iter(|| test(size))
        });
    }
    group.finish();
}

fn collect_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("collect_chunks");

    async fn test(chunks: Vec<Result<Bytes, de::Error>>) {
        let source = stream::iter(chunks);

        #[allow(unused_variables)]
        let result: Value = black_box(try_decode((), source).await.unwrap());
    }

    group.sample_size(10);
    for size in [10, 1 * KB, 10 * KB, 30 * KB, 50 * KB, 100 * KB].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(FuturesExecutor).iter_batched(
                || {
                    DATA.chunks(size)
                        .map(Bytes::from_static)
                        .map(Result::<Bytes, de::Error>::Ok)
                        .collect::<Vec<Result<Bytes, de::Error>>>()
                },
                |chunks| test(chunks),
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn all_at_once(c: &mut Criterion) {
    async fn test(bytes: Result<Bytes, de::Error>) {
        let source = Box::pin(stream::once(async { bytes }));

        #[allow(unused_variables)]
        let result: Value = black_box(try_decode((), source).await.unwrap());
    }

    c.bench_function("all_at_once", |b| {
        b.to_async(FuturesExecutor).iter_batched(
            || Result::<_, de::Error>::Ok(Bytes::from_static(DATA)),
            |bytes| test(bytes),
            criterion::BatchSize::LargeInput,
        );
    });
}

criterion_group!(benches, iter_chunks, collect_chunks, all_at_once);
criterion_main!(benches);
