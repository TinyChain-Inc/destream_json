use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::TryStreamExt;

async fn measure_stream<S>(stream: S) -> Result<(usize, usize), destream_json::en::Error>
where
    S: futures::Stream<Item = Result<bytes::Bytes, destream_json::en::Error>>,
{
    stream
        .try_fold((0_usize, 0_usize), |(chunks, bytes), chunk| async move {
            Ok((chunks + 1, bytes + chunk.len()))
        })
        .await
}

fn bench_encode_chunking(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let value: Vec<u64> = (0..100_000).map(|i| i as u64).collect();
    let buffered_targets = [256_usize, 1024, 8192, 65536];

    let mut cases = Vec::with_capacity(1 + buffered_targets.len());

    let (base_chunks, base_bytes) = rt
        .block_on(measure_stream(
            destream_json::encode(&value).expect("encode json value"),
        ))
        .expect("measure encode");

    cases.push((None, base_chunks, base_bytes));

    for target in buffered_targets {
        let (chunks, bytes) = rt
            .block_on(measure_stream(
                destream_json::en::encode_buffered(&value, target)
                    .expect("encode buffered json value"),
            ))
            .expect("measure encode");

        cases.push((Some(target), chunks, bytes));
    }

    {
        let mut group = c.benchmark_group("encode_bytes");
        group.measurement_time(Duration::from_secs(3));

        for (target, _chunks, bytes) in &cases {
            group.throughput(Throughput::Bytes(*bytes as u64));

            match target {
                None => {
                    group.bench_function("baseline", |b| {
                        b.iter(|| {
                            rt.block_on(async {
                                let stream =
                                    destream_json::encode(&value).expect("encode json value");
                                let (chunks, bytes) =
                                    measure_stream(stream).await.expect("measure encode");
                                std::hint::black_box((chunks, bytes));
                            })
                        })
                    });
                }
                Some(target) => {
                    group.bench_with_input(
                        BenchmarkId::new("buffered", target),
                        target,
                        |b, &t| {
                            b.iter(|| {
                                rt.block_on(async {
                                    let stream = destream_json::en::encode_buffered(&value, t)
                                        .expect("encode buffered");
                                    let (chunks, bytes) =
                                        measure_stream(stream).await.expect("measure encode");
                                    std::hint::black_box((chunks, bytes));
                                })
                            })
                        },
                    );
                }
            }
        }

        group.finish();
    }

    {
        let mut group = c.benchmark_group("encode_chunks");
        group.measurement_time(Duration::from_secs(3));

        for (target, chunks, _bytes) in &cases {
            group.throughput(Throughput::Elements(*chunks as u64));

            match target {
                None => {
                    group.bench_function("baseline", |b| {
                        b.iter(|| {
                            rt.block_on(async {
                                let stream =
                                    destream_json::encode(&value).expect("encode json value");
                                let (chunks, bytes) =
                                    measure_stream(stream).await.expect("measure encode");
                                std::hint::black_box((chunks, bytes));
                            })
                        })
                    });
                }
                Some(target) => {
                    group.bench_with_input(
                        BenchmarkId::new("buffered", target),
                        target,
                        |b, &t| {
                            b.iter(|| {
                                rt.block_on(async {
                                    let stream = destream_json::en::encode_buffered(&value, t)
                                        .expect("encode buffered");
                                    let (chunks, bytes) =
                                        measure_stream(stream).await.expect("measure encode");
                                    std::hint::black_box((chunks, bytes));
                                })
                            })
                        },
                    );
                }
            }
        }

        group.finish();
    }
}

criterion_group!(benches, bench_encode_chunking);
criterion_main!(benches);
