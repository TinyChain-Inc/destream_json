use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

mod common;

fn bench_decode_chunk_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let payload = common::make_json_array_u64(100_000);
    let chunk_sizes = [1_usize, 8, 64, 1024, 8192, 65536];

    let mut group = c.benchmark_group("decode_chunk_size");
    group.throughput(Throughput::Bytes(payload.len() as u64));

    for &chunk_size in &chunk_sizes {
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &chunk_size,
            |b, &cs| {
                let bytes = payload.clone();
                b.iter(|| {
                    let decoded: Vec<u64> = rt
                        .block_on(destream_json::decode(
                            (),
                            common::chunk_stream(bytes.clone(), cs),
                        ))
                        .expect("decode json array");
                    std::hint::black_box(decoded.len());
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_decode_chunk_sizes);
criterion_main!(benches);
