use std::cmp::min;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream;

fn chunk_stream(bytes: Bytes, chunk_size: usize) -> impl futures::Stream<Item = Bytes> + Unpin {
    assert!(chunk_size > 0);

    let len = bytes.len();
    let chunks = (0..len)
        .step_by(chunk_size)
        .map(move |i| bytes.slice(i..min(i + chunk_size, len)));

    stream::iter(chunks)
}

fn make_json_array_u64(count: usize) -> Bytes {
    let mut json = String::with_capacity(count * 6);
    json.push('[');
    for i in 0..count {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&(i as u64).to_string());
    }
    json.push(']');

    Bytes::from(json)
}

async fn decode_once(bytes: Bytes, chunk_size: usize) -> Duration {
    let started = Instant::now();
    let decoded: Vec<u64> = destream_json::decode((), chunk_stream(bytes, chunk_size))
        .await
        .expect("decode json array");
    std::hint::black_box(decoded);
    started.elapsed()
}

// This is an intentionally minimal, dependency-free (no criterion) micro-benchmark to
// validate chunk-size sensitivity in the decoder.
//
// Run with:
// - `cargo test --test bench_chunk_size -- --ignored --nocapture`
//
// Optionally enable a very coarse regression check with:
// - `DESTREAM_JSON_BENCH_ASSERT=1 cargo test --test bench_chunk_size -- --ignored --nocapture`
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn bench_decode_chunk_sizes() {
    let payload = make_json_array_u64(100_000);

    let chunk_sizes = [1_usize, 8, 64, 1024, 8192, 65536];
    let iterations = 5_usize;

    eprintln!(
        "destream_json chunk-size bench: payload={} bytes, iterations={}",
        payload.len(),
        iterations
    );

    let mut results = Vec::with_capacity(chunk_sizes.len());
    for &chunk_size in &chunk_sizes {
        let mut total = Duration::ZERO;
        for _ in 0..iterations {
            total += decode_once(payload.clone(), chunk_size).await;
        }

        let avg = total / (iterations as u32);
        eprintln!("  chunk_size={chunk_size:>6} avg={avg:?}");
        results.push((chunk_size, avg));
    }

    if std::env::var_os("DESTREAM_JSON_BENCH_ASSERT").is_some() {
        let slowest = results.iter().max_by_key(|(_, d)| *d).unwrap();
        let fastest = results.iter().min_by_key(|(_, d)| *d).unwrap();

        eprintln!(
            "  fastest: chunk_size={} avg={:?}\n  slowest: chunk_size={} avg={:?}",
            fastest.0, fastest.1, slowest.0, slowest.1
        );

        assert!(
            slowest.1 <= fastest.1 * 5,
            "unexpected chunk-size sensitivity: slowest {:?} vs fastest {:?}",
            slowest,
            fastest
        );
    }
}
