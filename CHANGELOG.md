# Changelog

## Unreleased

- Dev-only: remove the `number-general` test dependency to avoid pulling in `destream 0.8`.
- Fix `--all-features` builds by removing the `number-general/stream` feature dependency (which
  pulled in `destream 0.8`) from the optional `value` feature.
- Tighten `Duration` decoding to reject `nanos >= 1_000_000_000`.
- Fix decode performance sensitivity to input chunk size by avoiding `Vec` front-removals in the
  streaming buffer.
- Reduce allocations when decoding from `tokio::io::AsyncRead` by reusing an internal read buffer.
- Tighten JSON string decoding/encoding to fully implement required escape sequences, including
  `\uXXXX` escapes and surrogate pairs.
- Tighten JSON parsing to accept only RFC 8259 whitespace and to reject invalid number forms like
  leading-zero integers (`01`).
- Make `read_from` strict about trailing bytes (consistent with `decode`/`try_decode`).
- Remove the `async-recursion` dependency by implementing `IgnoredAny` skipping via an explicit
  stack machine.
- Mitigate deep-nesting attacks by enforcing a maximum nesting depth of 1024 by default; expose
  `decode_with_max_depth`/`try_decode_with_max_depth` (and `read_from_with_max_depth` with
  `tokio-io`) to override.
- Add `encode_buffered`/`encode_seq_buffered`/`encode_map_buffered` to reduce the number of encoded
  output chunks (and downstream `await`s/writes) for common IO patterns.
- Fix large collection encoding stack overflow by replacing deep `Stream::chain` nesting with an
  explicit encoder state machine.
- Chunk-size micro-benchmark (release build, `cargo test --test bench_chunk_size -- --ignored --nocapture`):
  - payload=588891 bytes, iterations=5
  - chunk_size=1 avg=25.495012ms; 8 avg=12.098722ms; 64 avg=11.063938ms; 1024 avg=11.89015ms;
    8192 avg=11.408764ms; 65536 avg=10.376105ms

## 0.14.0

- Upgrade to `destream 0.9`.
- Encode `i128`/`u128` using strings (default `destream` impl); accept numeric tokens where possible.
- Add conformance tests for extended `destream` default impl coverage (128-bit integers, `Duration`,
  and standard net address types).
