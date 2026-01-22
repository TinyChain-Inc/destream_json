# destream_json
Rust library for encoding and decoding JSON streams

## Compatibility notes

- See `CHANGELOG.md` for behavior and dependency changes.
- This crate aims to produce/accept standard JSON; the one intentional divergence is support for
  non-string map keys (which is not spec-compliant JSON).
- This crate expects UTF-8 JSON text (no UTF-8 BOM). File an issue if you need UTF-16/UTF-32
  support or BOM handling.
- Duplicate object keys are not rejected; behavior depends on the target type.
- Decoding is strict about consuming the entire input stream; trailing non-whitespace bytes after
  the first value are treated as an error.
- Decoding enforces a maximum nesting depth of 1024 by default; use
  `decode_with_max_depth`/`try_decode_with_max_depth` (and `read_from_with_max_depth` with
  `tokio-io`) to override.
- There are no explicit size limits; hostile inputs may require significant CPU/memory.
- Decoding into `f32`/`f64` may yield non-finite values for extremely large numbers; such values
  cannot be re-encoded as JSON numbers.
- With the optional `value` feature, `number_general::Number::Complex` cannot be encoded as JSON.
- Default `destream` impl conventions used by this codec:
  - `i128`/`u128` encode as strings; decode accepts either strings or in-range integer tokens
  - `Duration` encodes as `[secs, nanos]` with `nanos < 1_000_000_000`

Example:
```rust
let expected = ("one".to_string(), 2.0, vec![3, 4]);
let stream = destream_json::encode(&expected).unwrap();
let actual = destream_json::try_decode((), stream).await;
assert_eq!(expected, actual);
```

## Chunk-size micro-benchmark

To inspect decode performance sensitivity to input chunk size:

`cargo test --test bench_chunk_size -- --ignored --nocapture`

## Criterion benchmark

For more stable measurements (and throughput reporting):

`cargo bench --bench chunk_size`

## Buffered encoding

If your transport does one write per stream item, buffering encoder output can reduce chunk count:

- `destream_json::en::encode_buffered(value, 8 * 1024)`
