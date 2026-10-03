# destream_json
Rust library for encoding and decoding JSON streams

Example:
```rust
let expected = ("one".to_string(), 2.0, vec![3, 4]);
let stream = destream_json::encode(&expected).unwrap();
let actual: (String, f64, Vec<i32>) = destream_json::try_decode((), stream).await.unwrap();
assert_eq!(expected, actual);
```

Decoding and structural inspection share a maximum nesting depth of 1,024
containers. Decoding rejects trailing input after the requested value.
`destream::de::Decoder::inspect_any` visits bounded text chunks and container
cardinalities without constructing decoded payloads; allocation policy belongs
to the value owner. The decoder retains the current input chunk by handle and
copies at most 4 KiB per refill into bounded inspection scratch.

For input-dependent nesting, follow `destream`'s
[iterative traversal guidance](https://docs.rs/destream/latest/destream/).
Nesting limits are separate from caller-owned input and allocation budgets.
Ordinary sequence/map encoding uses a flat child queue to keep polling depth
independent of container width.

With the `value` feature, `Value` owns a movable `ValueKind`. Use `kind`,
`kind_mut`, or `into_kind` to inspect or change its contents, and `ValueKind::into`
to construct it. Lists and maps retain their native containers. Cloning, comparison,
formatting, destruction, decoding, and owned/borrowed encoding traverse iteratively.
