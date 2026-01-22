# Changelog

## Unreleased

- Dev-only: remove the `number-general` test dependency to avoid pulling in `destream 0.8`.
- Tighten `Duration` decoding to reject `nanos >= 1_000_000_000`.

## 0.14.0

- Upgrade to `destream 0.9`.
- Encode `i128`/`u128` using strings (default `destream` impl); accept numeric tokens where possible.
- Add conformance tests for extended `destream` default impl coverage (128-bit integers, `Duration`,
  and standard net address types).
