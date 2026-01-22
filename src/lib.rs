//! Library for decoding and encoding JSON streams.
//!
//! Example:
//! ```
//! # use futures::StreamExt;
//! # use futures::executor::block_on;
//! let expected = ("one".to_string(), 2.0, vec![3, 4]);
//! let stream = destream_json::encode(&expected).unwrap();
//! let actual = block_on(destream_json::try_decode((), stream)).unwrap();
//! assert_eq!(expected, actual);
//! ```
//!
//! Buffered encoding:
//!  - If your transport does one write per stream item, prefer `encode_buffered` (or
//!    `encode_seq_buffered`/`encode_map_buffered`) to reduce the number of chunks (and thus
//!    `await`s/writes) at the cost of additional buffering.
//!
//! `Value` feature notes:
//!  - When using `destream_json::Value`, `number_general::Number::Complex` cannot be encoded as JSON.
//!
//! JSON compliance notes:
//!  - This codec expects UTF-8 JSON text. Other JSON encodings (UTF-16/UTF-32) and a UTF-8 BOM are
//!    not supported; please file an issue if you need them.
//!  - This codec intentionally supports a superset of JSON by allowing non-string object keys.
//!    This is not valid JSON per RFC 8259, and may not round-trip through other JSON libraries.
//!  - Duplicate object keys are not rejected. Behavior depends on the target type (e.g., for maps
//!    like `HashMap`, later entries typically overwrite earlier ones), so you should not rely on
//!    a stable policy unless you enforce uniqueness yourself.
//!  - Strings are encoded/decoded using standard JSON escapes, including `\uXXXX` and surrogate
//!    pairs.
//!  - The decoder is strict about JSON whitespace (`' '`, `'\t'`, `'\r'`, `'\n'`) and number
//!    grammar (e.g., leading zeroes like `01` are rejected).
//!  - Decoding is strict about consuming the entire input stream; trailing non-whitespace bytes
//!    after the first value are treated as an error.
//!  - Non-finite floats (`NaN`, `±inf`) cannot be encoded as JSON numbers and return an error.
//!  - Very large JSON numbers may fail to decode into a requested Rust numeric type (or overflow
//!    when decoding into `f32`/`f64`). Decoding into `f32`/`f64` may yield non-finite values (per
//!    Rust's float parsing), which cannot be re-encoded.
//!  - Decoding enforces a maximum nesting depth of 1024 by default; use
//!    `decode_with_max_depth`/`try_decode_with_max_depth` to override.
//!  - There are no explicit size limits; hostile inputs may require significant CPU/memory.

// `destream_json` implements `destream`'s `async fn` trait APIs on stable Rust, so we keep this
// `allow` until `async_fn_in_trait` is stabilized.
#![allow(async_fn_in_trait)]

pub use de::{decode, try_decode};
pub use en::{
    encode, encode_buffered, encode_map, encode_map_buffered, encode_seq, encode_seq_buffered,
};

#[cfg(feature = "value")]
pub use value::Value;

#[cfg(feature = "tokio-io")]
pub use de::read_from;

pub use de::{decode_with_max_depth, try_decode_with_max_depth};

#[cfg(feature = "tokio-io")]
pub use de::read_from_with_max_depth;

mod constants;
pub mod de;
pub mod en;

#[cfg(feature = "value")]
mod value;

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList, VecDeque};
    use std::fmt;
    use std::marker::PhantomData;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::num::{NonZeroI128, NonZeroU128};
    use std::time::Duration;

    use bytes::Bytes;
    use destream::de::{self, ArrayAccess, FromStream};
    use destream::en::IntoStream;
    use destream::IgnoredAny;
    use futures::future;
    use futures::stream::{self, Stream, StreamExt, TryStreamExt};

    use super::de::*;
    use super::en::*;

    async fn test_decode<T: FromStream<Context = ()> + PartialEq + fmt::Debug>(
        encoded: &str,
        expected: T,
    ) {
        for i in (1..encoded.len()).rev() {
            let source = stream::iter(encoded.as_bytes().iter().cloned())
                .chunks(i)
                .map(Bytes::from);

            let actual: T = decode((), source).await.unwrap();
            assert_eq!(expected, actual);
        }
    }

    async fn test_encode<'en, S: Stream<Item = Result<Bytes, super::en::Error>> + 'en>(
        encoded_stream: S,
        expected: &str,
    ) {
        let encoded = encoded_stream
            .try_fold(vec![], |mut buffer, chunk| {
                buffer.extend(chunk);
                future::ready(Ok(buffer))
            })
            .await
            .unwrap();

        assert_eq!(expected, String::from_utf8(encoded).unwrap());
    }

    async fn test_encode_value<'en, T: IntoStream<'en> + PartialEq + fmt::Debug + 'en>(
        value: T,
        expected: &str,
    ) {
        test_encode(encode(value).unwrap(), expected).await;
    }

    async fn encode_to_string<'en, S: Stream<Item = Result<Bytes, super::en::Error>> + 'en>(
        encoded_stream: S,
    ) -> String {
        let encoded = encoded_stream
            .try_fold(vec![], |mut buffer, chunk| {
                buffer.extend(chunk);
                future::ready(Ok(buffer))
            })
            .await
            .unwrap();

        String::from_utf8(encoded).unwrap()
    }

    #[tokio::test]
    async fn test_encode_buffered_equivalent() {
        let value: Vec<u64> = (0..10_000).map(|i| i as u64).collect();

        let baseline = encode_to_string(encode(&value).unwrap()).await;
        let buffered = encode_to_string(encode_buffered(&value, 1024).unwrap()).await;

        assert_eq!(baseline, buffered);
    }

    #[tokio::test]
    async fn test_encode_buffered_small_target_equivalent() {
        let value = vec!["hello".to_string(), "world".to_string()];

        let baseline = encode_to_string(encode(&value).unwrap()).await;
        let buffered = encode_to_string(encode_buffered(&value, 1).unwrap()).await;

        assert_eq!(baseline, buffered);
    }

    #[tokio::test]
    async fn test_encode_large_seq_no_stack_overflow() {
        let value: Vec<u64> = (0..100_000).map(|i| i as u64).collect();

        let encoded = encode(value)
            .unwrap()
            .try_fold(Vec::new(), |mut buffer, chunk| {
                buffer.extend_from_slice(&chunk);
                future::ready(Ok(buffer))
            })
            .await
            .unwrap();

        assert!(encoded.starts_with(b"["));
        assert!(encoded.ends_with(b"]"));
    }

    #[tokio::test]
    async fn test_encode_large_map_no_stack_overflow() {
        let mut value = BTreeMap::new();
        for i in 0..50_000_u64 {
            value.insert(i, i + 1);
        }

        let encoded = encode(value)
            .unwrap()
            .try_fold(Vec::new(), |mut buffer, chunk| {
                buffer.extend_from_slice(&chunk);
                future::ready(Ok(buffer))
            })
            .await
            .unwrap();

        assert!(encoded.starts_with(b"{"));
        assert!(encoded.ends_with(b"}"));
    }

    async fn test_encode_list<
        'en,
        T: IntoStream<'en> + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
    >(
        seq: S,
        expected: &str,
    ) {
        test_encode(encode_seq(seq), expected).await;
    }

    async fn test_encode_map<
        'en,
        K: IntoStream<'en> + 'en,
        V: IntoStream<'en> + 'en,
        S: Stream<Item = (K, V)> + Send + Unpin + 'en,
    >(
        map: S,
        expected: &str,
    ) {
        test_encode(encode_map(map), expected).await;
    }

    async fn roundtrip<T>(value: T)
    where
        T: FromStream<Context = ()> + PartialEq + fmt::Debug,
        for<'en> T: destream::en::ToStream<'en>,
    {
        let encoded = encode(&value).unwrap();
        let decoded: T = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, value);
    }

    async fn assert_decode_fails<T: FromStream<Context = ()>>(encoded: &str) {
        for chunk_size in 1..=encoded.len().max(1).min(8) {
            let source = stream::iter(encoded.as_bytes().iter().copied())
                .chunks(chunk_size)
                .map(Bytes::from);

            let result: Result<T, _> = decode((), source).await;
            assert!(result.is_err(), "expected decode to fail, but succeeded");
        }
    }

    async fn decode_from_str_chunks<T: FromStream<Context = ()>>(
        chunks: &[&str],
    ) -> Result<T, super::de::Error> {
        let source =
            stream::iter(chunks.iter().copied()).map(|s| Bytes::from(s.as_bytes().to_vec()));

        decode((), source).await
    }

    #[tokio::test]
    async fn test_truncated_inputs() {
        let encoded = "true";
        test_decode(encoded, true).await;
        for i in 0..encoded.len() {
            assert_decode_fails::<bool>(&encoded[..i]).await;
        }

        let encoded = "\"hello world\"";
        test_decode(encoded, "hello world".to_string()).await;
        for i in 0..encoded.len() {
            assert_decode_fails::<String>(&encoded[..i]).await;
        }

        let encoded = "[1,2,3]";
        test_decode(encoded, vec![1u8, 2, 3]).await;
        for i in 0..encoded.len() {
            assert_decode_fails::<Vec<u8>>(&encoded[..i]).await;
        }

        let encoded = "{\"a\":1,\"b\":2}";
        let expected =
            HashMap::<String, u8>::from_iter([("a".to_string(), 1u8), ("b".to_string(), 2u8)]);
        test_decode(encoded, expected).await;
        for i in 0..encoded.len() {
            assert_decode_fails::<HashMap<String, u8>>(&encoded[..i]).await;
        }
    }

    #[tokio::test]
    async fn test_default_impl_roundtrips() {
        roundtrip(()).await;

        roundtrip(true).await;
        roundtrip(false).await;

        roundtrip(1u8).await;
        roundtrip(65_535u16).await;
        roundtrip(1_000_000u32).await;
        roundtrip(9_223_372_036_854_775_808u64).await;

        roundtrip(-1i8).await;
        roundtrip(-32_000i16).await;
        roundtrip(1_000_000i32).await;
        roundtrip(-9_000_000_000_000_000_000i64).await;

        roundtrip(3.25f32).await;
        roundtrip(-14140.0f64).await;

        roundtrip("hello world".to_string()).await;
        roundtrip("string \"within\" string".to_string()).await;

        roundtrip(Some(123u8)).await;
        roundtrip::<Option<u8>>(None).await;

        roundtrip(vec![1i32, 2, 3, 4]).await;
        roundtrip(VecDeque::from([1u8, 2, 3, 4])).await;
        roundtrip(LinkedList::from(["a".to_string(), "b".to_string()])).await;

        let array = [1u8, 2, 3, 4];
        let array_ref: &[u8; 4] = &array;
        let encoded = encode(&array_ref).unwrap();
        let decoded: [u8; 4] = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, array);
        roundtrip((true, 7u8, "x".to_string(), None::<i32>)).await;

        let map: HashMap<String, i32> =
            HashMap::from_iter([("a".to_string(), 1i32), ("b".to_string(), -2i32)]);
        roundtrip(map).await;

        let map = BTreeMap::from_iter([("a".to_string(), true), ("b".to_string(), false)]);
        roundtrip(map).await;

        roundtrip(HashSet::from([1u8, 2u8, 3u8])).await;
        roundtrip(BTreeSet::from([1u8, 2u8, 3u8])).await;

        // This is a non-standard JSON extension, but it's a useful default impl
        // conformance check: `HashMap<K, V>` roundtrips even when `K` is not `String`.
        roundtrip(HashMap::<u8, u8>::from_iter([(1u8, 2u8), (3u8, 4u8)])).await;

        roundtrip(i128::MAX).await;
        roundtrip(u128::MAX).await;
        roundtrip(NonZeroI128::new(-5_i128).unwrap()).await;
        roundtrip(NonZeroU128::new(5_u128).unwrap()).await;

        roundtrip(Duration::new(5, 7)).await;

        roundtrip(Ipv4Addr::new(127, 0, 0, 1)).await;
        roundtrip(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)).await;
        roundtrip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await;
        roundtrip(SocketAddr::from((Ipv4Addr::new(127, 0, 0, 1), 80))).await;

        // IgnoredAny must be able to consume any JSON value.
        let source = stream::iter(br#"{"a":[1,2,{"b":true}],"c":null}"#.iter().copied())
            .chunks(3)
            .map(Bytes::from);
        let _: IgnoredAny = decode((), source).await.unwrap();
    }

    #[tokio::test]
    async fn test_decode_string_escapes() {
        test_decode("\"a\\nb\"", "a\nb".to_string()).await;
        test_decode("\"a\\tb\"", "a\tb".to_string()).await;
        test_decode("\"a\\rb\"", "a\rb".to_string()).await;
        test_decode("\"a\\bb\"", format!("a{}b", '\u{0008}')).await;
        test_decode("\"a\\fb\"", format!("a{}b", '\u{000C}')).await;
        test_decode("\"\\\"\\\\\"", "\"\\".to_string()).await;
        test_decode("\"\\/\"", "/".to_string()).await;
    }

    #[tokio::test]
    async fn test_decode_string_unicode_escapes() {
        test_decode("\"\\u0041\"", "A".to_string()).await;
        test_decode("\"\\u03BB\"", "λ".to_string()).await;
    }

    #[tokio::test]
    async fn test_decode_string_surrogate_pairs() {
        test_decode("\"\\uD83D\\uDCA1\"", "💡".to_string()).await;
    }

    #[tokio::test]
    async fn test_decode_reject_unescaped_control_chars() {
        assert_decode_fails::<String>("\"a\nb\"").await;
        assert_decode_fails::<String>("\"a\rb\"").await;
        assert_decode_fails::<String>("\"a\tb\"").await;
    }

    #[tokio::test]
    async fn test_decode_reject_invalid_string_escapes() {
        assert_decode_fails::<String>("\"\\x\"").await;
        assert_decode_fails::<String>("\"\\u12\"").await;
        assert_decode_fails::<String>("\"\\uD800\"").await; // missing low surrogate
        assert_decode_fails::<String>("\"\\uD800x\"").await; // missing escape for low surrogate
        assert_decode_fails::<String>("\"\\uDC00\"").await; // low surrogate without high surrogate
    }

    #[tokio::test]
    async fn test_decode_reject_non_json_whitespace() {
        assert_decode_fails::<bool>("\u{000B}true").await; // vertical tab
        assert_decode_fails::<bool>("\u{000C}true").await; // form feed
    }

    #[tokio::test]
    async fn test_decode_reject_leading_zero_numbers() {
        assert_decode_fails::<u64>("01").await;
        assert_decode_fails::<i64>("-01").await;
    }

    #[tokio::test]
    async fn test_decode_string_escapes_across_chunk_boundaries() {
        let actual: String = decode_from_str_chunks(&["\"a\\", "nb\""])
            .await
            .expect("decode across chunk boundary");
        assert_eq!(actual, "a\nb");

        let actual: String = decode_from_str_chunks(&["\"\\u", "00", "41\""])
            .await
            .expect("decode unicode escape across chunk boundary");
        assert_eq!(actual, "A");

        let actual: String = decode_from_str_chunks(&["\"\\uD83D\\u", "DCA1\""])
            .await
            .expect("decode surrogate pair across chunk boundary");
        assert_eq!(actual, "💡");
    }

    #[tokio::test]
    async fn test_encode_string_escapes() {
        test_encode_value("a\nb".to_string(), "\"a\\nb\"").await;
        test_encode_value("a\tb".to_string(), "\"a\\tb\"").await;
        test_encode_value("a\rb".to_string(), "\"a\\rb\"").await;
        test_encode_value(format!("a{}b", '\u{0008}'), "\"a\\bb\"").await;
        test_encode_value(format!("a{}b", '\u{000C}'), "\"a\\fb\"").await;
        test_encode_value("\"\\", "\"\\\"\\\\\"").await;
        test_encode_value(format!("a{}b", '\u{0001}'), "\"a\\u0001b\"").await;
    }

    #[tokio::test]
    async fn test_decode_ignored_any_deep_nesting() {
        use destream::Visitor;

        struct Ignore;
        struct IgnoreVisitor;

        impl Visitor for IgnoreVisitor {
            type Value = Ignore;
            fn expecting() -> &'static str {
                "any json to be ignored"
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Ignore)
            }
        }

        impl FromStream for Ignore {
            type Context = ();
            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                decoder.decode_ignored_any(IgnoreVisitor).await
            }
        }

        let depth = 2048;
        let mut encoded = String::with_capacity(depth * 2 + 1);
        encoded.extend(std::iter::repeat('[').take(depth));
        encoded.push('0');
        encoded.extend(std::iter::repeat(']').take(depth));

        let source = stream::iter(encoded.as_bytes().iter().copied())
            .chunks(1)
            .map(Bytes::from);

        let _: Ignore = decode_with_max_depth((), source, depth + 1).await.unwrap();
    }

    #[tokio::test]
    async fn test_decode_reject_too_deep_nesting() {
        let depth = 1025;
        let mut encoded = String::with_capacity(depth * 2 + 1);
        encoded.extend(std::iter::repeat('[').take(depth));
        encoded.push('0');
        encoded.extend(std::iter::repeat(']').take(depth));

        let source = stream::iter(encoded.as_bytes().iter().copied())
            .chunks(1)
            .map(Bytes::from);

        let result: Result<u64, _> = decode((), source).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_extended_default_impl_numeric_tokens() {
        test_decode("123", 123_i128).await;
        test_decode("-123", -123_i128).await;
        test_decode("123", 123_u128).await;

        // Larger-than-`u64` numeric tokens must fail (JSON numbers are not self-describing beyond
        // the decoder's native numeric range).
        assert_decode_fails::<u128>("18446744073709551616").await; // u64::MAX + 1

        // Very large numbers are parsed as floats and should not decode into an integer type.
        assert_decode_fails::<i128>("1e40").await;
    }

    #[tokio::test]
    async fn test_extended_default_impl_decode_errors() {
        assert_decode_fails::<u128>("-1").await;
        assert_decode_fails::<NonZeroU128>("0").await;
        assert_decode_fails::<NonZeroI128>("0").await;

        assert_decode_fails::<u128>(&format!("\"{}0\"", u128::MAX)).await;
        assert_decode_fails::<i128>(&format!("\"{}0\"", i128::MAX)).await;

        assert_decode_fails::<Duration>("[5]").await;
        assert_decode_fails::<Duration>("[5,\"7\"]").await;
        assert_decode_fails::<Duration>("[5,7,9]").await;
        assert_decode_fails::<Duration>(&format!("[5,{}]", 1_000_000_000_u64)).await;

        assert_decode_fails::<Ipv4Addr>("\"999.0.0.1\"").await;
        assert_decode_fails::<Ipv6Addr>("\"not an ip\"").await;
        assert_decode_fails::<IpAddr>("\"not an ip\"").await;
        assert_decode_fails::<SocketAddr>("\"127.0.0.1\"").await;
    }

    #[tokio::test]
    async fn test_json_primitives() {
        test_decode("null", ()).await;

        test_decode("true", true).await;
        test_decode("false", false).await;

        test_encode_value(true, "true").await;
        test_encode_value(false, "false").await;

        test_decode("1", 1u8).await;
        test_decode(" 2 ", 2u16).await;
        test_decode("4658 ", 4658_u32).await;
        test_decode(&2u64.pow(63).to_string(), 2u64.pow(63)).await;

        test_encode_value(1u8, "1").await;
        test_encode_value(2u16, "2").await;
        test_encode_value(4658_u32, "4658").await;
        test_encode_value(2u64.pow(63), &2u64.pow(63).to_string()).await;

        test_decode("-1", -1i8).await;
        test_decode("\t\n-32", -32i16).await;
        test_decode("53\t", 53i32).await;
        test_decode(&(-2i64).pow(63).to_string(), (-2i64).pow(63)).await;

        test_encode_value(-1i8, "-1").await;
        test_encode_value(-32i16, "-32").await;
        test_encode_value(53i32, "53").await;
        test_encode_value((-2i64).pow(63), &(-2i64).pow(63).to_string()).await;

        test_decode("1e-6", 1e-6).await;
        test_decode("2e2", 2e2_f32).await;
        test_decode("-2e-3", -2e-3_f64).await;
        // This is a literal value under test; a named constant would be less readable here.
        #[allow(clippy::approx_constant)]
        test_decode("3.14", 3.14_f32).await;
        test_decode("-1.414e4", -1.414e4_f64).await;

        test_encode_value(2e2_f32, "200").await;
        test_encode_value(-2e3, "-2000").await;
        // This is a literal value under test; a named constant would be less readable here.
        #[allow(clippy::approx_constant)]
        test_encode_value(3.14_f32, "3.14").await;
        test_encode_value(-1.414e4_f64, "-14140").await;

        test_decode("\t\r\n\" hello world \"", " hello world ".to_string()).await;
        test_encode_value("hello world", "\"hello world\"").await;

        let nested = "string \"within\" string".to_string();
        let expected = "\"string \\\"within\\\" string\"";
        test_encode_value(nested.clone(), expected).await;
        test_decode(expected, nested).await;

        let terminal = "ends in a \\".to_string();
        let expected = "\"ends in a \\\\\"";
        test_encode_value(terminal.clone(), expected).await;
        test_decode(expected, terminal).await;
    }

    #[tokio::test]
    async fn test_array() {
        #[derive(PartialEq)]
        struct TestArray {
            data: Vec<f64>,
        }

        struct TestVisitor;

        impl destream::de::Visitor for TestVisitor {
            type Value = TestArray;

            fn expecting() -> &'static str {
                "a TestArray"
            }

            async fn visit_array_f64<A: ArrayAccess<f64>>(
                self,
                mut array: A,
            ) -> Result<Self::Value, A::Error> {
                let mut data = Vec::with_capacity(3);
                let mut buffer = [0.; 100];
                loop {
                    let num_items = array.buffer(&mut buffer).await?;
                    if num_items > 0 {
                        data.extend(&buffer[..num_items]);
                    } else {
                        break;
                    }
                }

                Ok(TestArray { data })
            }
        }

        impl FromStream for TestArray {
            type Context = ();

            async fn from_stream<D: destream::de::Decoder>(
                _: (),
                decoder: &mut D,
            ) -> Result<Self, D::Error> {
                decoder.decode_array_f64(TestVisitor).await
            }
        }

        impl<'en> destream::en::ToStream<'en> for TestArray {
            fn to_stream<E: destream::en::Encoder<'en>>(
                &'en self,
                encoder: E,
            ) -> Result<E::Ok, E::Error> {
                encoder.encode_array_f64(stream::once(future::ready(self.data.clone())))
            }
        }

        impl fmt::Debug for TestArray {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                fmt::Debug::fmt(&self.data, f)
            }
        }

        let test = TestArray {
            data: vec![1e-6, 123.4, 3f64],
        };

        let mut encoded = encode(&test).unwrap();
        let mut buf = Vec::new();
        while let Some(chunk) = encoded.try_next().await.unwrap() {
            buf.extend(chunk.to_vec());
        }

        let encoded = String::from_utf8(buf).unwrap();
        assert_eq!(&encoded, "[0.000001,123.4,3]");

        let decoded: TestArray = decode(
            (),
            stream::once(future::ready(Bytes::copy_from_slice(encoded.as_bytes()))),
        )
        .await
        .unwrap();

        assert_eq!(test, decoded);
    }

    #[tokio::test]
    async fn test_seq() {
        test_encode_list(stream::empty::<u8>(), "[]").await;

        test_decode("[1, 2, 3]", vec![1, 2, 3]).await;
        test_encode_value(&[1u8, 2u8, 3u8], "[1,2,3]").await;

        test_decode("[1, 2, null]", (1, 2, ())).await;
        test_encode_value((1u8, 2u8, ()), "[1,2,null]").await;

        test_encode_list(stream::iter(&[1u8, 2u8, 3u8]), "[1,2,3]").await;
        test_encode_list(
            stream::iter(vec![vec![1, 2, 3], vec![], vec![4]]),
            "[[1,2,3],[],[4]]",
        )
        .await;

        test_decode(
            "\t[\r\n\rtrue,\r\n\t-1,\r\n\t\"hello world. \"\r\n]",
            (true, -1i16, "hello world. ".to_string()),
        )
        .await;
        test_encode_value(
            (true, -1i16, "hello world. "),
            "[true,-1,\"hello world. \"]",
        )
        .await;
        test_encode_list(
            stream::iter(vec!["hello ", "\tworld"]),
            "[\"hello \",\"\\tworld\"]",
        )
        .await;

        test_decode(
            " [ 10e-06, 1.23, 4e-3, -3.45]\n",
            [10e-6, 1.23, 4e-3, -3.45],
        )
        .await;
        test_encode_value(&[10e-6, 1.23, 4e-3, -3.45], "[0.00001,1.23,0.004,-3.45]").await;

        test_decode(
            "[\"one\", \"two\", \"three\"]",
            HashSet::<String>::from_iter(vec!["one", "two", "three"].into_iter().map(String::from)),
        )
        .await;
        test_encode_value(&["one", "two", "three"], "[\"one\",\"two\",\"three\"]").await;
    }

    #[tokio::test]
    async fn test_map() {
        let mut map = HashMap::<String, bool>::from_iter(vec![
            ("k1".to_string(), true),
            ("k2".to_string(), false),
        ]);

        test_decode("\r\n\t{ \"k1\":\ttrue  , \"k2\": false\r\n}", map.clone()).await;

        map.remove("k2");
        test_encode_value(map.clone(), "{\"k1\":true}").await;
        test_encode_map(stream::iter(map), "{\"k1\":true}").await;

        let map = BTreeMap::<i8, Option<bool>>::from_iter(vec![(-1, Some(true)), (2, None)]);

        test_decode("\r\n\t{ -1:\ttrue, 2:null}", map.clone()).await;
        test_encode_value(map.clone(), "{-1:true,2:null}").await;
        test_encode_map(stream::iter(map), "{-1:true,2:null}").await;
    }

    #[cfg(feature = "value")]
    #[tokio::test]
    async fn test_generic_value() {
        use crate::Value;
        use std::iter;

        let expected = Value::List(vec![
            Value::List(vec![
                Value::String("baz".to_string()),
                Value::Map(HashMap::from_iter(iter::once((
                    "spam".to_string(),
                    Value::Map(HashMap::new()),
                )))),
                Value::Number(100u64.into()),
            ]),
            Value::List(vec![
                Value::String("foo".to_string()),
                Value::Map(HashMap::from_iter(iter::once((
                    "bar".to_string(),
                    Value::List(vec![
                        Value::Number(true.into()),
                        Value::Number(false.into()),
                    ]),
                )))),
            ]),
        ]);

        test_decode(
            "[[\"baz\", {\"spam\": {}}, 100], [\"foo\", {\"bar\": [true, false]}]]",
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn test_err() {
        #[derive(Debug, Default, Eq, PartialEq)]
        struct TestMap;

        impl FromStream for TestMap {
            type Context = ();

            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                decoder.decode_map(TestVisitor::<Self>::default()).await
            }
        }

        #[derive(Debug, Default, Eq, PartialEq)]
        struct TestSeq;

        impl FromStream for TestSeq {
            type Context = ();

            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                decoder.decode_seq(TestVisitor::<Self>::default()).await
            }
        }

        #[derive(Default)]
        struct TestVisitor<T> {
            phantom: PhantomData<T>,
        }

        impl<T: Default + Send> de::Visitor for TestVisitor<T> {
            type Value = T;

            fn expecting() -> &'static str {
                "a Test struct"
            }

            async fn visit_map<A: de::MapAccess>(self, mut access: A) -> Result<T, A::Error> {
                let _key = access.next_key::<String>(()).await?;

                assert!(access.next_value::<String>(()).await.is_err());
                assert!(access.next_value::<Vec<i64>>(()).await.is_ok());

                Ok(T::default())
            }

            async fn visit_seq<A: de::SeqAccess>(self, mut access: A) -> Result<T, A::Error> {
                assert!(access.next_element::<String>(()).await.is_err());
                assert!(access.next_element::<Vec<i64>>(()).await.is_err());
                assert!(access.next_element::<i64>(()).await.is_ok());
                assert!(access.next_element::<i64>(()).await.is_ok());
                assert!(access.next_element::<i64>(()).await.is_ok());

                Ok(T::default())
            }
        }

        let encoded = "{\"k1\": [1, 2, 3]}";
        let source = stream::iter(encoded.as_bytes().iter().copied())
            .chunks(5)
            .map(Bytes::from);

        let actual: TestMap = decode((), source).await.unwrap();
        assert_eq!(actual, TestMap);

        let encoded = "\t[ 1,2, 3]";
        let source = stream::iter(encoded.as_bytes().iter().copied())
            .chunks(2)
            .map(Bytes::from);

        let actual: TestSeq = decode((), source).await.unwrap();
        assert_eq!(actual, TestSeq);
    }

    #[cfg(feature = "value")]
    #[tokio::test]
    async fn test_ignored_any() {
        enum IgnoredValue {
            None,
        }

        impl FromStream for IgnoredValue {
            type Context = ();
            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                use destream::Visitor;
                struct IgnoredVisitor;
                impl Visitor for IgnoredVisitor {
                    type Value = IgnoredValue;
                    fn expecting() -> &'static str {
                        "any json to be ignored"
                    }
                    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                        Ok(Self::Value::None)
                    }
                }
                decoder.decode_ignored_any(IgnoredVisitor).await
            }
        }

        let encoded = r#"{
            "string": "Hello, world!",
            "number": 42,
            "boolean": true,
            "array": [1, 2, 3],
            "object": {"key": "value"},
            "null_value": null,
            "nested_object": {
              "nested_string": "Nested string",
              "nested_number": 3.14,
              "nested_boolean": false,
              "nested_array": ["apple", "banana", "orange"],
              "nested_null": null
            },
            "unicode_characters": "💡🌟🔑",
            "empty_array": [],
            "empty_object": {},
            "multiline_string": "This is a\nmultiline\nstring.",
            "escaped_characters": "Escaped characters: \" \\ \/ \b \f \n \r \t \u1234"
          }"#;

        let source = stream::iter(encoded.as_bytes().iter().copied())
            .chunks(2)
            .map(Bytes::from);

        let _: IgnoredValue = decode((), source).await.unwrap();
    }

    #[cfg(feature = "value")]
    #[tokio::test]
    async fn test_complex_list_with_err() {
        use crate::Value;
        use destream::de::Visitor;
        use futures::TryFutureExt;

        #[derive(Eq, PartialEq)]
        struct Class {
            name: String,
        }

        impl FromStream for Class {
            type Context = ();

            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                decoder.decode_any(ClassVisitor).await
            }
        }

        impl fmt::Debug for Class {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "class: {}", self.name)
            }
        }

        struct ClassVisitor;

        impl Visitor for ClassVisitor {
            type Value = Class;

            fn expecting() -> &'static str {
                "a Class"
            }

            async fn visit_map<A: destream::de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let name = map.next_key(()).await?;
                let name = name.unwrap();

                if let Ok(list) = map
                    .next_value::<Vec<Value>>(())
                    .inspect_err(|err| println!("list error is {}", err))
                    .await
                {
                    if list.is_empty() {
                        Ok(Class { name })
                    } else {
                        Err(de::Error::invalid_value("list", "empty list"))
                    }
                } else if let Ok(map) = map
                    .next_value::<HashMap<String, Value>>(())
                    .inspect_err(|err| println!("map error is {}", err))
                    .await
                {
                    if map.is_empty() {
                        Ok(Class { name })
                    } else {
                        Err(de::Error::invalid_value("map", "empty map"))
                    }
                } else {
                    Err(de::Error::invalid_length(0, Self::expecting()))
                }
            }
        }

        #[derive(Eq, PartialEq)]
        struct Entry {
            name: String,
            class: Class,
            len: Option<usize>,
        }

        impl Entry {
            fn new<C: fmt::Display, N: fmt::Display>(name: N, class: C) -> Self {
                Self {
                    name: name.to_string(),
                    class: Class {
                        name: class.to_string(),
                    },
                    len: None,
                }
            }

            fn with_len<C: fmt::Display, N: fmt::Display>(name: N, class: C, len: usize) -> Self {
                Self {
                    name: name.to_string(),
                    class: Class {
                        name: class.to_string(),
                    },
                    len: Some(len),
                }
            }
        }

        impl FromStream for Entry {
            type Context = ();

            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                decoder.decode_seq(EntryVisitor).await
            }
        }

        impl fmt::Debug for Entry {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "entry: {} {} {:?}", self.name, self.class.name, self.len)
            }
        }

        struct EntryVisitor;

        impl Visitor for EntryVisitor {
            type Value = Entry;

            fn expecting() -> &'static str {
                "an Entry"
            }

            async fn visit_seq<A: destream::de::SeqAccess>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let name = seq.next_element(()).await?;
                let name = name.unwrap();

                let class = seq.next_element(()).await?;
                let class = class.unwrap();

                let len = seq.next_element(()).await?;

                Ok(Entry { name, class, len })
            }
        }

        let expected = Class {
            name: "one".to_string(),
        };
        test_decode("{\"one\": {}}", expected).await;

        let expected = Entry::new("two", "class two");
        test_decode("[\"two\", {\"class two\": {}}]", expected).await;

        let expected = vec![Entry::with_len("one", "class one", 1)];
        test_decode("[[\"one\", {\"class one\": {}}, 1]]", expected).await;

        let expected = vec![
            Entry::with_len("one", "class one", 1),
            Entry::new("two", "class two"),
        ];
        test_decode(
            "[[\"one\", {\"class one\": {}}, 1], [\"two\", {\"class two\": {}}]]",
            expected,
        )
        .await
    }

    #[cfg(feature = "tokio-io")]
    #[tokio::test]
    async fn test_async_read() {
        use std::io::Cursor;

        let encoded = "[\"hello\", 1, {}]";
        let cursor = Cursor::new(encoded.as_bytes());
        let decoded: (String, i64, HashMap<String, bool>) = read_from((), cursor).await.unwrap();

        assert_eq!(
            decoded,
            ("hello".to_string(), 1i64, HashMap::<String, bool>::new())
        );
    }

    #[cfg(feature = "tokio-io")]
    #[tokio::test]
    async fn test_async_read_trailing_bytes_error() {
        use std::io::Cursor;

        let encoded = "[\"hello\", 1, {}] true";
        let cursor = Cursor::new(encoded.as_bytes());
        let decoded: Result<(String, i64, HashMap<String, bool>), _> = read_from((), cursor).await;
        assert!(decoded.is_err());
    }

    #[cfg(feature = "tokio-io")]
    #[tokio::test]
    async fn test_async_write() {
        use std::io;
        use std::path::PathBuf;

        use tokio_util::io::StreamReader;

        let mut value = HashMap::new();
        value.insert("one".to_string(), Some(1.0_f64));
        value.insert("two".to_string(), None);
        value.insert("three".to_string(), Some(std::f64::consts::PI));

        let path = PathBuf::from(".tmp");

        let encoded = encode(&value).unwrap();
        let mut reader =
            StreamReader::new(encoded.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)));

        {
            let mut file = tokio::fs::File::create(&path).await.unwrap();
            tokio::io::copy(&mut reader, &mut file).await.unwrap();
        }

        let file = tokio::fs::File::open(path).await.unwrap();
        let actual = read_from((), file).await.unwrap();
        assert_eq!(value, actual);
    }
}
