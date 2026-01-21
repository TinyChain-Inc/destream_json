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
//! Deviations from the [JSON spec](https://www.json.org/):
//!  - `destream_json` will not error out if asked to decode or encode a non-string key in a JSON
//!    object (i.e., it supports a superset of the official JSON spec). This may cause issues
//!    when using another JSON library to decode a stream encoded by `destream_json`. This behavior
//!    can be altered by using only strings as keys, or adding an explicit check at encoding time.

#![allow(async_fn_in_trait)]

pub use de::{decode, try_decode};
pub use en::{encode, encode_map, encode_seq};

#[cfg(feature = "value")]
pub use value::Value;

#[cfg(feature = "tokio-io")]
pub use de::read_from;

mod constants;
pub mod de;
pub mod en;

#[cfg(feature = "value")]
mod value;

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::fmt;
    use std::marker::PhantomData;

    use bytes::Bytes;
    use destream::de::{self, ArrayAccess, FromStream};
    use destream::en::IntoStream;
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

    async fn assert_decode_fails<T: FromStream<Context = ()>>(encoded: &str) {
        for chunk_size in 1..=encoded.len().max(1).min(8) {
            let source = stream::iter(encoded.as_bytes().iter().copied())
                .chunks(chunk_size)
                .map(Bytes::from);

            let result: Result<T, _> = decode((), source).await;
            assert!(result.is_err(), "expected decode to fail, but succeeded");
        }
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
        #[allow(clippy::approx_constant)]
        test_decode("3.14", 3.14_f32).await;
        test_decode("-1.414e4", -1.414e4_f64).await;

        test_encode_value(2e2_f32, "200").await;
        test_encode_value(-2e3, "-2000").await;
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
            "[\"hello \",\"\tworld\"]",
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
    async fn test_async_write() {
        use std::io;
        use std::path::PathBuf;

        use number_general::Number;
        use tokio_util::io::StreamReader;

        let mut value = HashMap::new();
        value.insert("one".to_string(), Some(Number::from(1)));
        value.insert("two".to_string(), None);
        value.insert(
            "three".to_string(),
            Some(Number::from(std::f32::consts::PI)),
        );

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
