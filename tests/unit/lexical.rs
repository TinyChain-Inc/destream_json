use bytes::Bytes;
use futures::executor::block_on;
use futures::{stream, TryStreamExt};

#[test]
fn duration_sequences_require_exact_completion() {
    use std::time::Duration;

    block_on(async {
        for chunk in [1, 2, 7] {
            let source = |wire: &[u8]| {
                stream::iter(
                    wire.chunks(chunk)
                        .map(Bytes::copy_from_slice)
                        .collect::<Vec<_>>(),
                )
            };
            let expected = vec![Duration::new(5, 7)];
            let encoded: Vec<Bytes> = crate::en::encode(&expected)
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let actual: Vec<Duration> = crate::de::decode((), stream::iter(encoded)).await.unwrap();
            assert_eq!(actual, expected);

            for wire in [b"[5,7".as_slice(), b"[5,7,", b"[5,7,9]"] {
                assert!(crate::de::decode::<_, Duration>((), source(wire))
                    .await
                    .is_err());
            }
            assert!(
                crate::de::decode::<_, Vec<Duration>>((), source(b"[[5,7,]"))
                    .await
                    .is_err()
            );

            let baseline = crate::de::decode::<_, Duration>((), source(b"[5,1000000000]"))
                .await
                .unwrap_err();
            let extra = crate::de::decode::<_, Duration>((), source(b"[5,1000000000,9]"))
                .await
                .unwrap_err();
            assert_eq!(extra.to_string(), baseline.to_string());
        }
    });
}

#[test]
fn json_whitespace_matches_the_lexical_grammar() {
    block_on(async {
        for (whitespace, valid) in [
            (b' ', true),
            (b'\t', true),
            (b'\n', true),
            (b'\r', true),
            (0x00, false),
            (0x0b, false),
            (0x0c, false),
        ] {
            for wire in [vec![whitespace, b'7'], vec![b'7', whitespace]] {
                let result = crate::de::decode::<_, u64>(
                    (),
                    stream::iter(wire.into_iter().map(|byte| Bytes::from(vec![byte]))),
                )
                .await;
                assert_eq!(result.is_ok(), valid);
                if let Ok(value) = result {
                    assert_eq!(value, 7);
                }
            }
            let result = crate::de::decode::<_, Vec<u64>>(
                (),
                stream::iter([Bytes::from(vec![b'[', b'7', b',', whitespace, b'7', b']'])]),
            )
            .await;
            assert_eq!(result.is_ok(), valid);
            if let Ok(values) = result {
                assert_eq!(values, [7, 7]);
            }
        }
    });
}

#[test]
fn json_escapes_unicode_and_numeric_grammar() {
    block_on(async {
        for chunk in [1, 2, 7, 4096] {
            for (wire, expected) in [
                (r#""a\n\t\r\b\f\\\"\/""#, "a\n\t\r\u{8}\u{c}\\\"/"),
                (r#""\u03bb\uD83D\uDE00""#, "λ😀"),
                ("\"λ😀\"", "λ😀"),
            ] {
                let source =
                    stream::iter(wire.as_bytes().chunks(chunk).map(Bytes::copy_from_slice));
                let actual: String = crate::de::decode((), source).await.unwrap();
                assert_eq!(actual, expected);
            }
            for wire in [
                r#""\x00""#,
                r#""\uD800""#,
                r#""\uDC00""#,
                "\"\n\"",
                "\"unfinished",
            ] {
                let source =
                    stream::iter(wire.as_bytes().chunks(chunk).map(Bytes::copy_from_slice));
                assert!(crate::de::decode::<_, String>((), source).await.is_err());
            }
        }
        for value in ["\0\u{1}\n\r\t\u{1f}", "λ😀\\\""] {
            let chunks: Vec<Bytes> = crate::en::encode(value)
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let actual: String = crate::de::decode((), stream::iter(chunks)).await.unwrap();
            assert_eq!(actual, value);
        }
        for wire in ["01", "+1", "1.", ".1", "1e", "1e+", "--1"] {
            assert!(crate::de::decode::<_, f64>(
                (),
                stream::iter([Bytes::copy_from_slice(wire.as_bytes())])
            )
            .await
            .is_err());
        }
        let value: f64 = crate::de::decode((), stream::iter([Bytes::from_static(b"1E+3")]))
            .await
            .unwrap();
        assert_eq!(value, 1000.0);
        let value: number_general::Number =
            crate::de::decode((), stream::iter([Bytes::from(u64::MAX.to_string())]))
                .await
                .unwrap();
        assert_eq!(value, number_general::Number::from(u64::MAX));
        let value: Vec<i8> =
            crate::de::decode((), stream::iter([Bytes::from_static(b"[-128,0,127]")]))
                .await
                .unwrap();
        assert_eq!(value, [-128, 0, 127]);
        assert!(crate::de::decode::<_, [u8; 2]>(
            (),
            stream::iter([Bytes::from_static(b"[1,2,3]")])
        )
        .await
        .is_err());
        assert!(crate::de::decode::<_, (u8, u8)>(
            (),
            stream::iter([Bytes::from_static(b"[1,2,3]")])
        )
        .await
        .is_err());
    });
}

#[test]
fn typed_signed_arrays_dispatch_to_the_signed_visitor() {
    struct Signed;

    impl destream::Visitor for Signed {
        type Value = Vec<i8>;

        fn expecting() -> &'static str {
            "signed-byte array"
        }

        async fn visit_array_i8<A: destream::de::ArrayAccess<i8>>(
            self,
            mut access: A,
        ) -> Result<Self::Value, A::Error> {
            let mut output = Vec::new();
            let mut buffer = [0i8; 2];
            loop {
                let len = access.buffer(&mut buffer).await?;
                if len == 0 {
                    return Ok(output);
                }
                output.extend_from_slice(&buffer[..len]);
            }
        }
    }
    block_on(async {
        let source = stream::iter(
            b"[-128,0,127]"
                .iter()
                .map(|&b| Ok::<_, crate::de::Error>(Bytes::from(vec![b]))),
        );
        let mut decoder = crate::de::Decoder::from_stream(source);
        let values = destream::Decoder::decode_array_i8(&mut decoder, Signed)
            .await
            .unwrap();
        assert_eq!(values, [-128, 0, 127]);
    });
}
