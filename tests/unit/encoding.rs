use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use destream::en::{EncodeMap, Encoder, IntoStream, ToStream};
use futures::executor::block_on;
use futures::{stream, Stream, StreamExt, TryStreamExt};

struct Observed {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

struct RetriedMap;

impl<'en> IntoStream<'en> for RetriedMap {
    fn into_stream<E: Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        let mut map = encoder.encode_map(Some(1))?;
        map.encode_key("key")?;
        let value = Observed {
            calls: Arc::new(AtomicUsize::new(0)),
            fail: true,
        };
        assert!(map.encode_value(value).is_err());
        assert!(map.encode_key("other").is_err());
        map.encode_value(13u64)?;
        map.end()
    }
}

impl Observed {
    fn encode<'en, E: Encoder<'en>>(&self, encoder: E) -> Result<E::Ok, E::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            Err(destream::en::Error::custom("fixture child error"))
        } else {
            encoder.encode_u64(13)
        }
    }
}

impl<'en> IntoStream<'en> for Observed {
    fn into_stream<E: Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        self.encode(encoder)
    }
}

impl<'en> ToStream<'en> for Observed {
    fn to_stream<E: Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        self.encode(encoder)
    }
}

fn observed(width: usize, calls: &Arc<AtomicUsize>) -> Vec<Observed> {
    (0..width)
        .map(|_| Observed {
            calls: calls.clone(),
            fail: false,
        })
        .collect()
}

fn retained<S: Stream + Send + Unpin>(
    source: S,
) -> (impl Stream<Item = S::Item> + Unpin, Weak<()>) {
    let lease = Arc::new(());
    let observer = Arc::downgrade(&lease);
    let source = source.map(move |item| {
        let _lease = &lease;
        item
    });
    (source, observer)
}

async fn encoded<S>(source: S) -> Vec<u8>
where
    S: Stream<Item = Result<Bytes, crate::en::Error>> + Unpin,
{
    source
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap()
}

async fn partial<S>(mut source: S, calls: &AtomicUsize, observer: Weak<()>, first: &[u8])
where
    S: Stream<Item = Result<Bytes, crate::en::Error>> + Unpin,
{
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(observer.upgrade().is_some());
    loop {
        let chunk = source.try_next().await.unwrap().expect("first child value");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        if chunk.as_ref() == first {
            break;
        }
    }
    drop(source);
    assert!(observer.upgrade().is_none());
}

#[test]
fn ordinary_children_are_lazy_and_release_on_drop() {
    block_on(async {
        let retried = encoded(crate::en::encode(RetriedMap).unwrap()).await;
        let expected = std::collections::BTreeMap::from([("key", 13u64)]);
        assert_eq!(retried, encoded(crate::en::encode(expected).unwrap()).await);
        assert_eq!(
            encoded(crate::en::encode((13u64, 13u64)).unwrap()).await,
            encoded(crate::en::encode(vec![13u64, 13u64]).unwrap()).await
        );
        let first = encoded(crate::en::encode(13u64).unwrap()).await;
        for width in [1, 4_097] {
            let calls = Arc::new(AtomicUsize::new(0));
            let values = observed(width, &calls);
            let (input, observer) = retained(stream::iter(values.iter()));
            partial(crate::en::encode_seq(input), &calls, observer, &first).await;
            calls.store(0, Ordering::Relaxed);
            let (input, observer) = retained(stream::iter(values));
            partial(crate::en::encode_seq(input), &calls, observer, &first).await;

            calls.store(0, Ordering::Relaxed);
            let values = observed(width, &calls);
            let (input, observer) = retained(stream::iter(values));
            drop(crate::en::encode_seq(input));
            assert_eq!(calls.load(Ordering::Relaxed), 0);
            assert!(observer.upgrade().is_none());

            let values = observed(width, &calls);
            let borrowed = encoded(crate::en::encode_seq(stream::iter(values.iter()))).await;
            assert_eq!(calls.load(Ordering::Relaxed), width);
            calls.store(0, Ordering::Relaxed);
            let owned = encoded(crate::en::encode_seq(stream::iter(values))).await;
            assert_eq!(calls.load(Ordering::Relaxed), width);
            assert_eq!(borrowed, owned);
            assert_eq!(
                owned,
                encoded(crate::en::encode(vec![13u64; width]).unwrap()).await
            );

            calls.store(0, Ordering::Relaxed);
            let entries: Vec<_> = observed(width, &calls)
                .into_iter()
                .enumerate()
                .map(|(index, value)| (format!("entry-{index}"), value))
                .collect();
            let (input, observer) = retained(stream::iter(
                entries.iter().map(|(key, value)| (key, value)),
            ));
            partial(crate::en::encode_map(input), &calls, observer, &first).await;
            calls.store(0, Ordering::Relaxed);
            let (input, observer) = retained(stream::iter(entries));
            partial(crate::en::encode_map(input), &calls, observer, &first).await;

            calls.store(0, Ordering::Relaxed);
            let entries: Vec<_> = observed(width, &calls)
                .into_iter()
                .enumerate()
                .map(|(index, value)| (format!("entry-{index}"), value))
                .collect();
            let (input, observer) = retained(stream::iter(entries));
            drop(crate::en::encode_map(input));
            assert_eq!(calls.load(Ordering::Relaxed), 0);
            assert!(observer.upgrade().is_none());

            calls.store(0, Ordering::Relaxed);
            let entries: Vec<_> = observed(width, &calls)
                .into_iter()
                .enumerate()
                .map(|(index, value)| (format!("entry-{index}"), value))
                .collect();
            let borrowed = encoded(crate::en::encode_map(stream::iter(
                entries.iter().map(|(key, value)| (key, value)),
            )))
            .await;
            assert_eq!(calls.load(Ordering::Relaxed), width);
            calls.store(0, Ordering::Relaxed);
            let owned = encoded(crate::en::encode_map(stream::iter(entries))).await;
            assert_eq!(calls.load(Ordering::Relaxed), width);
            assert_eq!(borrowed, owned);
        }
    });
}

struct PendingChild(Arc<()>);

impl<'en> IntoStream<'en> for PendingChild {
    fn into_stream<E: Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        let source = stream::pending::<u64>().map(move |value| {
            let _lease = &self.0;
            value
        });
        encoder.encode_seq_stream(source)
    }
}

#[test]
fn ordinary_child_errors_and_cancellation_release_ownership() {
    block_on(async {
        for map in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let values = [
                Observed {
                    calls: calls.clone(),
                    fail: false,
                },
                Observed {
                    calls: calls.clone(),
                    fail: true,
                },
            ];
            let (input, observer) = retained(stream::iter(values.iter()));
            let result = if map {
                crate::en::encode_map(input.enumerate().map(|(key, value)| (key as u64, value)))
                    .try_collect::<Vec<_>>()
                    .await
            } else {
                crate::en::encode_seq(input).try_collect::<Vec<_>>().await
            };
            assert!(result.is_err());
            assert_eq!(calls.load(Ordering::Relaxed), 2);
            assert!(observer.upgrade().is_none());
            calls.store(0, Ordering::Relaxed);
            let (input, observer) = retained(stream::iter(values));
            let result = if map {
                crate::en::encode_map(input.enumerate().map(|(key, value)| (key as u64, value)))
                    .try_collect::<Vec<_>>()
                    .await
            } else {
                crate::en::encode_seq(input).try_collect::<Vec<_>>().await
            };
            assert!(result.is_err());
            assert_eq!(calls.load(Ordering::Relaxed), 2);
            assert!(observer.upgrade().is_none());
        }
        let lease = Arc::new(());
        let observer = Arc::downgrade(&lease);
        let mut source = crate::en::encode_seq(stream::iter([PendingChild(lease)]));
        assert!(source.try_next().await.unwrap().is_some());
        {
            let next = source.next();
            futures::pin_mut!(next);
            assert!(futures::poll!(next).is_pending());
        }
        assert!(observer.upgrade().is_some());
        drop(source);
        assert!(observer.upgrade().is_none());
    });
}

const CASES: &[(&str, &[u8])] = &[
    ("", b"\"\""),
    ("ASCII/", b"\"ASCII/\""),
    ("\"\\", b"\"\\\"\\\\\""),
    ("\0\n\t\r\u{1f}", b"\"\\u0000\\u000a\\u0009\\u000d\\u001f\""),
    ("λ雪", "\"λ雪\"".as_bytes()),
];

const FRAGMENTED: &[u8] = "\"a\\\"\\\\\\u000aλ\"".as_bytes();

struct OwnedText(String);

impl AsRef<str> for OwnedText {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

struct FormattedText<'a> {
    parts: &'a [&'a str],
    fail: bool,
}

impl fmt::Display for FormattedText<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for part in self.parts {
            f.write_str(part)?;
        }

        if self.fail {
            Err(fmt::Error)
        } else {
            Ok(())
        }
    }
}

#[test]
fn string_inputs_and_formatted_values_preserve_wire_bytes() {
    block_on(async {
        async fn check<'en>(text: impl AsRef<str> + Send + 'en, expected: &[u8]) {
            assert_eq!(
                encoded(crate::en::Encoder.encode_str(text).unwrap()).await,
                expected
            );
        }

        for &(text, expected) in CASES {
            let borrowed = text.to_owned();
            check(borrowed.as_str(), expected).await;
            check(text.to_owned(), expected).await;
            check(Arc::<str>::from(text), expected).await;
            check(Box::<str>::from(text), expected).await;
            check(Cow::Borrowed(text), expected).await;
            check(Cow::<str>::Owned(text.to_owned()), expected).await;
            check(OwnedText(text.to_owned()), expected).await;
            assert_eq!(
                encoded(crate::en::Encoder.collect_str(text).unwrap()).await,
                expected
            );
        }

        let stream = {
            let formatted = FormattedText {
                parts: &["a\"", "\\", "\n", "λ"],
                fail: false,
            };

            crate::en::Encoder.collect_str(&formatted).unwrap()
        };

        assert_eq!(encoded(stream).await, FRAGMENTED);
    });

    let failed = FormattedText {
        parts: &["partial\"\\"],
        fail: true,
    };

    assert!(crate::en::Encoder.collect_str(&failed).is_err());
}

#[test]
fn base64_writes_padded_wire_data_for_bytes_and_iterators() {
    block_on(async {
        for (input, expected) in [
            (&b""[..], "\"\""),
            (&b"f"[..], "\"Zg==\""),
            (&b"fo"[..], "\"Zm8=\""),
            (&b"foo"[..], "\"Zm9v\""),
            (&b"foob"[..], "\"Zm9vYg==\""),
            (&b"fooba"[..], "\"Zm9vYmE=\""),
            (&b"foobar"[..], "\"Zm9vYmFy\""),
            (&[251, 255][..], "\"+/8=\""),
        ] {
            let bytes = Bytes::copy_from_slice(input);
            assert_eq!(
                encoded(crate::en::Encoder.encode_bytes(bytes).unwrap()).await,
                expected.as_bytes()
            );

            assert_eq!(
                encoded(
                    crate::en::Encoder
                        .collect_bytes(input.iter().copied())
                        .unwrap()
                )
                .await,
                expected.as_bytes()
            );
        }

        for len in [255, 256, 257, 4095, 4096, 4097] {
            let input = (0..len).map(|n| n as u8).collect::<Vec<_>>();
            let wire = encoded(
                crate::en::Encoder
                    .encode_bytes(Bytes::from(input.clone()))
                    .unwrap(),
            )
            .await;

            let decoded: Bytes = crate::try_decode(
                (),
                stream::iter([Ok::<_, crate::en::Error>(Bytes::from(wire))]),
            )
            .await
            .unwrap();

            assert_eq!(decoded.as_ref(), input);
        }
    });
}
