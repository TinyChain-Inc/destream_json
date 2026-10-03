use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::task::Poll;

use bytes::Bytes;
use destream::{
    de::{self, FromStream},
    en::{self, IntoStream},
};
use futures::{stream, StreamExt, TryStreamExt};

use super::*;

struct Events<S>(S);

impl<'en, T, S> IntoStream<'en> for Events<S>
where
    T: IntoStream<'en> + 'en,
    S: futures::Stream<Item = Result<en::Event<T>, crate::en::Error>> + Send + 'en,
{
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        encoder.encode_events(self.0.map(|event| event.map_err(en::Error::custom)))
    }
}

async fn scan<D: de::Decoder>(decoder: &mut D) -> Result<(usize, u64), D::Error> {
    let mut stack = Vec::new();
    let mut depth = 0;
    let mut value = 0;
    loop {
        let kind = decoder.peek_kind().await?;
        if kind == de::Kind::Leaf {
            value += u64::from_stream((), decoder).await?;
        } else {
            let cursor = decoder.open_container(kind, None).await?;
            if cursor.slot() != de::Slot::End {
                stack.push(cursor);
                depth = depth.max(stack.len());
                continue;
            }
        }
        loop {
            let Some(parent) = stack.last_mut() else {
                return Ok((depth, value));
            };
            decoder.finish_child(parent).await?;
            if parent.slot() != de::Slot::End {
                break;
            }
            stack.pop();
        }
    }
}

#[tokio::test]
async fn shallow_consumption_preserves_depth_limits_and_borrowed_inputs() {
    for depth in [0, 1, 1024, 1025] {
        let events = std::iter::repeat_with(|| Ok(en::Event::SeqStart(None)))
            .take(depth)
            .chain(std::iter::once(Ok(en::Event::Value(7u64))))
            .chain(std::iter::repeat_with(|| Ok(en::Event::End)).take(depth));
        let encoded = crate::en::encode(Events(stream::iter(events))).unwrap();
        let bytes = encoded
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .unwrap();
        for width in [1, 4096] {
            // Pin a borrowed, non-Unpin source rather than requiring ownership or 'static.
            let source = stream::iter(bytes.chunks(width))
                .then(|bytes| async move { Ok::<_, Error>(Bytes::copy_from_slice(bytes)) });
            futures::pin_mut!(source);
            let mut decoder = Decoder::from_stream(source);
            let result = scan(&mut decoder).await;
            if depth <= 1024 {
                assert_eq!(result.unwrap(), (depth, 7));
                decoder.ensure_eof().await.unwrap();
            } else {
                assert!(result.is_err());
            }
        }
    }
    let text = String::from("borrowed");
    let events = stream::once(async { Ok(en::Event::Value(text.as_str())) });
    let encoded = crate::en::encode(Events(events)).unwrap();
    assert!(encoded.try_collect::<Vec<_>>().await.is_ok());
}

struct Lease(Arc<AtomicBool>);

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn event_encoding_releases_sources_on_eof_error_and_drop() {
    for mode in ["eof", "error", "cancel", "unpolled"] {
        let released = Arc::new(AtomicBool::new(false));
        let lease = Lease(released.clone());
        let mut first = true;
        let source = stream::poll_fn(move |_| {
            let _retain = &lease;
            if first {
                first = false;
                return Poll::Ready(Some(Ok(en::Event::Value(1u64))));
            }
            match mode {
                "eof" => Poll::Ready(None),
                "error" => Poll::Ready(Some(Err(en::Error::custom("event source failure")))),
                _ => Poll::Pending,
            }
        });
        let mut encoded = crate::en::encode(Events(source)).unwrap();
        if mode != "unpolled" {
            assert!(encoded.try_next().await.unwrap().is_some());
            if mode == "eof" {
                assert!(encoded.try_next().await.unwrap().is_none());
                assert!(released.load(Ordering::SeqCst));
            }
            if mode == "error" {
                assert!(encoded.try_next().await.is_err());
                assert!(released.load(Ordering::SeqCst));
                assert!(encoded.next().await.is_none());
            }
        }
        drop(encoded);
        assert!(released.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn malformed_event_framing_fails() {
    use en::Event::*;
    let cases: Vec<Vec<en::Event<u64>>> = vec![
        vec![],
        vec![End],
        vec![SeqStart(None)],
        vec![MapStart(None), Value(1), End],
        vec![Value(1), Value(2)],
    ];
    for events in cases {
        assert!(
            crate::en::encode(Events(stream::iter(events.into_iter().map(Ok))))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }
}

async fn encoded_bytes<'en>(value: impl IntoStream<'en> + 'en) -> Vec<u8> {
    crate::en::encode(value)
        .unwrap()
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn nested_event_framing_matches_ordinary_encoding() {
    use en::Event::*;
    use std::collections::BTreeMap;

    let ordinary = BTreeMap::from([
        (
            "a",
            vec![
                BTreeMap::from([("x", "one"), ("y", "two")]),
                BTreeMap::new(),
            ],
        ),
        ("b", Vec::new()),
    ]);
    let events = [
        MapStart(Some(2)),
        Value("a"),
        SeqStart(Some(2)),
        MapStart(Some(2)),
        Value("x"),
        Value("one"),
        Value("y"),
        Value("two"),
        End,
        MapStart(Some(0)),
        End,
        End,
        Value("b"),
        SeqStart(Some(0)),
        End,
        End,
    ];
    assert_eq!(
        encoded_bytes(Events(stream::iter(events.into_iter().map(Ok)))).await,
        encoded_bytes(ordinary).await,
    );
}

struct ControlledLeaf {
    lease: Lease,
    ready: Option<Arc<AtomicBool>>,
    fail: bool,
}

impl<'en> IntoStream<'en> for ControlledLeaf {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        let Self { lease, ready, fail } = self;
        let Some(ready) = ready else {
            return Err(en::Error::custom("leaf construction failure"));
        };
        let mut first = true;
        encoder.encode_events(stream::poll_fn(move |_| {
            let _retain = &lease;
            if first {
                first = false;
                Poll::Ready(Some(Ok::<_, E::Error>(en::Event::Value(1u64))))
            } else if !ready.load(Ordering::SeqCst) {
                Poll::Pending
            } else if fail {
                Poll::Ready(Some(Err(en::Error::custom("leaf stream failure"))))
            } else {
                Poll::Ready(None)
            }
        }))
    }
}

#[tokio::test]
async fn event_leaves_preserve_error_precedence_backpressure_and_cleanup() {
    let source_released = Arc::new(AtomicBool::new(false));
    let leaf_released = Arc::new(AtomicBool::new(false));
    let source_lease = Lease(source_released.clone());
    let events = [
        en::Event::SeqStart(Some(0)),
        en::Event::End,
        en::Event::Value(ControlledLeaf {
            lease: Lease(leaf_released.clone()),
            ready: None,
            fail: false,
        }),
    ];
    let source = stream::iter(events).map(move |event| {
        let _retain = &source_lease;
        Ok(event)
    });
    let mut encoded = crate::en::encode(Events(source)).unwrap();
    let error = encoded.by_ref().try_collect::<Vec<_>>().await.unwrap_err();
    assert_eq!(error.to_string(), "leaf construction failure");
    assert!(source_released.load(Ordering::SeqCst));
    assert!(leaf_released.load(Ordering::SeqCst));
    assert!(encoded.next().await.is_none());

    for mode in ["complete", "error", "cancel"] {
        let source_released = Arc::new(AtomicBool::new(false));
        let leaf_released = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let source_polls = polls.clone();
        let source_lease = Lease(source_released.clone());
        let mut leaf = Some(ControlledLeaf {
            lease: Lease(leaf_released.clone()),
            ready: Some(ready.clone()),
            fail: mode == "error",
        });
        let source = stream::poll_fn(move |_| {
            let _retain = &source_lease;
            source_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(leaf.take().map(|leaf| Ok(en::Event::Value(leaf))))
        });
        let mut encoded = crate::en::encode(Events(source)).unwrap();
        assert!(encoded.try_next().await.unwrap().is_some());
        assert!(futures::poll!(encoded.next()).is_pending());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(!source_released.load(Ordering::SeqCst));
        assert!(!leaf_released.load(Ordering::SeqCst));
        ready.store(true, Ordering::SeqCst);
        if mode == "complete" {
            assert!(encoded.try_next().await.unwrap().is_none());
            assert_eq!(polls.load(Ordering::SeqCst), 2);
        } else if mode == "error" {
            assert_eq!(
                encoded.try_next().await.unwrap_err().to_string(),
                "leaf stream failure"
            );
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert!(encoded.next().await.is_none());
        }
        if mode != "cancel" {
            assert!(source_released.load(Ordering::SeqCst));
            assert!(leaf_released.load(Ordering::SeqCst));
        }
        drop(encoded);
        assert!(source_released.load(Ordering::SeqCst));
        assert!(leaf_released.load(Ordering::SeqCst));
    }
}
