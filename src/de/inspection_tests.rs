use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use destream::de::{Decoder as _, Inspection};
use futures::stream;

use super::*;

fn chunks(
    input: &[u8],
    size: usize,
) -> Decoder<SourceStream<impl Stream<Item = Result<Bytes, Error>> + '_>> {
    Decoder::from_stream(stream::iter(
        input
            .chunks(size)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
    ))
}

#[tokio::test]
async fn inspection_counts_structure_and_bounds_text_across_chunks() {
    let text = format!("{}/{}", "é".repeat(9000), r#"\"\\\/\u002f\n"#);
    let input = format!("\"{text}\"");
    let mut ordinary = chunks(input.as_bytes(), 17);
    let expected = String::from_stream((), &mut ordinary).await.unwrap();
    for size in [1, 7, 4096, input.len()] {
        let mut decoder = chunks(input.as_bytes(), size);
        let mut actual = Vec::new();
        let mut capacity = None;
        decoder
            .inspect_any(|event| {
                match event {
                    Inspection::TextChunk(bytes) => {
                        assert!(bytes.len() <= 4096);
                        actual.extend_from_slice(bytes);
                    }
                    Inspection::TextEnd { capacity_bound } => capacity = Some(capacity_bound),
                    _ => panic!("unexpected container"),
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(actual, expected.as_bytes());
        assert!(capacity.unwrap() >= expected.capacity());
        assert!(decoder.buffer.capacity() <= 8192);
    }
    let mut decoder = chunks(br#"[{},[],{"a":[null,true,"x"],"b":[]}]"#, 1);
    let mut containers = Vec::new();
    decoder
        .inspect_any(|event| {
            match event {
                Inspection::Sequence { len, size_hint } => {
                    assert_eq!(size_hint, None);
                    containers.push(('s', len));
                }
                Inspection::Map { len } => containers.push(('m', len)),
                _ => {}
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        containers,
        [('m', 0), ('s', 0), ('s', 3), ('s', 0), ('m', 2), ('s', 3)]
    );
    // This codec deliberately permits non-string map keys. Inspection must
    // retain the same grammar as ordinary map decoding.
    let mut decoder = chunks(b"{1:2}", 1);
    let values = std::collections::BTreeMap::<u64, u64>::from_stream((), &mut decoder)
        .await
        .unwrap();
    assert_eq!(values.get(&1), Some(&2));
    let mut decoder = chunks(b"{1:2}", 1);
    let mut entries = None;
    decoder
        .inspect_any(|event| {
            if let Inspection::Map { len } = event {
                entries = Some(len);
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(entries, Some(1));
}

#[tokio::test]
async fn inspection_and_ignored_values_reject_malformed_input() {
    for input in [
        b"".as_slice(),
        b"[",
        b"{\"x\":}",
        b"[1,]",
        b"[1}",
        b"{\"x\" 1}",
        b"\"unfinished",
        b"\"\\u00\"",
        b"\"\\x\"",
        b"\"\xff\"",
        b"01",
        b"1.",
        b"1e",
        b"true false",
    ] {
        for size in [1, 7] {
            let mut decoder = chunks(input, size);
            let result = async {
                decoder.inspect_any(|_| Ok(())).await?;
                decoder.ensure_eof().await
            }
            .await;
            assert!(result.is_err(), "accepted {input:?}");
            let source = stream::iter(input.chunks(size).map(Bytes::copy_from_slice));
            assert!(
                decode::<_, de::IgnoredAny>((), source).await.is_err(),
                "ignored {input:?}"
            );
        }
    }
}

#[tokio::test]
async fn inspection_and_ignored_values_share_depth_limit() {
    for depth in [1024, 1025] {
        let input = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        let mut decoder = chunks(input.as_bytes(), 17);
        assert_eq!(decoder.inspect_any(|_| Ok(())).await.is_ok(), depth == 1024);
        let mut decoder = chunks(input.as_bytes(), 17);
        assert_eq!(
            de::IgnoredAny::from_stream((), &mut decoder).await.is_ok(),
            depth == 1024
        );
    }
}

#[tokio::test]
async fn whitespace_is_consumed_incrementally_and_callback_errors_stop() {
    let input = format!("{}[\"first\",\"second\"]", " ".repeat(128 * 1024));
    let mut decoder = chunks(input.as_bytes(), 31);
    let mut events = 0;
    let error = decoder
        .inspect_any(|_| {
            events += 1;
            Err(de::Error::custom("stop inspection"))
        })
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "stop inspection");
    assert_eq!(events, 1);
    assert!(decoder.buffer.capacity() <= 4096);
    let source = stream::iter([
        Ok(Bytes::from_static(b"[")),
        Err(de::Error::custom("injected source error")),
    ]);
    let mut decoder = Decoder::from_stream(source);
    let error = decoder.inspect_any(|_| Ok(())).await.unwrap_err();
    assert_eq!(error.to_string(), "injected source error");
}

struct Released(Arc<AtomicBool>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_and_unpolled_drop_release_inspection_callback() {
    for poll in [false, true] {
        let released = Arc::new(AtomicBool::new(false));
        let guard = Released(Arc::clone(&released));
        let source = stream::iter([Ok(Bytes::from_static(b"["))]).chain(stream::pending());
        let mut decoder = Decoder::from_stream(source);
        let mut inspect = Box::pin(decoder.inspect_any(move |_| {
            let _keep = &guard;
            Ok(())
        }));
        if poll {
            assert!(futures::poll!(&mut inspect).is_pending());
        }
        drop(inspect);
        assert!(released.load(Ordering::SeqCst));
    }
}

struct OneChunk(Option<Bytes>);

impl Read for OneChunk {
    async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        self.0.take().map(Ok)
    }

    fn is_terminated(&self) -> bool {
        self.0.is_none()
    }
}

#[tokio::test]
async fn large_input_lease_is_not_copied_and_eof_includes_pending_bytes() {
    let input: Arc<[u8]> = format!("\"{}\"", "x".repeat(64 * 1024)).into_bytes().into();
    let bytes = Bytes::from_owner(Arc::clone(&input));
    let mut decoder = Decoder::from(OneChunk(Some(bytes)));
    let error = decoder
        .inspect_any(|_| Err(de::Error::custom("stop")))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "stop");
    assert!(!decoder.pending.is_empty());
    assert!(decoder.buffer.capacity() <= 8192);
    assert!(!decoder.source_terminated());
    assert_eq!(Arc::strong_count(&input), 2);
    drop(decoder);
    assert_eq!(Arc::strong_count(&input), 1);

    let bytes = Bytes::from_owner(Arc::clone(&input));
    let mut ordinary = Decoder::from(OneChunk(Some(bytes)));
    assert_eq!(
        String::from_stream((), &mut ordinary).await.unwrap().len(),
        64 * 1024
    );
    ordinary.ensure_eof().await.unwrap();
    let mut ordinary = Decoder::from(OneChunk(Some(Bytes::from_static(b"1.5"))));
    assert_eq!(
        number_general::Number::from_stream((), &mut ordinary)
            .await
            .unwrap(),
        number_general::Number::from(1.5)
    );

    let bytes = Bytes::from_owner(Arc::clone(&input));
    let mut decoder = Decoder::from(OneChunk(Some(bytes)));
    decoder.inspect_any(|_| Ok(())).await.unwrap();
    decoder.ensure_eof().await.unwrap();
    assert!(decoder.pending.is_empty());
    assert!(decoder.buffer.capacity() <= 8192);

    let mut trailing = Vec::from(input.as_ref());
    trailing.extend_from_slice(b"[]");
    let bytes = Bytes::from(trailing);
    let mut decoder = Decoder::from(OneChunk(Some(bytes)));
    decoder.inspect_any(|_| Ok(())).await.unwrap();
    assert!(decoder.ensure_eof().await.is_err());
}
