use std::cmp::min;

use bytes::Bytes;
use futures::stream;

pub fn chunk_stream(bytes: Bytes, chunk_size: usize) -> impl futures::Stream<Item = Bytes> + Unpin {
    assert!(chunk_size > 0);

    let len = bytes.len();
    let chunks = (0..len)
        .step_by(chunk_size)
        .map(move |i| bytes.slice(i..min(i + chunk_size, len)));

    stream::iter(chunks)
}

pub fn make_json_array_u64(count: usize) -> Bytes {
    let mut json = String::with_capacity(count * 6);
    json.push('[');
    for i in 0..count {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&(i as u64).to_string());
    }
    json.push(']');

    Bytes::from(json)
}
