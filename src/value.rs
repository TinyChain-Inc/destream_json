use std::borrow::Cow;
use std::collections::{hash_map, HashMap};
use std::fmt;

use destream::de::{self, Decoder, FromStream, Visitor};
use destream::en::{self, Encoder, IntoStream, ToStream};
use futures::{stream, StreamExt};
use number_general::Number;

/// An owned JSON value with iterative traversal and destruction.
pub struct Value {
    kind: Option<ValueKind>,
}

/// The movable contents of a JSON value. Recursive children remain owned Values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ValueKind {
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    #[default]
    None,
    Number(Number),
    String(String),
}

impl Value {
    pub fn kind(&self) -> &ValueKind {
        self.kind.as_ref().expect("owned value")
    }

    pub fn kind_mut(&mut self) -> &mut ValueKind {
        self.kind.as_mut().expect("owned value")
    }

    pub fn into_kind(mut self) -> ValueKind {
        self.kind.take().expect("owned value")
    }
}

impl From<ValueKind> for Value {
    fn from(kind: ValueKind) -> Self {
        Self { kind: Some(kind) }
    }
}

impl Default for Value {
    fn default() -> Self {
        ValueKind::None.into()
    }
}

impl FromIterator<Value> for Value {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        ValueKind::List(iter.into_iter().collect()).into()
    }
}

impl FromIterator<(String, Value)> for Value {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        ValueKind::Map(iter.into_iter().collect()).into()
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Self::default()
    }
}
macro_rules! from_number {
    ($($t:ty),+) => {
        $(
            impl From<$t> for Value {
                fn from(n: $t) -> Self {
                    ValueKind::Number(n.into()).into()
                }
            }
        )+
    };
}

from_number!(bool, u8, u16, u32, u64, i16, i32, i64, f32, f64);

impl From<String> for Value {
    fn from(value: String) -> Self {
        ValueKind::String(value).into()
    }
}

enum Children {
    List(std::vec::IntoIter<Value>),
    Map(hash_map::IntoValues<String, Value>),
}

impl Children {
    fn next(&mut self) -> Option<Value> {
        match self {
            Self::List(items) => items.next(),
            Self::Map(items) => items.next(),
        }
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        let mut current = self.kind.take();
        let mut parents: Vec<Children> = Vec::new();
        loop {
            match current.take() {
                Some(ValueKind::List(items)) => parents.push(Children::List(items.into_iter())),
                Some(ValueKind::Map(items)) => parents.push(Children::Map(items.into_values())),
                _ => {}
            }
            loop {
                let Some(parent) = parents.last_mut() else {
                    return;
                };
                if let Some(mut child) = parent.next() {
                    current = child.kind.take();
                    break;
                }
                parents.pop();
            }
        }
    }
}

enum CloneFrame<'a> {
    List(std::slice::Iter<'a, Value>, Vec<Value>),
    Map(
        hash_map::Iter<'a, String, Value>,
        Option<String>,
        HashMap<String, Value>,
    ),
}

impl<'a> CloneFrame<'a> {
    fn next(&mut self) -> Option<&'a Value> {
        match self {
            Self::List(items, _) => items.next(),
            Self::Map(items, key, _) => items.next().map(|(k, value)| {
                *key = Some(k.clone());
                value
            }),
        }
    }

    fn push(&mut self, value: Value) {
        match self {
            Self::List(_, items) => items.push(value),
            Self::Map(_, key, items) => {
                items.insert(key.take().expect("map key"), value);
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Self::List(_, items) => ValueKind::List(items).into(),
            Self::Map(_, _, items) => ValueKind::Map(items).into(),
        }
    }
}

impl Clone for Value {
    fn clone(&self) -> Self {
        let mut next = self;
        let mut parents: Vec<CloneFrame<'_>> = Vec::new();
        'visit: loop {
            let mut result = match next.kind() {
                ValueKind::List(items) => {
                    let mut frame = CloneFrame::List(items.iter(), Vec::with_capacity(items.len()));
                    if let Some(child) = frame.next() {
                        next = child;
                        parents.push(frame);
                        continue;
                    }
                    frame.finish()
                }
                ValueKind::Map(items) => {
                    let mut frame = CloneFrame::Map(
                        items.iter(),
                        None,
                        HashMap::with_capacity_and_hasher(items.len(), items.hasher().clone()),
                    );
                    if let Some(child) = frame.next() {
                        next = child;
                        parents.push(frame);
                        continue;
                    }
                    frame.finish()
                }
                ValueKind::None => Value::default(),
                ValueKind::Number(number) => ValueKind::Number(*number).into(),
                ValueKind::String(text) => ValueKind::String(text.clone()).into(),
            };
            loop {
                let Some(parent) = parents.last_mut() else {
                    return result;
                };
                parent.push(result);
                if let Some(child) = parent.next() {
                    next = child;
                    continue 'visit;
                }
                result = parents.pop().expect("parent").finish();
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        enum Frame<'a> {
            List(std::iter::Zip<std::slice::Iter<'a, Value>, std::slice::Iter<'a, Value>>),
            Map(
                hash_map::Iter<'a, String, Value>,
                &'a HashMap<String, Value>,
            ),
        }
        let mut frames = Vec::new();
        let (mut left, mut right) = (self, other);
        loop {
            match (left.kind(), right.kind()) {
                (ValueKind::None, ValueKind::None) => {}
                (ValueKind::Number(a), ValueKind::Number(b)) if a == b => {}
                (ValueKind::String(a), ValueKind::String(b)) if a == b => {}
                (ValueKind::List(a), ValueKind::List(b)) if a.len() == b.len() => {
                    frames.push(Frame::List(a.iter().zip(b)));
                }
                (ValueKind::Map(a), ValueKind::Map(b)) if a.len() == b.len() => {
                    frames.push(Frame::Map(a.iter(), b));
                }
                _ => return false,
            }
            loop {
                let Some(frame) = frames.last_mut() else {
                    return true;
                };
                let next = match frame {
                    Frame::List(items) => items.next(),
                    Frame::Map(items, other) => match items.next() {
                        Some((key, value)) => {
                            let Some(other) = other.get(key) else {
                                return false;
                            };
                            Some((value, other))
                        }
                        None => None,
                    },
                };
                if let Some(pair) = next {
                    (left, right) = pair;
                    break;
                }
                frames.pop();
            }
        }
    }
}

impl Eq for Value {}

struct ValueVisitor;

impl Visitor for ValueVisitor {
    type Value = Value;

    fn expecting() -> &'static str {
        "a JSON Value"
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_i8<E: de::Error>(self, v: i8) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_i16<E: de::Error>(self, v: i16) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_i32<E: de::Error>(self, v: i32) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_u8<E: de::Error>(self, v: u8) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_u16<E: de::Error>(self, v: u16) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_u32<E: de::Error>(self, v: u32) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(ValueKind::Number(v.into()).into())
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(ValueKind::String(v).into())
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(ValueKind::None.into())
    }

    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(ValueKind::None.into())
    }
}

enum DecodeFrame {
    List(de::Container, Vec<Value>),
    Map(de::Container, HashMap<String, Value>, String),
}

impl FromStream for Value {
    type Context = ();

    async fn from_stream<D: Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        let mut parents: Vec<DecodeFrame> = Vec::new();
        'value: loop {
            let kind = decoder.peek_kind().await?;
            let mut value = match kind {
                de::Kind::Leaf => decoder.decode_any(ValueVisitor).await?,
                de::Kind::Seq => {
                    let cursor = decoder.open_container(kind, None).await?;
                    if cursor.slot() != de::Slot::End {
                        parents.push(DecodeFrame::List(cursor, Vec::new()));
                        continue;
                    }
                    ValueKind::List(Vec::new()).into()
                }
                de::Kind::Map => {
                    let mut cursor = decoder.open_container(kind, None).await?;
                    if cursor.slot() != de::Slot::End {
                        let key = String::from_stream((), decoder).await?;
                        decoder.finish_child(&mut cursor).await?;
                        parents.push(DecodeFrame::Map(cursor, HashMap::new(), key));
                        continue;
                    }
                    ValueKind::Map(HashMap::new()).into()
                }
            };
            loop {
                let Some(parent) = parents.last_mut() else {
                    return Ok(value);
                };
                match parent {
                    DecodeFrame::List(cursor, items) => {
                        decoder.finish_child(cursor).await?;
                        items.push(value);
                        if cursor.slot() != de::Slot::End {
                            continue 'value;
                        }
                    }
                    DecodeFrame::Map(cursor, items, key) => {
                        decoder.finish_child(cursor).await?;
                        items.insert(std::mem::take(key), value);
                        if cursor.slot() != de::Slot::End {
                            *key = String::from_stream((), decoder).await?;
                            decoder.finish_child(cursor).await?;
                            continue 'value;
                        }
                    }
                }
                value = match parents.pop().expect("parent") {
                    DecodeFrame::List(_, items) => ValueKind::List(items).into(),
                    DecodeFrame::Map(_, items, _) => ValueKind::Map(items).into(),
                };
            }
        }
    }
}

#[derive(Clone)]
enum Leaf<'a> {
    None,
    Number(Number),
    String(Cow<'a, str>),
}

impl<'a: 'en, 'en> IntoStream<'en> for Leaf<'a> {
    fn into_stream<E: Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::None => ().into_stream(encoder),
            Self::Number(number) => number.into_stream(encoder),
            Self::String(Cow::Borrowed(text)) => text.into_stream(encoder),
            Self::String(Cow::Owned(text)) => text.into_stream(encoder),
        }
    }
}

enum BorrowFrame<'a> {
    Value(&'a Value),
    List(std::slice::Iter<'a, Value>),
    Map(hash_map::Iter<'a, String, Value>),
}

struct BorrowEvents<'a>(Vec<BorrowFrame<'a>>);

impl<'a> Iterator for BorrowEvents<'a> {
    type Item = en::Event<Leaf<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.last_mut()? {
                BorrowFrame::List(items) => {
                    if let Some(value) = items.next() {
                        self.0.push(BorrowFrame::Value(value));
                        continue;
                    } else {
                        self.0.pop();
                        return Some(en::Event::End);
                    }
                }
                BorrowFrame::Map(items) => {
                    if let Some((key, value)) = items.next() {
                        self.0.push(BorrowFrame::Value(value));
                        return Some(en::Event::Value(Leaf::String(Cow::Borrowed(key))));
                    } else {
                        self.0.pop();
                        return Some(en::Event::End);
                    }
                }
                BorrowFrame::Value(_) => {}
            }
            let BorrowFrame::Value(value) = self.0.pop()? else {
                unreachable!()
            };
            return Some(match value.kind() {
                ValueKind::List(items) => {
                    self.0.push(BorrowFrame::List(items.iter()));
                    en::Event::SeqStart(Some(items.len()))
                }
                ValueKind::Map(items) => {
                    self.0.push(BorrowFrame::Map(items.iter()));
                    en::Event::MapStart(Some(items.len()))
                }
                ValueKind::None => en::Event::Value(Leaf::None),
                ValueKind::Number(number) => en::Event::Value(Leaf::Number(*number)),
                ValueKind::String(text) => en::Event::Value(Leaf::String(Cow::Borrowed(text))),
            });
        }
    }
}

enum OwnedFrame {
    Value(Value),
    List(std::vec::IntoIter<Value>),
    Map(hash_map::IntoIter<String, Value>),
}

struct OwnedEvents(Vec<OwnedFrame>);

impl Iterator for OwnedEvents {
    type Item = en::Event<Leaf<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.last_mut()? {
                OwnedFrame::List(items) => {
                    if let Some(value) = items.next() {
                        self.0.push(OwnedFrame::Value(value));
                        continue;
                    } else {
                        self.0.pop();
                        return Some(en::Event::End);
                    }
                }
                OwnedFrame::Map(items) => {
                    if let Some((key, value)) = items.next() {
                        self.0.push(OwnedFrame::Value(value));
                        return Some(en::Event::Value(Leaf::String(Cow::Owned(key))));
                    } else {
                        self.0.pop();
                        return Some(en::Event::End);
                    }
                }
                OwnedFrame::Value(_) => {}
            }
            let OwnedFrame::Value(value) = self.0.pop()? else {
                unreachable!()
            };
            return Some(match value.into_kind() {
                ValueKind::List(items) => {
                    let len = items.len();
                    self.0.push(OwnedFrame::List(items.into_iter()));
                    en::Event::SeqStart(Some(len))
                }
                ValueKind::Map(items) => {
                    let len = items.len();
                    self.0.push(OwnedFrame::Map(items.into_iter()));
                    en::Event::MapStart(Some(len))
                }
                ValueKind::None => en::Event::Value(Leaf::None),
                ValueKind::Number(number) => en::Event::Value(Leaf::Number(number)),
                ValueKind::String(text) => en::Event::Value(Leaf::String(Cow::Owned(text))),
            });
        }
    }
}

impl<'en> IntoStream<'en> for Value {
    fn into_stream<E: Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        encoder.encode_events(stream::iter(OwnedEvents(vec![OwnedFrame::Value(self)])).map(Ok))
    }
}

impl<'en> ToStream<'en> for Value {
    fn to_stream<E: Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        encoder.encode_events(stream::iter(BorrowEvents(vec![BorrowFrame::Value(self)])).map(Ok))
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        enum Frame<'a> {
            Value(&'a Value),
            List(std::slice::Iter<'a, Value>, bool),
            Map(hash_map::Iter<'a, String, Value>, bool),
        }
        let mut stack = vec![Frame::Value(self)];
        while let Some(frame) = stack.last_mut() {
            match frame {
                Frame::List(items, first) => {
                    if let Some(value) = items.next() {
                        if !*first {
                            f.write_str(", ")?;
                        }
                        *first = false;
                        stack.push(Frame::Value(value));
                        continue;
                    } else {
                        f.write_str("]")?;
                    }
                }
                Frame::Map(items, first) => {
                    if let Some((key, value)) = items.next() {
                        if !*first {
                            f.write_str(", ")?;
                        }
                        *first = false;
                        write!(f, "{key:?}: ")?;
                        stack.push(Frame::Value(value));
                        continue;
                    } else {
                        f.write_str("}")?;
                    }
                }
                Frame::Value(value) => {
                    let value = *value;
                    stack.pop();
                    match value.kind() {
                        ValueKind::List(items) => {
                            f.write_str("[")?;
                            stack.push(Frame::List(items.iter(), true));
                        }
                        ValueKind::Map(items) => {
                            f.write_str("{")?;
                            stack.push(Frame::Map(items.iter(), true));
                        }
                        ValueKind::None => f.write_str("None")?,
                        ValueKind::Number(number) => write!(f, "{number:?}")?,
                        ValueKind::String(text) => write!(f, "{text:?}")?,
                    }
                    continue;
                }
            }
            stack.pop();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn owned_values_traverse_and_drop_iteratively() {
        let mut value = Value::from(7u64);
        for depth in 0..20_000 {
            value = if depth % 2 == 0 {
                ValueKind::List(vec![value]).into()
            } else {
                ValueKind::Map(HashMap::from([(String::from("k"), value)])).into()
            };
        }
        let copy = value.clone();
        assert_eq!(value, copy);
        assert!(!format!("{value:?}").is_empty());
        let borrowed = crate::encode(&value)
            .unwrap()
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .unwrap();
        let owned = crate::encode(copy)
            .unwrap()
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .unwrap();
        assert_eq!(borrowed, owned);
        drop(crate::encode(value.clone()).unwrap());
        drop(value);
    }

    #[tokio::test]
    async fn value_decode_accepts_boundary_and_rejects_excess_depth() {
        for depth in [1024, 1025] {
            let input = format!("{}7{}", "[".repeat(depth), "]".repeat(depth));
            let result: Result<Value, _> =
                crate::decode((), stream::iter([bytes::Bytes::from(input)])).await;
            assert_eq!(result.is_ok(), depth == 1024);
        }
    }
}
