use std::collections::HashMap;
use std::fmt;

use destream::de::{self, Decoder, FromStream, MapAccess, SeqAccess, Visitor};
use destream::en::{Encoder, IntoStream, ToStream};
use number_general::Number;

#[derive(Clone, Eq, PartialEq)]
pub enum Value {
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    None,
    Number(Number),
    String(String),
}

impl Default for Value {
    fn default() -> Self {
        Self::None
    }
}

impl FromIterator<Value> for Value {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        Self::List(iter.into_iter().collect())
    }
}

impl FromIterator<(String, Value)> for Value {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        Self::Map(iter.into_iter().collect())
    }
}

impl From<()> for Value {
    fn from(_unit: ()) -> Self {
        Self::None
    }
}

macro_rules! from_number {
    ($t:ty) => {
        impl From<$t> for Value {
            fn from(n: $t) -> Self {
                Self::Number(n.into())
            }
        }
    };
}

from_number!(bool);
from_number!(u8);
from_number!(u16);
from_number!(u32);
from_number!(u64);
from_number!(i16);
from_number!(i32);
from_number!(i64);
from_number!(f32);
from_number!(f64);

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

struct ValueVisitor;

impl Visitor for ValueVisitor {
    type Value = Value;

    fn expecting() -> &'static str {
        "a JSON Value"
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_i8<E: de::Error>(self, v: i8) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_i16<E: de::Error>(self, v: i16) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_i32<E: de::Error>(self, v: i32) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u8<E: de::Error>(self, v: u8) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u16<E: de::Error>(self, v: u16) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u32<E: de::Error>(self, v: u32) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::None)
    }

    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::None)
    }

    async fn visit_some<D: Decoder>(self, decoder: &mut D) -> Result<Value, D::Error> {
        Value::from_stream((), decoder).await
    }

    async fn visit_map<A: MapAccess>(self, mut map: A) -> Result<Value, A::Error> {
        let mut decoded = HashMap::new();

        while let Some(key) = map.next_key(()).await? {
            decoded.insert(key, map.next_value(()).await?);
        }

        Ok(Value::Map(decoded))
    }

    async fn visit_seq<A: SeqAccess>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut decoded = Vec::new();

        while let Some(item) = seq.next_element(()).await? {
            decoded.push(item);
        }

        Ok(Value::List(decoded))
    }
}

impl FromStream for Value {
    type Context = ();

    async fn from_stream<D: Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        decoder.decode_any(ValueVisitor).await
    }
}

impl<'en> IntoStream<'en> for Value {
    fn into_stream<E: Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::List(list) => list.into_stream(encoder),
            Self::None => ().into_stream(encoder),
            Self::Map(map) => map.into_stream(encoder),
            Self::Number(n) => n.into_stream(encoder),
            Self::String(s) => s.into_stream(encoder),
        }
    }
}

impl<'en> ToStream<'en> for Value {
    fn to_stream<E: Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::List(list) => list.to_stream(encoder),
            Self::None => ().into_stream(encoder),
            Self::Map(map) => map.to_stream(encoder),
            Self::Number(n) => n.to_stream(encoder),
            Self::String(s) => s.to_stream(encoder),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::List(list) => fmt::Debug::fmt(list, f),
            Self::None => f.write_str("None"),
            Self::Map(map) => fmt::Debug::fmt(map, f),
            Self::Number(n) => fmt::Debug::fmt(n, f),
            Self::String(s) => fmt::Debug::fmt(s, f),
        }
    }
}
