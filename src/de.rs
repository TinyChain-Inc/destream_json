//! Decode a JSON stream to a Rust data structure.

use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use bytes::{BufMut, Bytes};
use destream::{de, FromStream, Visitor};
use futures::{
    stream::{Fuse, FusedStream, Stream, StreamExt, TryStreamExt},
    FutureExt as _,
};

#[cfg(feature = "tokio-io")]
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::constants::*;

const SNIPPET_LEN: usize = 50;
const CHUNK_SIZE: usize = 4096;
const MAX_DEPTH: usize = 1024;

fn container_observation(container: &de::Container) -> de::Inspection<'static> {
    if container.kind() == de::Kind::Map {
        de::Inspection::Map {
            len: container.len(),
        }
    } else {
        de::Inspection::Sequence {
            len: container.len(),
            size_hint: container.size_hint(),
        }
    }
}

/// Methods common to any decodable [`Stream`]
#[trait_variant::make(Send)]
pub trait Read: Send + Unpin {
    /// Read the next chunk of [`Bytes`] in this [`Stream`].
    async fn next(&mut self) -> Option<Result<Bytes, Error>>;

    /// Return `true` if there are no more contents to be read from this [`Stream`].
    fn is_terminated(&self) -> bool;
}

/// A decodable [`Stream`]
pub struct SourceStream<S> {
    source: Fuse<S>,
}

impl<S: Stream<Item = Result<Bytes, Error>> + Send + Unpin> Read for SourceStream<S> {
    async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        self.source.next().await
    }

    fn is_terminated(&self) -> bool {
        self.source.is_terminated()
    }
}

impl<S: Stream> From<S> for SourceStream<S> {
    fn from(source: S) -> Self {
        Self {
            source: source.fuse(),
        }
    }
}

#[cfg(feature = "tokio-io")]
pub struct SourceReader<R: AsyncRead> {
    reader: BufReader<R>,
    terminated: bool,
}

#[cfg(feature = "tokio-io")]
impl<R: AsyncRead + Send + Unpin> Read for SourceReader<R> {
    async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        let mut chunk = Vec::new();
        match self.reader.read_buf(&mut chunk).await {
            Ok(0) => {
                self.terminated = true;
                Some(Ok(chunk.into()))
            }
            Ok(size) => {
                debug_assert_eq!(chunk.len(), size);
                Some(Ok(chunk.into()))
            }
            Err(cause) => Some(Err(de::Error::custom(format!("io error: {}", cause)))),
        }
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[cfg(feature = "tokio-io")]
impl<R: AsyncRead> From<R> for SourceReader<R> {
    fn from(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            terminated: false,
        }
    }
}

/// An error encountered while decoding a JSON stream.
#[derive(PartialEq)]
pub struct Error {
    message: String,
}

impl Error {
    fn invalid_utf8<I: fmt::Display>(info: I) -> Self {
        de::Error::custom(format!("invalid UTF-8: {}", info))
    }

    fn unexpected_end() -> Self {
        de::Error::custom("unexpected end of stream")
    }
}

impl std::error::Error for Error {}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.message, f)
    }
}

struct MapAccess<'a, S> {
    decoder: &'a mut Decoder<S>,
    container: de::Container,
}

impl<'a, S: Read + 'a> MapAccess<'a, S> {
    async fn new(decoder: &'a mut Decoder<S>, size_hint: Option<usize>) -> Result<Self, Error> {
        let container = de::Decoder::open_container(decoder, de::Kind::Map, size_hint).await?;
        Ok(Self { decoder, container })
    }
}

impl<'a, S: Read + 'a> de::MapAccess for MapAccess<'a, S> {
    type Error = Error;

    async fn next_key<K: FromStream>(&mut self, context: K::Context) -> Result<Option<K>, Error> {
        if self.container.slot() == de::Slot::End {
            return Ok(None);
        }
        self.decoder.expect_whitespace().await?;
        let key = K::from_stream(context, self.decoder).await?;
        de::Decoder::finish_child(self.decoder, &mut self.container).await?;
        Ok(Some(key))
    }

    async fn next_value<V: FromStream>(&mut self, context: V::Context) -> Result<V, Error> {
        if self.container.slot() != de::Slot::Value {
            return Err(de::Error::custom("expected a map value after its key"));
        }
        let value = V::from_stream(context, self.decoder).await?;
        de::Decoder::finish_child(self.decoder, &mut self.container).await?;
        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        self.container.size_hint()
    }
}

struct SeqAccess<'a, S> {
    decoder: &'a mut Decoder<S>,
    container: de::Container,
}

impl<'a, S: Read + 'a> SeqAccess<'a, S> {
    async fn new(decoder: &'a mut Decoder<S>, size_hint: Option<usize>) -> Result<Self, Error> {
        let container = de::Decoder::open_container(decoder, de::Kind::Seq, size_hint).await?;
        Ok(Self { decoder, container })
    }
}

impl<'a, S: Read + 'a> de::SeqAccess for SeqAccess<'a, S> {
    type Error = Error;

    async fn next_element<T: FromStream>(
        &mut self,
        context: T::Context,
    ) -> Result<Option<T>, Self::Error> {
        if self.container.slot() == de::Slot::End {
            return Ok(None);
        }
        self.decoder.expect_whitespace().await?;
        let value = T::from_stream(context, self.decoder).await?;
        de::Decoder::finish_child(self.decoder, &mut self.container).await?;
        Ok(Some(value))
    }

    fn size_hint(&self) -> Option<usize> {
        self.container.size_hint()
    }
}

impl<'a, S: Read + 'a, T: FromStream<Context = ()> + 'a> de::ArrayAccess<T> for SeqAccess<'a, S> {
    type Error = Error;

    async fn buffer(&mut self, buffer: &mut [T]) -> Result<usize, Self::Error> {
        let mut i = 0;
        let len = buffer.len();
        while i < len {
            match de::SeqAccess::next_element(self, ()).await {
                Ok(Some(b)) => {
                    buffer[i] = b;
                    i += 1;
                }
                Ok(None) => break,
                Err(cause) => {
                    let message = match self.decoder.contents(SNIPPET_LEN) {
                        Ok(snippet) => format!("array decode error: {} at {}...", cause, snippet),
                        Err(_) => format!("array decode error: {}", cause),
                    };
                    return Err(de::Error::custom(message));
                }
            }
        }

        Ok(i)
    }
}

/// A structure that decodes Rust values from a JSON stream.
pub struct Decoder<S> {
    source: S,
    pending: Bytes,
    buffer: Vec<u8>,
    numeric: HashSet<u8>,
    depth: usize,
}

#[cfg(feature = "tokio-io")]
impl<A: AsyncRead> Decoder<A>
where
    SourceReader<A>: Read,
{
    pub fn from_reader(reader: A) -> Decoder<SourceReader<A>> {
        Decoder {
            source: SourceReader::from(reader),
            buffer: Vec::new(),
            pending: Bytes::new(),
            numeric: NUMERIC.iter().cloned().collect(),
            depth: 0,
        }
    }
}

impl<S> Decoder<S> {
    fn push_depth(&mut self) -> Result<(), Error> {
        if self.depth == MAX_DEPTH {
            return Err(de::Error::custom("nesting depth limit exceeded (max 1024)"));
        }
        self.depth += 1;
        Ok(())
    }

    fn pop_depth(&mut self) {
        debug_assert!(self.depth > 0);
        self.depth -= 1;
    }

    fn contents(&self, max_len: usize) -> Result<String, Error> {
        let len = Ord::min(self.buffer.len(), max_len);
        String::from_utf8(self.buffer[..len].to_vec()).map_err(Error::invalid_utf8)
    }
}

impl<S: Stream> Decoder<SourceStream<S>>
where
    SourceStream<S>: Read,
{
    /// Construct a new [`Decoder`] from the given source `stream`.
    pub fn from_stream(stream: S) -> Decoder<SourceStream<S>> {
        Decoder {
            source: SourceStream::from(stream),
            buffer: Vec::new(),
            pending: Bytes::new(),
            numeric: NUMERIC.iter().cloned().collect(),
            depth: 0,
        }
    }

    /// Return `true` if this [`Decoder`] has no more data to be decoded.
    pub fn is_terminated(&self) -> bool {
        self.source_terminated()
    }
}

impl<S: Read> Decoder<S> {
    fn source_terminated(&self) -> bool {
        self.pending.is_empty() && self.source.is_terminated()
    }

    async fn buffer(&mut self) -> Result<(), Error> {
        if self.pending.is_empty() {
            if let Some(data) = self.source.next().await {
                self.pending = data?;
            }
        }
        let len = self.pending.len().min(CHUNK_SIZE);
        self.buffer.extend_from_slice(&self.pending.split_to(len));
        Ok(())
    }

    async fn buffer_string(&mut self) -> Result<Vec<u8>, Error> {
        self.expect_delimiter(QUOTE).await?;

        let mut i = 0;
        let mut escaped = false;
        loop {
            while i >= self.buffer.len() && !self.source_terminated() {
                self.buffer().await?;
            }

            if i < self.buffer.len() && &self.buffer[i..i + 1] == QUOTE && !escaped {
                break;
            } else if i >= self.buffer.len() && self.source_terminated() {
                return Err(Error::unexpected_end());
            }

            if escaped {
                escaped = false;
            } else if self.buffer[i] == ESCAPE[0] {
                escaped = true;
            }

            i += 1;
        }

        let mut s = Vec::with_capacity(i);
        let mut escape = false;
        for byte in self.buffer.drain(0..i) {
            let as_slice = std::slice::from_ref(&byte);
            if escape {
                s.put_u8(byte);
                escape = false;
            } else if as_slice == ESCAPE {
                escape = true;
            } else {
                s.put_u8(byte);
            }
        }

        self.buffer.remove(0); // process the end quote
        self.buffer.shrink_to_fit();
        Ok(s)
    }

    async fn buffer_while<F: Fn(u8) -> bool>(&mut self, cond: F) -> Result<usize, Error> {
        let mut i = 0;
        loop {
            while i >= self.buffer.len() && !self.source_terminated() {
                self.buffer().await?;
            }

            if i < self.buffer.len() && cond(self.buffer[i]) {
                i += 1;
            } else if self.source_terminated() {
                return Ok(i);
            } else {
                break;
            }
        }

        Ok(i)
    }

    async fn peek(&mut self) -> Result<Option<u8>, Error> {
        while self.buffer.is_empty() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.buffer[0]))
        }
    }

    async fn next_char(&mut self) -> Result<Option<u8>, Error> {
        while self.buffer.is_empty() && !self.source_terminated() {
            self.buffer().await?;
        }

        match self.buffer.len() {
            0 => Ok(None),
            _ => Ok(Some(self.buffer.remove(0))),
        }
    }

    async fn eat_char(&mut self) -> Result<(), Error> {
        self.next_char().await?;
        Ok(())
    }

    async fn decode_number<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Error> {
        let mut i = 0;
        loop {
            if self.buffer[i] == DECIMAL[0] || self.buffer[i] == E[0] {
                return de::Decoder::decode_f64(self, visitor).await;
            } else if !self.numeric.contains(&self.buffer[i]) {
                return de::Decoder::decode_i64(self, visitor).await;
            }

            i += 1;
            while i >= self.buffer.len() && !self.source_terminated() {
                self.buffer().await?;
            }

            if i >= self.buffer.len() && self.source_terminated() {
                return de::Decoder::decode_i64(self, visitor).await;
            }
        }
    }

    async fn expect_delimiter(&mut self, delimiter: &'static [u8]) -> Result<(), Error> {
        while self.buffer.is_empty() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.is_empty() {
            return Err(Error::unexpected_end());
        }

        if &self.buffer[0..1] == delimiter {
            self.buffer.remove(0);
            Ok(())
        } else {
            let contents = self.contents(SNIPPET_LEN)?;
            Err(de::Error::custom(format!(
                "unexpected delimiter {}, expected {} at `{}`...",
                self.buffer[0] as char, delimiter[0] as char, contents
            )))
        }
    }

    async fn ensure_eof(&mut self) -> Result<(), Error> {
        self.expect_whitespace().await?;
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(de::Error::custom(format!(
                "expected end of stream, found `{}...`",
                self.contents(SNIPPET_LEN)?
            )))
        }
    }

    async fn expect_whitespace(&mut self) -> Result<(), Error> {
        loop {
            while self.buffer.is_empty() && !self.source_terminated() {
                self.buffer().await?;
            }
            let count = self
                .buffer
                .iter()
                .take_while(|byte| byte.is_ascii_whitespace())
                .count();
            let done = count < self.buffer.len() || self.source_terminated();
            self.buffer.drain(..count);
            if done {
                return Ok(());
            }
        }
    }

    async fn inspect_string<F>(&mut self, inspect: &mut F) -> Result<(), Error>
    where
        F: for<'a> FnMut(de::Inspection<'a>) -> Result<(), Error> + Send,
    {
        self.expect_delimiter(QUOTE).await?;
        let mut output = [0; 4096];
        let mut len = 0;
        let mut capacity_bound = 0usize;
        let mut escaped = false;
        let mut hex_digits = 0;
        loop {
            while self.buffer.is_empty() && !self.source_terminated() {
                self.buffer().await?;
            }
            if self.buffer.is_empty() {
                return Err(Error::unexpected_end());
            }
            let mut consumed = 0;
            let mut done = false;
            for &byte in &self.buffer {
                if len == output.len() {
                    break;
                }
                consumed += 1;
                if !escaped && hex_digits == 0 && byte == b'"' {
                    done = true;
                    break;
                }
                capacity_bound = capacity_bound
                    .checked_add(1)
                    .ok_or_else(|| de::Error::custom("string size overflow"))?;
                if hex_digits != 0 {
                    decode_hex_val(byte)
                        .ok_or_else(|| Error::invalid_utf8("invalid escape decoding hex escape"))?;
                    hex_digits -= 1;
                } else if escaped {
                    match byte {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                        b'u' => hex_digits = 4,
                        _ => return Err(Error::invalid_utf8("invalid escape character in string")),
                    }
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                    continue;
                } else if byte < 0x20 {
                    return Err(Error::invalid_utf8(format!(
                        "invalid control character in string: {byte}"
                    )));
                }
                // Match the ordinary decoder: it removes the escape backslash,
                // retaining the following bytes rather than expanding escapes.
                output[len] = byte;
                len += 1;
            }
            self.buffer.drain(..consumed);
            if len == output.len() || done {
                let valid = match std::str::from_utf8(&output[..len]) {
                    Ok(_) => len,
                    Err(error) if !done && error.error_len().is_none() => error.valid_up_to(),
                    Err(error) => return Err(Error::invalid_utf8(error)),
                };
                if valid != 0 {
                    inspect(de::Inspection::TextChunk(&output[..valid]))?;
                    output.copy_within(valid..len, 0);
                    len -= valid;
                }
            }
            if done {
                return inspect(de::Inspection::TextEnd { capacity_bound });
            }
        }
    }

    async fn inspect_value<F>(&mut self, inspect: &mut F) -> Result<(), Error>
    where
        F: for<'a> FnMut(de::Inspection<'a>) -> Result<(), Error> + Send,
    {
        let mut stack = Vec::new();
        loop {
            let kind = de::Decoder::peek_kind(self).await?;
            if kind != de::Kind::Leaf {
                let container = de::Decoder::open_container(self, kind, None).await?;
                if container.slot() == de::Slot::End {
                    inspect(container_observation(&container))?;
                } else {
                    stack.push(container);
                    continue;
                }
            } else {
                match self.peek().await?.ok_or_else(Error::unexpected_end)? {
                    b'"' => self.inspect_string(inspect).await?,
                    b'-' => {
                        self.eat_char().await?;
                        self.ignore_number().await?;
                    }
                    b'0'..=b'9' => self.ignore_number().await?,
                    b't' => self.ignore_exactly("true").await?,
                    b'f' => self.ignore_exactly("false").await?,
                    b'n' => self.ignore_exactly("null").await?,
                    byte => {
                        return Err(Error::invalid_utf8(format!(
                            "unexpected token ignoring value: {byte}"
                        )))
                    }
                }
            }
            loop {
                let Some(container) = stack.last_mut() else {
                    return Ok(());
                };
                de::Decoder::finish_child(self, container).await?;
                if container.slot() == de::Slot::End {
                    inspect(container_observation(container))?;
                    stack.pop();
                } else {
                    break;
                }
            }
        }
    }

    async fn maybe_delimiter(&mut self, delimiter: &'static [u8]) -> Result<bool, Error> {
        while self.buffer.is_empty() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.is_empty() {
            Ok(false)
        } else if self.buffer.starts_with(delimiter) {
            self.buffer.remove(0);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn parse_bool(&mut self) -> Result<bool, Error> {
        self.expect_whitespace().await?;

        while self.buffer.len() < TRUE.len() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.is_empty() {
            return Err(Error::unexpected_end());
        } else if self.buffer.starts_with(TRUE) {
            self.buffer.drain(0..TRUE.len());
            return Ok(true);
        }

        while self.buffer.len() < FALSE.len() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.is_empty() {
            return Err(Error::unexpected_end());
        } else if self.buffer.starts_with(FALSE) {
            self.buffer.drain(0..FALSE.len());
            return Ok(false);
        }

        let i = Ord::min(self.buffer.len(), SNIPPET_LEN);
        let unknown = String::from_utf8(self.buffer[..i].to_vec()).map_err(Error::invalid_utf8)?;
        Err(de::Error::invalid_value(unknown, "a boolean"))
    }

    async fn parse_number<N: FromStr>(&mut self) -> Result<N, Error>
    where
        <N as FromStr>::Err: fmt::Display,
    {
        self.expect_whitespace().await?;

        let numeric = self.numeric.clone();
        let i = self.buffer_while(|b| numeric.contains(&b)).await?;
        let n = String::from_utf8(self.buffer[0..i].to_vec()).map_err(Error::invalid_utf8)?;

        match n.parse() {
            Ok(number) => {
                self.buffer.drain(..i);
                Ok(number)
            }
            Err(cause) => Err(de::Error::invalid_value(cause, std::any::type_name::<N>())),
        }
    }

    async fn parse_string(&mut self) -> Result<String, Error> {
        let s = self.buffer_string().await?;
        String::from_utf8(s).map_err(Error::invalid_utf8)
    }

    async fn parse_unit(&mut self) -> Result<(), Error> {
        self.expect_whitespace().await?;

        while self.buffer.len() < NULL.len() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.starts_with(NULL) {
            self.buffer.drain(..NULL.len());
            Ok(())
        } else {
            let i = Ord::min(self.buffer.len(), SNIPPET_LEN);
            let as_str =
                String::from_utf8(self.buffer[..i].to_vec()).map_err(Error::invalid_utf8)?;

            Err(de::Error::invalid_type(as_str, "null"))
        }
    }

    async fn ignore_exactly(&mut self, s: &str) -> Result<(), Error> {
        for ch in s.as_bytes() {
            match self.peek().await?.as_ref() {
                None => return Err(Error::unexpected_end()),
                Some(next) if next == ch => self.eat_char().await?,
                Some(next) => {
                    return Err(Error::invalid_utf8(format!(
                        "invalid char {next}, expected {ch}"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn ignore_number(&mut self) -> Result<(), Error> {
        let ch = self.next_char().await?;
        match ch {
            Some(b'0') => {
                // There cannot be any leading zeroes.
                // If there is a leading '0', it cannot be followed by more digits.
                if let Some(b'0'..=b'9') = self.peek().await? {
                    return Err(Error::invalid_utf8("invalid number, two leading zeroes"));
                }
            }
            Some(b'1'..=b'9') => {
                while let Some(b'0'..=b'9') = self.peek().await? {
                    self.eat_char().await?;
                }
            }
            Some(ch) => {
                return Err(Error::invalid_utf8(format!("invalid number: {}", ch)));
            }
            None => return Err(Error::unexpected_end()),
        }

        match self.peek().await? {
            Some(b'.') => self.ignore_decimal().await,
            Some(b'e' | b'E') => self.ignore_exponent().await,
            _ => Ok(()),
        }
    }

    async fn ignore_decimal(&mut self) -> Result<(), Error> {
        self.eat_char().await?;

        let mut at_least_one_digit = false;
        while let Some(b'0'..=b'9') = self.peek().await? {
            self.eat_char().await?;
            at_least_one_digit = true;
        }

        if !at_least_one_digit {
            return Err(Error::invalid_utf8(
                "invalid number, expected at least one digit after decimal",
            ));
        }

        match self.peek().await? {
            Some(b'e' | b'E') => self.ignore_exponent().await,
            _ => Ok(()),
        }
    }

    async fn ignore_exponent(&mut self) -> Result<(), Error> {
        self.eat_char().await?;

        if let Some(b'+' | b'-') = self.peek().await? {
            self.eat_char().await?;
        }

        // Make sure a digit follows the exponent place.
        match self.next_char().await? {
            Some(b'0'..=b'9') => {}
            Some(ch) => {
                return Err(Error::invalid_utf8(format!(
                    "expected a digit to follow the exponent, found {ch}"
                )));
            }
            None => return Err(Error::unexpected_end()),
        }

        while let Some(b'0'..=b'9') = self.peek().await? {
            self.eat_char().await?;
        }

        Ok(())
    }
}

impl<S: Read> de::Decoder for Decoder<S> {
    type Error = Error;

    async fn peek_kind(&mut self) -> Result<de::Kind, Self::Error> {
        self.expect_whitespace().await?;
        let byte = self.peek().await?.ok_or_else(Error::unexpected_end)?;
        Ok(match byte {
            b'[' => de::Kind::Seq,
            b'{' => de::Kind::Map,
            _ => de::Kind::Leaf,
        })
    }

    async fn open_container(
        &mut self,
        kind: de::Kind,
        size_hint: Option<usize>,
    ) -> Result<de::Container, Self::Error> {
        let (begin, end) = match kind {
            de::Kind::Seq => (LIST_BEGIN, LIST_END),
            de::Kind::Map => (MAP_BEGIN, MAP_END),
            de::Kind::Leaf => return Err(de::Error::custom("a leaf is not a container")),
        };
        self.expect_whitespace().await?;
        self.expect_delimiter(begin).await?;
        self.push_depth()?;
        self.expect_whitespace().await?;
        let empty = self.maybe_delimiter(end).await?;
        if empty {
            self.pop_depth();
        }
        de::Container::new(kind, size_hint, empty)
    }

    async fn finish_child(&mut self, container: &mut de::Container) -> Result<(), Self::Error> {
        if container.slot() == de::Slot::End {
            return Err(de::Error::custom("container has already ended"));
        }
        if container.slot() == de::Slot::Key {
            self.expect_whitespace().await?;
            self.expect_delimiter(COLON).await?;
            self.expect_whitespace().await?;
            return container.advance(false);
        }
        self.expect_whitespace().await?;
        let end = if container.kind() == de::Kind::Map {
            MAP_END
        } else {
            LIST_END
        };
        let ended = self.maybe_delimiter(end).await?;
        if !ended {
            self.expect_delimiter(COMMA).await?;
        }
        container.advance(ended)?;
        if ended {
            self.pop_depth();
        }
        Ok(())
    }

    async fn inspect_any<F>(&mut self, mut inspect: F) -> Result<(), Self::Error>
    where
        F: for<'a> FnMut(de::Inspection<'a>) -> Result<(), Self::Error> + Send,
    {
        let depth = self.depth;
        let result = self.inspect_value(&mut inspect).await;
        self.depth = depth;
        result
    }

    async fn decode_any<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.expect_whitespace().await?;

        while self.buffer.is_empty() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.is_empty() {
            Err(Error::unexpected_end())
        } else if self.buffer.starts_with(QUOTE) {
            self.decode_string(visitor).await
        } else if self.buffer.starts_with(LIST_BEGIN) {
            self.decode_seq(visitor).await
        } else if self.buffer.starts_with(MAP_BEGIN) {
            self.decode_map(visitor).await
        } else if self.numeric.contains(&self.buffer[0]) {
            self.decode_number(visitor).await
        } else if (self.buffer.len() >= FALSE.len() && self.buffer.starts_with(FALSE))
            || (self.buffer.len() >= TRUE.len() && self.buffer.starts_with(TRUE))
        {
            self.decode_bool(visitor).await
        } else if self.buffer.len() >= NULL.len() && self.buffer.starts_with(NULL) {
            self.decode_option(visitor).await
        } else {
            while self.buffer.len() < TRUE.len() && !self.source_terminated() {
                self.buffer().await?;
            }

            if self.buffer.is_empty() {
                Err(Error::unexpected_end())
            } else if self.buffer.starts_with(TRUE) {
                self.decode_bool(visitor).await
            } else if self.buffer.starts_with(NULL) {
                self.decode_option(visitor).await
            } else {
                while self.buffer.len() < FALSE.len() && !self.source_terminated() {
                    self.buffer().await?;
                }

                if self.buffer.is_empty() {
                    Err(Error::unexpected_end())
                } else if self.buffer.starts_with(FALSE) {
                    self.decode_bool(visitor).await
                } else {
                    let i = Ord::min(self.buffer.len(), SNIPPET_LEN);
                    let s = String::from_utf8(self.buffer[0..i].to_vec())
                        .map_err(Error::invalid_utf8)?;

                    Err(de::Error::invalid_value(
                        s,
                        std::any::type_name::<V::Value>(),
                    ))
                }
            }
        }
    }

    async fn decode_bool<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let b = self.parse_bool().await?;
        visitor.visit_bool(b)
    }

    async fn decode_bytes<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.parse_string().await?;
        visitor.visit_string(s)
    }

    async fn decode_i8<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_number().await?;
        visitor.visit_i8(i)
    }

    async fn decode_i16<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_number().await?;
        visitor.visit_i16(i)
    }

    async fn decode_i32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_number().await?;
        visitor.visit_i32(i)
    }

    async fn decode_i64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_number().await?;
        visitor.visit_i64(i)
    }

    async fn decode_u8<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_number().await?;
        visitor.visit_u8(u)
    }

    async fn decode_u16<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_number().await?;
        visitor.visit_u16(u)
    }

    async fn decode_u32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_number().await?;
        visitor.visit_u32(u)
    }

    async fn decode_u64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_number().await?;
        visitor.visit_u64(u)
    }

    async fn decode_f32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let f = self.parse_number().await?;
        visitor.visit_f32(f)
    }

    async fn decode_f64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let f = self.parse_number().await?;
        visitor.visit_f64(f)
    }

    async fn decode_array_bool<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        // was getting an error about S not living long enough, so boxed based on
        // https://github.com/rust-lang/rust/issues/100013#issuecomment-2052045872
        // once this issue is closed, we can remove the `.boxed()`
        visitor.visit_array_bool(access).boxed().await
    }

    async fn decode_array_i8<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_bool(access).boxed().await
    }

    async fn decode_array_i16<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_i16(access).boxed().await
    }

    async fn decode_array_i32<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_i32(access).boxed().await
    }

    async fn decode_array_i64<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_i64(access).boxed().await
    }

    async fn decode_array_u8<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u8(access).boxed().await
    }

    async fn decode_array_u16<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u16(access).boxed().await
    }

    async fn decode_array_u32<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u32(access).boxed().await
    }

    async fn decode_array_u64<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u64(access).boxed().await
    }

    async fn decode_array_f32<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_f32(access).boxed().await
    }

    async fn decode_array_f64<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_f64(access).boxed().await
    }

    async fn decode_string<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.expect_whitespace().await?;

        let s = self.parse_string().await?;
        visitor.visit_string(s)
    }

    async fn decode_option<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.expect_whitespace().await?;

        while self.buffer.len() < NULL.len() && !self.source_terminated() {
            self.buffer().await?;
        }

        if self.buffer.starts_with(NULL) {
            self.buffer.drain(0..NULL.len());
            visitor.visit_none()
        } else {
            visitor.visit_some(self).await
        }
    }

    async fn decode_seq<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_seq(access).boxed().await
    }

    async fn decode_unit<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        self.parse_unit().await?;
        visitor.visit_unit()
    }

    async fn decode_uuid<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let s = self.parse_string().await?;
        visitor.visit_string(s)
    }

    async fn decode_tuple<V: Visitor>(
        &mut self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let access = SeqAccess::new(self, Some(len)).await?;
        visitor.visit_seq(access).boxed().await
    }

    async fn decode_map<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = MapAccess::new(self, None).await?;
        visitor.visit_map(access).boxed().await
    }

    async fn decode_ignored_any<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inspect_any(|_| Ok(())).await?;
        visitor.visit_unit()
    }
}

impl<S: Read> From<S> for Decoder<S> {
    fn from(source: S) -> Self {
        Self {
            source,
            buffer: vec![],
            pending: Bytes::new(),
            numeric: NUMERIC.iter().cloned().collect(),
            depth: 0,
        }
    }
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn decode<S: Stream<Item = Bytes> + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    let source = source.map(Result::<Bytes, Error>::Ok);
    let mut decoder = Decoder::from(SourceStream::from(source));

    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.ensure_eof().await?;
    Ok(decoded)
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn try_decode<
    E: fmt::Display,
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    T: FromStream,
>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    let mut decoder = Decoder::from_stream(source.map_err(|e| de::Error::custom(e)));
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.ensure_eof().await?;
    Ok(decoded)
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
#[cfg(feature = "tokio-io")]
/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn read_from<S: AsyncReadExt + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    let mut decoder = Decoder::from(SourceReader::from(source));
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.ensure_eof().await?;
    Ok(decoded)
}

fn decode_hex_val(val: u8) -> Option<u16> {
    let n = HEX[val as usize] as u16;
    if n == 255 {
        None
    } else {
        Some(n)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::max;

    use futures::stream;
    use test_case::test_case;

    use super::*;

    /// Character consumption crosses chunks and reports EOF without a payload.
    #[tokio::test]
    async fn test_next_char() {
        let s = b"bar";
        for num_chunks in (1..s.len()).rev() {
            let source = stream::iter(s.iter().copied())
                .chunks(num_chunks)
                .map(Bytes::from)
                .map(Result::<Bytes, Error>::Ok);

            let mut decoder = Decoder::from_stream(source);
            for expected in s {
                let actual = decoder.next_char().await.unwrap().unwrap();
                assert_eq!(&actual, expected);
            }
            assert_eq!(decoder.next_char().await.unwrap(), None);
        }
    }

    fn test_decoder(
        source: &str,
    ) -> Decoder<SourceStream<impl Stream<Item = Result<Bytes, Error>> + '_>> {
        let chunk_size = max(source.len(), 1);
        let source = stream::iter(source.as_bytes().iter().copied())
            .chunks(chunk_size)
            .map(Bytes::from)
            .map(Result::<Bytes, Error>::Ok);

        Decoder::from_stream(source)
    }

    /// ignore_exactly takes a vector of bytes, and consumes exactly those characters.
    #[test_case("foo", "foo", true, 0; "ignore foo")]
    #[test_case("foobar", "foo", true, 3; "ignore foo not bar")]
    #[test_case("foobar", "bar", false, 6; "wrong expected str")]
    #[test_case("", "", true, 0; "empty good")]
    #[test_case("", "a", false, 0; "empty bad")]
    #[tokio::test]
    async fn test_ignore_exactly(source: &str, to_ignore: &str, success: bool, chars_left: usize) {
        let mut decoder = test_decoder(source);
        let res = decoder.ignore_exactly(to_ignore).await;

        assert_eq!(res.is_ok(), success);
        assert_eq!(decoder.buffer.len(), chars_left);
    }

    #[test_case(r#""""#, Ok(0); "empty string")]
    #[test_case(r#""","#, Ok(1); "empty string then leave char")]
    #[test_case("\"foo\"bar", Ok(3); "ends correctly")]
    #[test_case("\"test\"", Ok(0); "string value")]
    #[test_case("\"\"", Ok(0); "empty")]
    #[test_case("\"\\r\"", Ok(0); "carriage return")]
    #[test_case("\"hello\"world\"", Ok(6); "multiple quotes")]
    #[test_case("\"   hello\"", Ok(0); "whitespace before")]
    #[test_case("\"hello   \"   ", Ok(3); "whitespace after")]
    #[test_case("\"\\t\\n\\r\"", Ok(0); "whitespace chars")]
    #[test_case("\"\"test\\\"", Ok(6); "chars after empty string")]
    #[test_case("\"\\\\\\\\\"", Ok(0); "backslashes")]
    #[test_case("", Err(Error::unexpected_end()); "eof")]
    #[test_case(r#""a"#, Err(Error::unexpected_end()); "unterminatedstring")]
    #[test_case("\"\x01\"", Err(Error::invalid_utf8("invalid control character in string: 1")); "invalid control char")]
    #[test_case(r#""\u00""#, Err(Error::invalid_utf8("invalid escape decoding hex escape")); "unfinished hex char")]
    #[test_case(
        r#""\x01""#,
        Err(Error::invalid_utf8("invalid escape character in string"))
    )]
    #[tokio::test]
    async fn test_ignore_string(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);

        let res = decoder.inspect_string(&mut |_| Ok(())).await;

        match expected {
            Ok(end_length) => assert_eq!(decoder.buffer.len(), end_length),
            Err(e) => assert_eq!(Err(e), res),
        }
    }

    #[test_case("-123", Ok(0); "negative number")]
    #[test_case("-123.45", Ok(0); "negative float")]
    #[test_case("abc", Err(Error::invalid_utf8("unexpected token ignoring value: 97")); "non number")]
    #[test_case("", Err(Error::unexpected_end()); "empty source")]
    #[tokio::test]
    async fn test_ignore_value(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);

        // Shared inspection handles the leading sign before numerical traversal.
        let res = de::Decoder::inspect_any(&mut decoder, |_| Ok(())).await;

        if let Ok(end_length) = expected {
            res.unwrap();
            assert_eq!(decoder.buffer.len(), end_length);
        } else {
            assert_eq!(res.unwrap_err(), expected.unwrap_err())
        }
    }

    #[test_case("0", Ok(0); "zero")]
    #[test_case("00", Err(Error::invalid_utf8("invalid number, two leading zeroes")); "double zero")]
    #[test_case("123", Ok(0); "positive number")]
    #[test_case("123.45", Ok(0); "positive float")]
    #[test_case("0.0", Ok(0); "zero float")]
    #[test_case("123, 45", Ok(4); "parses only one number")]
    #[test_case("1e30, 45", Ok(4); "parses exponent")]
    #[test_case("1.2e3, 45", Ok(4); "parses decimal exponent")]
    #[test_case("abc", Err(Error::invalid_utf8("invalid number: 97")); "unexpected token")]
    #[test_case("", Err(Error::unexpected_end()); "unexpected end")]
    #[test_case("1.", Err(Error::invalid_utf8("invalid number, expected at least one digit after decimal")); "expected a number after the decimal")]
    #[test_case("1.1e-1", Ok(0); "negative exponent")]
    #[test_case("1.1e-a", Err(Error::invalid_utf8("expected a digit to follow the exponent, found 97")); "invalid exponent")]
    #[test_case("1.1e", Err(Error::unexpected_end()); "unterminated number")]
    #[tokio::test]
    async fn test_ignore_number(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);
        let res = decoder.ignore_number().await;

        if let Ok(end_length) = expected {
            res.unwrap();
            assert_eq!(decoder.buffer.len(), end_length);
        } else {
            assert_eq!(res.unwrap_err(), expected.unwrap_err())
        }
    }

    #[test_case("[]", Ok(0); "empty array")]
    #[test_case("[1]", Ok(0); "single array")]
    #[test_case("[ ] ", Ok(1); "whitespace empty array")]
    #[test_case("[ 1 ] ", Ok(1); "whitespace single array")]
    #[test_case("[1,2]", Ok(0); "multi array")]
    #[test_case("[],[]", Ok(3); "ends correctly")]
    #[test_case("[\"foo\",\"bar\"]", Ok(0); "string array")]
    #[test_case(r#""#, Err(Error::unexpected_end()); "unexpected end")]
    #[test_case(r#"["test""test"]"#, Err(de::Error::custom(r#"unexpected delimiter ", expected , at `"test"]`..."#)); "no comma")]
    #[tokio::test]
    async fn test_ignore_array(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);
        let res = de::Decoder::inspect_any(&mut decoder, |_| Ok(())).await;

        match expected {
            Ok(end_length) => assert_eq!(decoder.buffer.len(), end_length),
            Err(e) => assert_eq!(Err(e), res),
        }
    }

    #[test_case("{}", Ok(0); "empty object")]
    #[test_case("{},{}", Ok(3); "ends correctly")]
    #[test_case(r#"{"k":2, "k":3}"#, Ok(0); "multi object")]
    #[test_case(r#"{"k":1}"#, Ok(0); "single object")]
    #[test_case(r#"{"foo":"bar"}"#, Ok(0); "string value")]
    #[test_case(r#"{ } "#, Ok(1); "whitespace empty object")]
    #[test_case(r#"{"k" : 2 , " k " : 3 }"#, Ok(0); "whitespace multi object")]
    #[test_case(r#"{ " k " : 1 } "#, Ok(1); "whitespace single object")]
    #[test_case(r#"{"k""v"}"#, Err(de::Error::custom(r#"unexpected delimiter ", expected : at `"v"}`..."#)); "missing colon")]
    #[test_case(r#"{"k","v"}"#, Err(de::Error::custom(r#"unexpected delimiter ,, expected : at `,"v"}`..."#)); "comma when expecting colon")]
    #[test_case(r#"{,"k":"v"}"#, Err(Error::invalid_utf8("unexpected token ignoring value: 44")); "comma when expecting value")]
    #[test_case(r#"{"k":"v"asdf}"#, Err(de::Error::custom("unexpected delimiter a, expected , at `asdf}`...")); "value when expecting comma")]
    #[tokio::test]
    async fn test_ignore_object(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);
        let res = de::Decoder::inspect_any(&mut decoder, |_| Ok(())).await;

        match expected {
            Err(e) => assert_eq!(Err(e), res),
            Ok(end_length) => assert_eq!(decoder.buffer.len(), end_length),
        }
    }
}

#[cfg(test)]
mod inspection_tests;

#[cfg(test)]
mod shallow_tests;
