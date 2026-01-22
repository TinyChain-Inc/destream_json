//! Decode a JSON stream to a Rust data structure.

use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
use destream::{de, FromStream, Visitor};
use futures::stream::{Fuse, FusedStream, Stream, StreamExt, TryStreamExt};

#[cfg(feature = "tokio-io")]
use bytes::BytesMut;
#[cfg(feature = "tokio-io")]
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::constants::*;

const SNIPPET_LEN: usize = 50;
const DEFAULT_MAX_DEPTH: usize = 1024;

#[derive(Debug)]
enum IgnoreFrame {
    Array,
    Object { expecting_key: bool },
}

#[derive(Copy, Clone)]
enum IgnorePhase {
    Value,
    AfterValue,
}

/// Methods common to any decodable [`Stream`]
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
    scratch: BytesMut,
}

#[cfg(feature = "tokio-io")]
impl<R: AsyncRead + Send + Unpin> Read for SourceReader<R> {
    async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        self.scratch.clear();
        match self.reader.read_buf(&mut self.scratch).await {
            Ok(0) => {
                self.terminated = true;
                None
            }
            Ok(size) => {
                debug_assert_eq!(self.scratch.len(), size);
                Some(Ok(self.scratch.split().freeze()))
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
            scratch: BytesMut::new(),
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
    size_hint: Option<usize>,
    done: bool,
}

impl<'a, S: Read + 'a> MapAccess<'a, S> {
    async fn new(
        decoder: &'a mut Decoder<S>,
        size_hint: Option<usize>,
    ) -> Result<MapAccess<'a, S>, Error> {
        decoder.expect_whitespace().await?;

        decoder.expect_delimiter(MAP_BEGIN).await?;
        decoder.push_depth()?;
        decoder.expect_whitespace().await?;

        let done = decoder.maybe_delimiter(MAP_END).await?;
        if done {
            decoder.pop_depth();
        }

        Ok(MapAccess {
            decoder,
            size_hint,
            done,
        })
    }
}

impl<'a, S: Read + 'a> de::MapAccess for MapAccess<'a, S> {
    type Error = Error;

    async fn next_key<K: FromStream>(&mut self, context: K::Context) -> Result<Option<K>, Error> {
        if self.done {
            return Ok(None);
        }

        self.decoder.expect_whitespace().await?;
        let key = K::from_stream(context, self.decoder).await?;

        self.decoder.expect_whitespace().await?;
        self.decoder.expect_delimiter(COLON).await?;
        self.decoder.expect_whitespace().await?;

        Ok(Some(key))
    }

    async fn next_value<V: FromStream>(&mut self, context: V::Context) -> Result<V, Error> {
        if self.done {
            return Err(de::Error::custom(
                "called MapAccess::next_value but the map has already ended",
            ));
        }

        let value = V::from_stream(context, self.decoder).await?;

        self.decoder.expect_whitespace().await?;

        if self.decoder.maybe_delimiter(MAP_END).await? {
            self.done = true;
            self.decoder.pop_depth();
        } else {
            self.decoder.expect_delimiter(COMMA).await?;
        }

        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        self.size_hint
    }
}

struct SeqAccess<'a, S> {
    decoder: &'a mut Decoder<S>,
    size_hint: Option<usize>,
    done: bool,
}

impl<'a, S: Read + 'a> SeqAccess<'a, S> {
    async fn new(
        decoder: &'a mut Decoder<S>,
        size_hint: Option<usize>,
    ) -> Result<SeqAccess<'a, S>, Error> {
        decoder.expect_whitespace().await?;
        decoder.expect_delimiter(LIST_BEGIN).await?;
        decoder.push_depth()?;
        decoder.expect_whitespace().await?;

        let done = decoder.maybe_delimiter(LIST_END).await?;
        if done {
            decoder.pop_depth();
        }

        Ok(SeqAccess {
            decoder,
            size_hint,
            done,
        })
    }
}

impl<'a, S: Read + 'a> de::SeqAccess for SeqAccess<'a, S> {
    type Error = Error;

    async fn next_element<T: FromStream>(
        &mut self,
        context: T::Context,
    ) -> Result<Option<T>, Self::Error> {
        if self.done {
            return Ok(None);
        }

        self.decoder.expect_whitespace().await?;
        let value = T::from_stream(context, self.decoder).await?;
        self.decoder.expect_whitespace().await?;

        if self.decoder.maybe_delimiter(LIST_END).await? {
            self.done = true;
            self.decoder.pop_depth();
        } else {
            self.decoder.expect_delimiter(COMMA).await?;
        }

        Ok(Some(value))
    }

    fn size_hint(&self) -> Option<usize> {
        self.size_hint
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
    buffer: Vec<u8>,
    offset: usize,
    max_depth: usize,
    depth: usize,
}

#[cfg(feature = "tokio-io")]
impl<A: AsyncRead> Decoder<A>
where
    SourceReader<A>: Read,
{
    pub fn from_reader(reader: A) -> Decoder<SourceReader<A>> {
        Decoder::with_max_depth(SourceReader::from(reader), DEFAULT_MAX_DEPTH)
    }
}

impl<S> Decoder<S> {
    fn with_max_depth(source: S, max_depth: usize) -> Self {
        Self {
            source,
            buffer: Vec::new(),
            offset: 0,
            max_depth,
            depth: 0,
        }
    }

    fn push_depth(&mut self) -> Result<(), Error> {
        if self.depth >= self.max_depth {
            return Err(de::Error::custom(format!(
                "nesting depth limit exceeded (max {})",
                self.max_depth
            )));
        }

        self.depth += 1;
        Ok(())
    }

    fn pop_depth(&mut self) {
        debug_assert!(self.depth > 0);
        self.depth -= 1;
    }

    fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.offset)
    }

    fn available(&self) -> &[u8] {
        &self.buffer[self.offset..]
    }

    fn maybe_compact(&mut self) {
        if self.offset == 0 {
            return;
        }

        if self.offset == self.buffer.len() {
            self.buffer.clear();
            self.offset = 0;
        } else if self.offset > 8192 && self.offset > self.buffer.len() / 2 {
            self.buffer.drain(..self.offset);
            self.offset = 0;
        }
    }

    fn consume(&mut self, n: usize) {
        debug_assert!(self.remaining() >= n);
        self.offset += n;
        self.maybe_compact();
    }

    fn contents(&self, max_len: usize) -> Result<String, Error> {
        let buf = self.available();
        let len = Ord::min(buf.len(), max_len);
        String::from_utf8(buf[..len].to_vec()).map_err(Error::invalid_utf8)
    }
}

impl<S: Stream> Decoder<SourceStream<S>>
where
    SourceStream<S>: Read,
{
    /// Construct a new [`Decoder`] from the given source `stream`.
    pub fn from_stream(stream: S) -> Decoder<SourceStream<S>> {
        Decoder::with_max_depth(SourceStream::from(stream), DEFAULT_MAX_DEPTH)
    }
}

impl<S: Read> Decoder<S> {
    /// Return `true` if this [`Decoder`] has no more data to be decoded.
    pub fn is_terminated(&self) -> bool {
        self.source.is_terminated() && self.remaining() == 0
    }

    async fn ignore_inner(
        &mut self,
        mut stack: Vec<IgnoreFrame>,
        allow_empty: bool,
    ) -> Result<(), Error> {
        let mut phase = IgnorePhase::Value;

        loop {
            match phase {
                IgnorePhase::Value => {
                    self.expect_whitespace().await?;

                    while self.remaining() == 0 && !self.source.is_terminated() {
                        self.buffer().await?;
                    }

                    if self.remaining() == 0 {
                        if allow_empty && stack.is_empty() {
                            return Ok(());
                        } else {
                            return Err(Error::unexpected_end());
                        }
                    }

                    match self.available()[0] {
                        b'"' => self.ignore_string().await?,
                        b'-' => {
                            self.eat_char().await?;
                            self.ignore_number().await?;
                        }
                        b'0'..=b'9' => self.ignore_number().await?,
                        b't' => self.ignore_exactly("true").await?,
                        b'f' => self.ignore_exactly("false").await?,
                        b'n' => self.ignore_exactly("null").await?,
                        b'[' => {
                            self.eat_char().await?; // '['
                            self.push_depth()?;
                            self.expect_whitespace().await?;
                            if self.peek().await? == Some(b']') {
                                self.eat_char().await?;
                                self.pop_depth();
                            } else {
                                stack.push(IgnoreFrame::Array);
                                phase = IgnorePhase::Value;
                                continue;
                            }
                        }
                        b'{' => {
                            self.eat_char().await?; // '{'
                            self.push_depth()?;
                            self.expect_whitespace().await?;
                            if self.peek().await? == Some(b'}') {
                                self.eat_char().await?;
                                self.pop_depth();
                            } else {
                                stack.push(IgnoreFrame::Object {
                                    expecting_key: true,
                                });
                                phase = IgnorePhase::Value;
                                continue;
                            }
                        }
                        other => {
                            return Err(Error::invalid_utf8(format!(
                                "unexpected token ignoring value: {other}"
                            )))
                        }
                    }

                    phase = IgnorePhase::AfterValue;
                }
                IgnorePhase::AfterValue => loop {
                    match stack.last_mut() {
                        None => return Ok(()),
                        Some(IgnoreFrame::Array) => {
                            self.expect_whitespace().await?;
                            match self.peek().await? {
                                Some(b',') => {
                                    self.eat_char().await?;
                                    phase = IgnorePhase::Value;
                                    break;
                                }
                                Some(b']') => {
                                    self.eat_char().await?;
                                    self.pop_depth();
                                    stack.pop();
                                    continue;
                                }
                                Some(ch) => {
                                    return Err(Error::invalid_utf8(format!(
                                        "invalid char {ch}, expected , or ]"
                                    )))
                                }
                                None => return Err(Error::unexpected_end()),
                            }
                        }
                        Some(IgnoreFrame::Object { expecting_key }) => {
                            if *expecting_key {
                                self.expect_whitespace().await?;
                                self.ignore_exactly(":").await?;
                                *expecting_key = false;
                                phase = IgnorePhase::Value;
                                break;
                            } else {
                                self.expect_whitespace().await?;
                                match self.peek().await? {
                                    Some(b',') => {
                                        self.eat_char().await?;
                                        *expecting_key = true;
                                        phase = IgnorePhase::Value;
                                        break;
                                    }
                                    Some(b'}') => {
                                        self.eat_char().await?;
                                        self.pop_depth();
                                        stack.pop();
                                        continue;
                                    }
                                    Some(ch) => {
                                        return Err(Error::invalid_utf8(format!(
                                            "invalid char {ch}, expected , or }}"
                                        )))
                                    }
                                    None => return Err(Error::unexpected_end()),
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    async fn buffer(&mut self) -> Result<(), Error> {
        if let Some(data) = self.source.next().await {
            self.maybe_compact();
            self.buffer.extend(data?);
        }

        Ok(())
    }

    async fn parse_hex_escape(&mut self) -> Result<u16, Error> {
        let mut n: u16 = 0;
        for _ in 0..4 {
            let ch = self.next_or_eof().await?;
            let val = decode_hex_val(ch).ok_or_else(|| {
                de::Error::custom(format!("invalid escape decoding hex escape: {ch}"))
            })?;
            n = (n << 4) + val;
        }

        Ok(n)
    }

    async fn buffer_while<F: Fn(u8) -> bool>(&mut self, cond: F) -> Result<usize, Error> {
        let mut i = 0;
        loop {
            while i >= self.remaining() && !self.source.is_terminated() {
                self.buffer().await?;
            }

            match self.available().get(i) {
                Some(b) if cond(*b) => i += 1,
                Some(_) => break,
                None if self.source.is_terminated() => return Ok(i),
                None => continue,
            }
        }

        Ok(i)
    }

    async fn peek(&mut self) -> Result<Option<u8>, Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            Ok(None)
        } else {
            Ok(Some(self.available()[0]))
        }
    }

    async fn next_char(&mut self) -> Result<Option<u8>, Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            Ok(None)
        } else {
            let b = self.available()[0];
            self.consume(1);
            Ok(Some(b))
        }
    }

    async fn eat_char(&mut self) -> Result<(), Error> {
        self.next_char().await?;
        Ok(())
    }

    async fn next_or_eof(&mut self) -> Result<u8, Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }
        if self.remaining() == 0 {
            Err(Error::unexpected_end())
        } else {
            let b = self.available()[0];
            self.consume(1);
            Ok(b)
        }
    }

    async fn decode_number<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Error> {
        let mut i = 0;
        loop {
            while i >= self.remaining() && !self.source.is_terminated() {
                self.buffer().await?;
            }

            if i >= self.remaining() {
                return de::Decoder::decode_i64(self, visitor).await;
            }

            let b = self.available()[i];
            if b == DECIMAL[0] || b == E[0] || b == b'E' {
                return de::Decoder::decode_f64(self, visitor).await;
            } else if !is_numeric_byte(b) {
                return de::Decoder::decode_i64(self, visitor).await;
            }

            i += 1;
        }
    }

    async fn expect_delimiter(&mut self, delimiter: &'static [u8]) -> Result<(), Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        }

        if self.available().starts_with(delimiter) {
            self.consume(delimiter.len());
            Ok(())
        } else {
            let contents = self.contents(SNIPPET_LEN)?;
            Err(de::Error::custom(format!(
                "unexpected delimiter {}, expected {} at `{}`...",
                self.available()[0] as char,
                delimiter[0] as char,
                contents
            )))
        }
    }

    async fn expect_whitespace(&mut self) -> Result<(), Error> {
        let i = self.buffer_while(is_json_whitespace_byte).await?;
        self.consume(i);
        Ok(())
    }

    async fn ignore_value(&mut self) -> Result<(), Error> {
        self.ignore_inner(Vec::new(), true).await
    }

    async fn ignore_string(&mut self) -> Result<(), Error> {
        // eat the first char, which is a quote
        self.eat_char().await?;
        loop {
            if self.remaining() == 0 {
                self.buffer().await?;
            }

            if self.remaining() == 0 && self.source.is_terminated() {
                return Err(Error::unexpected_end());
            }

            let ch = self.next_or_eof().await?;
            if !ESCAPE_CHARS[ch as usize] {
                continue;
            }

            match ch {
                b'"' => {
                    return Ok(());
                }
                b'\\' => {
                    self.ignore_escaped_char().await?;
                }
                ch => {
                    return Err(Error::invalid_utf8(format!(
                        "invalid control character in string: {ch}"
                    )));
                }
            }
        }
    }

    /// Parses a JSON escape sequence and discards the value. Assumes the previous
    /// byte read was a backslash.
    async fn ignore_escaped_char(&mut self) -> Result<(), Error> {
        let ch = self.next_or_eof().await?;

        match ch {
            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
            b'u' => {
                // At this point we don't care if the codepoint is valid. We just
                // want to consume it. We don't actually know what is valid or not
                // at this point, because that depends on if this string will
                // ultimately be parsed into a string or a byte buffer in the "real"
                // parse.

                self.decode_hex_escape().await?;
            }
            _ => {
                return Err(Error::invalid_utf8("invalid escape character in string"));
            }
        }

        Ok(())
    }

    async fn decode_hex_escape(&mut self) -> Result<u16, Error> {
        let mut n = 0;
        for _ in 0..4 {
            let ch = decode_hex_val(self.next_or_eof().await?);
            match ch {
                None => return Err(Error::invalid_utf8("invalid escape decoding hex escape")),
                Some(val) => {
                    n = (n << 4) + val;
                }
            }
        }
        Ok(n)
    }

    async fn maybe_delimiter(&mut self, delimiter: &'static [u8]) -> Result<bool, Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            Ok(false)
        } else if self.available().starts_with(delimiter) {
            self.consume(delimiter.len());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn parse_bool(&mut self) -> Result<bool, Error> {
        self.expect_whitespace().await?;

        while self.remaining() < TRUE.len() && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        } else if self.available().starts_with(TRUE) {
            self.consume(TRUE.len());
            return Ok(true);
        }

        while self.remaining() < FALSE.len() && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        } else if self.available().starts_with(FALSE) {
            self.consume(FALSE.len());
            return Ok(false);
        }

        let buf = self.available();
        let i = Ord::min(buf.len(), SNIPPET_LEN);
        let unknown = String::from_utf8(buf[..i].to_vec()).map_err(Error::invalid_utf8)?;
        Err(de::Error::invalid_value(unknown, "a boolean"))
    }

    async fn parse_number<N: FromStr>(&mut self) -> Result<N, Error>
    where
        <N as FromStr>::Err: fmt::Display,
    {
        self.expect_whitespace().await?;

        let mut encoded = Vec::<u8>::new();

        // optional leading '-'
        if self.peek().await? == Some(b'-') {
            encoded.push(b'-');
            self.eat_char().await?;
        }

        // integer part
        match self.peek().await? {
            Some(b'0') => {
                encoded.push(b'0');
                self.eat_char().await?;

                if matches!(self.peek().await?, Some(b'0'..=b'9')) {
                    return Err(de::Error::custom("invalid number: leading zero"));
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.peek().await?, Some(b'0'..=b'9')) {
                    let digit = self.next_or_eof().await?;
                    encoded.push(digit);
                }
            }
            Some(other) => {
                return Err(de::Error::invalid_value(other as char, "a JSON number"));
            }
            None => return Err(Error::unexpected_end()),
        }

        // fraction
        if self.peek().await? == Some(b'.') {
            encoded.push(b'.');
            self.eat_char().await?;

            if !matches!(self.peek().await?, Some(b'0'..=b'9')) {
                return Err(de::Error::custom(
                    "invalid number: expected at least one digit after decimal point",
                ));
            }

            while matches!(self.peek().await?, Some(b'0'..=b'9')) {
                let digit = self.next_or_eof().await?;
                encoded.push(digit);
            }
        }

        // exponent
        if matches!(self.peek().await?, Some(b'e' | b'E')) {
            let e = self.next_or_eof().await?;
            encoded.push(e);

            if matches!(self.peek().await?, Some(b'+' | b'-')) {
                let sign = self.next_or_eof().await?;
                encoded.push(sign);
            }

            if !matches!(self.peek().await?, Some(b'0'..=b'9')) {
                return Err(de::Error::custom(
                    "invalid number: expected at least one digit after exponent",
                ));
            }

            while matches!(self.peek().await?, Some(b'0'..=b'9')) {
                let digit = self.next_or_eof().await?;
                encoded.push(digit);
            }
        }

        let n = std::str::from_utf8(&encoded).map_err(Error::invalid_utf8)?;

        match n.parse() {
            Ok(number) => Ok(number),
            Err(cause) => Err(de::Error::invalid_value(cause, std::any::type_name::<N>())),
        }
    }

    async fn parse_string(&mut self) -> Result<String, Error> {
        self.expect_delimiter(QUOTE).await?;

        let mut out = Vec::<u8>::new();
        loop {
            let ch = self.next_or_eof().await?;
            match ch {
                b'"' => break,
                b'\\' => {
                    let esc = self.next_or_eof().await?;
                    match esc {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(BACKSPACE),
                        b'f' => out.push(FORM_FEED),
                        b'n' => out.push(LF),
                        b'r' => out.push(CR),
                        b't' => out.push(TAB),
                        b'u' => {
                            let code_unit = self.parse_hex_escape().await?;
                            let ch = if (0xD800..=0xDBFF).contains(&code_unit) {
                                // high surrogate; must be followed by a low surrogate escape
                                self.expect_delimiter(ESCAPE).await?;
                                let u = self.next_or_eof().await?;
                                if u != b'u' {
                                    return Err(de::Error::custom(format!(
                                        "invalid unicode escape: expected \\u after high surrogate, found \\{u}"
                                    )));
                                }

                                let low = self.parse_hex_escape().await?;
                                if !(0xDC00..=0xDFFF).contains(&low) {
                                    return Err(de::Error::custom(
                                        "invalid unicode escape: expected low surrogate",
                                    ));
                                }

                                let hi = (code_unit as u32) - 0xD800;
                                let lo = (low as u32) - 0xDC00;
                                let codepoint = 0x10000 + ((hi << 10) | lo);
                                char::from_u32(codepoint).ok_or_else(|| {
                                    de::Error::custom("invalid unicode escape: invalid codepoint")
                                })?
                            } else if (0xDC00..=0xDFFF).contains(&code_unit) {
                                return Err(de::Error::custom(
                                    "invalid unicode escape: unexpected low surrogate",
                                ));
                            } else {
                                char::from_u32(code_unit as u32).ok_or_else(|| {
                                    de::Error::custom("invalid unicode escape: invalid codepoint")
                                })?
                            };

                            let mut buf = [0_u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        other => {
                            return Err(de::Error::custom(format!(
                                "invalid escape character in string: {other}"
                            )));
                        }
                    }
                }
                0x00..=0x1F => {
                    return Err(de::Error::custom(format!(
                        "invalid control character in string: {ch}"
                    )));
                }
                other => out.push(other),
            }
        }

        String::from_utf8(out).map_err(Error::invalid_utf8)
    }

    async fn parse_unit(&mut self) -> Result<(), Error> {
        self.expect_whitespace().await?;

        while self.remaining() < NULL.len() && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.available().starts_with(NULL) {
            self.consume(NULL.len());
            Ok(())
        } else {
            let buf = self.available();
            let i = Ord::min(buf.len(), SNIPPET_LEN);
            let as_str = String::from_utf8(buf[..i].to_vec()).map_err(Error::invalid_utf8)?;

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

    #[cfg(test)]
    async fn ignore_array(&mut self) -> Result<(), Error> {
        self.expect_whitespace().await?;
        self.expect_delimiter(LIST_BEGIN).await?;
        self.push_depth()?;
        self.expect_whitespace().await?;

        if self.peek().await? == Some(b']') {
            self.eat_char().await?;
            self.pop_depth();
            Ok(())
        } else {
            self.ignore_inner(vec![IgnoreFrame::Array], false).await
        }
    }

    #[cfg(test)]
    async fn ignore_object(&mut self) -> Result<(), Error> {
        self.expect_whitespace().await?;
        self.expect_delimiter(MAP_BEGIN).await?;
        self.push_depth()?;
        self.expect_whitespace().await?;

        if self.peek().await? == Some(b'}') {
            self.eat_char().await?;
            self.pop_depth();
            Ok(())
        } else {
            self.ignore_inner(
                vec![IgnoreFrame::Object {
                    expecting_key: true,
                }],
                false,
            )
            .await
        }
    }
}

impl<S: Read> de::Decoder for Decoder<S> {
    type Error = Error;

    async fn decode_any<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.expect_whitespace().await?;

        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            Err(Error::unexpected_end())
        } else if self.available().starts_with(QUOTE) {
            self.decode_string(visitor).await
        } else if self.available().starts_with(LIST_BEGIN) {
            self.decode_seq(visitor).await
        } else if self.available().starts_with(MAP_BEGIN) {
            self.decode_map(visitor).await
        } else if is_number_start(self.available()[0]) {
            self.decode_number(visitor).await
        } else if (self.remaining() >= FALSE.len() && self.available().starts_with(FALSE))
            || (self.remaining() >= TRUE.len() && self.available().starts_with(TRUE))
        {
            self.decode_bool(visitor).await
        } else if self.remaining() >= NULL.len() && self.available().starts_with(NULL) {
            self.decode_option(visitor).await
        } else {
            while self.remaining() < TRUE.len() && !self.source.is_terminated() {
                self.buffer().await?;
            }

            if self.remaining() == 0 {
                Err(Error::unexpected_end())
            } else if self.available().starts_with(TRUE) {
                self.decode_bool(visitor).await
            } else if self.available().starts_with(NULL) {
                self.decode_option(visitor).await
            } else {
                while self.remaining() < FALSE.len() && !self.source.is_terminated() {
                    self.buffer().await?;
                }

                if self.remaining() == 0 {
                    Err(Error::unexpected_end())
                } else if self.available().starts_with(FALSE) {
                    self.decode_bool(visitor).await
                } else {
                    let buf = self.available();
                    let i = Ord::min(buf.len(), SNIPPET_LEN);
                    let s = String::from_utf8(buf[0..i].to_vec()).map_err(Error::invalid_utf8)?;

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
        visitor.visit_array_bool(access).await
    }

    async fn decode_array_i8<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_bool(access).await
    }

    async fn decode_array_i16<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_i16(access).await
    }

    async fn decode_array_i32<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_i32(access).await
    }

    async fn decode_array_i64<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_i64(access).await
    }

    async fn decode_array_u8<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u8(access).await
    }

    async fn decode_array_u16<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u16(access).await
    }

    async fn decode_array_u32<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u32(access).await
    }

    async fn decode_array_u64<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_u64(access).await
    }

    async fn decode_array_f32<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_f32(access).await
    }

    async fn decode_array_f64<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<<V as Visitor>::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_array_f64(access).await
    }

    async fn decode_string<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.expect_whitespace().await?;

        let s = self.parse_string().await?;
        visitor.visit_string(s)
    }

    async fn decode_option<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.expect_whitespace().await?;

        while self.remaining() < NULL.len() && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.available().starts_with(NULL) {
            self.consume(NULL.len());
            visitor.visit_none()
        } else {
            visitor.visit_some(self).await
        }
    }

    async fn decode_seq<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_seq(access).await
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
        visitor.visit_seq(access).await
    }

    async fn decode_map<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = MapAccess::new(self, None).await?;
        visitor.visit_map(access).await
    }

    async fn decode_ignored_any<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.ignore_value().await?;
        visitor.visit_unit()
    }
}

impl<S: Read> From<S> for Decoder<S> {
    fn from(source: S) -> Self {
        Decoder::with_max_depth(source, DEFAULT_MAX_DEPTH)
    }
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn decode<S: Stream<Item = Bytes> + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    decode_with_max_depth(context, source, DEFAULT_MAX_DEPTH).await
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context,
/// enforcing a maximum nesting depth for arrays/maps.
pub async fn decode_with_max_depth<S: Stream<Item = Bytes> + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
    max_depth: usize,
) -> Result<T, Error> {
    let source = source.map(Result::<Bytes, Error>::Ok);
    let mut decoder = Decoder::with_max_depth(SourceStream::from(source), max_depth);

    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.expect_whitespace().await?;

    if decoder.is_terminated() {
        Ok(decoded)
    } else {
        let buffer = decoder.contents(SNIPPET_LEN)?;
        Err(de::Error::custom(format!(
            "expected end of stream, found `{}...`",
            buffer
        )))
    }
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
    try_decode_with_max_depth(context, source, DEFAULT_MAX_DEPTH).await
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context,
/// enforcing a maximum nesting depth for arrays/maps.
pub async fn try_decode_with_max_depth<
    E: fmt::Display,
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    T: FromStream,
>(
    context: T::Context,
    source: S,
    max_depth: usize,
) -> Result<T, Error> {
    let mut decoder = Decoder::with_max_depth(
        SourceStream::from(source.map_err(|e| de::Error::custom(e))),
        max_depth,
    );
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.expect_whitespace().await?;

    if decoder.is_terminated() {
        Ok(decoded)
    } else {
        let snippet = decoder.contents(SNIPPET_LEN)?;
        Err(de::Error::custom(format!(
            "expected end of stream, found `{}...`",
            snippet
        )))
    }
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
#[cfg(feature = "tokio-io")]
/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn read_from<S: AsyncReadExt + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    read_from_with_max_depth(context, source, DEFAULT_MAX_DEPTH).await
}

/// Decode the given JSON-encoded stream of bytes into an instance of `T` using the given context,
/// enforcing a maximum nesting depth for arrays/maps.
#[cfg(feature = "tokio-io")]
pub async fn read_from_with_max_depth<S: AsyncReadExt + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
    max_depth: usize,
) -> Result<T, Error> {
    let mut decoder = Decoder::with_max_depth(SourceReader::from(source), max_depth);
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.expect_whitespace().await?;

    if decoder.is_terminated() {
        Ok(decoded)
    } else {
        let snippet = decoder.contents(SNIPPET_LEN)?;
        Err(de::Error::custom(format!(
            "expected end of stream, found `{}...`",
            snippet
        )))
    }
}

fn decode_hex_val(val: u8) -> Option<u16> {
    let n = HEX[val as usize] as u16;
    if n == 255 {
        None
    } else {
        Some(n)
    }
}

#[inline]
fn is_numeric_byte(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'-' | b'e' | b'E' | b'.')
}

#[inline]
fn is_number_start(b: u8) -> bool {
    b == b'-' || b.is_ascii_digit()
}

#[inline]
fn is_json_whitespace_byte(b: u8) -> bool {
    matches!(b, SPACE | LF | CR | TAB)
}

#[cfg(test)]
mod tests {
    use std::cmp::max;

    use futures::stream;
    use test_case::test_case;

    use super::*;

    /// next_or_eof should return the next char in the buffer/stream, or
    /// if we've hit the EOF, throw an error.
    #[tokio::test]
    async fn test_next_or_eof() {
        let s = b"bar";
        for num_chunks in (1..s.len()).rev() {
            let source = stream::iter(s.iter().copied())
                .chunks(num_chunks)
                .map(Bytes::from)
                .map(Result::<Bytes, Error>::Ok);

            let mut decoder = Decoder::from_stream(source);
            for expected in s {
                let actual = decoder.next_or_eof().await.unwrap();
                assert_eq!(&actual, expected);
            }
            let res = decoder.next_or_eof().await;
            assert!(res.is_err());
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
        assert_eq!(decoder.remaining(), chars_left);
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

        let res = decoder.ignore_string().await;

        match expected {
            Ok(end_length) => assert_eq!(decoder.remaining(), end_length),
            Err(e) => assert_eq!(Err(e), res),
        }
    }

    #[test_case("-123", Ok(0); "negative number")]
    #[test_case("-123.45", Ok(0); "negative float")]
    #[test_case("abc", Err(Error::invalid_utf8("unexpected token ignoring value: 97")); "non number")]
    #[test_case("", Ok(0); "empty source")]
    #[tokio::test]
    async fn test_ignore_value(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);

        // `ignore_number` only works on positive numbers.  `ignore_value` will eat that b'-'
        let res = decoder.ignore_value().await;

        if let Ok(end_length) = expected {
            res.unwrap();
            assert_eq!(decoder.remaining(), end_length);
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
            assert_eq!(decoder.remaining(), end_length);
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
    #[test_case(r#"["test""test"]"#, Err(Error::invalid_utf8("invalid char 34, expected , or ]")); "no comma")]
    #[tokio::test]
    async fn test_ignore_array(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);
        let res = decoder.ignore_array().await;

        match expected {
            Ok(end_length) => assert_eq!(decoder.remaining(), end_length),
            Err(e) => assert_eq!(res.unwrap_err(), e),
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
    #[test_case(r#"{"k""v"}"#, Err(Error::invalid_utf8("invalid char 34, expected 58")); "missing colon")]
    #[test_case(r#"{"k","v"}"#, Err(Error::invalid_utf8("invalid char 44, expected 58")); "comma when expecting colon")]
    #[test_case(r#"{,"k":"v"}"#, Err(Error::invalid_utf8("unexpected token ignoring value: 44")); "comma when expecting value")]
    #[test_case(r#"{"k":"v"asdf}"#, Err(Error::invalid_utf8("invalid char 97, expected , or }")); "value when expecting comma")]
    #[tokio::test]
    async fn test_ignore_object(source: &str, expected: Result<usize, Error>) {
        let mut decoder = test_decoder(source);
        let res = decoder.ignore_object().await;

        match expected {
            Err(e) => assert_eq!(Err(e), res),
            Ok(end_length) => assert_eq!(decoder.remaining(), end_length),
        }
    }
}
