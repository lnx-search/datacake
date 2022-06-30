use std::collections::BTreeMap;
use std::io;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use bytecheck::CheckBytes;
use rkyv::collections::ArchivedBTreeMap;
use rkyv::string::ArchivedString;
use rkyv::{Archive, Deserialize, Serialize};

use crate::value::ser::Formatter;

#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Document {
    pub created: u64,
    pub inner: BTreeMap<String, FixedStructureValue>,
}

impl From<BTreeMap<String, FixedStructureValue>> for Document {
    fn from(inner: BTreeMap<String, FixedStructureValue>) -> Self {
        Self {
            created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Get unix timestamp")
                .as_millis() as u64,  // OK to do as we're not anywhere near the u64 cap in milli.
            inner
        }
    }
}

impl ArchivedDocument {
    pub fn to_json_string(&self) -> io::Result<String> {
        let data = self.to_json()?;

        // SAFETY:
        //  We never produce invalid utf8.
        let s = unsafe { String::from_utf8_unchecked(data) };

        Ok(s)
    }

    pub fn to_json(&self) -> io::Result<Vec<u8>> {
        let mut writer = Vec::new();
        self.to_json_writer(&mut writer)?;

        Ok(writer)
    }

    pub fn to_json_writer<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + Write,
    {
        let mut formatter = ser::CompactFormatter;

        self.write_json(writer, &mut formatter)?;

        Ok(())
    }
}

impl JsonSerializable for ArchivedDocument {
    fn write_json<W, F>(&self, writer: &mut W, formatter: &mut F) -> io::Result<()>
    where
        W: ?Sized + Write,
        F: Formatter,
    {
        write_object(&self.inner, writer, formatter)
    }
}

#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes, Debug))]
/// A rigid structure value.
///
/// This has a variant for each 'core' type and the multi-value variant.
/// This is done in order to optimise how fields are serialized and deserialized.
pub enum FixedStructureValue {
    /// A single u64 value.
    U64(u64),
    /// A single i64 value.
    I64(i64),
    /// A single f64 value.
    F64(f64),
    /// A single UTF-8 string.
    String(String),
    /// An array of u64 values.
    MultiU64(Vec<u64>),
    /// An array of i64 values.
    MultiI64(Vec<i64>),
    /// An array of f64 values.
    MultiF64(Vec<f64>),
    /// A array of UTF-8 strings.
    MultiString(Vec<String>),
    /// A dynamic JSON object.
    Dynamic(BTreeMap<String, JsonValue>),
}

impl ToJson for ArchivedFixedStructureValue {
    fn to_json_string(&self) -> io::Result<String> {
        let data = self.to_json()?;

        // SAFETY:
        //  We never produce invalid utf8.
        let s = unsafe { String::from_utf8_unchecked(data) };

        Ok(s)
    }
}

impl JsonSerializable for ArchivedFixedStructureValue {
    fn write_json<W, F>(&self, writer: &mut W, formatter: &mut F) -> io::Result<()>
    where
        W: ?Sized + Write,
        F: Formatter,
    {
        match self {
            ArchivedFixedStructureValue::U64(v) => formatter.write_u64(writer, *v)?,
            ArchivedFixedStructureValue::I64(v) => formatter.write_i64(writer, *v)?,
            ArchivedFixedStructureValue::F64(v) => formatter.write_f64(writer, *v)?,
            ArchivedFixedStructureValue::String(v) => {
                ser::format_escaped_str(writer, formatter, v)?
            },
            ArchivedFixedStructureValue::MultiU64(values) => {
                formatter.begin_array(writer)?;

                for (i, v) in values.iter().enumerate() {
                    formatter.begin_array_value(writer, i == 0)?;
                    formatter.write_u64(writer, *v)?;
                }

                formatter.end_array(writer)?;
            },
            ArchivedFixedStructureValue::MultiI64(values) => {
                formatter.begin_array(writer)?;

                for (i, v) in values.iter().enumerate() {
                    formatter.begin_array_value(writer, i == 0)?;
                    formatter.write_i64(writer, *v)?;
                }

                formatter.end_array(writer)?;
            },
            ArchivedFixedStructureValue::MultiF64(values) => {
                formatter.begin_array(writer)?;

                for (i, v) in values.iter().enumerate() {
                    formatter.begin_array_value(writer, i == 0)?;
                    formatter.write_f64(writer, *v)?;
                }

                formatter.end_array(writer)?;
            },
            ArchivedFixedStructureValue::MultiString(values) => {
                formatter.begin_array(writer)?;

                for (i, v) in values.iter().enumerate() {
                    formatter.begin_array_value(writer, i == 0)?;
                    ser::format_escaped_str(writer, formatter, v)?;
                }

                formatter.end_array(writer)?;
            },
            ArchivedFixedStructureValue::Dynamic(json) => {
                write_object(json, writer, formatter)?
            },
        };

        Ok(())
    }
}

#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes, Debug))]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(#[omit_bounds] Vec<JsonValue>),
    Object(#[omit_bounds] BTreeMap<String, JsonValue>),
}

impl ToJson for ArchivedJsonValue {
    fn to_json_string(&self) -> io::Result<String> {
        let data = self.to_json()?;

        // SAFETY:
        //  We never produce invalid utf8.
        let s = unsafe { String::from_utf8_unchecked(data) };

        Ok(s)
    }
}

impl JsonSerializable for ArchivedJsonValue {
    fn write_json<W, F>(&self, writer: &mut W, formatter: &mut F) -> io::Result<()>
    where
        W: ?Sized + Write,
        F: Formatter,
    {
        match self {
            ArchivedJsonValue::Null => formatter.write_null(writer)?,
            ArchivedJsonValue::Bool(v) => formatter.write_bool(writer, *v)?,
            ArchivedJsonValue::Number(n) => match n {
                ArchivedJsonNumber::Float(v) => formatter.write_f64(writer, *v)?,
                ArchivedJsonNumber::NegInt(v) => formatter.write_i64(writer, *v)?,
                ArchivedJsonNumber::PosInt(v) => formatter.write_u64(writer, *v)?,
            },
            ArchivedJsonValue::String(v) => {
                ser::format_escaped_str(writer, formatter, v)?
            },
            ArchivedJsonValue::Array(values) => {
                formatter.begin_array(writer)?;

                for (i, v) in values.iter().enumerate() {
                    formatter.begin_array_value(writer, i == 0)?;

                    let data = v.to_json()?;
                    formatter.write_raw_fragment(writer, &data)?;
                }

                formatter.end_array(writer)?;
            },
            ArchivedJsonValue::Object(values) => {
                write_object(values, writer, formatter)?
            },
        }

        Ok(())
    }
}

fn write_object<V, W, F>(
    object: &ArchivedBTreeMap<ArchivedString, V>,
    writer: &mut W,
    formatter: &mut F,
) -> io::Result<()>
where
    V: JsonSerializable,
    W: ?Sized + Write,
    F: Formatter,
{
    formatter.begin_object(writer)?;

    for (i, (k, v)) in object.iter().enumerate() {
        formatter.begin_object_key(writer, i == 0)?;
        ser::format_escaped_str(writer, formatter, k)?;

        formatter.begin_object_value(writer)?;
        v.write_json(writer, formatter)?;
    }

    formatter.end_object(writer)?;

    Ok(())
}

#[derive(Archive, Debug, Deserialize, Serialize, Copy, Clone)]
#[archive_attr(derive(CheckBytes, Debug))]
pub enum JsonNumber {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

/// Implements JSON serialization for our zero-copy values.
///
/// The performance should be roughly along the same lines as serde_json.
trait ToJson: JsonSerializable {
    fn to_json_string(&self) -> io::Result<String>;

    fn to_json(&self) -> io::Result<Vec<u8>> {
        let mut writer = Vec::new();
        self.to_json_writer(&mut writer)?;

        Ok(writer)
    }

    fn to_json_writer<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + Write,
    {
        let mut formatter = ser::CompactFormatter;

        self.write_json(writer, &mut formatter)?;

        Ok(())
    }
}

trait JsonSerializable {
    fn write_json<W, F>(&self, writer: &mut W, formatter: &mut F) -> io::Result<()>
    where
        W: ?Sized + Write,
        F: Formatter;
}

mod ser {
    use std::io;

    const BB: u8 = b'b'; // \x08
    const TT: u8 = b't'; // \x09
    const NN: u8 = b'n'; // \x0A
    const FF: u8 = b'f'; // \x0C
    const RR: u8 = b'r'; // \x0D
    const QU: u8 = b'"'; // \x22
    const BS: u8 = b'\\'; // \x5C
    const UU: u8 = b'u'; // \x00...\x1F except the ones above

    #[allow(non_upper_case_globals)]
    const __: u8 = 0;

    pub(crate) fn format_escaped_str<W, F>(
        writer: &mut W,
        formatter: &mut F,
        value: &str,
    ) -> io::Result<()>
    where
        W: ?Sized + io::Write,
        F: ?Sized + Formatter,
    {
        formatter.begin_string(writer)?;
        format_escaped_str_contents(writer, formatter, value)?;
        formatter.end_string(writer)?;
        Ok(())
    }

    pub(crate) fn format_escaped_str_contents<W, F>(
        writer: &mut W,
        formatter: &mut F,
        value: &str,
    ) -> io::Result<()>
    where
        W: ?Sized + io::Write,
        F: ?Sized + Formatter,
    {
        let bytes = value.as_bytes();

        let mut start = 0;

        for (i, &byte) in bytes.iter().enumerate() {
            let escape = ESCAPE[byte as usize];
            if escape == 0 {
                continue;
            }

            if start < i {
                formatter.write_string_fragment(writer, &value[start..i])?;
            }

            let char_escape = CharEscape::from_escape_table(escape, byte);
            formatter.write_char_escape(writer, char_escape)?;

            start = i + 1;
        }

        if start != bytes.len() {
            formatter.write_string_fragment(writer, &value[start..])?;
        }

        Ok(())
    }

    // Lookup table of escape sequences. A value of b'x' at index i means that byte
    // i is escaped as "\x" in JSON. A value of 0 means that byte i is not escaped.
    static ESCAPE: [u8; 256] = [
        //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
        UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
        UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
        __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 3
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
        __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __, // 5
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
    ];

    /// Represents a character escape code in a type-safe manner.
    pub enum CharEscape {
        /// An escaped quote `"`
        Quote,
        /// An escaped reverse solidus `\`
        ReverseSolidus,
        #[allow(unused)]
        /// An escaped solidus `/`
        Solidus,
        /// An escaped backspace character (usually escaped as `\b`)
        Backspace,
        /// An escaped form feed character (usually escaped as `\f`)
        FormFeed,
        /// An escaped line feed character (usually escaped as `\n`)
        LineFeed,
        /// An escaped carriage return character (usually escaped as `\r`)
        CarriageReturn,
        /// An escaped tab character (usually escaped as `\t`)
        Tab,
        /// An escaped ASCII plane control character (usually escaped as
        /// `\u00XX` where `XX` are two hex characters)
        AsciiControl(u8),
    }

    impl CharEscape {
        #[inline]
        fn from_escape_table(escape: u8, byte: u8) -> CharEscape {
            match escape {
                BB => CharEscape::Backspace,
                TT => CharEscape::Tab,
                NN => CharEscape::LineFeed,
                FF => CharEscape::FormFeed,
                RR => CharEscape::CarriageReturn,
                QU => CharEscape::Quote,
                BS => CharEscape::ReverseSolidus,
                UU => CharEscape::AsciiControl(byte),
                _ => unreachable!(),
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct CompactFormatter;

    impl Formatter for CompactFormatter {}

    /// This trait abstracts away serializing the JSON control characters, which allows the user to
    /// optionally pretty print the JSON output.
    pub trait Formatter {
        /// Writes a `null` value to the specified writer.
        #[inline]
        fn write_null<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"null")
        }

        /// Writes a `true` or `false` value to the specified writer.
        #[inline]
        fn write_bool<W>(&mut self, writer: &mut W, value: bool) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            let s = if value {
                b"true" as &[u8]
            } else {
                b"false" as &[u8]
            };
            writer.write_all(s)
        }

        /// Writes an integer value like `-123` to the specified writer.
        #[inline]
        fn write_i64<W>(&mut self, writer: &mut W, value: i64) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            let mut buffer = itoa::Buffer::new();
            let s = buffer.format(value);
            writer.write_all(s.as_bytes())
        }

        /// Writes an integer value like `123` to the specified writer.
        #[inline]
        fn write_u64<W>(&mut self, writer: &mut W, value: u64) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            let mut buffer = itoa::Buffer::new();
            let s = buffer.format(value);
            writer.write_all(s.as_bytes())
        }

        /// Writes a floating point value like `-31.26e+12` to the specified writer.
        #[inline]
        fn write_f64<W>(&mut self, writer: &mut W, value: f64) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            let mut buffer = ryu::Buffer::new();
            let s = buffer.format_finite(value);
            writer.write_all(s.as_bytes())
        }

        /// Writes a number that has already been rendered to a string.
        #[inline]
        fn write_number_str<W>(&mut self, writer: &mut W, value: &str) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(value.as_bytes())
        }

        /// Called before each series of `write_string_fragment` and
        /// `write_char_escape`.  Writes a `"` to the specified writer.
        #[inline]
        fn begin_string<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"\"")
        }

        /// Called after each series of `write_string_fragment` and
        /// `write_char_escape`.  Writes a `"` to the specified writer.
        #[inline]
        fn end_string<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"\"")
        }

        /// Writes a string fragment that doesn't need any escaping to the
        /// specified writer.
        #[inline]
        fn write_string_fragment<W>(
            &mut self,
            writer: &mut W,
            fragment: &str,
        ) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(fragment.as_bytes())
        }

        /// Writes a character escape code to the specified writer.
        #[inline]
        fn write_char_escape<W>(
            &mut self,
            writer: &mut W,
            char_escape: CharEscape,
        ) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            use self::CharEscape::*;

            let s = match char_escape {
                Quote => b"\\\"",
                ReverseSolidus => b"\\\\",
                Solidus => b"\\/",
                Backspace => b"\\b",
                FormFeed => b"\\f",
                LineFeed => b"\\n",
                CarriageReturn => b"\\r",
                Tab => b"\\t",
                AsciiControl(byte) => {
                    static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
                    let bytes = &[
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        HEX_DIGITS[(byte >> 4) as usize],
                        HEX_DIGITS[(byte & 0xF) as usize],
                    ];
                    return writer.write_all(bytes);
                },
            };

            writer.write_all(s)
        }

        /// Called before every array.  Writes a `[` to the specified
        /// writer.
        #[inline]
        fn begin_array<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"[")
        }

        /// Called after every array.  Writes a `]` to the specified
        /// writer.
        #[inline]
        fn end_array<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"]")
        }

        /// Called before every array value.  Writes a `,` if needed to
        /// the specified writer.
        #[inline]
        fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            if first {
                Ok(())
            } else {
                writer.write_all(b",")
            }
        }

        /// Called before every object.  Writes a `{` to the specified
        /// writer.
        #[inline]
        fn begin_object<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"{")
        }

        /// Called after every object.  Writes a `}` to the specified
        /// writer.
        #[inline]
        fn end_object<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b"}")
        }

        /// Called before every object key.
        #[inline]
        fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            if first {
                Ok(())
            } else {
                writer.write_all(b",")
            }
        }

        /// Called before every object value.  A `:` should be written to
        /// the specified writer by either this method or
        /// `end_object_key`.
        #[inline]
        fn begin_object_value<W>(&mut self, writer: &mut W) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(b":")
        }

        /// Writes a raw JSON fragment that doesn't need any escaping to the
        /// specified writer.
        #[inline]
        fn write_raw_fragment<W>(
            &mut self,
            writer: &mut W,
            fragment: &[u8],
        ) -> io::Result<()>
        where
            W: ?Sized + io::Write,
        {
            writer.write_all(fragment)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! archived_test {
        ($v:expr, $expected:expr) => {{
            let data = rkyv::to_bytes::<_, 1024>($v).unwrap();
            let json = unsafe { rkyv::archived_root::<JsonValue>(&data) }
                .to_json_string()
                .expect("ok JSON serialize");
            assert_eq!(json, $expected, "Expected {:?} got {:?}", $expected, json)
        }};
    }

    #[test]
    fn test_archived_json_null() {
        archived_test!(&JsonValue::Null, "null");
    }

    #[test]
    fn test_archived_json_bool() {
        archived_test!(&JsonValue::Bool(true), "true");
        archived_test!(&JsonValue::Bool(false), "false");
    }

    #[test]
    fn test_archived_json_number() {
        archived_test!(&JsonValue::Number(JsonNumber::Float(1.213)), "1.213");
        archived_test!(&JsonValue::Number(JsonNumber::PosInt(123)), "123");
        archived_test!(&JsonValue::Number(JsonNumber::NegInt(-1213)), "-1213");
    }

    #[test]
    fn test_archived_json_str() {
        archived_test!(
            &JsonValue::String("Hello, world!".to_string()),
            "\"Hello, world!\""
        );
        archived_test!(
            &JsonValue::String("Exca\"test // ' today \\u200b ".to_string()),
            "\"Exca\\\"test // ' today \\\\u200b \""
        );
    }

    #[test]
    fn test_archived_json_array() {
        archived_test!(&JsonValue::Array(vec![JsonValue::Null,]), "[null]");

        archived_test!(
            &JsonValue::Array(vec![JsonValue::Bool(true), JsonValue::Bool(false),]),
            "[true,false]"
        );

        archived_test!(
            &JsonValue::Array(vec![
                JsonValue::Number(JsonNumber::Float(1.213)),
                JsonValue::Number(JsonNumber::PosInt(123)),
                JsonValue::Number(JsonNumber::NegInt(-1213)),
            ]),
            "[1.213,123,-1213]"
        );

        archived_test!(
            &JsonValue::Array(vec![
                JsonValue::String("Hello, world!".to_string()),
                JsonValue::String("Exca\"test // ' today \\u200b ".to_string()),
            ]),
            "[\"Hello, world!\",\"Exca\\\"test // ' today \\\\u200b \"]"
        );

        archived_test!(
            &JsonValue::Array(vec![
                JsonValue::String("Hello, world!".to_string()),
                JsonValue::Number(JsonNumber::Float(1.213)),
                JsonValue::Bool(false),
                JsonValue::Number(JsonNumber::PosInt(123)),
                JsonValue::Null,
                JsonValue::Number(JsonNumber::NegInt(-1213)),
            ]),
            "[\"Hello, world!\",1.213,false,123,null,-1213]"
        );
    }

    #[test]
    fn test_archived_json_object() {
        let mut basic_object = BTreeMap::new();
        basic_object.insert("name".to_string(), JsonValue::String("Jeremy".to_string()));
        basic_object.insert(
            "age".to_string(),
            JsonValue::Number(JsonNumber::Float(123.33)),
        );

        archived_test!(
            &JsonValue::Object(basic_object),
            "{\"age\":123.33,\"name\":\"Jeremy\"}"
        );

        let mut big_object = BTreeMap::new();
        big_object.insert("name".to_string(), JsonValue::String("Jeremy".to_string()));
        big_object.insert(
            "age".to_string(),
            JsonValue::Number(JsonNumber::Float(123.33)),
        );
        big_object.insert(
            "entries".to_string(),
            JsonValue::Array(vec![
                JsonValue::String("Hello, world!".to_string()),
                JsonValue::Number(JsonNumber::Float(1.213)),
                JsonValue::Bool(false),
                JsonValue::Number(JsonNumber::PosInt(123)),
                JsonValue::Null,
                JsonValue::Number(JsonNumber::NegInt(-1213)),
            ]),
        );

        archived_test!(&JsonValue::Object(big_object), "{\"age\":123.33,\"entries\":[\"Hello, world!\",1.213,false,123,null,-1213],\"name\":\"Jeremy\"}");
    }
}
