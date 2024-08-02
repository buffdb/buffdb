use std::collections::BTreeMap;

use duckdb::types::Value as DuckdbValue;
use prost::{Message, Name};
use prost_types::value::Kind;
use prost_types::*;

#[derive(Debug)]
pub(crate) struct Unsupported {
    message: &'static str,
}

impl Unsupported {
    pub(crate) const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl From<Unsupported> for tonic::Status {
    fn from(value: Unsupported) -> Self {
        Self::unimplemented(value.message)
    }
}

pub(crate) fn duckdb_value_to_protobuf_any(value: DuckdbValue) -> Result<Any, Unsupported> {
    Ok(match value {
        DuckdbValue::Null => ConcreteValue::NullValue,
        DuckdbValue::Boolean(val) => ConcreteValue::BoolValue(val),
        DuckdbValue::TinyInt(val) => ConcreteValue::Int32Value(val as _),
        DuckdbValue::SmallInt(val) => ConcreteValue::Int32Value(val as _),
        DuckdbValue::Int(val) => ConcreteValue::Int32Value(val),
        DuckdbValue::BigInt(val) => ConcreteValue::Int64Value(val),
        DuckdbValue::HugeInt(_) => Err(Unsupported::new(
            "128 bit integers are unsupported because they are not representable in \
            google.protobuf; may be supported in the future",
        ))?,
        DuckdbValue::UTinyInt(val) => ConcreteValue::UInt32Value(val as _),
        DuckdbValue::USmallInt(val) => ConcreteValue::UInt32Value(val as _),
        DuckdbValue::UInt(val) => ConcreteValue::UInt32Value(val),
        DuckdbValue::UBigInt(val) => ConcreteValue::UInt64Value(val),
        DuckdbValue::Float(val) => ConcreteValue::FloatValue(val),
        DuckdbValue::Double(val) => ConcreteValue::DoubleValue(val),
        DuckdbValue::Decimal(val) => ConcreteValue::DoubleValue(val.try_into().unwrap_or(f64::NAN)),
        DuckdbValue::Timestamp(unit, scale) | DuckdbValue::Time64(unit, scale) => {
            let (seconds, nanos) = match unit {
                duckdb::types::TimeUnit::Second => (scale, 0),
                duckdb::types::TimeUnit::Millisecond => {
                    (scale / 1_000, (scale % 1_000) * 1_000_000)
                }
                duckdb::types::TimeUnit::Microsecond => {
                    (scale / 1_000_000, (scale % 1_000_000) * 1_000)
                }
                duckdb::types::TimeUnit::Nanosecond => {
                    (scale / 1_000_000_000, scale % 1_000_000_000)
                }
            };
            ConcreteValue::Timestamp(Timestamp {
                seconds,
                nanos: nanos as i32,
            })
        }
        DuckdbValue::Text(val) | DuckdbValue::Enum(val) => ConcreteValue::StringValue(val),
        DuckdbValue::Blob(val) => ConcreteValue::BytesValue(val),
        DuckdbValue::Date32(_) => {
            return Err(Unsupported::new(
                "Date32 is unsupported because the meaning of the value is not clear; may be \
                supported in the future",
            ))
        }
        DuckdbValue::Interval {
            months,
            days,
            nanos,
        } => ConcreteValue::Struct(Struct {
            fields: BTreeMap::from_iter([
                (
                    "months".to_owned(),
                    Value {
                        kind: Some(Kind::NumberValue(months as f64)),
                    },
                ),
                (
                    "days".to_owned(),
                    Value {
                        kind: Some(Kind::NumberValue(days as f64)),
                    },
                ),
                (
                    "nanos".to_owned(),
                    Value {
                        kind: Some(Kind::NumberValue(nanos as f64)),
                    },
                ),
            ]),
        }),
        DuckdbValue::List(val) | DuckdbValue::Array(val) => ConcreteValue::ListValue(ListValue {
            values: val
                .iter()
                .map(duckdb_value_to_protobuf_value)
                .collect::<Result<_, _>>()?,
        }),
        DuckdbValue::Struct(val) => ConcreteValue::Struct(Struct {
            fields: val
                .iter()
                .map(|(k, v)| duckdb_value_to_protobuf_value(v).map(|val| (k.to_string(), val)))
                .collect::<Result<_, _>>()?,
        }),
        DuckdbValue::Map(_) => {
            return Err(Unsupported::new(
                "maps cannot be represented in google.protobuf; may be supported in the future",
            ))
        }
        DuckdbValue::Union(val) => ConcreteValue::Value(duckdb_value_to_protobuf_value(&val)?),
    }
    .into_any())
}

fn duckdb_value_to_protobuf_value(value: &DuckdbValue) -> Result<Value, Unsupported> {
    match value {
        DuckdbValue::Null => Ok(Value {
            kind: Some(Kind::NullValue(0)),
        }),
        DuckdbValue::Boolean(val) => Ok(Value {
            kind: Some(Kind::BoolValue(*val)),
        }),
        DuckdbValue::TinyInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::SmallInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::Int(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::BigInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::HugeInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::UTinyInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::USmallInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::UInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::UBigInt(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::Float(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val as _)),
        }),
        DuckdbValue::Double(val) => Ok(Value {
            kind: Some(Kind::NumberValue(*val)),
        }),
        DuckdbValue::Decimal(val) => Ok(Value {
            kind: Some(Kind::NumberValue((*val).try_into().unwrap_or(f64::NAN))),
        }),
        DuckdbValue::Timestamp(unit, scale) | DuckdbValue::Time64(unit, scale) => {
            use duckdb::types::TimeUnit;
            let kind = match unit {
                TimeUnit::Second => Kind::NumberValue(*scale as f64),
                TimeUnit::Millisecond => Kind::NumberValue(*scale as f64 / 1_000.),
                TimeUnit::Microsecond => Kind::NumberValue(*scale as f64 / 1_000_000.),
                TimeUnit::Nanosecond => Kind::NumberValue(*scale as f64 / 1_000_000_000.),
            };
            Ok(Value { kind: Some(kind) })
        }
        DuckdbValue::Text(val) | DuckdbValue::Enum(val) => Ok(Value {
            kind: Some(Kind::StringValue(val.clone())),
        }),
        DuckdbValue::Blob(_) => Ok(Value { kind: None }),
        DuckdbValue::Date32(_) => Err(Unsupported::new(
            "Date32 type is unsupported because the meaning of the value is not clear; may be \
            supported in the future",
        )),
        DuckdbValue::Interval {
            months,
            days,
            nanos,
        } => Ok(Value {
            kind: Some(Kind::StringValue(format!(
                "P{months}M{days}D{}S",
                nanos / 1_000_000_000
            ))),
        }),
        DuckdbValue::List(val) | DuckdbValue::Array(val) => Ok(Value {
            kind: Some(Kind::ListValue(ListValue {
                values: val
                    .iter()
                    .map(duckdb_value_to_protobuf_value)
                    .collect::<Result<_, _>>()?,
            })),
        }),
        DuckdbValue::Struct(val) => Ok(Value {
            kind: Some(Kind::StructValue(Struct {
                fields: val
                    .iter()
                    .map(|(k, v)| duckdb_value_to_protobuf_value(v).map(|val| (k.to_string(), val)))
                    .collect::<Result<_, _>>()?,
            })),
        }),
        DuckdbValue::Map(_) => Err(Unsupported::new(
            "maps cannot be represented in google.protobuf; may be supported in the future",
        )),
        DuckdbValue::Union(val) => duckdb_value_to_protobuf_value(val),
    }
}

enum ConcreteValue {
    NullValue,
    BoolValue(bool),
    Int32Value(i32),
    Int64Value(i64),
    UInt32Value(u32),
    UInt64Value(u64),
    FloatValue(f32),
    DoubleValue(f64),
    StringValue(String),
    BytesValue(Vec<u8>),
    Timestamp(Timestamp),
    ListValue(ListValue),
    Struct(Struct),
    Value(Value),
}

impl ConcreteValue {
    fn into_any(self) -> Any {
        macro_rules! google_proto {
            ($file:literal) => {
                concat!(
                    "https://github.com/protocolbuffers/protobuf/raw/",
                    "db48abbef49312d76d442c0cde6a551961726b28/src/google/protobuf/",
                    $file
                )
                .to_owned()
            };
        }

        let (type_url, value) = match self {
            // TODO What's the correct encoding for NullValue?
            Self::NullValue => (google_proto!("struct.proto"), vec![]),
            Self::BoolValue(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::Int32Value(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::Int64Value(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::UInt32Value(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::UInt64Value(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::FloatValue(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::DoubleValue(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::StringValue(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::BytesValue(val) => (google_proto!("wrappers.proto"), val.encode_to_vec()),
            Self::Timestamp(val) => (Timestamp::type_url(), val.encode_to_vec()),
            Self::ListValue(val) => (google_proto!("struct.proto"), val.encode_to_vec()),
            Self::Struct(val) => (google_proto!("struct.proto"), val.encode_to_vec()),
            Self::Value(val) => (google_proto!("struct.proto"), val.encode_to_vec()),
        };

        Any { type_url, value }
    }
}
