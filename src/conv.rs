#[cfg(any(feature = "duckdb", feature = "sqlite"))]
use prost::{Message, Name};
#[cfg(feature = "duckdb")]
use prost_types::value::Kind;
use prost_types::*;
#[cfg(feature = "duckdb")]
use std::collections::BTreeMap;

#[cfg(any(feature = "duckdb", feature = "sqlite"))]
pub(crate) fn try_into_protobuf_any<T>(value: T) -> Result<Any, Unsupported>
where
    T: TryIntoProtobufAny,
{
    value.try_into_protobuf_any()
}

#[cfg(any(feature = "duckdb", feature = "sqlite"))]
pub(crate) trait TryIntoProtobufAny {
    fn try_into_protobuf_any(self) -> Result<Any, Unsupported>;
}

#[derive(Debug)]
pub(crate) struct Unsupported {
    pub(crate) message: &'static str,
}

impl From<Unsupported> for tonic::Status {
    fn from(value: Unsupported) -> Self {
        Self::unimplemented(value.message)
    }
}

#[allow(dead_code)] // not all backends use every variant
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
    #[cfg(any(feature = "duckdb", feature = "sqlite"))]
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

#[cfg(feature = "duckdb")]
impl TryIntoProtobufAny for duckdb::types::Value {
    fn try_into_protobuf_any(self) -> Result<Any, Unsupported> {
        Ok(match self {
            Self::Null => ConcreteValue::NullValue,
            Self::Boolean(val) => ConcreteValue::BoolValue(val),
            Self::TinyInt(val) => ConcreteValue::Int32Value(val as _),
            Self::SmallInt(val) => ConcreteValue::Int32Value(val as _),
            Self::Int(val) => ConcreteValue::Int32Value(val),
            Self::BigInt(val) => ConcreteValue::Int64Value(val),
            Self::HugeInt(_) => Err(Unsupported {
                message: "128 bit integers are unsupported because they are not representable in \
                    google.protobuf; may be supported in the future",
            })?,
            Self::UTinyInt(val) => ConcreteValue::UInt32Value(val as _),
            Self::USmallInt(val) => ConcreteValue::UInt32Value(val as _),
            Self::UInt(val) => ConcreteValue::UInt32Value(val),
            Self::UBigInt(val) => ConcreteValue::UInt64Value(val),
            Self::Float(val) => ConcreteValue::FloatValue(val),
            Self::Double(val) => ConcreteValue::DoubleValue(val),
            Self::Decimal(val) => ConcreteValue::DoubleValue(val.try_into().unwrap_or(f64::NAN)),
            Self::Timestamp(unit, scale) | Self::Time64(unit, scale) => {
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
            Self::Text(val) | Self::Enum(val) => ConcreteValue::StringValue(val),
            Self::Blob(val) => ConcreteValue::BytesValue(val),
            Self::Date32(_) => {
                return Err(Unsupported {
                    message: "Date32 is unsupported because the meaning of the value is not \
                        clear; may be supported in the future",
                })
            }
            Self::Interval {
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
            Self::List(val) | Self::Array(val) => ConcreteValue::ListValue(ListValue {
                values: val
                    .iter()
                    .map(duckdb_value_to_protobuf_value)
                    .collect::<Result<_, _>>()?,
            }),
            Self::Struct(val) => ConcreteValue::Struct(Struct {
                fields: val
                    .iter()
                    .map(|(k, v)| duckdb_value_to_protobuf_value(v).map(|val| (k.to_string(), val)))
                    .collect::<Result<_, _>>()?,
            }),
            Self::Map(_) => {
                return Err(Unsupported {
                    message: "maps cannot be represented in google.protobuf; may be supported in \
                        the future",
                })
            }
            Self::Union(val) => ConcreteValue::Value(duckdb_value_to_protobuf_value(&val)?),
        }
        .into_any())
    }
}

#[cfg(feature = "duckdb")]
fn duckdb_value_to_protobuf_value(value: &duckdb::types::Value) -> Result<Value, Unsupported> {
    use duckdb::types::Value as DuckdbValue;
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
        DuckdbValue::Date32(_) => Err(Unsupported {
            message: "Date32 type is unsupported because the meaning of the value is not clear; \
                may be supported in the future",
        }),
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
        DuckdbValue::Map(_) => Err(Unsupported {
            message: "maps cannot be represented in google.protobuf; may be supported in the \
                future",
        }),
        DuckdbValue::Union(val) => duckdb_value_to_protobuf_value(val),
    }
}

#[cfg(feature = "sqlite")]
impl TryIntoProtobufAny for rusqlite::types::Value {
    fn try_into_protobuf_any(self) -> Result<Any, Unsupported> {
        Ok(match self {
            Self::Null => ConcreteValue::NullValue,
            Self::Integer(val) => ConcreteValue::Int64Value(val),
            Self::Real(val) => ConcreteValue::DoubleValue(val),
            Self::Text(val) => ConcreteValue::StringValue(val),
            Self::Blob(val) => ConcreteValue::BytesValue(val),
        }
        .into_any())
    }
}
