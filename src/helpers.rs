// workaround until duckdb/duckdb-rs#69 is implemented

use duckdb::types::ToSqlOutput;
use duckdb::{params_from_iter, Params, Result, ToSql};

pub(crate) fn params2<A, B>(a: A, b: B) -> impl Params
where
    A: ToSql,
    B: ToSql,
{
    params_from_iter([PairToSql::A(a), PairToSql::B(b)])
}

pub(crate) fn params3<A, B, C>(a: A, b: B, c: C) -> impl Params
where
    A: ToSql,
    B: ToSql,
    C: ToSql,
{
    params_from_iter([TripleToSql::A(a), TripleToSql::B(b), TripleToSql::C(c)])
}

enum PairToSql<A, B> {
    A(A),
    B(B),
}

impl<A, B> ToSql for PairToSql<A, B>
where
    A: ToSql,
    B: ToSql,
{
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        match self {
            Self::A(a) => a.to_sql(),
            Self::B(b) => b.to_sql(),
        }
    }
}

enum TripleToSql<A, B, C> {
    A(A),
    B(B),
    C(C),
}

impl<A, B, C> ToSql for TripleToSql<A, B, C>
where
    A: ToSql,
    B: ToSql,
    C: ToSql,
{
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        match self {
            Self::A(a) => a.to_sql(),
            Self::B(b) => b.to_sql(),
            Self::C(c) => c.to_sql(),
        }
    }
}

pub(crate) fn duckdb_value_to_string(value: duckdb::types::Value) -> String {
    // TODO either figure out a way to stringify all values unambiguously or convert the values into
    // Google Protobuf types
    #[allow(clippy::todo)]
    match value {
        duckdb::types::Value::Null => "NULL".to_owned(),
        duckdb::types::Value::Boolean(val) => val.to_string(),
        duckdb::types::Value::TinyInt(val) => val.to_string(),
        duckdb::types::Value::SmallInt(val) => val.to_string(),
        duckdb::types::Value::Int(val) => val.to_string(),
        duckdb::types::Value::BigInt(val) => val.to_string(),
        duckdb::types::Value::HugeInt(val) => val.to_string(),
        duckdb::types::Value::UTinyInt(val) => val.to_string(),
        duckdb::types::Value::USmallInt(val) => val.to_string(),
        duckdb::types::Value::UInt(val) => val.to_string(),
        duckdb::types::Value::UBigInt(val) => val.to_string(),
        duckdb::types::Value::Float(val) => val.to_string(),
        duckdb::types::Value::Double(val) => val.to_string(),
        duckdb::types::Value::Decimal(val) => val.to_string(),
        duckdb::types::Value::Timestamp(_, _) => todo!(),
        duckdb::types::Value::Text(val) => val,
        duckdb::types::Value::Blob(_) => todo!(),
        duckdb::types::Value::Date32(val) => val.to_string(),
        duckdb::types::Value::Time64(_, _) => todo!(),
        duckdb::types::Value::Interval {
            months: _,
            days: _,
            nanos: _,
        } => todo!(),
        duckdb::types::Value::List(_) => todo!(),
        duckdb::types::Value::Enum(_) => todo!(),
        duckdb::types::Value::Struct(_) => todo!(),
        duckdb::types::Value::Array(_) => todo!(),
        duckdb::types::Value::Map(_) => todo!(),
        duckdb::types::Value::Union(_) => todo!(),
    }
}
