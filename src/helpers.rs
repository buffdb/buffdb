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
            PairToSql::A(a) => a.to_sql(),
            PairToSql::B(b) => b.to_sql(),
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
            TripleToSql::A(a) => a.to_sql(),
            TripleToSql::B(b) => b.to_sql(),
            TripleToSql::C(c) => c.to_sql(),
        }
    }
}
