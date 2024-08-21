//! A database that supports raw query execution.

use crate::backend::DatabaseBackend;
use crate::structs::query::{ExecuteResponse, QueryResponse};
use futures::Stream;
use std::future::Future;

/// A trait for types that can execute raw queries.
pub trait Queryable<FrontendError>: DatabaseBackend {
    /// The type of a stream containing the query results.
    type QueryStream: Stream<Item = Result<QueryResponse<Self::Any>, FrontendError>> + Unpin;

    /// The type of a single field in a query result.
    type Any;

    /// Execute a query and return a stream of results.
    ///
    /// The connection is passed by ownership, but must be returned in the output tuple.
    fn query(
        query: String,
        conn: Self::Connection,
    ) -> impl Future<Output = (Self::QueryStream, Self::Connection)> + Send;

    /// Execute a query that does not return rows, but returns the number of rows changed.
    ///
    /// This is used for queries that modify the database.
    fn execute(
        query: String,
        conn: Self::Connection,
    ) -> impl Future<Output = (Result<ExecuteResponse, FrontendError>, Self::Connection)> + Send;
}
