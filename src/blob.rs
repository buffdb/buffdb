//! A store for binary large objects (BLOBs) with an optional metadata field.

use crate::backend::{BlobBackend, DatabaseBackend, QueryBackend};
use crate::interop::IntoTonicStatus;
use crate::proto::blob::{
    DeleteRequest, EqDataRequest, GetRequest, NotEqDataRequest, StoreRequest, UpdateRequest,
};
use crate::proto::query::RawQuery;
use crate::service::blob::BlobRpc;
use crate::{Location, RpcResponse, StreamingRequest};
use std::path::PathBuf;

/// A store for binary large objects (BLOBs) with an optional metadata field.
///
/// Metadata, if stored, is a string that can be used to store any additional information about the
/// BLOB, such as a description or a name. Neither the BLOB or the metadata are required to be
/// unique.
#[must_use]
#[derive(Debug)]
pub struct BlobStore<Backend> {
    backend: Backend,
}

// impl<Backend> DbConnectionInfo for BlobStore<Backend>
// where
//     Backend: DatabaseBackend,
// {
//     fn initialize(&self, db: &Database) -> duckdb::Result<()> {
//         db.execute_batch(
//             "CREATE SEQUENCE IF NOT EXISTS blob_id_seq START 1;
//             CREATE TABLE IF NOT EXISTS blob(
//                 id INTEGER PRIMARY KEY DEFAULT nextval('blob_id_seq'),
//                 data BLOB,
//                 metadata TEXT
//             );",
//         )?;
//         self.initialized.store(true, Ordering::Relaxed);
//         Ok(())
//     }

//     fn was_initialized(&self) -> bool {
//         self.initialized.load(Ordering::Relaxed)
//     }

//     fn location(&self) -> &Location {
//         &self.location
//     }
// }

impl<Backend> BlobStore<Backend>
where
    Backend: DatabaseBackend,
{
    /// Create a new key-value store at the given location. If not pre-existing, the store will not
    /// be initialized until the first connection is made.
    #[inline]
    pub fn at_location(location: Location) -> Result<Self, Backend::Error> {
        Ok(Self {
            backend: Backend::at_location(location)?,
        })
    }

    /// Create a new key-value at the given path on disk. If not pre-existing, the store will not be
    /// initialized until the first connection is made.
    #[inline]
    pub fn at_path<P>(path: P) -> Result<Self, Backend::Error>
    where
        P: Into<PathBuf>,
    {
        Self::at_location(Location::OnDisk { path: path.into() })
    }

    /// Create a new in-memory key-value store. This is useful for short-lived data.
    ///
    /// Note that all in-memory connections share the same stream, so any asynchronous calls have a
    /// nondeterministic order. This is not a problem for on-disk connections.
    #[inline]
    pub fn in_memory() -> Result<Self, Backend::Error> {
        Self::at_location(Location::InMemory)
    }
}

#[tonic::async_trait]
impl<Backend> BlobRpc for BlobStore<Backend>
where
    Backend: QueryBackend<Error: IntoTonicStatus, QueryStream: Send, ExecuteStream: Send>
        + BlobBackend<
            Error: IntoTonicStatus,
            GetStream: Send,
            StoreStream: Send,
            UpdateStream: Send,
            DeleteStream: Send,
        > + 'static,
{
    type QueryStream = Backend::QueryStream;
    type ExecuteStream = Backend::ExecuteStream;
    type GetStream = Backend::GetStream;
    type StoreStream = Backend::StoreStream;
    type UpdateStream = Backend::UpdateStream;
    type DeleteStream = Backend::DeleteStream;

    async fn query(&self, request: StreamingRequest<RawQuery>) -> RpcResponse<Self::QueryStream> {
        self.backend.query(request).await
    }

    async fn execute(
        &self,
        request: StreamingRequest<RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream> {
        self.backend.execute(request).await
    }

    async fn get(&self, request: StreamingRequest<GetRequest>) -> RpcResponse<Self::GetStream> {
        self.backend.get(request).await
    }

    async fn store(
        &self,
        request: StreamingRequest<StoreRequest>,
    ) -> RpcResponse<Self::StoreStream> {
        self.backend.store(request).await
    }

    async fn update(
        &self,
        request: StreamingRequest<UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream> {
        self.backend.update(request).await
    }

    async fn delete(
        &self,
        request: StreamingRequest<DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        self.backend.delete(request).await
    }

    async fn eq_data(&self, request: StreamingRequest<EqDataRequest>) -> RpcResponse<bool> {
        self.backend.eq_data(request).await
    }

    async fn not_eq_data(&self, request: StreamingRequest<NotEqDataRequest>) -> RpcResponse<bool> {
        self.backend.not_eq_data(request).await
    }
}
