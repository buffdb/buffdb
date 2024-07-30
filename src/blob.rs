use crate::db_connection::{Database, DbConnectionInfo};
use crate::helpers::{params2, params3};
use crate::interop::duckdb_err_to_tonic_status;
pub use crate::schema::blob::blob_client::BlobClient;
pub use crate::schema::blob::blob_server::{Blob as BlobRpc, BlobServer};
pub use crate::schema::blob::{BlobData, BlobId, UpdateRequest};
use crate::schema::common::Bool;
use crate::{Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::{Stream, StreamExt};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::Pin;
use tonic::{Response, Status};

#[derive(Debug)]
pub struct BlobStore {
    location: Location,
}

impl DbConnectionInfo for BlobStore {
    fn initialize(db: &Database) -> duckdb::Result<()> {
        db.execute_batch(
            "CREATE SEQUENCE IF NOT EXISTS blob_id_seq START 1;
            CREATE TABLE IF NOT EXISTS blob(
                id INTEGER PRIMARY KEY DEFAULT nextval('blob_id_seq'),
                data BLOB,
                metadata TEXT
            );",
        )
    }

    fn location(&self) -> &Location {
        &self.location
    }
}

impl BlobStore {
    #[inline]
    pub fn at_location(location: Location) -> Self {
        Self { location }
    }

    #[inline]
    pub fn at_path<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            location: Location::OnDisk { path: path.into() },
        }
    }

    #[inline]
    pub fn in_memory() -> Self {
        Self {
            location: Location::InMemory,
        }
    }
}

#[tonic::async_trait]
impl BlobRpc for BlobStore {
    type GetStream = Pin<Box<dyn Stream<Item = Result<BlobData, Status>> + Send + 'static>>;
    type StoreStream = Pin<Box<dyn Stream<Item = Result<BlobId, Status>> + Send + 'static>>;
    type UpdateStream = Pin<Box<dyn Stream<Item = Result<BlobId, Status>> + Send + 'static>>;
    type DeleteStream = Pin<Box<dyn Stream<Item = Result<BlobId, Status>> + Send + 'static>>;

    async fn get(&self, request: StreamingRequest<BlobId>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect();

        let stream = stream! {
            while let Some(BlobId { id }) = stream.message().await? {
                let (data, metadata) = db
                    .query_row(
                        "SELECT data, metadata FROM blob WHERE id = ?",
                        [id],
                        |row| {
                            let data: Vec<u8> = row.get(0)?;
                            let metadata: Option<String> = row.get(1)?;
                            Ok((data, metadata))
                        },
                    )
                    .map_err(duckdb_err_to_tonic_status)?;

                yield Ok(BlobData {
                    bytes: data,
                    metadata,
                });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::GetStream))
    }

    async fn store(&self, request: StreamingRequest<BlobData>) -> RpcResponse<Self::StoreStream> {
        let mut stream = request.into_inner();
        let db = self.connect();

        let stream = stream! {
            while let Some(BlobData { bytes, metadata }) = stream.message().await? {
                let id = db
                    .query_row(
                        "INSERT INTO blob(data, metadata) VALUES(?, ?) RETURNING id",
                        params2(bytes, metadata),
                        |row| row.get(0),
                    )
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(BlobId { id });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::StoreStream))
    }

    async fn update(
        &self,
        request: StreamingRequest<UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream> {
        let mut stream = request.into_inner();
        let db = self.connect();

        let stream = stream! {
            while let Some(UpdateRequest {
                id,
                bytes,
                should_update_metadata,
                metadata,
            }) = stream.message().await?
            {
                match (bytes, should_update_metadata) {
                    (None, false) => {}
                    (Some(bytes), true) => {
                        db.execute(
                            "UPDATE blob SET data = ?, metadata = ? WHERE id = ?",
                            params3(bytes, metadata, id),
                        )
                        .map_err(duckdb_err_to_tonic_status)?;
                    }
                    (None, true) => {
                        db.execute(
                            "UPDATE blob SET metadata = ? WHERE id = ?",
                            params2(metadata, id),
                        )
                        .map_err(duckdb_err_to_tonic_status)?;
                    }
                    (Some(bytes), false) => {
                        db.execute("UPDATE blob SET data = ? WHERE id = ?", params2(bytes, id))
                            .map_err(duckdb_err_to_tonic_status)?;
                    }
                }
                yield Ok(BlobId { id });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::UpdateStream))
    }

    async fn delete(&self, request: StreamingRequest<BlobId>) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect();
        let stream = stream! {
            while let Some(BlobId { id }) = stream.message().await? {
                db.execute("DELETE FROM blob WHERE id = ?", [id])
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(BlobId { id });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::DeleteStream))
    }

    async fn eq_data(&self, request: StreamingRequest<BlobId>) -> RpcResponse<Bool> {
        let mut values = self.get(request).await?.into_inner();

        let value = match values.next().await {
            Some(Ok(BlobData { bytes, .. })) => bytes,
            Some(Err(err)) => return Err(err),
            // If there are no keys, then all values are by definition equal.
            None => return Ok(Response::new(Bool { value: true })),
        };
        // Hash the values to avoid storing it fully in memory.
        let first_hash = Sha256::digest(&value);

        while let Some(value) = values.next().await {
            let BlobData { bytes, .. } = value?;

            if first_hash != Sha256::digest(&bytes) {
                return Ok(Response::new(Bool { value: false }));
            }
        }

        Ok(Response::new(Bool { value: true }))
    }

    async fn not_eq_data(&self, request: StreamingRequest<BlobId>) -> RpcResponse<Bool> {
        let mut unique_values = BTreeSet::new();

        let mut values = self.get(request).await?.into_inner();
        while let Some(value) = values.next().await {
            let BlobData { bytes, .. } = value?;

            // `insert` returns false if the value already exists.
            // Hash the values to avoid storing it fully in memory.
            if !unique_values.insert(Sha256::digest(&bytes)) {
                return Ok(Response::new(Bool { value: false }));
            }
        }

        Ok(Response::new(Bool { value: true }))
    }
}
