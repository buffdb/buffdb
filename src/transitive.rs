//! A temporary client for a data store.

use crate::backend::{BlobBackend, DatabaseBackend, KvBackend};
use crate::client::blob::BlobClient;
use crate::client::kv::KvClient;
use crate::client::query::QueryClient;
use crate::interop::{into_tonic_status, IntoTonicStatus};
use crate::query::QueryHandler;
use crate::queryable::Queryable;
use crate::server::blob::BlobServer;
use crate::server::kv::KvServer;
use crate::server::query::QueryServer;
use crate::store::{BlobStore, KvStore};
use crate::Location;
use hyper_util::rt::TokioIo;
use std::fmt;
use std::ops::{Deref, DerefMut};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Status;

const DUPLEX_SIZE: usize = 1024;

/// An error from a transitive client.
#[derive(Debug)]
pub enum TransitiveError {
    /// An error from the server.
    Status(Status),
    /// An error from the transport layer.
    Transport(tonic::transport::Error),
}

impl fmt::Display for TransitiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(err) => err.fmt(f),
            Self::Transport(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for TransitiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Status(err) => Some(err),
            Self::Transport(err) => Some(err),
        }
    }
}

impl From<Status> for TransitiveError {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

impl From<tonic::transport::Error> for TransitiveError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

// TODO Add a way to shut down the server, both manually and automatically on drop.
/// A temporary client for a data store.
///
/// When a transitive client is created, it spawns a server in the background. This is deliberately
/// transparent to the user. A `Transitive` client implements [`Deref`] and [`DerefMut`] to the
/// inner client, so it can be used as if it _were_ the inner client.
#[derive(Debug)]
pub struct Transitive<T> {
    client: T,
}

impl<T> Deref for Transitive<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<T> DerefMut for Transitive<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

macro_rules! declare_clients {
    ($(fn $fn_name:ident<$client:ident, $server:ident, $store:ident, $backend:ident>[$($bounds:tt)*];)*) => {$(
        #[doc = concat!("Create a new client for the `", stringify!($client), "` service.")]
        ///
        /// For in-memory connections, it is highly recommended to only call this function once. All
        /// in-memory connections share the same stream, so any asynchronous calls have a
        /// nondeterministic order. This is not a problem for on-disk connections.
        ///
        /// It is **not** guaranteed that the server spawned by this function will be shut down
        /// properly. This is a known issue and will be fixed in a future release.
        pub async fn $fn_name<L, Backend>(
            location: L,
        ) -> Result<Transitive<$client<Channel>>, TransitiveError>
        where
            L: Into<Location> + Send,
            Backend: DatabaseBackend<Error: IntoTonicStatus>
                + $backend<$($bounds)*>
                + 'static
        {
            let location = location.into();
            let (client, server) = tokio::io::duplex(DUPLEX_SIZE);

            let _join_handle = tokio::spawn(async move {
                Server::builder()
                    .add_service($server::new(
                        $store::<Backend>::at_location(location).map_err(into_tonic_status)?
                    ))
                    .serve_with_incoming(
                        tokio_stream::once(Ok::<_, std::io::Error>(server)),
                    )
                    .await?;
                Ok::<_, TransitiveError>(())
            });

            let mut client = Some(client);
            let channel = Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(tower::service_fn(move |_| {
                    let client = client.take();
                    async move {
                        if let Some(client) = client {
                            Ok(TokioIo::new(client))
                        } else {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "Client already taken",
                            ))
                        }
                    }
                }))
                .await?;

            Ok(Transitive {
                client: $client::new(channel),
            })
        }
    )*};
}

declare_clients! {
    fn kv_client<KvClient, KvServer, KvStore, KvBackend>[
        GetStream: Send,
        SetStream: Send,
        DeleteStream: Send,
    ];
    fn blob_client<BlobClient, BlobServer, BlobStore, BlobBackend>[
        GetStream: Send,
        StoreStream: Send,
        UpdateStream: Send,
        DeleteStream: Send,
    ];
    // fn query_client<QueryClient, QueryServer, QueryHandler, QueryBackend>[];
}

/// Create a new client for the `QueryClient` service.
///
/// For in-memory connections, it is highly recommended to only call this function once. All
/// in-memory connections share the same stream, so any asynchronous calls have a nondeterministic
/// order. This is not a problem for on-disk connections.
///
/// It is **not** guaranteed that the server spawned by this function will be shut down properly.
/// This is a known issue and will be fixed in a future release.
pub async fn query_client<L1, L2, Backend>(
    kv_path: L1,
    blob_path: L2,
) -> Result<Transitive<QueryClient<Channel>>, TransitiveError>
where
    L1: Into<Location> + Send,
    L2: Into<Location> + Send,
    Backend: DatabaseBackend<Error: IntoTonicStatus, Connection: Send>
        + Queryable<Connection = <Backend as DatabaseBackend>::Connection, QueryStream: Send>
        + Send
        + Sync
        + 'static,
{
    let (kv_path, blob_path) = (kv_path.into(), blob_path.into());
    if matches!(kv_path, Location::InMemory) && matches!(blob_path, Location::InMemory) {
        return Err(TransitiveError::Status(Status::internal(
            "cannot use in-memory locations for both key-value and blob stores",
        )));
    }

    let (client, server) = tokio::io::duplex(DUPLEX_SIZE);

    let _join_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(QueryServer::new(
                QueryHandler::<Backend>::at_location(kv_path, blob_path)
                    .map_err(into_tonic_status)?,
            ))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
            .await?;
        Ok::<_, TransitiveError>(())
    });

    let mut client = Some(client);
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(tower::service_fn(move |_| {
            let client = client.take();
            async move {
                if let Some(client) = client {
                    Ok(TokioIo::new(client))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Client already taken",
                    ))
                }
            }
        }))
        .await?;

    Ok(Transitive {
        client: QueryClient::new(channel),
    })
}
