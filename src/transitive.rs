//! A temporary client for a data store.

use crate::blob::{BlobClient, BlobServer, BlobStore};
use crate::kv::{KvClient, KvServer, KvStore};
use crate::Location;
use hyper_util::rt::TokioIo;
use std::ops::{Deref, DerefMut};
use tonic::transport::{Channel, Endpoint, Server};

const DUPLEX_SIZE: usize = 1024;

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
    ($(fn $fn_name:ident<$client:ident, $server:ident, $store:ident>;)*) => {$(
        #[doc = concat!("Create a new client for the `", stringify!($client), "` service.")]
        ///
        /// For in-memory connections, it is highly recommended to only call this function once. All
        /// in-memory connections share the same stream, so any asynchronous calls have a
        /// nondeterministic order. This is not a problem for on-disk connections.
        ///
        /// It is **not** guaranteed that the server spawned by this function will be shut down
        /// properly. This is a known issue and will be fixed in a future release.
        pub async fn $fn_name<L>(
            location: L,
        ) -> Result<Transitive<$client<Channel>>, tonic::transport::Error>
        where
            L: Into<Location> + Send,
        {
            let location = location.into();
            let (client, server) = tokio::io::duplex(DUPLEX_SIZE);

            let _join_handle = tokio::spawn(async move {
                Server::builder()
                    .add_service($server::new($store::at_location(location)))
                    .serve_with_incoming(
                        tokio_stream::once(Ok::<_, std::io::Error>(server)),
                    )
                    .await
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
    fn kv_client<KvClient, KvServer, KvStore>;
    fn blob_client<BlobClient, BlobServer, BlobStore>;
}
