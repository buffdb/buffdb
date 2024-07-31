use crate::blob::{BlobClient, BlobServer, BlobStore};
use crate::kv::{KvClient, KvServer, KvStore};
use crate::Location;
use hyper_util::rt::TokioIo;
use std::ops::{Deref, DerefMut};
use tonic::transport::{Channel, Endpoint, Server};

const DUPLEX_SIZE: usize = 1024;

// TODO Add a way to shut down the server
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
