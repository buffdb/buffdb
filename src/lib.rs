pub mod blob;
mod db_connection;
mod interop;
pub mod kv;

pub mod schema {
    pub(crate) mod blob {
        tonic::include_proto!("blob");
    }
    pub mod common {
        tonic::include_proto!("common");
    }
    pub(crate) mod kv {
        tonic::include_proto!("kv");
    }
}

pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type StreamingRequest<T> = tonic::Request<tonic::Streaming<T>>;

pub use crate::db_connection::Location;

// pub mod clients {
//     pub async fn kv_attempt(KvArgs { store, command }: KvArgs) -> Result<ExitCode> {
//         let (client, server) = tokio::io::duplex(1024);

//         tokio::spawn(async move {
//             Server::builder()
//                 .add_service(KvServer::new(KvStore::new(store)))
//                 .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
//                 .await
//         });

//         // Move client to an option so we can _move_ the inner value
//         // on the first attempt to connect. All other attempts will fail.
//         let mut client = Some(client);
//         let channel = Endpoint::try_from("http://[::]:50051")?
//             .connect_with_connector(tower::service_fn(move |_: Uri| {
//                 let client = client.take();

//                 async move {
//                     if let Some(client) = client {
//                         Ok(TokioIo::new(client))
//                     } else {
//                         Err(std::io::Error::new(
//                             std::io::ErrorKind::Other,
//                             "Client already taken",
//                         ))
//                     }
//                 }
//             }))
//             .await?;

//         let mut client = KvClient::new(channel);

//         panic!("everything else works?");

//         // let request = tonic::Request::new(HelloRequest {
//         //     name: "Tonic".into(),
//         // });

//         // let _response = client.say_hello(request).await?;

//         Ok(SUCCESS)
//     }
// }
