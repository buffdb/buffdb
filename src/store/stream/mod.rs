use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod streamstore {
    tonic::include_proto!("streamstore");
}

use streamstore::stream_service_server::{StreamService, StreamServiceServer};
use streamstore::{StreamMessage, StreamRequest};

#[derive(Default)]
pub struct StreamStore {}

#[tonic::async_trait]
impl StreamService for StreamStore {
    type StreamDataStream = ReceiverStream<Result<StreamMessage, Status>>;

    async fn stream_data(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamDataStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Here you would fetch data from SQLite and send it via the channel
        tokio::spawn(async move {
            // Example: Loop and send messages
            for i in 0..10 {
                let message = StreamMessage {
                    id: i,
                    content: format!("Message {}", i),
                };
                tx.send(Ok(message)).await.unwrap();
            }
        });

        Ok(Response::new(rx.into()))
    }
}
