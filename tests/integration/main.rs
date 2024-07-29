mod blob;
mod kv;

use anyhow::Result;
use buffdb::blob::{BlobClient, BlobServer, BlobStore};
use buffdb::kv::{KvClient, KvServer, KvStore};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tonic::transport::{Channel, Server};

const SERVER_ADDR: &str = "[::1]:50051";
const CLIENT_ADDR: &str = "http://[::1]:50051";

fn run_server() -> tokio::task::JoinHandle<Result<()>> {
    tokio::task::spawn(async {
        let kv_store = KvStore::new("test_kv_store");
        let blob_store = BlobStore::new("test_blob_store");

        Server::builder()
            .add_service(KvServer::new(kv_store))
            .add_service(BlobServer::new(blob_store))
            .serve(SERVER_ADDR.parse().unwrap())
            .await?;
        Ok(())
    })
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

struct Test<Client> {
    name: &'static str,
    file: &'static str,
    f: fn(Client) -> BoxFuture<Result<()>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let server = run_server();

    // Give the server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let kv_client = KvClient::connect(CLIENT_ADDR)
        .await
        .expect("Failed to connect to server");
    let blob_client = BlobClient::connect(CLIENT_ADDR)
        .await
        .expect("Failed to connect to server");

    // kv tests
    for test in inventory::iter::<Test<KvClient<Channel>>> {
        match (test.f)(kv_client.clone()).await {
            Ok(_) => println!("{}:{} passed", test.file, test.name),
            Err(e) => eprintln!("{}::{} failed: {e:?}", test.file, test.name),
        }
    }

    // blob tests
    for test in inventory::iter::<Test<BlobClient<Channel>>> {
        match (test.f)(blob_client.clone()).await {
            Ok(_) => println!("{}:{} passed", test.file, test.name),
            Err(e) => eprintln!("{}::{} failed: {e:?}", test.file, test.name),
        }
    }

    server.abort();

    Ok(())
}

inventory::collect!(Test<KvClient<Channel>>);
inventory::collect!(Test<BlobClient<Channel>>);
