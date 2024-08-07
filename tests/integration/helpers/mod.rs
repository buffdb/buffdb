use std::fmt::Debug;
use tonic::Streaming;

pub(crate) async fn assert_stream_eq<S, X>(mut stream: Streaming<S>, expected: X)
where
    S: PartialEq<X::Item> + Debug,
    X: IntoIterator<Item: Debug, IntoIter: Send> + Send,
{
    let mut expected = expected.into_iter();

    while let Ok(Some(stream_item)) = stream.message().await {
        match (stream_item, expected.next()) {
            (stream_item, Some(expected_item)) => {
                assert!(stream_item == expected_item);
            }
            (_, None) => panic!("stream has more items than expected"),
        }
    }

    assert!(expected.next().is_none());
}
