use futures::Stream;
use futures::StreamExt as _;
use tonic::Status;

pub(crate) async fn assert_stream_eq<S, ST, X>(mut stream: S, expected: X)
where
    S: Stream<Item = Result<ST, Status>> + Unpin,
    ST: PartialEq<X::Item>,
    X: IntoIterator,
{
    let mut expected = expected.into_iter();

    while let Some(stream_item) = stream.next().await {
        match (stream_item, expected.next()) {
            (Ok(stream_item), Some(expected_item)) => {
                assert!(stream_item == expected_item);
            }
            (Ok(_), None) => {
                panic!("stream has more items than expected");
            }
            (Err(err), _) => {
                panic!("stream error: {err:?}");
            }
        }
    }

    assert!(expected.next().is_none())
}
