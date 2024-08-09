use futures::{Stream, StreamExt as _};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;

#[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
pub(super) async fn all_eq<S, T, E>(mut stream: S) -> Result<bool, E>
where
    S: Stream<Item = Result<T, E>> + Unpin + Send,
    T: AsRef<[u8]> + Send,
{
    let value = match stream.next().await {
        Some(Ok(bytes)) => bytes,
        Some(Err(err)) => return Err(err),
        // If there are no keys, then all values are by definition equal.
        None => return Ok(true),
    };
    // Hash the values to avoid storing it fully in memory.
    let first_hash = Sha256::digest(&value);

    while let Some(value) = stream.next().await {
        if first_hash != Sha256::digest(&value?) {
            return Ok(false);
        }
    }

    Ok(true)
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
pub(super) async fn all_not_eq<S, T, E>(mut stream: S) -> Result<bool, E>
where
    S: Stream<Item = Result<T, E>> + Unpin + Send,
    T: AsRef<[u8]>,
{
    let mut unique_values = BTreeSet::new();

    while let Some(value) = stream.next().await {
        // `insert` returns false if the value already exists.
        // Hash the values to avoid storing it fully in memory.
        if !unique_values.insert(Sha256::digest(value?)) {
            return Ok(false);
        }
    }

    Ok(true)
}
