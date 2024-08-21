#[non_exhaustive]
#[derive(Debug)]
pub enum TargetStore {
    KeyValue,
    Blob,
}

#[derive(Debug)]
pub struct RawQuery {
    pub query: String,
    pub target: TargetStore,
}

#[derive(Debug)]
pub struct QueryResponse<T> {
    pub fields: Vec<T>,
}

#[derive(Debug)]
pub struct ExecuteResponse {
    pub rows_changed: u64,
}
