use std::any::Any;

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
pub struct QueryResponse {
    // TODO get a better type than Any, possibly varying by frontend
    pub fields: Vec<Box<dyn Any>>,
}

#[derive(Debug)]
pub struct ExecuteResponse {
    pub rows_changed: u64,
}
