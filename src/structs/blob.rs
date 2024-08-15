#[derive(Debug)]
pub struct GetRequest {
    pub id: u64,
}

#[derive(Debug)]
pub struct GetResponse {
    pub value: String,
}

#[derive(Debug)]
pub struct StoreRequest {
    pub data: Vec<u8>,
    pub metadata: Option<String>,
}

#[derive(Debug)]
pub struct StoreResponse {
    pub id: u64,
}

#[derive(Debug)]
pub struct UpdateRequest {
    pub id: u64,
    pub data: Option<Vec<u8>>,
    pub metadata: Option<Option<String>>,
}

#[derive(Debug)]
pub struct UpdateResponse {
    pub id: u64,
}

#[derive(Debug)]
pub struct DeleteRequest {
    pub id: u64,
}

#[derive(Debug)]
pub struct DeleteResponse {
    pub id: u64,
}

#[derive(Debug)]
pub struct EqDataRequest {
    pub id: u64,
}

#[derive(Debug)]
pub struct NotEqDataRequest {
    pub id: u64,
}
