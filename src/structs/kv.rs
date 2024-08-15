#[derive(Debug)]
pub struct GetRequest {
    pub key: String,
}

#[derive(Debug)]
pub struct GetResponse {
    pub value: String,
}

#[derive(Debug)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct SetResponse {
    pub key: String,
}

#[derive(Debug)]
pub struct DeleteRequest {
    pub key: String,
}

#[derive(Debug)]
pub struct DeleteResponse {
    pub key: String,
}

#[derive(Debug)]
pub struct EqRequest {
    pub key: String,
}

#[derive(Debug)]
pub struct NotEqRequest {
    pub key: String,
}
