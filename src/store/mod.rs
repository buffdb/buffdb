use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

pub mod blob;
pub mod kv;

thread_local! {
    /// Map of a database's path to a connection to that database.
    static DB: RefCell<HashMap<Location, rusqlite::Connection>> = RefCell::new(HashMap::new());
}

#[non_exhaustive] // future-proofing for options like network storage
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    InMemory,
    OnDisk { path: PathBuf },
}

impl Location {
    pub fn to_connection(&self) -> rusqlite::Connection {
        match self {
            Self::InMemory => rusqlite::Connection::open_in_memory().unwrap(),
            Self::OnDisk { path } => rusqlite::Connection::open(path).unwrap(),
        }
    }
}
