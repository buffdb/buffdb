use std::path::PathBuf;

pub(crate) type Database = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The location of a database.
///
/// To store data in memory, use `OnDisk` with a path pointing to a tmpfs or ramfs.
#[non_exhaustive] // future-proofing for options like network storage
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    OnDisk { path: PathBuf },
}

impl Location {
    pub(crate) fn to_connection<T: Connect + ?Sized>(&self) -> Database {
        match self {
            Self::OnDisk { path } => {
                let mut opts = rocksdb::Options::default();
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                T::adjust_opts(&mut opts);

                match T::fields() {
                    None => Database::open(&opts, path).unwrap(),
                    Some(fields) => Database::open_cf(&opts, path, fields).unwrap(),
                }
            }
        }
    }
}

pub(crate) trait Connect {
    fn adjust_opts(_opts: &mut rocksdb::Options) {}
    fn fields() -> Option<impl Iterator<Item = &'static str>> {
        None::<std::array::IntoIter<&str, 0>>
    }
}
