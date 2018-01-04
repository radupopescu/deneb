use lmdb;
use nix;

use std::ffi::OsString;
use std::path::PathBuf;

pub type DenebResult<T> = ::std::result::Result<T, ::failure::Error>;

// Errors from the nix crate

#[derive(Debug, Fail)]
#[fail(display = "Nix error: {}", inner)]
pub struct UnixError {
    #[cause] inner: nix::Error,
}

impl From<nix::Error> for UnixError {
    fn from(ne: nix::Error) -> UnixError {
        UnixError { inner: ne }
    }
}

// Errors from the LMDB crate

#[derive(Debug, Fail)]
#[fail(display = "LMDB error: {}", inner)]
pub struct LMDBError {
    #[cause] inner: lmdb::Error,
}

impl From<lmdb::Error> for LMDBError {
    fn from(ne: lmdb::Error) -> LMDBError {
        LMDBError { inner: ne }
    }
}

// Common Deneb errors

#[derive(Debug, Fail)]
pub enum DenebError {
    #[fail(display = "Disk IO error")]
    DiskIO,
    #[fail(display = "Command line parameter parsing error: {}", _0)]
    CommandLineParameter(String),
    #[fail(display = "Directory visit error: {:?}", _0)]
    DirectoryVisit(PathBuf),
    #[fail(display = "Invalid path encountered: {:?}", _0)]
    InvalidPath(PathBuf),
    #[fail(display = "Index generator error")]
    IndexGenerator,
    #[fail(display = "Digest read error")]
    DigestFromSlice,
}

// Object store errors

#[derive(Debug, Fail)]
pub enum StoreError {
    #[fail(display = "Get error for: {}", _0)]
    ChunkGet(String),
    #[fail(display = "Put error for: {}", _0)]
    ChunkPut(String),
}

// Catalog errors

#[derive(Debug, Fail)]
pub enum CatalogError {
    #[fail(display = "INode serialization error for index: {}", _0)]
    INodeSerialization(u64),
    #[fail(display = "Dir entry serialization error for index: {}", _0)]
    DEntrySerialization(u64),
    #[fail(display = "INode deserialization error for index: {}", _0)]
    INodeDeserialization(u64),
    #[fail(display = "Dir entry deserialization error for index: {}", _0)]
    DEntryDeserialization(u64),
    #[fail(display = "INode read error for index: {}", _0)]
    INodeRead(u64),
    #[fail(display = "INode write error for index: {}", _0)]
    INodeWrite(u64),
    #[fail(display = "Dir entry read error for index: {}", _0)]
    DEntryRead(u64),
    #[fail(display = "Dir entry write error for index: {}", _0)]
    DEntryWrite(u64),
    #[fail(display = "Dir entry {:?} not found at index: {}", _0, _1)]
    DEntryNotFound(OsString, u64),
    #[fail(display = "Invalid catalog version: {}", _0)]
    Version(u32),
}
