use std::error::Error;
use std::io;
use std::fmt;
use std::string::FromUtf8Error;
use std::str::Utf8Error;

#[derive(Debug)]
pub enum MerkleError {
    Hash(io::Error),
    String(FromUtf8Error),
    Str(Utf8Error),
    EmptyInput,
}

impl fmt::Display for MerkleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MerkleError::Hash(ref err) => err.fmt(f),
            MerkleError::String(ref err) => err.fmt(f),
            MerkleError::Str(ref err) => err.fmt(f),
            MerkleError::EmptyInput => f.write_str("Empty input"),
        }
    }
}

impl Error for MerkleError {
    fn description(&self) -> &str {
        match *self {
            MerkleError::Hash(ref err) => err.description(),
            MerkleError::String(ref err) => err.description(),
            MerkleError::Str(ref err) => err.description(),
            MerkleError::EmptyInput => "Empty input",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            MerkleError::Hash(ref err) => Some(err),
            MerkleError::String(ref err) => Some(err),
            MerkleError::Str(ref err) => Some(err),
            MerkleError::EmptyInput => None,
        }
    }
}

impl From<io::Error> for MerkleError {
    fn from(err: io::Error) -> MerkleError {
        MerkleError::Hash(err)
    }
}

impl From<FromUtf8Error> for MerkleError {
    fn from(err: FromUtf8Error) -> MerkleError {
        MerkleError::String(err)
    }
}

impl From<Utf8Error> for MerkleError {
    fn from(err: Utf8Error) -> MerkleError {
        MerkleError::Str(err)
    }
}
