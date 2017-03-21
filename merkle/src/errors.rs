use std::io;
use std::string::FromUtf8Error;
use std::str::Utf8Error;

use log;

error_chain! {
    types {}

    links {}

    foreign_links {
        FromUtf8Error(FromUtf8Error) #[doc="from-utf8 error"];
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Utf8Error(Utf8Error) #[doc="utf8 error"];
    }

    errors {
        EmptyInputError {
            description("Could not build empty Merkle tree")
            display("Could not build empty Merkle tree")
        }
    }
}

