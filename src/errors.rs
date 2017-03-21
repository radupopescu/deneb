use log;
use log4rs;

use std::io;

use merkle;

error_chain! {
    types {}

    links {
        Merkle(merkle::errors::Error, merkle::errors::ErrorKind);
    }

    foreign_links {
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Log4rsConfigError(log4rs::config::Errors) #[doc="log4rs config error"];
    }

    errors {}
}

