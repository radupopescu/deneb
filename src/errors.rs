use log;
use log4rs;

use std::io;
use merkle;
use nix;

error_chain! {
    types {}

    links {
        Merkle(merkle::errors::Error, merkle::errors::ErrorKind);
    }

    foreign_links {
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Log4rsConfigError(log4rs::config::Errors) #[doc="log4rs config error"];
        NixError(nix::Error) #[doc="nix error"];
    }

    errors {
        MissingCommandLineParameter(p: String) {
            description("Missing command-line parameters")
            display("Missing command-line parameters: '{}'", p)
        }
    }
}

