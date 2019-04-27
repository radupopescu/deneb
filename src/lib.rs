#![recursion_limit = "1024"]

extern crate crossbeam_channel;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

#[cfg(feature = "fuse_module")]
extern crate deneb_fuse;

pub mod app;
pub mod logging;
pub mod talk;
pub mod util;
