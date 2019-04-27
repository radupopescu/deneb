#![recursion_limit = "1024"]

#[cfg(feature = "fuse_module")]
extern crate deneb_fuse;

pub mod app;
pub mod logging;
pub mod talk;
pub mod util;
