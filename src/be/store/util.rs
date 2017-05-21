use nix::unistd::mkstemp;

#[cfg(target_os="macos")]
use std::os::unix::io::FromRawFd;
#[cfg(target_os="linux")]
use std::os::linux::io::FromRawFd;

use std::fs::File;
use std::path::{Path, PathBuf};

use common::errors::*;

// Can this be made faster up? Is it worth it?
pub fn create_temp_file(prefix: &Path) -> Result<(File, PathBuf)> {
    if let Some(template) = prefix.to_str() {
        let template = template.to_owned() + "_XXXXXX";
        let (fd, temp_path) = mkstemp(template.as_str())?;
        let f = unsafe { File::from_raw_fd(fd) };
        Ok((f, temp_path))
    } else {
        bail!("Could not generate file name template");
    }
}
