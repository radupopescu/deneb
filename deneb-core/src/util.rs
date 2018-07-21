use nix::{libc::{getegid, geteuid, gid_t, uid_t},
          unistd::mkstemp};
use time::precise_time_ns;

use std::fs::{remove_file, rename, File};
use std::io::Write;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};

use errors::{DenebResult, UnixError};

/// Atomically writes a buffer to a file
///
/// The buffer is first written to a temporary file, then, upon success,
/// the temporary file is atomically renamed to the final file name.
pub fn atomic_write(file_name: &Path, bytes: &[u8]) -> DenebResult<()> {
    let (mut f, temp_path) = create_temp_file(file_name)?;
    if let Ok(()) = f.write_all(bytes) {
        rename(temp_path, file_name)?;
    } else {
        remove_file(temp_path)?;
    }
    Ok(())
}

pub fn tick() -> i64 {
    precise_time_ns() as i64
}

pub fn tock(t0: i64) -> i64 {
    precise_time_ns() as i64 - t0
}

pub fn run<F: Fn() -> DenebResult<()>>(f: F) {
    if let Err(e) = f() {
        eprintln!("Error: {}", e);
        ::std::process::exit(1);
    }
}

// Safe wrappers on top of  some libc functions
pub fn get_egid() -> gid_t {
    unsafe { getegid() }
}

pub fn get_euid() -> uid_t {
    unsafe { geteuid() }
}

// Can this be made faster? Is it worth it?
fn create_temp_file(prefix: &Path) -> Result<(File, PathBuf), UnixError> {
    let mut template = prefix.as_os_str().to_os_string();
    template.push("_XXXXXX");
    let (fd, temp_path) = mkstemp(template.as_os_str())?;
    let f = unsafe { File::from_raw_fd(fd) };
    Ok((f, temp_path))
}
