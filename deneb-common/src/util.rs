use nix::sys::signal::{pthread_sigmask, SigSet, SigmaskHow, Signal};
use nix::unistd::mkstemp;
use time::precise_time_ns;

use std::fs::{remove_file, rename, File};
use std::io::Write;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::thread::{spawn, JoinHandle};

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

pub fn block_signals() -> Result<(), UnixError> {
    let mut sigs = SigSet::empty();
    sigs.add(Signal::SIGINT);
    pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&sigs), None)?;
    Ok(())
}

pub fn set_sigint_handler(tx: Sender<()>) -> JoinHandle<()> {
    spawn(move || {
        let mut sigs = SigSet::empty();
        sigs.add(Signal::SIGINT);
        if let Ok(sig) = sigs.wait() {
            if let Signal::SIGINT = sig {
                info!("Ctrl-C received. Exiting.");
                let _ = tx.send(());
            }
        }
    })
}

pub fn tick() -> i64 {
    precise_time_ns() as i64
}

pub fn tock(t0: &i64) -> i64 {
    precise_time_ns() as i64 - t0
}

// Can this be made faster? Is it worth it?
fn create_temp_file(prefix: &Path) -> Result<(File, PathBuf), UnixError> {
    let mut template = prefix.as_os_str().to_os_string();
    template.push("_XXXXXX");
    let (fd, temp_path) = mkstemp(template.as_os_str())?;
    let f = unsafe { File::from_raw_fd(fd) };
    Ok((f, temp_path))
}
