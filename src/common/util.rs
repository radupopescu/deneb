pub mod file {
    use nix::unistd::mkstemp;

    use std::fs::{remove_file, rename};
    use std::io::Write;
    use std::os::unix::io::FromRawFd;

    use std::fs::File;
    use std::path::{Path, PathBuf};

    use common::errors::*;

    // Can this be made faster? Is it worth it?
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

    /// Atomically writes a buffer to a file
    ///
    /// The buffer is first written to a temporary file, then, upon success,
    /// the temporary file is atomically renamed to the final file name.
    pub fn atomic_write(file_name: &Path, bytes: &[u8]) -> Result<()> {
        let (mut f, temp_path) = create_temp_file(file_name)?;
        if let Ok(()) = f.write_all(bytes) {
            rename(temp_path, file_name)?;
        } else {
            remove_file(temp_path)?;
        }
        Ok(())
    }
}

use nix::sys::signal::{SigmaskHow, Signal, SigSet, pthread_sigmask};
use time::precise_time_ns;

use std::sync::mpsc::Sender;
use std::thread::{JoinHandle, spawn};

use common::errors::*;

pub fn block_signals() -> Result<()> {
    let mut sigs = SigSet::empty();
    sigs.add(Signal::SIGINT);
    pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&sigs), None)?;
    Ok(())
}

pub fn set_sigint_handler(tx: Sender<()>) -> Result<JoinHandle<()>> {
    Ok(spawn(move || {
        let mut sigs = SigSet::empty();
        sigs.add(Signal::SIGINT);
        if let Ok(sig) = sigs.wait() {
            if let Signal::SIGINT = sig {
                debug!("Ctrl-C received. Exiting.");
                let _ = tx.send(());
            }
        }
    }))
}

pub fn tick() -> i64 {
    precise_time_ns() as i64
}

pub fn tock(t0: &i64) -> i64 {
    precise_time_ns() as i64 - t0
}
