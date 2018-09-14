use nix::{
    sys::signal::{pthread_sigmask, SigSet, SigmaskHow, Signal},
    unistd::{fork as nix_fork, ForkResult}
};

use std::sync::mpsc::Sender;
use std::thread::{spawn, JoinHandle};

use deneb_core::errors::UnixError;

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
                let _ = tx.send(());
            }
        }
    })
}

/// Do a fork and return true if we are the child process
///
/// Calling fork with twice == true will perform a second
/// fork in the child process to completely detach the
/// grandchild process from any console groups.
pub fn fork(twice: bool) -> bool {
    match nix_fork() {
        Ok(ForkResult::Parent { .. }) => { false },
        Ok(ForkResult::Child) => {
            if twice {
                match nix_fork() {
                    Ok(ForkResult::Parent { .. }) => { false },
                    Ok(ForkResult::Child) => { true },
                    Err(_) => panic!("Fork failed!"),
                }
            } else {
                true
            }
        },
        Err(_) => panic!("Fork failed!"),
    }
}
