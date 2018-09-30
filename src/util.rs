use nix::{
    sys::signal::{pthread_sigmask, SigSet, SigmaskHow, Signal},
    unistd::{fork as nix_fork, ForkResult},
};

use std::sync::mpsc::Sender;
use std::thread::{spawn, JoinHandle};

use deneb_core::errors::UnixError;

pub fn block_signals() -> Result<(), UnixError> {
    let mut sigs = SigSet::empty();
    sigs.add(Signal::SIGINT);
    sigs.add(Signal::SIGTERM);
    sigs.add(Signal::SIGHUP);
    pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&sigs), None)?;
    Ok(())
}

pub fn set_signal_handler(tx: Sender<()>) -> JoinHandle<()> {
    spawn(move || {
        let mut sigs = SigSet::empty();
        sigs.add(Signal::SIGINT);
        sigs.add(Signal::SIGTERM);
        sigs.add(Signal::SIGHUP);
        if let Ok(sig) = sigs.wait() {
            println!("Received signal: {:?}", sig);
            if sig == Signal::SIGINT || sig == Signal::SIGTERM || sig == Signal::SIGHUP {
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
#[allow(match_wild_err_arm)]
pub fn fork(twice: bool) -> bool {
    match nix_fork() {
        Ok(ForkResult::Parent { .. }) => false,
        Ok(ForkResult::Child) => {
            if twice {
                match nix_fork() {
                    Ok(ForkResult::Parent { .. }) => false,
                    Ok(ForkResult::Child) => true,
                    Err(_) => panic!("Fork failed!"),
                }
            } else {
                true
            }
        }
        Err(_) => panic!("Fork failed!"),
    }
}
