use nix::sys::signal::{pthread_sigmask, SigSet, SigmaskHow, Signal};

use std::sync::mpsc::Sender;
use std::thread::{spawn, JoinHandle};

use deneb_common::errors::UnixError;

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

