use nix::sys::signal::{SigmaskHow, Signal, SigSet, pthread_sigmask};

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
