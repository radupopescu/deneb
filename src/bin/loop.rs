//#![feature(test)]

//extern crate test;

extern crate deneb;
extern crate futures;
extern crate tokio_core;

use futures::{Future, Sink, Stream};
use futures::sync::mpsc::channel as future_chan;
use tokio_core::reactor::Core;

use std::sync::mpsc::channel as std_chan;

use deneb::common::util::{tick, tock};

fn main() {
    let t0 = tick();

    let mut core = Core::new().unwrap();

    let (task_tx, task_rx) = future_chan(10);

    let niter = 100000;
    let t1 = std::thread::spawn(move || {
        let mut sum: u64 = 0;
        let (tx, rx) = std_chan();
        for v in 0..niter {
            let _ = task_tx.clone().send((v, tx.clone())).wait();
            if let Ok(reply) = rx.recv() {
                sum += reply;
            }
        }
        println!("Sum: {}", sum);
    });

    let fut = task_rx.for_each(move |(v, tx)| {
                                   let _ = tx.send(v * 2);
                                   Ok(())
                               });

    let _ = core.run(fut);

    let _ = t1.join();

    let dt = tock(&t0);

    println!("Time = {}ms", dt as f64 / 1000000.0);
    println!("T/iter = {}ns", dt as f64 / niter as f64);
}

/*
#[cfg(test)]
mod tests {
    use test::Bencher;
    use futures::{Future, Sink, Stream};
    use futures::sync::mpsc::channel as future_chan;

    use std::sync::mpsc::channel as std_chan;
    use tokio_core::reactor::Core;

    #[bench]
    fn bench_add_two(b: &mut Bencher) {
        b.iter(move || {
        let mut core = Core::new().unwrap();

        let (task_tx, task_rx) = future_chan(10);

        let niter = 100;
        let t1 = ::std::thread::spawn(move || {
            let mut _sum: u64 = 0;
            let (tx, rx) = std_chan();
            for v in 0..niter {
                let _ = task_tx.clone().send((v, tx.clone())).wait();
                if let Ok(reply) = rx.recv() {
                    _sum += reply;
                }
            }
        });

        let fut = task_rx.for_each(move |(v, tx)| {
                                    let _ = tx.send(v * 2);
                                    Ok(())
                                });

        let _ = core.run(fut);

        let _ = t1.join();
        });
    }
}   
*/