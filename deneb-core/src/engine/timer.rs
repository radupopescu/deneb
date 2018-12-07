use crossbeam_channel::{unbounded, Sender};

use std::{
    collections::{HashMap, LinkedList},
    thread::{sleep, spawn, JoinHandle},
    time::{Duration, Instant},
};

use errors::EngineError;

/// Resolution of the timer
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) enum Resolution {
    Ms,
    TenMs,
    HundredMs,
    Second,
}

/// A type that can be used to run actions after a specified delay
///
/// The `Timer` type is an implementation of the Hashed Wheel
/// Timer data structure:
///     http://www.cs.columbia.edu/~nahum/w6998/papers/sosp87-timing-wheels.pdf
///
/// Tasks can be scheduled with the timer to run after a specified
/// delay. When the timer is constructed, a background thread is
/// spawned to manage the scheduling and expiration of the actions.
///
/// This is not a precise timer. The delays specified when
/// scheduling actions can be slightly exceeded.
pub(crate) struct Timer {
    joiner: JoinHandle<()>,
    event_queue: Sender<Event>,
    quit: Sender<()>,
}

impl Timer {
    /// Construct a new `Timer` instance
    ///
    /// This function takes a `Resolution` parameter which gives
    /// the tick length of the timer and the number of "buckets"
    /// in a second. For example, Resolution::HundredMs gives a
    /// tick length of 100ms, with ten buckets per second.
    pub(crate) fn new(resolution: Resolution) -> Timer {
        let (event_queue, new_events) = unbounded();
        let (quit_tx, quit_rx) = unbounded();
        let joiner = spawn(move || {
            let mut wheel = Wheel::new(resolution);
            loop {
                let t0 = Instant::now();
                if quit_rx.try_recv().is_ok() {
                    break;
                }
                while let Ok(ev) = new_events.try_recv() {
                    wheel.schedule(ev);
                }
                let triggered = wheel.tick();
                for mut ev in triggered {
                    (ev.action)();
                    if ev.repeat {
                        wheel.schedule(ev);
                    }
                }
                let t1 = Instant::now();
                let dt = t1 - t0;
                if wheel.tick_time > dt {
                    sleep(wheel.tick_time - dt);
                }
            }
        });
        Timer {
            joiner,
            event_queue,
            quit: quit_tx,
        }
    }

    /// Schedule an action to run after a delay
    ///
    /// If `repeat` is true, the action will be repeated after successive
    /// waits of length `delay`.
    pub(crate) fn schedule<F>(&mut self, delay: Duration, repeat: bool, action: F)
    where
        F: FnMut() + Send + 'static,
    {
        let event = Event::new(action, delay, repeat);
        self.event_queue.send(event).map_err(|_| EngineError::Send).unwrap();
    }

    /// Stop the timer
    ///
    /// This stops the timer loop and joins the background thread. After
    /// calling `stop`, the timer is consumed and can no longer be used.
    pub(crate) fn stop(self) {
        self.quit.send(()).map_err(|_| EngineError::Send).unwrap();
        let _ = self.joiner.join();
    }
}

struct Event {
    action: Box<FnMut() + Send>,
    delay: Duration,
    repeat: bool,
}

impl Event {
    fn new<F>(action: F, delay: Duration, repeat: bool) -> Event
    where
        F: FnMut() + Send + 'static,
    {
        Event {
            action: Box::new(action),
            delay,
            repeat,
        }
    }
}

struct ScheduledEvent {
    event: Event,
    rounds: i64,
}

struct Wheel {
    buckets: HashMap<usize, LinkedList<ScheduledEvent>>,
    tick_time: Duration,
    pos: usize,
}

impl Wheel {
    fn new(resolution: Resolution) -> Wheel {
        let (num_buckets, tick_time) = match resolution {
            Resolution::Ms => (1000, Duration::from_millis(1)),
            Resolution::TenMs => (100, Duration::from_millis(10)),
            Resolution::HundredMs => (10, Duration::from_millis(100)),
            Resolution::Second => (1, Duration::from_secs(1)),
        };
        let mut buckets = HashMap::new();
        for idx in 0..num_buckets {
            buckets.insert(idx, LinkedList::new());
        }
        Wheel {
            buckets,
            tick_time,
            pos: 0,
        }
    }

    fn schedule(&mut self, event: Event) {
        let rounds = event.delay.as_secs() as i64;
        let bucket_id = compute_bucket_id(event.delay, self.pos, self.buckets.len());
        if let Some(b) = self.buckets.get_mut(&bucket_id) {
            b.push_front(ScheduledEvent { event, rounds });
        }
    }

    #[must_use]
    fn tick(&mut self) -> LinkedList<Event> {
        let mut expired = LinkedList::new();
        let mut kept = LinkedList::new();
        if let Some(bucket) = self.buckets.remove(&self.pos) {
            for mut e in bucket {
                if e.rounds > 0 {
                    e.rounds -= 1;
                    kept.push_front(e);
                } else {
                    expired.push_front(e.event);
                }
            }
        }
        self.buckets.insert(self.pos, kept);
        self.pos = (self.pos + 1) % self.buckets.len();

        expired
    }
}

fn compute_bucket_id(dt: Duration, pos: usize, nb: usize) -> usize {
    (pos + dt.subsec_millis() as usize) % nb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wheel_one_shot() {
        let mut wheel = Wheel::new(Resolution::Ms);
        let (tx, rx) = unbounded();
        let event = Event::new(
            move || {
                tx.send(()).map_err(|_| EngineError::Send).unwrap();
            },
            Duration::from_millis(1),
            false,
        );
        wheel.schedule(event);
        assert_eq!(wheel.tick().len(), 0);
        let evs = wheel.tick();
        for mut e in evs {
            (e.action)();
        }
        let mut counter = 0;
        rx.iter().for_each(|_| counter += 1);

        assert_eq!(counter, 1);
    }

    #[test]
    fn wheel_repeat() {
        let mut wheel = Wheel::new(Resolution::Ms);
        let (tx, rx) = unbounded();
        let event = Event::new(
            move || {
                tx.send(()).map_err(|_| EngineError::Send).unwrap();
            },
            Duration::from_millis(1),
            false,
        );
        wheel.schedule(event);
        assert_eq!(wheel.tick().len(), 0);
        let evs1 = wheel.tick();
        for mut e in evs1 {
            (e.action)();
            wheel.schedule(e);
        }

        assert_eq!(wheel.tick().len(), 0);
        let evs2 = wheel.tick();
        for mut e in evs2 {
            (e.action)();
        }

        let mut counter = 0;
        rx.iter().for_each(|_| counter += 1);

        assert_eq!(counter, 2);
    }

    #[test]
    fn timer_one_shot() {
        let (tx, rx) = unbounded();
        let mut timer = Timer::new(Resolution::Ms);
        timer.schedule(Duration::from_millis(1), false, move || {
            tx.send(1).map_err(|_| EngineError::Send).unwrap();
        });
        let mut sum = 0;
        rx.iter().for_each(|v| sum += v);
        timer.stop();
        assert_eq!(sum, 1);
    }

    #[test]
    fn timer_multiple_one_shot() {
        let (tx, rx) = unbounded();
        let mut timer = Timer::new(Resolution::TenMs);
        let num_events = 5;
        for _ in 0..num_events {
            let txc = tx.clone();
            timer.schedule(Duration::from_millis(5), false, move || {
                txc.send(1).map_err(|_| EngineError::Send).unwrap();
            });
        }
        drop(tx);
        let mut sum = 0;
        rx.iter().for_each(|v| sum += v);
        timer.stop();
        assert_eq!(sum, num_events);
    }

    #[test]
    fn timer_repeat() {
        let (tx, rx) = unbounded();
        let mut timer = Timer::new(Resolution::HundredMs);
        timer.schedule(Duration::from_millis(100), true, move || {
            tx.send(1).map_err(|_| EngineError::Send).unwrap();
        });
        ::std::thread::sleep(Duration::from_secs(1));
        timer.stop();
        let mut sum = 0;
        rx.iter().for_each(|v| sum += v);
    }

    #[test]
    fn timer_check_order() {
        enum Op {
            Sum(usize),
            Mul(usize),
        }

        let (tx, rx) = unbounded();
        let txc = tx.clone();
        let mut timer = Timer::new(Resolution::Ms);
        timer.schedule(Duration::from_millis(5), false, move || {
            tx.send(Op::Sum(1)).map_err(|_| EngineError::Send).unwrap();
        });
        timer.schedule(Duration::from_millis(1), false, move || {
            txc.send(Op::Mul(2)).map_err(|_| EngineError::Send).unwrap();
        });

        let mut res = 1;
        rx.iter().for_each(|v| match v {
            Op::Sum(x) => res += x,
            Op::Mul(x) => res *= x,
        });

        // (1 * 2) + 1
        assert_eq!(res, 3);
    }
}
