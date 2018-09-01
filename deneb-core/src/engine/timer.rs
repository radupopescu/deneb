use crossbeam_channel::{unbounded, Sender};

use std::{
    collections::{HashMap, LinkedList},
    thread::{sleep, spawn, JoinHandle},
    time::{Duration, Instant},
};

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

#[allow(dead_code)]
pub(crate) enum Resolution {
    Ms,
    TenMs,
    HundredMs,
    Second,
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
            for mut e in bucket.into_iter() {
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

pub(crate) struct Timer {
    joiner: JoinHandle<()>,
    event_queue: Sender<Event>,
    quit: Sender<()>,
}

impl Timer {
    pub(crate) fn new(resolution: Resolution) -> Timer {
        let (event_queue, new_events) = unbounded();
        let (quit_tx, quit_rx) = unbounded();
        let joiner = spawn(move || {
            let mut wheel = Wheel::new(resolution);
            loop {
                let t0 = Instant::now();
                if let Some(_) = quit_rx.try_recv() {
                    break;
                }
                while let Some(ev) = new_events.try_recv() {
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

    pub(crate) fn schedule<F>(&mut self, delay: Duration, repeat: bool, action: F)
    where
        F: FnMut() + Send + 'static,
    {
        let event = Event::new(action, delay, repeat);
        self.event_queue.send(event);
    }

    pub(crate) fn stop(self) {
        self.quit.send(());
        let _ = self.joiner.join();
    }
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
                tx.send(());
            },
            Duration::from_millis(1),
            false,
        );
        wheel.schedule(event);
        assert_eq!(wheel.tick().len(), 0);
        let evs = wheel.tick();
        for mut e in evs.into_iter() {
            (e.action)();
        }
        let mut counter = 0;
        rx.for_each(|_| counter += 1);

        assert_eq!(counter, 1);
    }

    #[test]
    fn wheel_repeat() {
        let mut wheel = Wheel::new(Resolution::Ms);
        let (tx, rx) = unbounded();
        let event = Event::new(
            move || {
                tx.send(());
            },
            Duration::from_millis(1),
            false,
        );
        wheel.schedule(event);
        assert_eq!(wheel.tick().len(), 0);
        let evs1 = wheel.tick();
        for mut e in evs1.into_iter() {
            (e.action)();
            wheel.schedule(e);
        }

        assert_eq!(wheel.tick().len(), 0);
        let evs2 = wheel.tick();
        for mut e in evs2.into_iter() {
            (e.action)();
        }

        let mut counter = 0;
        rx.for_each(|_| counter += 1);

        assert_eq!(counter, 2);
    }

    #[test]
    fn timer_one_shot() {
        let (tx, rx) = unbounded();
        let mut timer = Timer::new(Resolution::Ms);
        timer.schedule(Duration::from_millis(1), false, move || {
            tx.send(1);
        });
        let mut sum = 0;
        rx.for_each(|v| sum += v);
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
                txc.send(1);
            });
        }
        drop(tx);
        let mut sum = 0;
        rx.for_each(|v| sum += v);
        timer.stop();
        assert_eq!(sum, num_events);
    }

    #[test]
    fn timer_repeat() {
        let (tx, rx) = unbounded();
        let mut timer = Timer::new(Resolution::HundredMs);
        timer.schedule(Duration::from_millis(100), true, move || {
            tx.send(1);
        });
        ::std::thread::sleep(Duration::from_secs(1));
        timer.stop();
        let mut sum = 0;
        rx.for_each(|v| sum += v);
    }
}
