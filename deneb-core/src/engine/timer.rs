use crossbeam_channel::{unbounded, Sender};

use std::{
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
    buckets: Vec<Vec<ScheduledEvent>>,
    tick_time: Duration,
    pos: usize,
}

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
        let mut buckets = vec![];
        for _ in 0..num_buckets {
            buckets.push(vec![]);
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
        println!("Schedule: rounds: {}, bucket_id: {}", rounds, bucket_id);
        self.buckets[bucket_id].push(ScheduledEvent { event, rounds })
    }

    #[must_use]
    fn tick(&mut self) -> Vec<Event> {
        let mut expired = vec![];
        let mut to_delete = vec![];

        let bucket = &mut self.buckets[self.pos];
        for (idx, e) in bucket.iter_mut().enumerate() {
            if e.rounds <= 0 {
                to_delete.push(idx);
            }
            e.rounds -= 1;
        }
        for idx in to_delete {
            let ev = bucket.remove(idx);
            expired.push(ev.event);
        }
        self.pos += 1;

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
                    println!("Adding event");
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
                sleep(wheel.tick_time - (t1 - t0));
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
                println!("Increment counter");
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
                println!("Increment counter");
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
}
