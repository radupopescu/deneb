use std::{
    thread::sleep,
    time::{Duration, Instant},
};

struct Event {
    action: Box<FnMut()>,
    delay: Duration,
    repeat: bool,
}

impl Event {
    fn new<F>(action: F, delay: Duration, repeat: bool) -> Event
    where
        F: FnMut() + 'static,
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

enum Resolution {
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

/*
struct Timer {
    joiner: JoinHandle<()>,
    tx: Sender<Event>,
}

impl Timer {
    fn new() -> Timer {
        let (tx, rx) = unbounded();
        let joiner = spawn(move || {
            let t0 = Instant::now();
            let timer = tick(dt);
            for t in &timer {
                println!("Tick: {:#?}", t.duration_since(t0));
                if quit.try_recv().is_some() {
                    break;
                }
            }
        });
        Timer { joiner, tx }
    }

    fn schedule(&mut self, init_delay: Duration, tick: Option<Duration>, action:)
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    use crossbeam_channel::unbounded;

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

    /*

    #[test]
    fn timer_works() {
        let (tx, rx) = unbounded();
        let j = create_timer(Duration::from_millis(10), rx);
        ::std::thread::sleep(Duration::from_secs(2));
        tx.send(());
        let _ = j.join();
    }
    */
}
