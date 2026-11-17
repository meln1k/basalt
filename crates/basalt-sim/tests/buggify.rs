//! BUGGIFY's switches and firing rates, per-seed knobs and swarm configs, and the site that
//! cancels a random task.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::rc::Rc;

use basalt_env::{Duration, Priority, buggify};
use basalt_sim::Sim;

/// Reaches one `buggify!()` site `reaches` times in a fresh world for `seed`, and one
/// `buggify!(0.5)` site as often. Returns how often each fired.
fn fire_counts(seed: u64, reaches: u32) -> (u32, u32) {
    let _sim = Sim::new(seed);
    let (mut default, mut half) = (0, 0);
    for _ in 0..reaches {
        default += u32::from(buggify!());
        half += u32::from(buggify!(0.5));
    }
    (default, half)
}

#[test]
fn buggify_fires_at_one_in_sixteen_and_a_quarter_of_p() {
    const SEEDS: u64 = 4000;
    const REACHES: u32 = 40;
    let (mut default, mut half) = (0u32, 0u32);
    let (mut default_on, mut half_on) = (0u32, 0u32);
    for seed in 0..SEEDS {
        let (d, h) = fire_counts(seed, REACHES);
        assert_eq!((d, h), fire_counts(seed, REACHES), "seed {seed}");
        default += d;
        half += h;
        default_on += u32::from(d > 0);
        half_on += u32::from(h > 0);
    }
    let reaches = f64::from(REACHES) * SEEDS as f64;
    let rate = |fired: u32| f64::from(fired) / reaches;
    let share = |on: u32| f64::from(on) / SEEDS as f64;
    // Each tolerance is about 5 standard deviations, where most of the variance comes from
    // the per-run switch.
    assert!(
        (rate(default) - 1.0 / 16.0).abs() < 0.01,
        "{}",
        rate(default)
    );
    assert!((rate(half) - 0.25 * 0.5).abs() < 0.015, "{}", rate(half));
    // A switched-on site fires within 40 reaches all but never, so these are the switch rates.
    assert!(
        (share(default_on) - 0.25).abs() < 0.035,
        "{}",
        share(default_on)
    );
    assert!((share(half_on) - 0.25).abs() < 0.035, "{}", share(half_on));
}

#[test]
fn a_site_switched_off_never_fires_in_that_run() {
    let mut off = 0;
    for seed in 0..200 {
        let (d, _) = fire_counts(seed, 200);
        // On, it fires about 50 times in 200 reaches; off, never.
        assert!(d == 0 || d > 20, "seed {seed}: {d}");
        off += u32::from(d == 0);
    }
    assert!((100..200).contains(&off), "{off}");
}

#[test]
fn lifting_faults_turns_buggify_off() {
    for seed in 0..100 {
        let sim = Sim::new(seed);
        let fired = (0..100).filter(|_| buggify!(1.0)).count();
        // Switched on in about a quarter of the seeds, and then firing every time.
        assert!(fired == 0 || fired == 100, "seed {seed}: {fired}");
        sim.control().lift_faults();
        assert!(!(0..100).any(|_| buggify!(1.0)), "seed {seed}");
    }
}

#[test]
fn the_world_counts_what_each_site_reached_and_fired() {
    let sim = Sim::new(1);
    let mut fired = 0;
    for _ in 0..30 {
        fired += u64::from(buggify!());
        basalt_env::probe!("test.counted");
    }
    let coverage = sim.coverage();
    let here = |file: &str| file == file!();
    let sites: Vec<_> = coverage
        .buggify
        .iter()
        .filter(|(s, _)| here(s.file))
        .collect();
    assert_eq!(sites.len(), 1, "{sites:?}");
    assert_eq!((sites[0].1.reached, sites[0].1.fired), (30, fired));
    let probes: Vec<_> = coverage
        .probes
        .iter()
        .filter(|(s, _)| here(s.file))
        .collect();
    assert_eq!(probes.len(), 1);
    assert_eq!(*probes[0].1, 30);
}

#[test]
fn knobs_and_swarm_configs_vary_across_seeds_and_repeat_for_a_seed() {
    let draw = |seed| {
        let sim = Sim::new(seed);
        let knobs = basalt_env::knobs();
        let swarm = sim.control().swarm().clone();
        (knobs.max_buggified_delay, swarm)
    };
    let mut delays = BTreeSet::new();
    let mut faults = BTreeSet::new();
    for seed in 0..50 {
        let (delay, swarm) = draw(seed);
        assert_eq!((delay, swarm.clone()), draw(seed), "seed {seed}");
        assert!(delay <= Duration::from_secs(1), "seed {seed}: {delay:?}");
        let randomized = swarm.randomized_knobs().contains(&"max_buggified_delay");
        let default = basalt_env::Knobs::default().max_buggified_delay;
        assert!(randomized || delay == default, "seed {seed}");
        delays.insert(delay);
        faults.insert(swarm.faults().to_vec());
    }
    assert!(delays.len() > 10, "{delays:?}");
    assert!(faults.len() > 10, "{faults:?}");
}

/// Sets its flag when dropped.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn buggify_cancels_a_random_task_in_some_seeds_but_never_a_main_task() {
    let mut cancelled = 0;
    const SEEDS: u64 = 40;
    for seed in 0..SEEDS {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let m = ctrl.add_machine("z", 0);
        let (worker_dropped, main_dropped) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(false)));
        let (w, md) = (worker_dropped.clone(), main_dropped.clone());
        ctrl.add_process(m, "p", move |ctx| {
            let (w, md) = (w.clone(), md.clone());
            Box::pin(async move {
                let _main = DropFlag(md);
                let c = ctx.clone();
                ctx.spawn("worker", Priority::Default, async move {
                    let _worker = DropFlag(w);
                    loop {
                        c.sleep(Duration::from_millis(1)).await;
                    }
                })
                .detach();
                loop {
                    ctx.sleep(Duration::from_millis(1)).await;
                }
            })
        });
        let h = ctrl.add_machine("harness", 0);
        ctrl.protect(h);
        let harness = ctrl.add_process(h, "harness", |_| Box::pin(async {}));
        let c = harness.clone();
        let timer = harness.spawn("timer", Priority::Default, async move {
            c.sleep(Duration::from_secs(10)).await;
        });
        sim.block_on(timer).unwrap();

        let traced = sim.trace().iter().any(|r| r.name == "TaskCancelled");
        assert_eq!(traced, worker_dropped.get(), "seed {seed}");
        assert!(!main_dropped.get(), "seed {seed}");
        cancelled += u32::from(worker_dropped.get());
    }
    // The site is switched on in about a quarter of the seeds, and then fires within 10 s.
    assert!((1..SEEDS as u32).contains(&cancelled), "{cancelled}");
}

/// A task someone awaits is never picked: its awaiter would wait forever, which only a kill of
/// the task's process can cause, and a kill takes the awaiter's process too.
#[test]
fn buggify_never_cancels_a_task_someone_awaits() {
    let mut cancelled = 0;
    for seed in 0..100 {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let m = ctrl.add_machine("z", 0);
        let done = Rc::new(Cell::new(false));
        let d = done.clone();
        ctrl.add_process(m, "p", move |ctx| {
            let d = d.clone();
            Box::pin(async move {
                let c = ctx.clone();
                let child = ctx.spawn("child", Priority::Default, async move {
                    // At most 100 s even if BUGGIFY delays every sleep by the most it may.
                    for _ in 0..100 {
                        c.sleep(Duration::from_millis(1)).await;
                    }
                });
                // A detached sibling the site may pick instead.
                let c = ctx.clone();
                ctx.spawn("sibling", Priority::Default, async move {
                    loop {
                        c.sleep(Duration::from_millis(10)).await;
                    }
                })
                .detach();
                child.await;
                d.set(true);
            })
        });
        let h = ctrl.add_machine("harness", 0);
        ctrl.protect(h);
        let harness = ctrl.add_process(h, "harness", |_| Box::pin(async {}));
        let c = harness.clone();
        let timer = harness.spawn("timer", Priority::Default, async move {
            c.sleep(Duration::from_secs(120)).await;
        });
        sim.block_on(timer).unwrap();
        assert!(done.get(), "seed {seed}");
        let names: Vec<_> = sim
            .trace()
            .iter()
            .filter(|r| r.name == "TaskCancelled")
            .map(|r| r.fields.clone())
            .collect();
        for fields in &names {
            assert!(
                fields.contains(&("name", "\"sibling\"".to_owned())),
                "{fields:?}"
            );
        }
        cancelled += names.len();
    }
    assert!(
        cancelled > 0,
        "the site never fired, so the test shows nothing"
    );
}
