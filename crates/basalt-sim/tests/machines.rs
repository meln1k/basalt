//! Machines, processes, kills, reboots, and clocks, through `SimControl`.

use std::cell::{Cell, RefCell};
use std::future::{pending, poll_fn};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, Waker};

use basalt_env::{Cost, Ctx, Duration, Elapsed, Instant, Priority, Task, UNIX_EPOCH, trace};
use basalt_sim::{KillType, Sim, SimControl};

mod common;
use common::process;

/// Sets its flag when dropped.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

fn flag() -> Rc<Cell<bool>> {
    Rc::default()
}

/// When dropped, spawns a task of its process that would set `polled` if it ever ran.
struct SpawnOnDrop {
    ctx: Ctx,
    polled: Rc<Cell<bool>>,
    dropped: Rc<Cell<bool>>,
}

impl Drop for SpawnOnDrop {
    fn drop(&mut self) {
        let (polled, dropped) = (self.polled.clone(), DropFlag(self.dropped.clone()));
        self.ctx
            .spawn("spawned-in-drop", Priority::Default, async move {
                let _dropped = dropped;
                polled.set(true);
            })
            .detach();
    }
}

#[test]
fn a_kill_drops_every_future_of_the_machines_processes() {
    for seed in 0..10 {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let a = ctrl.add_machine("zone-a", 0);
        let flags: [Rc<Cell<bool>>; 4] = Default::default();
        let [main, sleeping, parked, spawned] = flags.clone();
        let spawned_polled = flag();
        // The handle of a parked task of `a`, which `b` holds.
        let held: Rc<RefCell<Option<Task<()>>>> = Rc::default();
        let (h, polled) = (held.clone(), spawned_polled.clone());
        ctrl.add_process(a, "a", move |ctx| {
            let [main, sleeping, parked] = [&main, &sleeping, &parked].map(|f| DropFlag(f.clone()));
            let (h, polled, spawned) = (h.clone(), polled.clone(), spawned.clone());
            Box::pin(async move {
                let _main = main;
                let c = ctx.clone();
                ctx.spawn("sleeping", Priority::Default, async move {
                    let _flag = sleeping;
                    c.sleep(Duration::from_secs(100)).await;
                })
                .detach();
                let task = ctx.spawn("parked", Priority::Default, async move {
                    let _flag = parked;
                    pending::<()>().await;
                });
                *h.borrow_mut() = Some(task);
                let spawner = SpawnOnDrop {
                    ctx: ctx.clone(),
                    polled,
                    dropped: spawned,
                };
                ctx.spawn("spawns-on-drop", Priority::Background, async move {
                    let _spawner = spawner;
                    pending::<()>().await;
                })
                .detach();
                pending::<()>().await;
            })
        });

        let b = process(&sim, "b");
        let c = b.clone();
        let killer = b.spawn("killer", Priority::Default, async move {
            c.sleep(Duration::from_secs(1)).await;
            let held = held.take().expect("a's entry point ran");
            ctrl.kill(a, KillType::Crash);
            let dropped = flags.each_ref().map(|f| f.get());
            let awaited = c.timeout(Duration::from_secs(10), held).await;
            (dropped, awaited, ctrl.is_up(a))
        });
        let (dropped, awaited, up) = sim.block_on(killer).unwrap();
        assert_eq!(
            dropped, [true; 4],
            "seed {seed}: main, sleeping, parked, spawned in drop"
        );
        assert!(
            !spawned_polled.get(),
            "a task spawned by a dying process never runs"
        );
        assert_eq!(
            awaited,
            Err(Elapsed),
            "a dead process's task never completes"
        );
        assert!(!up);
        // A task of `a` left behind would hang the run.
        sim.run().unwrap();
    }
}

/// Starts a machine whose process traces `Started` and then ticks forever, crashes it and
/// reboots it, then reboots it while it's up. Returns each incarnation's `Ctx`, as `Debug`,
/// the machine's trace events, and the unseed.
fn reboot_run(seed: u64) -> (Vec<String>, Vec<&'static str>, u64) {
    let sim = Sim::new(seed);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 1);
    let starts = Rc::new(RefCell::new(Vec::new()));
    let s = starts.clone();
    ctrl.add_process(m, "replica", move |ctx| {
        s.borrow_mut().push(format!("{ctx:?}"));
        Box::pin(async move {
            trace!(Info, "Started");
            loop {
                ctx.sleep(Duration::from_millis(300)).await;
            }
        })
    });
    let driver = process(&sim, "driver");
    let (c, ctrl2) = (driver.clone(), ctrl.clone());
    let task = driver.spawn("driver", Priority::Default, async move {
        c.sleep(Duration::from_secs(1)).await;
        ctrl2.kill(m, KillType::Crash);
        assert!(!ctrl2.is_up(m));
        c.sleep(Duration::from_secs(1)).await;
        ctrl2.reboot(m);
        assert!(ctrl2.is_up(m));
        c.sleep(Duration::from_secs(1)).await;
        ctrl2.reboot(m);
        c.sleep(Duration::from_secs(1)).await;
    });
    sim.block_on(task).unwrap();
    let events = sim.trace().iter().map(|r| r.name).collect();
    (starts.take(), events, sim.unseed())
}

#[test]
fn a_reboot_reruns_the_entry_point_as_a_new_incarnation() {
    let (starts, events, unseed) = reboot_run(4);
    assert_eq!(starts.len(), 3);
    assert!(
        starts[0] != starts[1] && starts[1] != starts[2] && starts[0] != starts[2],
        "each incarnation has a fresh process id: {starts:?}"
    );
    assert_eq!(
        events,
        [
            "Started",
            "MachineKilled",
            "MachineRebooted",
            "Started",
            // Rebooting a machine that's up crashes it first.
            "MachineKilled",
            "MachineRebooted",
            "Started"
        ]
    );
    assert_eq!(
        reboot_run(4).2,
        unseed,
        "kills and reboots are deterministic"
    );
}

#[test]
fn a_task_woken_after_its_process_died_is_never_polled() {
    for seed in 0..10 {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let a = ctrl.add_machine("zone-a", 0);
        let polls = Rc::new(Cell::new(0));
        let waker: Rc<RefCell<Option<Waker>>> = Rc::default();
        let (p, w) = (polls.clone(), waker.clone());
        ctrl.add_process(a, "a", move |ctx| {
            let (p, w) = (p.clone(), w.clone());
            ctx.spawn(
                "parked",
                Priority::Default,
                poll_fn(move |cx| {
                    p.set(p.get() + 1);
                    *w.borrow_mut() = Some(cx.waker().clone());
                    Poll::<()>::Pending
                }),
            )
            .detach();
            Box::pin(async {})
        });

        let b = process(&sim, "b");
        let c = b.clone();
        let task = b.spawn("wakes-a-dead-task", Priority::Default, async move {
            c.sleep(Duration::from_secs(1)).await;
            let stale = waker.take().expect("parked ran once");
            ctrl.kill(a, KillType::Crash);
            stale.wake();
            c.sleep(Duration::from_secs(1)).await;
        });
        sim.block_on(task).unwrap();
        assert_eq!(polls.get(), 1, "seed {seed}");
        sim.run().unwrap();
    }
}

/// What a task that kills its own machine sees, and what another process sees right after.
#[derive(Default)]
struct SelfKill {
    spins: Cell<u32>,
    spins_at_kill: Cell<u32>,
    killer_dropped: Rc<Cell<bool>>,
    killer_resumed: Cell<bool>,
    /// The waker of a task of another process, which the killer wakes right after the kill.
    observer: RefCell<Option<Waker>>,
}

#[test]
fn a_task_killing_its_own_machine_is_dropped_before_any_other_task_runs() {
    for seed in 0..10 {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let a = ctrl.add_machine("zone-a", 0);
        let state = Rc::new(SelfKill::default());
        let (st, ctrl2) = (state.clone(), ctrl.clone());
        ctrl.add_process(a, "a", move |ctx| {
            let (st, ctrl) = (st.clone(), ctrl2.clone());
            Box::pin(async move {
                let (c, spin) = (ctx.clone(), st.clone());
                ctx.spawn("spinner", Priority::Default, async move {
                    loop {
                        spin.spins.set(spin.spins.get() + 1);
                        c.sleep(Duration::from_millis(1)).await;
                    }
                })
                .detach();
                let _killer = DropFlag(st.killer_dropped.clone());
                ctx.sleep(Duration::from_secs(1)).await;
                st.spins_at_kill.set(st.spins.get());
                ctrl.kill(a, KillType::Crash);
                st.observer.take().expect("the observer is parked").wake();
                ctx.yield_now().await;
                st.killer_resumed.set(true);
            })
        });

        let b = process(&sim, "b");
        let (st, woken) = (state.clone(), Cell::new(false));
        let observed = b.spawn(
            "observer",
            Priority::Coordination,
            poll_fn(move |cx| {
                if woken.replace(true) {
                    return Poll::Ready(st.killer_dropped.get());
                }
                *st.observer.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }),
        );
        assert_eq!(sim.block_on(observed), Ok(true), "seed {seed}");
        assert!(!state.killer_resumed.get(), "seed {seed}");
        assert!(state.spins_at_kill.get() > 900, "seed {seed}");
        assert_eq!(state.spins.get(), state.spins_at_kill.get(), "seed {seed}");
        sim.run().unwrap();
    }
}

#[test]
fn a_ctx_kept_from_before_a_kill_stays_dead() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 2);
    let old = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    let disks = old.disks();
    sim.run().unwrap();
    ctrl.kill(m, KillType::Crash);
    ctrl.reboot(m);

    let (polled, dropped) = (flag(), flag());
    let (p, d) = (polled.clone(), DropFlag(dropped.clone()));
    let late = old.spawn("late", Priority::Default, async move {
        let _dropped = d;
        p.set(true);
    });
    assert!(
        dropped.get(),
        "the future is dropped as soon as it's spawned"
    );

    let driver = process(&sim, "driver");
    let (c, o) = (driver.clone(), old.clone());
    let ran = Arc::new(AtomicBool::new(false));
    let r = ran.clone();
    let checks = driver.spawn("checks", Priority::Default, async move {
        let second = Duration::from_secs(1);
        let late = c.timeout(second, late).await;
        let slept = c.timeout(second, o.sleep(Duration::from_millis(1))).await;
        let job = move || r.store(true, Ordering::Relaxed);
        let computed = c.timeout(second, o.compute(Cost::ZERO, job)).await;
        let listen = o.listen(7000).await.err().map(|e| e.to_string());
        let list = o
            .disk(o.disks()[0])
            .list("")
            .await
            .err()
            .map(|e| e.to_string());
        (
            late,
            slept,
            computed,
            listen,
            list,
            o.disks(),
            o.now() == c.now(),
        )
    });
    let (late, slept, computed, listen, list, disks_after, same_clock) =
        sim.block_on(checks).unwrap();
    assert!(!polled.get());
    assert_eq!(late, Err(Elapsed));
    assert_eq!(slept, Err(Elapsed));
    assert_eq!(computed, Err(Elapsed));
    assert!(!ran.load(Ordering::Relaxed), "the compute job never runs");
    for error in [listen, list] {
        assert!(
            error.as_deref().is_some_and(|e| e.contains("is dead")),
            "{error:?}"
        );
    }
    assert_eq!(disks_after, disks, "clocks and disks keep working");
    assert!(same_clock);
}

#[test]
fn a_wipe_replaces_the_disks_and_a_crash_keeps_them() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("rack-1", 2);
    let other = ctrl.add_machine("rack-2", 1);
    let ctx = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    let before = ctx.disks();
    assert_eq!(before.len(), 2);
    assert_eq!(&*ctrl.zone(m), "rack-1");

    ctrl.kill(m, KillType::Crash);
    assert_eq!(ctx.disks(), before);
    // Wiping a machine that's already down still replaces its disks.
    ctrl.kill(m, KillType::Wipe);
    let after = ctx.disks();
    assert_eq!(after.len(), 2);
    assert!(
        after.iter().all(|d| !before.contains(d)),
        "{before:?} -> {after:?}"
    );
    let other_disks = process(&sim, "x").disks();
    assert!(after.iter().all(|d| !other_disks.contains(d)));
    assert!(!ctrl.is_up(m) && ctrl.is_up(other));
    ctrl.reboot(m);
    assert!(ctrl.is_up(m));
}

/// Runs `task` as a task of `ctx`'s process and returns when it finished, in virtual time.
fn finish_time(sim: &Sim, ctx: &Ctx, task: impl Future<Output = ()> + 'static) -> Instant {
    let ctrl: SimControl = sim.control();
    let task = ctx.spawn("timed", Priority::Default, async move {
        task.await;
        ctrl.now()
    });
    sim.block_on(task).unwrap()
}

fn secs(s: f64) -> Instant {
    Instant::from_nanos((s * 1e9) as u64)
}

#[test]
fn monotonic_clocks_drift_per_machine() {
    let sim = Sim::new(1);
    let ctrl = sim.control();
    let machine = |zone| {
        let m = ctrl.add_machine(zone, 0);
        (m, ctrl.add_process(m, zone, |_| Box::pin(async {})))
    };
    let (fast_m, fast) = machine("fast");
    let (slow_m, slow) = machine("slow");
    let (_, steady) = machine("steady");
    ctrl.set_clock_drift(fast_m, 1_000);
    ctrl.set_clock_drift(slow_m, -500_000);

    // A second on the slow machine takes two of virtual time.
    let s = slow.clone();
    let done = finish_time(
        &sim,
        &slow,
        async move { s.sleep(Duration::from_secs(1)).await },
    );
    assert!(done >= secs(2.0) && done <= secs(2.001), "{done:?}");
    assert!(slow.now() >= Instant::ZERO + Duration::from_secs(1));

    let (f, st) = (fast.clone(), steady.clone());
    let at = Rc::new(Cell::new((Instant::ZERO, Instant::ZERO)));
    let a = at.clone();
    finish_time(&sim, &steady, async move {
        st.sleep(Duration::from_secs(10)).await;
        a.set((f.now(), st.now()));
    });
    let (f, st) = at.get();
    let ahead = f - st;
    // 0.1% of the steady clock's reading, which is just past 12s.
    assert!(
        ahead >= Duration::from_millis(12) && ahead < Duration::from_micros(12_100),
        "{ahead:?}"
    );
}

#[test]
fn a_drift_change_during_a_sleep_moves_its_wake_up() {
    let sim = Sim::new(2);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 0);
    let ctx = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    let c = ctx.clone();
    let ctrl2 = ctrl.clone();
    ctx.spawn("slows-the-clock", Priority::Default, async move {
        c.sleep(Duration::from_secs(5)).await;
        ctrl2.set_clock_drift(m, -500_000);
    })
    .detach();
    let c = ctx.clone();
    let woke = finish_time(&sim, &ctx, async move {
        c.sleep(Duration::from_secs(10)).await;
        assert!(c.now() >= Instant::ZERO + Duration::from_secs(10));
    });
    // 5s at full speed, then the other 5s at half speed.
    assert!(woke >= secs(15.0) && woke <= secs(15.003), "{woke:?}");
}

#[test]
fn wall_clocks_skew_and_jump_per_machine() {
    let sim = Sim::new(3);
    let ctrl = sim.control();
    let (a, b) = (ctrl.add_machine("a", 0), ctrl.add_machine("b", 0));
    let ctx_a = ctrl.add_process(a, "a", |_| Box::pin(async {}));
    let ctx_b = ctrl.add_process(b, "b", |_| Box::pin(async {}));
    let start = ctrl.wall_clock(a);
    assert_eq!(start, UNIX_EPOCH + Duration::from_secs(1_767_225_600));
    assert_eq!(ctx_a.wall_clock(), ctx_b.wall_clock());

    ctrl.set_clock_drift(a, 2_000);
    let c = ctx_b.clone();
    finish_time(&sim, &ctx_b, async move {
        c.sleep(Duration::from_secs(5)).await
    });
    let skew = ctx_a
        .wall_clock()
        .duration_since(ctx_b.wall_clock())
        .unwrap();
    assert!(
        skew >= Duration::from_millis(10) && skew < Duration::from_micros(10_010),
        "{skew:?}"
    );
    assert_eq!(ctrl.wall_clock(b), ctx_b.wall_clock());

    // Jump b's wall clock back an hour: its monotonic clock and a's clocks don't move.
    let (mono_b, wall_a) = (ctx_b.now(), ctx_a.wall_clock());
    let back = ctx_b.wall_clock() - Duration::from_secs(3600);
    ctrl.set_wall_clock(b, back);
    assert_eq!(ctx_b.wall_clock(), back);
    assert_eq!(ctx_b.now(), mono_b);
    assert_eq!(ctx_a.wall_clock(), wall_a);
    // Even to before the epoch.
    let before_epoch = UNIX_EPOCH - Duration::from_millis(1500);
    ctrl.set_wall_clock(b, before_epoch);
    assert_eq!(ctrl.wall_clock(b), before_epoch);
}

#[test]
fn speeding_a_clock_up_during_a_sleep_moves_its_wake_up_earlier() {
    let sim = Sim::new(5);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 0);
    ctrl.set_clock_drift(m, -500_000);
    let ctx = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    let (c, ctrl2) = (ctx.clone(), ctrl.clone());
    ctx.spawn("restores-the-clock", Priority::Default, async move {
        c.sleep(Duration::from_millis(500)).await;
        ctrl2.set_clock_drift(m, 0);
    })
    .detach();
    let c = ctx.clone();
    let woke = finish_time(&sim, &ctx, async move {
        c.sleep(Duration::from_secs(10)).await;
    });
    // 0.5s local at half speed is 1s virtual, then the other 9.5s at full speed.
    assert!(woke >= secs(10.5) && woke <= secs(10.503), "{woke:?}");
}

#[test]
fn a_dead_process_may_still_use_a_disk_its_machine_lost() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 1);
    let old = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    let old_disk = old.disks()[0];
    // Taken while the process was alive.
    let held = old.disk(old_disk);
    ctrl.kill(m, KillType::Wipe);
    assert!(!old.disks().contains(&old_disk));
    let lost = old.disk(old_disk);

    let driver = process(&sim, "driver");
    let task = driver.spawn("checks", Priority::Default, async move {
        let held = held.list("").await.err().map(|e| e.to_string());
        let lost = lost.list("").await.err().map(|e| e.to_string());
        (held, lost)
    });
    let (held, lost) = sim.block_on(task).unwrap();
    for error in [held, lost] {
        assert!(
            error.as_deref().is_some_and(|e| e.contains("is dead")),
            "{error:?}"
        );
    }
}

#[test]
fn a_reboot_after_a_wipe_starts_on_the_new_disks() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 2);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let s = seen.clone();
    let first = ctrl.add_process(m, "p", move |ctx| {
        s.borrow_mut().push(ctx.disks());
        Box::pin(async {})
    });
    ctrl.kill(m, KillType::Wipe);
    ctrl.reboot(m);
    let seen = seen.take();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].len(), 2);
    assert!(seen[1].iter().all(|d| !seen[0].contains(d)), "{seen:?}");
    assert_eq!(
        first.disks(),
        seen[1],
        "a dead Ctx reads its machine's disks now"
    );
}

#[test]
fn detaching_a_task_of_a_dead_process_drops_it() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 0);
    let ctx = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    // A handle to a task of `p`, held by someone else, detached after the kill.
    let dropped = flag();
    let d = DropFlag(dropped.clone());
    let handle = ctx.spawn("parked", Priority::Default, async move {
        let _d = d;
        pending::<()>().await;
    });
    ctrl.kill(m, KillType::Crash);
    assert!(dropped.get());
    handle.detach();
    sim.run().unwrap();
}

#[test]
fn drop_code_of_a_killed_process_traces_under_its_own_label() {
    struct TraceOnDrop;
    impl Drop for TraceOnDrop {
        fn drop(&mut self) {
            trace!(Info, "VictimDropped");
        }
    }
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("z", 0);
    ctrl.add_process(m, "victim", |_| {
        Box::pin(async {
            let _t = TraceOnDrop;
            pending::<()>().await;
        })
    });
    let killer = process(&sim, "killer");
    let task = killer.spawn("kills", Priority::Default, async move {
        ctrl.kill(m, KillType::Crash);
    });
    sim.block_on(task).unwrap();
    let record = sim
        .trace()
        .iter()
        .find(|r| r.name == "VictimDropped")
        .cloned()
        .expect("traced");
    assert_eq!(record.process.as_deref(), Some("victim"));
}
