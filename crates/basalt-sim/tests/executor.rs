//! The executor's run loop, virtual time, and ambient world, through `Sim`'s public API.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::future::{Future, pending, poll_fn};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

use basalt_env::{Cost, Ctx, Duration, Instant, Priority, Severity, trace};
use basalt_sim::{Sim, SimError, TraceRecord};

/// What a run produced, to compare runs.
#[derive(Debug, PartialEq)]
struct Outcome {
    /// Which task ran each step, in order.
    order: Vec<String>,
    trace: Vec<TraceRecord>,
    unseed: u64,
}

/// Two processes, each with four equal-priority tasks that take three steps. Every step ends in
/// a sleep to a deadline the other tasks share, so they all become ready at once. With
/// `ctx_draws`, each step also draws that many numbers from `ctx.rng()` and ignores them.
fn equal_priority_run(seed: u64, ctx_draws: usize) -> Outcome {
    let sim = Sim::new(seed);
    let order = Rc::new(RefCell::new(Vec::new()));
    for process in ["a", "b"] {
        let ctx = sim.control().add_process(process);
        for i in 0..4 {
            let (o, c) = (order.clone(), ctx.clone());
            let name = format!("{process}{i}");
            ctx.spawn("step", Priority::Default, async move {
                for round in 1..=3u64 {
                    o.borrow_mut().push(name.clone());
                    for _ in 0..ctx_draws {
                        c.rng().u64();
                    }
                    trace!(Info, "Step", task = name, round = round);
                    let until = Duration::from_millis(10 * round);
                    c.sleep(until.saturating_sub(c.now() - Instant::ZERO)).await;
                }
            })
            .detach();
        }
    }
    sim.run().unwrap();
    Outcome {
        order: order.take(),
        trace: sim.trace().to_vec(),
        unseed: sim.unseed(),
    }
}

#[test]
fn the_same_seed_gives_the_same_trace_and_unseed() {
    let mut unseeds = BTreeSet::new();
    for seed in 0..20 {
        let first = equal_priority_run(seed, 0);
        assert_eq!(first, equal_priority_run(seed, 0), "seed {seed}");
        unseeds.insert(first.unseed);
    }
    assert_eq!(unseeds.len(), 20, "each seed has its own unseed");
}

#[test]
fn the_unseed_covers_the_trace() {
    let unseed = |value: u32| {
        let sim = Sim::new(9);
        let ctx = sim.control().add_process("p");
        ctx.spawn("t", Priority::Default, async move {
            trace!(Info, "Value", value = value);
        })
        .detach();
        sim.run().unwrap();
        sim.unseed()
    };
    assert_eq!(unseed(1), unseed(1));
    assert_ne!(unseed(1), unseed(2), "runs that differ only in their trace");
}

#[test]
fn trace_records_carry_virtual_time_and_the_process_label() {
    let run = equal_priority_run(1, 0);
    assert_eq!(run.trace.len(), 24);
    let last = run.trace.last().unwrap();
    assert!(last.time >= Instant::from_nanos(20_000_000));
    assert_eq!(last.severity, Severity::Info);
    let shown = last.to_string();
    assert!(
        shown.starts_with("0.02") && shown.contains("Step task=") && shown.contains("round=3"),
        "{shown}"
    );
    let processes: BTreeSet<_> = run.trace.iter().map(|r| r.process.clone()).collect();
    assert_eq!(
        processes,
        BTreeSet::from([Some("a".into()), Some("b".into())])
    );
}

#[test]
fn different_seeds_reorder_equal_priority_tasks() {
    let spawn_order: Vec<String> = ["a", "b"]
        .iter()
        .flat_map(|p| (0..4).map(move |i| format!("{p}{i}")))
        .collect();
    let orders: BTreeSet<Vec<String>> = (0..20)
        .map(|seed| equal_priority_run(seed, 0).order)
        .collect();
    let fifo: Vec<String> = (0..3).flat_map(|_| spawn_order.clone()).collect();
    assert!(
        orders.contains(&fifo),
        "some seeds run equal priorities FIFO"
    );
    assert!(orders.len() > 5, "other seeds shuffle them: {orders:?}");
}

#[test]
fn drawing_from_ctx_rng_does_not_change_the_schedule() {
    for seed in 0..20 {
        let plain = equal_priority_run(seed, 0);
        let drawing = equal_priority_run(seed, 7);
        assert_eq!(plain.order, drawing.order, "seed {seed}");
        assert_eq!(plain.trace, drawing.trace, "seed {seed}");
        assert_ne!(
            plain.unseed, drawing.unseed,
            "the ctx stream's state is part of the unseed"
        );
    }
}

#[test]
fn higher_priorities_always_run_first() {
    for seed in 0..20 {
        let sim = Sim::new(seed);
        let ctx = sim.control().add_process("p");
        let ran = Rc::new(RefCell::new(Vec::new()));
        // Lowest first, so FIFO alone would get it backwards.
        for priority in Priority::ALL.iter().rev() {
            for _ in 0..3 {
                let (r, c, p) = (ran.clone(), ctx.clone(), *priority);
                ctx.spawn("t", p, async move {
                    r.borrow_mut().push(p);
                    c.yield_now().await;
                    r.borrow_mut().push(p);
                })
                .detach();
            }
        }
        sim.run().unwrap();
        let ran = ran.take();
        assert_eq!(ran.len(), 24);
        assert!(ran.is_sorted(), "seed {seed}: {ran:?}");
    }
}

/// Completes once `flag` is set.
async fn wait_for(flag: Rc<Cell<bool>>, waiter: Rc<RefCell<Option<Waker>>>) {
    poll_fn(|cx| {
        if flag.get() {
            return Poll::Ready(());
        }
        *waiter.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    })
    .await
}

#[test]
fn a_deadlock_is_reported_with_the_blocked_tasks_names() {
    let sim = Sim::new(3);
    let ctrl = sim.control();
    let (a, b) = (ctrl.add_process("alpha"), ctrl.add_process("beta"));
    // Each waits for the other to go first.
    let (a_done, b_done) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(false)));
    let (a_waker, b_waker) = (Rc::default(), Rc::default());
    let (flag, waker, mine, c) = (b_done.clone(), b_waker, a_done.clone(), a.clone());
    a.spawn("alpha-waits-for-beta", Priority::Coordination, async move {
        c.sleep(Duration::from_secs(1)).await;
        wait_for(flag, waker).await;
        mine.set(true);
    })
    .detach();
    b.spawn("beta-waits-for-alpha", Priority::Default, async move {
        wait_for(a_done, a_waker).await;
        b_done.set(true);
    })
    .detach();
    b.spawn("finishes", Priority::Default, async {}).detach();

    let Err(SimError::Hang(hang)) = sim.run() else {
        panic!("expected a hang");
    };
    assert_eq!(hang.time, Instant::from_nanos(1_000_000_000));
    let blocked: Vec<_> = hang.blocked.iter().map(|t| (t.name, &*t.process)).collect();
    assert_eq!(
        blocked,
        [
            ("alpha-waits-for-beta", "alpha"),
            ("beta-waits-for-alpha", "beta")
        ]
    );
    let report = SimError::Hang(hang).to_string();
    assert!(
        report.contains("\"alpha-waits-for-beta\" on alpha (Coordination, task 0)"),
        "{report}"
    );
}

/// Runs one task that traces a `severity` event at 1s and an `Info` one at 2s.
fn trace_then_continue(severity: Severity) -> (Result<(), SimError>, Vec<TraceRecord>) {
    let sim = Sim::new(5);
    let ctx = sim.control().add_process("p");
    let (c, error) = (ctx.clone(), severity == Severity::Error);
    ctx.spawn("t", Priority::Default, async move {
        c.sleep(Duration::from_secs(1)).await;
        if error {
            trace!(Error, "Broken", why = "invariant");
        } else {
            trace!(Warn, "Degraded", why = "fault");
        }
        c.sleep(Duration::from_secs(1)).await;
        trace!(Info, "After");
    })
    .detach();
    let result = sim.run();
    (result, sim.trace().to_vec())
}

#[test]
fn an_error_trace_fails_the_run_and_a_warn_trace_does_not() {
    let (result, trace) = trace_then_continue(Severity::Warn);
    assert_eq!(result, Ok(()));
    let events: Vec<_> = trace.iter().map(|r| (r.severity, r.name)).collect();
    assert_eq!(
        events,
        [(Severity::Warn, "Degraded"), (Severity::Info, "After")]
    );

    let (result, trace) = trace_then_continue(Severity::Error);
    let Err(SimError::ErrorTrace(record)) = result else {
        panic!("expected the error trace to fail the run, got {result:?}");
    };
    assert_eq!(record.time, Instant::from_nanos(1_000_000_000));
    assert_eq!(record.name, "Broken");
    assert_eq!(record.fields, [("why", r#""invariant""#.to_owned())]);
    assert_eq!(trace, [record], "the run stops at the error");
}

#[test]
fn an_error_trace_outside_any_task_fails_the_next_run() {
    let sim = Sim::new(0);
    trace!(Error, "SetupBroken");
    let Err(SimError::ErrorTrace(record)) = sim.run() else {
        panic!("expected a failure");
    };
    assert_eq!(record.process, None);
}

#[test]
// The cross-thread wake is the point of this test.
#[allow(clippy::disallowed_methods)]
fn a_wake_from_another_thread_trips_the_assertion() {
    let sim = Sim::new(0);
    let ctx = sim.control().add_process("p");
    let waker: Arc<Mutex<Option<Waker>>> = Arc::default();
    let w = waker.clone();
    ctx.spawn("woken-elsewhere", Priority::Default, async move {
        poll_fn(|cx| {
            *w.lock().unwrap() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })
        .await
    })
    .detach();
    assert!(matches!(sim.run(), Err(SimError::Hang(_))));

    let waker = waker.lock().unwrap().take().unwrap();
    let panic = std::thread::spawn(move || waker.wake())
        .join()
        .expect_err("waking from another thread panics");
    let message = panic
        .downcast_ref::<&str>()
        .copied()
        .or(panic.downcast_ref::<String>().map(String::as_str))
        .unwrap();
    assert!(message.contains("woken from another thread"), "{message}");
}

#[test]
fn timers_fire_at_their_deadline_or_slightly_later() {
    let late = Rc::new(Cell::new(0));
    for seed in 0..20 {
        let sim = Sim::new(seed);
        let ctx = sim.control().add_process("p");
        for _ in 0..5 {
            let (c, l) = (ctx.clone(), late.clone());
            ctx.spawn("sleeper", Priority::Default, async move {
                for _ in 0..20 {
                    let sleep = c.sleep(c.rng().range(Duration::ZERO..Duration::from_secs(1)));
                    let deadline = sleep.deadline();
                    sleep.await;
                    let lateness = c.now() - deadline;
                    assert!(c.now() >= deadline);
                    assert!(lateness <= Duration::from_millis(1), "{lateness:?}");
                    if lateness > Duration::ZERO {
                        l.set(l.get() + 1);
                    }
                }
            })
            .detach();
        }
        sim.run().unwrap();
    }
    // Each sleeper's timer is late only when time jumped to it with an extra, about 1% of jumps.
    assert!((1..100).contains(&late.get()), "{} late timers", late.get());
}

#[test]
fn timeout_returns_elapsed_when_the_deadline_passes_first() {
    let sim = Sim::new(0);
    let ctx = sim.control().add_process("p");
    let c = ctx.clone();
    let task = ctx.spawn("t", Priority::Default, async move {
        let slow = c.timeout(Duration::from_secs(1), pending::<()>()).await;
        let fast = c
            .timeout(Duration::from_secs(1), c.sleep(Duration::from_millis(5)))
            .await;
        (slow, fast, c.now())
    });
    let (slow, fast, now) = sim.block_on(task).unwrap();
    assert!(slow.is_err() && fast.is_ok());
    assert!(now >= Instant::from_nanos(1_005_000_000));
}

#[test]
fn block_on_returns_while_other_tasks_still_run() {
    let sim = Sim::new(0);
    let ctx = sim.control().add_process("p");
    let c = ctx.clone();
    ctx.spawn("server", Priority::Default, async move {
        loop {
            c.sleep(Duration::from_secs(1)).await;
        }
    })
    .detach();
    let c = ctx.clone();
    let client = ctx.spawn("client", Priority::Default, async move {
        c.sleep(Duration::from_millis(2500)).await;
        42
    });
    assert_eq!(sim.block_on(client), Ok(42));
    assert!(sim.now() < Instant::from_nanos(3_000_000_000));
}

fn compute_in(ctx: &Ctx) -> impl Future<Output = (bool, bool, Instant)> + use<> {
    let c = ctx.clone();
    async move {
        let cost = Cost::from_duration(Duration::from_millis(3));
        let inside = c.compute(cost, basalt_env::is_simulated).await;
        (inside, basalt_env::is_simulated(), c.now())
    }
}

#[test]
fn compute_runs_with_no_world_and_charges_its_cost() {
    let sim = Sim::new(0);
    let ctx = sim.control().add_process("p");
    let task = ctx.spawn("compute", Priority::Default, compute_in(&ctx));
    let (inside, after, now) = sim.block_on(task).unwrap();
    assert!(!inside, "the job sees no world");
    assert!(after, "the world is back after it");
    assert!(now >= Instant::from_nanos(3_000_000));
}

#[test]
fn dropping_a_sim_drops_every_task_left() {
    struct DropFlag(Rc<Cell<bool>>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let (parked, sleeping) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(false)));
    // A handle that outlives the sim, as one held by another process would.
    let held;
    {
        let sim = Sim::new(0);
        let ctx = sim.control().add_process("p");
        let flag = DropFlag(parked.clone());
        held = ctx.spawn("parked", Priority::Default, async move {
            let _flag = flag;
            pending::<()>().await;
        });
        let (flag, c) = (DropFlag(sleeping.clone()), ctx.clone());
        ctx.spawn("sleeping", Priority::Default, async move {
            let _flag = flag;
            c.sleep(Duration::from_secs(100)).await;
        })
        .detach();
        sim.block_on(ctx.spawn("quick", Priority::Default, async {}))
            .unwrap();
    }
    assert!(parked.get() && sleeping.get());
    drop(held);
}
