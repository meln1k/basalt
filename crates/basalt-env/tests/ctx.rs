//! `Ctx`'s side of the `Backend` contract, run against a minimal single-threaded backend that
//! follows the task, kill, and dead-process rules in `backend.rs`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::{pending, poll_fn};
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

use basalt_env::backend::{
    Backend, ComputeJob, ComputeOutput, DetachedTask, DiskIo, ListenerIo, LocalBoxFuture, Runnable,
    Scheduler, StreamIo,
};
use basalt_env::world::{self, BuggifySite, Event, ProbeSite, World};
use basalt_env::{
    Cost, Ctx, DiskId, Duration, Elapsed, Instant, Knobs, Priority, ProcessId, Rng, SocketAddr,
    SystemTime, TaskId, UNIX_EPOCH,
};

#[derive(Default)]
struct Clock {
    now: Cell<u64>,
    timers: RefCell<Vec<(u64, Waker)>>,
}

struct TestBackend {
    queue: Arc<Mutex<VecDeque<Runnable>>>,
    clock: Rc<Clock>,
    next_task: Cell<u64>,
    /// Live tasks: their process, and their waker once started.
    tasks: RefCell<BTreeMap<TaskId, (ProcessId, Option<Waker>)>>,
    detached: RefCell<BTreeMap<TaskId, DetachedTask>>,
    dead: RefCell<BTreeSet<ProcessId>>,
    rng: Rng,
}

impl TestBackend {
    fn new() -> Rc<TestBackend> {
        Rc::new(TestBackend {
            queue: Arc::default(),
            clock: Rc::default(),
            next_task: Cell::new(0),
            tasks: RefCell::default(),
            detached: RefCell::default(),
            dead: RefCell::default(),
            rng: Rng::new(0),
        })
    }

    fn ctx(self: &Rc<Self>, process: u32) -> Ctx {
        Ctx::new(self.clone(), ProcessId::new(process))
    }

    fn is_dead(&self, process: ProcessId) -> bool {
        self.dead.borrow().contains(&process)
    }

    /// Runs ready tasks, without firing timers, until none is ready. Runnables of dead
    /// processes are dropped, which drops their futures.
    fn run_ready(&self) {
        loop {
            let next = self.queue.lock().unwrap().pop_front();
            let Some(runnable) = next else { return };
            if self.is_dead(runnable.metadata().process) {
                drop(runnable);
            } else {
                runnable.run();
            }
        }
    }

    /// Runs until nothing is ready and no timer is pending.
    fn run(&self) {
        loop {
            self.run_ready();
            let mut timers = self.clock.timers.take();
            let Some(&(earliest, _)) = timers.iter().min_by_key(|(at, _)| *at) else {
                return;
            };
            self.clock.now.set(earliest);
            let due: Vec<_> = timers.extract_if(.., |(at, _)| *at <= earliest).collect();
            self.clock.timers.borrow_mut().extend(timers);
            for (_, waker) in due {
                waker.wake();
            }
        }
    }

    /// Kills `process` as `backend.rs` describes: wakes all its live tasks, then drops its
    /// queued runnables until none is left.
    fn kill(&self, process: u32) {
        let process = ProcessId::new(process);
        self.dead.borrow_mut().insert(process);
        let wakers: Vec<Waker> = self
            .tasks
            .borrow()
            .values()
            .filter(|(p, _)| *p == process)
            .filter_map(|(_, waker)| waker.clone())
            .collect();
        for waker in wakers {
            waker.wake();
        }
        loop {
            let doomed: Vec<Runnable> = {
                let mut queue = self.queue.lock().unwrap();
                let (doomed, keep): (Vec<_>, Vec<_>) = queue
                    .drain(..)
                    .partition(|r| r.metadata().process == process);
                queue.extend(keep);
                doomed
            };
            if doomed.is_empty() {
                return;
            }
            drop(doomed);
        }
    }

    fn live(&self) -> usize {
        self.tasks.borrow().len()
    }

    fn live_in(&self, process: u32) -> usize {
        let process = ProcessId::new(process);
        self.tasks
            .borrow()
            .values()
            .filter(|(p, _)| *p == process)
            .count()
    }
}

impl Backend for TestBackend {
    fn register_task(&self, process: ProcessId, _: &'static str, _: Priority) -> TaskId {
        let id = TaskId::new(self.next_task.get());
        self.next_task.set(id.get() + 1);
        self.tasks.borrow_mut().insert(id, (process, None));
        id
    }

    fn start_task(&self, first: Runnable) {
        if self.is_dead(first.metadata().process) {
            drop(first);
            return;
        }
        if let Some(task) = self.tasks.borrow_mut().get_mut(&first.metadata().id) {
            task.1 = Some(first.waker());
        }
        self.queue.lock().unwrap().push_back(first);
    }

    fn scheduler(&self) -> Scheduler {
        let queue = self.queue.clone();
        Arc::new(move |runnable| queue.lock().unwrap().push_back(runnable))
    }

    fn task_ended(&self, task: TaskId) {
        assert!(
            self.tasks.borrow_mut().remove(&task).is_some(),
            "{task:?} ended twice"
        );
        let handle = self.detached.borrow_mut().remove(&task);
        drop(handle);
    }

    fn detach(&self, task: DetachedTask) {
        if self.is_dead(task.process()) {
            drop(task);
            return;
        }
        self.detached.borrow_mut().insert(task.id(), task);
    }

    fn now(&self, _: ProcessId) -> Instant {
        Instant::from_nanos(self.clock.now.get())
    }

    fn wall_clock(&self, _: ProcessId) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(self.clock.now.get())
    }

    fn sleep_until(&self, process: ProcessId, deadline: Instant) -> LocalBoxFuture<'static, ()> {
        if self.is_dead(process) || deadline == Instant::MAX {
            return Box::pin(pending());
        }
        let clock = self.clock.clone();
        Box::pin(poll_fn(move |cx| {
            if clock.now.get() >= deadline.as_nanos() {
                return Poll::Ready(());
            }
            clock
                .timers
                .borrow_mut()
                .push((deadline.as_nanos(), cx.waker().clone()));
            Poll::Pending
        }))
    }

    fn rng(&self) -> &Rng {
        &self.rng
    }

    fn listen(&self, _: ProcessId, _: u16) -> LocalBoxFuture<'_, io::Result<Box<dyn ListenerIo>>> {
        unimplemented!()
    }

    fn connect(
        &self,
        _: ProcessId,
        _: SocketAddr,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn StreamIo>>> {
        unimplemented!()
    }

    fn disks(&self, process: ProcessId) -> Rc<[DiskId]> {
        Rc::new([DiskId::new(process.get() * 10)])
    }

    fn disk(&self, _: ProcessId, _: DiskId) -> Rc<dyn DiskIo> {
        unimplemented!()
    }

    fn compute(
        &self,
        process: ProcessId,
        cost: Cost,
        job: ComputeJob,
    ) -> LocalBoxFuture<'_, ComputeOutput> {
        if self.is_dead(process) {
            return Box::pin(pending());
        }
        let output = world::without(job);
        let sleep = self.sleep_until(process, self.now(process) + cost.duration());
        Box::pin(async move {
            sleep.await;
            output
        })
    }
}

/// Sets a flag when dropped, to observe when a task's future is dropped.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

/// Runs a closure when dropped.
struct OnDrop(Option<Box<dyn FnOnce()>>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

#[test]
fn spawned_tasks_run_and_return_their_output() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    let out = Rc::new(Cell::new(0));
    let o = out.clone();
    let c = ctx.clone();
    ctx.spawn("outer", Priority::Default, async move {
        let inner = c.spawn("inner", Priority::Default, async { 41 });
        o.set(inner.await + 1);
    })
    .detach();
    backend.run();
    assert_eq!(out.get(), 42);
    assert_eq!(backend.live(), 0, "every task told the backend it ended");
    assert!(backend.detached.borrow().is_empty());
}

#[test]
fn a_task_dropped_before_it_runs_is_dropped_unpolled_when_its_runnable_comes_up() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    let dropped = Rc::new(Cell::new(false));
    let polled = Rc::new(Cell::new(false));
    let (flag, p) = (DropFlag(dropped.clone()), polled.clone());
    let task = ctx.spawn("never", Priority::Default, async move {
        let _flag = flag;
        p.set(true);
    });
    assert_eq!(backend.live(), 1);
    drop(task);
    backend.run_ready();
    assert!(dropped.get());
    assert!(!polled.get());
    assert_eq!(
        backend.live(),
        0,
        "task_ended fires even if the task never ran"
    );
}

#[test]
fn task_ended_comes_after_the_future_is_dropped() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    // What the backend says while the task's future is being dropped.
    let live_at_drop = |seen: &Rc<Cell<Option<usize>>>| {
        let (b, s) = (backend.clone(), seen.clone());
        OnDrop(Some(Box::new(move || s.set(Some(b.live())))))
    };

    // Cancelled before its first poll.
    let seen = Rc::new(Cell::new(None));
    let probe = live_at_drop(&seen);
    let task = ctx.spawn("unpolled", Priority::Default, async move {
        let _probe = probe;
    });
    drop(task);
    backend.run_ready();
    assert_eq!(
        seen.get(),
        Some(1),
        "the task was still live while its future dropped"
    );
    assert_eq!(backend.live(), 0);

    // Cancelled while suspended.
    let seen = Rc::new(Cell::new(None));
    let probe = live_at_drop(&seen);
    let task = ctx.spawn("suspended", Priority::Default, async move {
        let _probe = probe;
        pending::<()>().await;
    });
    backend.run_ready();
    drop(task);
    backend.run_ready();
    assert_eq!(
        seen.get(),
        Some(1),
        "the task was still live while its future dropped"
    );
    assert_eq!(backend.live(), 0);
}

#[test]
fn a_detached_task_runs_on_and_leaves_its_process_set_when_done() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    let done = Rc::new(Cell::new(false));
    let (d, c) = (done.clone(), ctx.clone());
    ctx.spawn("sleeper", Priority::Background, async move {
        c.sleep(Duration::from_secs(1)).await;
        d.set(true);
    })
    .detach();
    assert_eq!(backend.detached.borrow().len(), 1);
    backend.run();
    assert!(done.get());
    assert_eq!(backend.clock.now.get(), 1_000_000_000);
    assert!(backend.detached.borrow().is_empty());
    assert_eq!(backend.live(), 0);
}

#[test]
fn detaching_a_finished_task_does_not_hand_it_to_the_backend() {
    let backend = TestBackend::new();
    let task = backend.ctx(1).spawn("quick", Priority::Default, async {});
    backend.run();
    assert!(task.is_finished());
    task.detach();
    assert!(backend.detached.borrow().is_empty());
}

#[test]
fn a_kill_drops_every_task_of_the_process_before_time_moves() {
    let backend = TestBackend::new();
    let victim = backend.ctx(1);
    let watcher = backend.ctx(2);

    // Detached, and asleep on a timer.
    let detached_dropped = Rc::new(Cell::new(false));
    let (flag, v) = (DropFlag(detached_dropped.clone()), victim.clone());
    victim
        .spawn("detached", Priority::Default, async move {
            let _flag = flag;
            v.sleep(Duration::from_secs(10)).await;
        })
        .detach();

    // Not detached, parked on something that never wakes it, with its handle held outside the
    // process, as workload code holding a server object would.
    let parked_dropped = Rc::new(Cell::new(false));
    let flag = DropFlag(parked_dropped.clone());
    let parked = victim.spawn("parked", Priority::Default, async move {
        let _flag = flag;
        pending::<()>().await;
    });

    // Awaited from another process.
    let v = victim.clone();
    let remote = victim.spawn("remote", Priority::Default, async move {
        v.sleep(Duration::from_secs(1)).await;
        7
    });
    let outcome = Rc::new(Cell::new(None));
    let (o, w) = (outcome.clone(), watcher.clone());
    watcher
        .spawn("watcher", Priority::Default, async move {
            o.set(Some(w.timeout(Duration::from_secs(5), remote).await));
        })
        .detach();

    backend.run_ready();
    backend.kill(1);
    assert!(detached_dropped.get());
    assert!(
        parked_dropped.get(),
        "a parked task with an outside handle is dropped too"
    );
    assert_eq!(backend.live_in(1), 0);
    assert!(
        backend
            .detached
            .borrow()
            .values()
            .all(|t| t.process() != ProcessId::new(1))
    );
    assert_eq!(backend.clock.now.get(), 0, "all before time moves");

    backend.run();
    assert_eq!(
        outcome.get(),
        Some(Err(Elapsed)),
        "awaiting a killed task never completes"
    );
    drop(parked);
    assert_eq!(backend.live(), 0);
}

#[test]
fn a_dead_ctx_cannot_start_tasks() {
    let backend = TestBackend::new();
    let victim = backend.ctx(1);
    backend.kill(1);

    let (dropped, polled) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(false)));
    let (flag, p) = (DropFlag(dropped.clone()), polled.clone());
    victim
        .spawn("late", Priority::Default, async move {
            let _flag = flag;
            p.set(true);
        })
        .detach();
    assert!(
        dropped.get(),
        "dropped at once, without waiting for a runnable to come up"
    );
    backend.run();
    assert!(!polled.get());
    assert_eq!(backend.live(), 0);
    assert!(backend.detached.borrow().is_empty());
}

#[test]
fn tasks_spawned_from_drop_during_a_kill_die_with_their_process() {
    let backend = TestBackend::new();
    let victim = backend.ctx(1);
    let watcher = backend.ctx(2);
    let orphan_polled = Rc::new(Cell::new(false));
    let notified = Rc::new(Cell::new(false));

    let (v, w, op, n) = (
        victim.clone(),
        watcher.clone(),
        orphan_polled.clone(),
        notified.clone(),
    );
    let on_drop = OnDrop(Some(Box::new(move || {
        v.spawn("orphan", Priority::Default, async move { op.set(true) })
            .detach();
        w.spawn("notify", Priority::Default, async move { n.set(true) })
            .detach();
    })));
    victim
        .spawn("guarded", Priority::Default, async move {
            let _on_drop = on_drop;
            pending::<()>().await;
        })
        .detach();

    backend.run_ready();
    backend.kill(1);
    backend.run();
    assert!(
        !orphan_polled.get(),
        "a task spawned by a dying process never runs"
    );
    assert!(notified.get(), "a task it spawned for a live process runs");
    assert_eq!(backend.live(), 0);
}

#[test]
fn timeout_returns_the_output_or_elapsed_and_drops_the_loser() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    let results = Rc::new(RefCell::new(Vec::new()));
    let slow_dropped = Rc::new(Cell::new(false));
    let (r, c, flag) = (results.clone(), ctx.clone(), DropFlag(slow_dropped.clone()));
    ctx.spawn("t", Priority::Default, async move {
        let fast = c.timeout(Duration::from_secs(2), async {
            c.sleep(Duration::from_secs(1)).await;
            "fast"
        });
        let fast = fast.await;
        r.borrow_mut().push(fast);
        let slow = c.timeout(Duration::from_secs(1), async {
            let _flag = flag;
            c.sleep(Duration::from_secs(5)).await;
            "slow"
        });
        let slow = slow.await;
        r.borrow_mut().push(slow);
        assert_eq!(c.now(), Instant::from_nanos(2_000_000_000));
    })
    .detach();
    backend.run();
    assert_eq!(*results.borrow(), [Ok("fast"), Err(Elapsed)]);
    assert!(slow_dropped.get());
}

#[test]
fn sleeps_past_their_deadline_finish_at_once_and_endless_ones_never_do() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    let done = Rc::new(Cell::new(false));
    let (d, c) = (done.clone(), ctx.clone());
    ctx.spawn("t", Priority::Default, async move {
        // A sleep awaited after its deadline passed.
        let early = c.sleep(Duration::from_secs(1));
        c.sleep(Duration::from_secs(2)).await;
        early.await;
        assert_eq!(c.now(), Instant::from_nanos(2_000_000_000));
        c.sleep(Duration::ZERO).await;
        assert_eq!(c.now(), Instant::from_nanos(2_000_000_000));

        // Durations too long to represent mean "never", not a panic.
        assert_eq!(c.sleep(Duration::MAX).deadline(), Instant::MAX);
        let endless = c.timeout(Duration::from_secs(1), c.sleep(Duration::MAX));
        assert_eq!(endless.await, Err(Elapsed));
        assert_eq!(c.timeout(Duration::MAX, async { 5 }).await, Ok(5));
        d.set(true);
    })
    .detach();
    backend.run();
    assert!(done.get());
    assert_eq!(backend.clock.now.get(), 3_000_000_000);
}

#[test]
fn yield_now_lets_other_ready_tasks_run_first() {
    let backend = TestBackend::new();
    let ctx = backend.ctx(1);
    let order = Rc::new(RefCell::new(Vec::new()));
    let (o, c) = (order.clone(), ctx.clone());
    ctx.spawn("yielder", Priority::Default, async move {
        o.borrow_mut().push("a1");
        c.yield_now().await;
        o.borrow_mut().push("a2");
    })
    .detach();
    let o = order.clone();
    ctx.spawn("other", Priority::Default, async move {
        o.borrow_mut().push("b")
    })
    .detach();
    backend.run();
    assert_eq!(*order.borrow(), ["a1", "b", "a2"]);
}

/// A simulated world whose `buggify!` always fires.
struct AlwaysBuggify;

impl World for AlwaysBuggify {
    fn is_simulated(&self) -> bool {
        true
    }
    fn buggify(&self, _: &'static BuggifySite, _: f64) -> bool {
        true
    }
    fn probe(&self, _: &'static ProbeSite) {}
    fn trace(&self, _: &Event<'_>) {}
    fn knobs(&self) -> Rc<Knobs> {
        Rc::new(Knobs::default())
    }
}

#[test]
fn compute_runs_without_a_world_and_charges_its_cost() {
    let _world = world::install(Rc::new(AlwaysBuggify));
    let backend = TestBackend::new();
    let ctx = backend.ctx(3);
    let out = Rc::new(Cell::new(None));
    let (o, c) = (out.clone(), ctx.clone());
    ctx.spawn("compute", Priority::Default, async move {
        let cost = Cost::from_duration(Duration::from_millis(3));
        let seen = c
            .compute(cost, || {
                let product: u64 = (1..=10u64).product();
                (basalt_env::is_simulated(), basalt_env::buggify!(), product)
            })
            .await;
        assert!(
            basalt_env::is_simulated(),
            "the world is back after the job"
        );
        o.set(Some(seen));
    })
    .detach();
    backend.run();
    assert_eq!(out.get(), Some((false, false, 3_628_800)));
    assert_eq!(backend.clock.now.get(), 3_000_000);
}

#[test]
fn disks_come_from_the_backend() {
    let backend = TestBackend::new();
    assert_eq!(*backend.ctx(3).disks(), [DiskId::new(30)]);
}
