# Cancellation safety

Any `.await` in Basalt's code may be the last line of its function that ever runs. A future is cancelled by being dropped, and that happens often:

- Someone drops the `Task` handle, or the future itself, such as the losing side of a race.
- `ctx.timeout` expires and drops the future it wraps.
- The process is killed. Every one of its futures is dropped before any other task runs, including detached tasks and tasks whose handle another process holds.
- In simulation, a `buggify!` site in the run loop now and then drops a random detached task (see `cancel_random_task` in `basalt-sim/src/world.rs`).

When a future is dropped, the code after its pending `.await` never runs, but the `Drop` of every value it holds does. Write each async function so that stopping at any `.await` leaves the process in a state it can carry on from, and leaves the disk in a state a reboot can recover from.

## Rules

**Keep shared state consistent at every `.await`.** Finish each change to `Rc<RefCell<…>>` state before the next `.await`, or arrange the steps so that any prefix of them is a valid state. Record progress after the work it describes, never before. The register's replica marks a key durable only after `sync_dir` returns (`put` in `toy-register/src/replica.rs`). Marking it first would let a cancelled put leave a key that claims to be durable when it isn't.

**Release resources in `Drop`, not in code after an `.await`.** A lock, a slot, or a counter that code after an `.await` gives back is leaked when the future is dropped there. Hand it out as a guard whose `Drop` gives it back. The replica's per-key locks work this way (`KeyLocks` and `KeyGuard`). `Drop` code may also run during a kill, when the process's `Ctx` is dead: it may still call any `Ctx` method, but tasks it spawns never run and I/O fails (`basalt-env/src/backend.rs`, "Dead processes").

**Don't leave a half-used stream where the next caller finds it.** A write cancelled partway through leaves half a frame in the stream, and a read cancelled partway through leaves the rest of one. Take the stream out of shared state while using it and put it back only when the exchange has finished, so a cancelled exchange drops the connection instead. The register client does this with `conn: Rc<RefCell<Option<Stream>>>` (`toy-register/src/client.rs`).

**Treat a cancelled operation as maybe done.** A request may have reached the other side before its caller gave up. A cancelled write may or may not take effect, so callers must not retry it as if it never happened unless it is idempotent, and tests record it as indeterminate (`Outcome` in `basalt-harness/src/history.rs`). Disk operations go further: each is submitted when called, so a write, rename, or sync whose future is dropped still lands, and operations issued together may land in any order. Await each disk step before starting the next one that depends on it (`Disk` in `basalt-env/src/disk.rs`).

**Make on-disk changes atomic at every step.** Write a new version to a temporary file, `sync` it, `rename` it over the old one, then `sync_dir`. Stopping after any step leaves either the old version or the new one in place, and recovery deletes leftover temporary files. This is the replica's `put` and `recover`.

**Hold a `Task` when you need its result; detach only work you can lose.** A detached task can be dropped at any `.await`, by a kill or by the random-cancel site, and nothing tells the code that detached it. If something must happen, keep the handle and await it, or make the work idempotent and redo it on recovery.

## How simulation checks this

Kills and the random-cancel site both drop futures at `.await` points the code chose, at times the seed chose. A bug of this kind shows up as a failed check or an `Error` trace right after a `MachineKilled` or `TaskCancelled` event in the trace of the failing seed. Rerun it with `basalt sim run --test <test> --seed <seed>` to see which task was dropped (`TaskCancelled` names it) and what it traced before.
