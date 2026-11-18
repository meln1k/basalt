//! The register in simulation: reads and writes through 3 and 5 replicas with no faults, values
//! surviving a minority of replicas being killed and rebooted, and replica recovery under the
//! disk states a crash or a read fault can leave.

use std::io;

use basalt_env::{Ctx, Disk, Duration, OpenOptions, Priority, SocketAddr};
use basalt_sim::{KillType, MachineId, Sim, SimControl};
use toy_register::{Bugs, Client, DEFAULT_PORT, ReplicaConfig, run_replica};

/// Every operation in these tests finishes long before this with no faults.
const OP_DEADLINE: Duration = Duration::from_secs(30);

struct Cluster {
    sim: Sim,
    ctrl: SimControl,
    replicas: Vec<MachineId>,
    /// An idle process beside each replica, for shaping its disk. Dead after its machine's
    /// first kill.
    disk_procs: Vec<Ctx>,
    client: Ctx,
}

/// `n` replicas, each on its own machine with one disk, and a client process on another.
fn cluster(seed: u64, n: usize) -> Cluster {
    let sim = Sim::new(seed);
    let ctrl = sim.control();
    let (replicas, disk_procs) = (0..n)
        .map(|i| {
            let m = ctrl.add_machine(&format!("zone-{i}"), 1);
            ctrl.add_process(m, &format!("replica-{i}"), |ctx| {
                Box::pin(run_replica(ctx, ReplicaConfig::default()))
            });
            (
                m,
                ctrl.add_process(m, &format!("disk-{i}"), |_| Box::pin(async {})),
            )
        })
        .unzip();
    let m = ctrl.add_machine("client", 0);
    let client = ctrl.add_process(m, "client", |_| Box::pin(async {}));
    Cluster {
        sim,
        ctrl,
        replicas,
        disk_procs,
        client,
    }
}

impl Cluster {
    fn new_client(&self, writer: u64) -> Client {
        let addrs = self
            .replicas
            .iter()
            .map(|&m| SocketAddr::new(self.ctrl.ip(m), DEFAULT_PORT))
            .collect();
        Client::new(self.client.clone(), addrs, writer, Bugs::default())
    }

    /// Runs `f` as a task of the client process until it completes.
    fn run<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> T {
        let ctx = self.client.clone();
        let task = self.client.spawn("test", Priority::Default, async move {
            ctx.timeout(OP_DEADLINE, f)
                .await
                .expect("the operations finish with no faults")
        });
        self.sim.block_on(task).expect("the run succeeds")
    }

    /// Lets the replicas settle, such as finish recovering after a reboot.
    fn pause(&self) {
        let ctx = self.client.clone();
        self.run(async move { ctx.sleep(Duration::from_millis(200)).await });
    }

    /// Runs `f` with replica `i`'s disk until it completes. Only before `i`'s first kill.
    fn on_disk<T: 'static, F: Future<Output = T> + 'static>(
        &self,
        i: usize,
        f: impl FnOnce(Disk) -> F,
    ) -> T {
        let ctx = &self.disk_procs[i];
        let task = ctx.spawn("disk", Priority::Default, f(ctx.disk(ctx.disks()[0])));
        self.sim.block_on(task).expect("the run succeeds")
    }

    fn restart(&self, i: usize, kind: KillType) {
        self.ctrl.kill(self.replicas[i], kind);
        self.ctrl.reboot(self.replicas[i]);
    }
}

#[test]
fn writes_and_reads_through_3_and_5_replicas() {
    for n in [3, 5] {
        for seed in 0..5 {
            let cluster = cluster(seed, n);
            let (mut a, mut b) = (cluster.new_client(1), cluster.new_client(2));
            cluster.run(async move {
                assert_eq!(a.read(1).await, None, "a key never written");
                a.write(1, b"a1".to_vec()).await;
                assert_eq!(a.read(1).await.as_deref(), Some(&b"a1"[..]));
                assert_eq!(b.read(1).await.as_deref(), Some(&b"a1"[..]));
                // A second writer's write wins over the first's, and keys are independent.
                b.write(1, b"b1".to_vec()).await;
                a.write(2, b"a2".to_vec()).await;
                assert_eq!(a.read(1).await.as_deref(), Some(&b"b1"[..]));
                a.write(1, b"a3".to_vec()).await;
                assert_eq!(b.read(1).await.as_deref(), Some(&b"a3"[..]));
                assert_eq!(b.read(2).await.as_deref(), Some(&b"a2"[..]));
            });
        }
    }
}

#[test]
fn concurrent_writers_agree_on_one_value() {
    for n in [3, 5] {
        for seed in 0..5 {
            let cluster = cluster(seed, n);
            let ctx = cluster.client.clone();
            let mut clients: Vec<Client> = (0..4).map(|w| cluster.new_client(w)).collect();
            let mut reader = cluster.new_client(99);
            let finals = cluster.run(async move {
                let writes: Vec<_> = clients
                    .drain(..)
                    .enumerate()
                    .map(|(i, mut c)| {
                        ctx.spawn("writer", Priority::Default, async move {
                            c.write(7, format!("w{i}").into_bytes()).await;
                        })
                    })
                    .collect();
                for w in writes {
                    w.await;
                }
                [reader.read(7).await, reader.read(7).await]
            });
            let value = finals[0].clone().expect("some write won");
            assert!(value.starts_with(b"w"), "{value:?}");
            assert_eq!(finals[0], finals[1], "n={n} seed={seed}");
        }
    }
}

#[test]
fn a_value_survives_killing_and_rebooting_a_minority() {
    for (n, kind) in [
        (3, KillType::PowerCut),
        (5, KillType::PowerCut),
        (3, KillType::Crash),
    ] {
        for seed in 0..5 {
            let cluster = cluster(seed, n);
            let minority = n / 2;
            let mut writer = cluster.new_client(1);
            cluster.run(async move {
                writer.write(1, b"old".to_vec()).await;
                writer.write(1, b"kept".to_vec()).await;
            });

            // Kill and reboot each replica in turn, at most a minority down at once, so after
            // the last round no replica holds the value in memory, only on disk.
            for group in cluster.replicas.chunks(minority) {
                for &m in group {
                    cluster.ctrl.kill(m, kind);
                }
                cluster.run({
                    let ctx = cluster.client.clone();
                    async move { ctx.sleep(Duration::from_millis(100)).await }
                });
                for &m in group {
                    cluster.ctrl.reboot(m);
                }
            }
            // With a minority left down, the read's majority must come from rebooted replicas.
            for &m in &cluster.replicas[..minority] {
                cluster.ctrl.kill(m, kind);
            }
            let mut reader = cluster.new_client(2);
            let read = cluster.run(async move { reader.read(1).await });
            assert_eq!(
                read.as_deref(),
                Some(&b"kept"[..]),
                "n={n} {kind:?} seed={seed}"
            );
        }
    }
}

/// A replica that crashed between a put's rename and its `sync_dir` recovers a record whose name
/// a power cut can still lose. It must make the name durable before acknowledging that tag
/// again, or a read's write-back to it is lost and a later read goes back in time.
#[test]
fn a_record_recovered_from_the_page_cache_is_made_durable() {
    for seed in 0..20 {
        let cluster = cluster(seed, 3);
        let (a, b, c) = (0, 1, 2);
        let mut writer = cluster.new_client(1);
        cluster.run(async move { writer.write(1, b"x".to_vec()).await });

        // Leave `x` only on A, under a name that is in A's page cache but not durable: the
        // state a crash between a put's rename and its `sync_dir` leaves.
        // The write reached a majority, not necessarily every replica, so take its record from
        // any replica that has it, then remove it everywhere, durably.
        let mut record = None;
        for i in [a, b, c] {
            let found = cluster.on_disk(i, async |disk| {
                let file = match disk.open("register/1", OpenOptions::new().read(true)).await {
                    Ok(file) => file,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
                    Err(e) => panic!("opening register/1: {e}"),
                };
                let (n, record) = file.read_at(vec![0; file.len() as usize], 0).await;
                assert_eq!(n.unwrap(), record.len());
                disk.remove("register/1").await.unwrap();
                disk.sync_dir("register").await.unwrap();
                Some(record)
            });
            record = record.or(found);
        }
        let record = record.expect("a majority stored the write");
        cluster.on_disk(a, async |disk| {
            let opts = OpenOptions::new().write(true).create(true);
            let staged = disk.open("staging/1", opts).await.unwrap();
            staged.write_at(record, 0).await.0.unwrap();
            staged.sync().await.unwrap();
            disk.rename("staging/1", "register/1").await.unwrap();
        });
        for i in [a, b, c] {
            cluster.restart(i, KillType::Crash);
        }
        cluster.pause();

        // With C down, a read sees `x` on A and writes it back to A and B.
        cluster.ctrl.kill(cluster.replicas[c], KillType::Crash);
        let mut reader = cluster.new_client(2);
        let first = cluster.run(async move { reader.read(1).await });
        assert_eq!(first.as_deref(), Some(&b"x"[..]), "seed {seed}");
        cluster.ctrl.reboot(cluster.replicas[c]);
        cluster.pause();

        // One replica down at a time: a power cut on A, then B crashes, so A and C answer.
        cluster.restart(a, KillType::PowerCut);
        cluster.pause();
        cluster.ctrl.kill(cluster.replicas[b], KillType::Crash);
        let mut reader = cluster.new_client(3);
        let second = cluster.run(async move { reader.read(1).await });
        assert_eq!(second.as_deref(), Some(&b"x"[..]), "seed {seed}");
    }
}

/// Bit flips on read leave the data on disk intact, so a replica recovering through them must
/// retry until it reads its records, not forget them.
#[test]
fn bit_flips_during_recovery_dont_lose_a_record() {
    for seed in 0..5 {
        let cluster = cluster(seed, 3);
        let mut writer = cluster.new_client(1);
        cluster.run(async move { writer.write(1, b"kept".to_vec()).await });
        // A majority reboots one at a time, each reading every record through bit flips for a
        // while.
        for i in 0..2 {
            let disk = cluster.ctrl.disks(cluster.replicas[i])[0];
            cluster.ctrl.set_bit_flips(disk, 1.0);
            cluster.restart(i, KillType::Crash);
            cluster.pause();
            cluster.ctrl.restore_disks();
        }
        cluster.ctrl.kill(cluster.replicas[2], KillType::Crash);
        let mut reader = cluster.new_client(2);
        let read = cluster.run(async move { reader.read(1).await });
        assert_eq!(read.as_deref(), Some(&b"kept"[..]), "seed {seed}");
    }
}
