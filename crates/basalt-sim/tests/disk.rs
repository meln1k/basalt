//! Simulated disks: durability through crashes and power cuts, disk faults, latency, and the
//! path rules every backend follows.

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::rc::Rc;

use basalt_env::{Ctx, Disk, Duration, OpenOptions, Priority};
use basalt_sim::{KillType, MachineId, Sim, SimControl};

/// One machine with one disk, running a process whose entry point does nothing. Tests run their
/// disk code as tasks of the current incarnation.
struct Node {
    sim: Sim,
    ctrl: SimControl,
    machine: MachineId,
    ctx: Rc<RefCell<Option<Ctx>>>,
}

impl Node {
    fn new(seed: u64) -> Node {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let machine = ctrl.add_machine("zone", 1);
        let ctx: Rc<RefCell<Option<Ctx>>> = Rc::default();
        let slot = ctx.clone();
        ctrl.add_process(machine, "node", move |c| {
            *slot.borrow_mut() = Some(c);
            Box::pin(async {})
        });
        Node {
            sim,
            ctrl,
            machine,
            ctx,
        }
    }

    fn ctx(&self) -> Ctx {
        self.ctx.borrow().clone().expect("the process started")
    }

    /// Runs `f` with the current incarnation's first disk until it completes.
    fn run<T: 'static, F: Future<Output = T> + 'static>(&self, f: impl FnOnce(Disk) -> F) -> T {
        let ctx = self.ctx();
        let disk = ctx.disk(ctx.disks()[0]);
        let task = ctx.spawn("test", Priority::Default, f(disk));
        self.sim.block_on(task).expect("the run succeeds")
    }

    fn restart(&self, kind: KillType) {
        self.ctrl.kill(self.machine, kind);
        self.ctrl.reboot(self.machine);
    }
}

fn create() -> OpenOptions {
    OpenOptions::new().read(true).write(true).create(true)
}

fn read() -> OpenOptions {
    OpenOptions::new().read(true)
}

async fn write(disk: &Disk, path: &str, data: &[u8], sync: bool) {
    let file = disk.open(path, create()).await.unwrap();
    let (result, _) = file.write_at(data.to_vec(), 0).await;
    result.unwrap();
    if sync {
        file.sync().await.unwrap();
    }
}

/// The whole file, or `None` if it doesn't exist.
async fn contents(disk: &Disk, path: &str) -> Option<Vec<u8>> {
    let file = match disk.open(path, read()).await {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
        Err(e) => panic!("opening {path}: {e}"),
    };
    let (n, mut buf) = file.read_at(vec![0; file.len() as usize], 0).await;
    buf.truncate(n.unwrap());
    Some(buf)
}

fn data(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31) ^ salt)
        .collect()
}

fn kind<T>(result: io::Result<T>) -> io::ErrorKind {
    result.map(drop).unwrap_err().kind()
}

#[test]
fn synced_data_survives_a_power_cut_and_unsynced_data_may_not() {
    let synced = data(10_000, 1);
    let mut unsynced_lost = 0;
    for seed in 0..50 {
        let node = Node::new(seed);
        let s = synced.clone();
        node.run(|disk| async move {
            write(&disk, "dir/synced", &s, true).await;
            // Its name is durable, but not what's written to it next.
            write(&disk, "dir/unsynced", &[], true).await;
            disk.sync_dir("dir").await.unwrap();
            write(&disk, "dir/unsynced", &data(10_000, 2), false).await;
        });
        node.restart(KillType::PowerCut);
        let (s, unsynced) = node.run(|disk| async move {
            let s = contents(&disk, "dir/synced").await;
            (s, contents(&disk, "dir/unsynced").await)
        });
        assert_eq!(s.as_ref(), Some(&synced), "seed {seed}");
        if unsynced.expect("its name was synced") != data(10_000, 2) {
            unsynced_lost += 1;
        }
    }
    assert!(
        unsynced_lost > 0,
        "a power cut never lost unsynced data in 50 seeds"
    );
}

#[test]
fn unsynced_data_and_names_survive_a_crash() {
    for seed in 0..20 {
        let node = Node::new(seed);
        node.run(|disk| async move {
            write(&disk, "a", &data(9_000, 3), false).await;
            write(&disk, "d/b", &data(5_000, 4), false).await;
            disk.rename("d/b", "e/c").await.unwrap();
        });
        node.restart(KillType::Crash);
        node.run(|disk| async move {
            assert_eq!(contents(&disk, "a").await, Some(data(9_000, 3)));
            assert_eq!(disk.list("d").await.unwrap(), Vec::<String>::new());
            assert_eq!(contents(&disk, "e/c").await, Some(data(5_000, 4)));
        });
    }
}

#[test]
fn an_unsynced_rename_can_disappear_after_a_power_cut() {
    let mut outcomes = Vec::new();
    for seed in 0..40 {
        for sync_after in [false, true] {
            let node = Node::new(seed);
            node.run(move |disk| async move {
                write(&disk, "d/old", b"value", true).await;
                disk.sync_dir("d").await.unwrap();
                disk.rename("d/old", "d/new").await.unwrap();
                if sync_after {
                    disk.sync_dir("d").await.unwrap();
                }
            });
            node.restart(KillType::PowerCut);
            let names = node.run(|disk| async move {
                let names = disk.list("d").await.unwrap();
                assert_eq!(
                    contents(&disk, &format!("d/{}", names[0])).await.unwrap(),
                    b"value"
                );
                names
            });
            if sync_after {
                assert_eq!(names, ["new"], "seed {seed}");
            } else {
                assert!(
                    names == ["new"] || names == ["old"],
                    "seed {seed}: {names:?}"
                );
                outcomes.push(names[0].clone());
            }
        }
    }
    assert!(
        outcomes.iter().any(|n| n == "old"),
        "the rename always survived"
    );
    assert!(
        outcomes.iter().any(|n| n == "new"),
        "the rename never survived"
    );
}

#[test]
fn sync_dir_makes_the_path_durable_but_not_subdirectories() {
    let (mut nested_lost, mut both_names) = (0, 0);
    for seed in 0..40 {
        let node = Node::new(seed);
        node.run(|disk| async move {
            write(&disk, "x/y/z", b"deep", true).await;
            // Makes `x` and `x/y` durable too.
            disk.sync_dir("x/y").await.unwrap();
            // Syncing `d` doesn't make `d/e` or what's in it durable.
            write(&disk, "d/e/f", b"nested", true).await;
            disk.sync_dir("d").await.unwrap();
            // A rename between two directories, with both synced.
            write(&disk, "a/f", b"moved", true).await;
            disk.sync_dir("a").await.unwrap();
            disk.rename("a/f", "b/f").await.unwrap();
            disk.sync_dir("a").await.unwrap();
            disk.sync_dir("b").await.unwrap();
            // The same with only the target synced: the file may be left under both names.
            write(&disk, "p/f", b"both?", true).await;
            disk.sync_dir("p").await.unwrap();
            disk.rename("p/f", "q/f").await.unwrap();
            disk.sync_dir("q").await.unwrap();
        });
        node.restart(KillType::PowerCut);
        let (nested, both) = node.run(|disk| async move {
            assert_eq!(contents(&disk, "x/y/z").await.unwrap(), b"deep");
            assert_eq!(disk.list("a").await.unwrap(), Vec::<String>::new());
            assert_eq!(contents(&disk, "b/f").await.unwrap(), b"moved");
            assert_eq!(contents(&disk, "q/f").await.unwrap(), b"both?");
            // If it is, the two names are separate files.
            write(&disk, "q/f", b"q only", true).await;
            let p = contents(&disk, "p/f").await;
            if let Some(p) = &p {
                assert_eq!(p, b"both?");
            }
            (contents(&disk, "d/e/f").await, p.is_some())
        });
        nested_lost += usize::from(nested.is_none());
        both_names += usize::from(both);
    }
    assert!(nested_lost > 0, "a file never made durable always survived");
    assert!(both_names > 0, "a half-synced rename never left both names");
}

/// What a power cut did to one page, judged from its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Page {
    Kept,
    Dropped,
    /// Some sectors new, some old.
    Torn,
    Corrupted,
}

const PAGE: usize = 4096;
const SECTOR: usize = 512;
const OLD: u8 = 0xaa;
const NEW: u8 = 0x55;

fn classify(page: &[u8]) -> Page {
    let sectors: Vec<Option<u8>> = page
        .chunks(SECTOR)
        .map(|s| s.iter().all(|&b| b == s[0]).then_some(s[0]))
        .collect();
    if sectors.iter().all(|&s| s == Some(NEW)) {
        Page::Kept
    } else if sectors.iter().all(|&s| s == Some(OLD)) {
        Page::Dropped
    } else if sectors.iter().all(|&s| s == Some(OLD) || s == Some(NEW)) {
        Page::Torn
    } else {
        Page::Corrupted
    }
}

#[test]
fn torn_and_corrupted_pages_appear_only_in_full_corruption_mode() {
    const FILES: usize = 8;
    const PAGES: usize = 8;
    // How many pages of each outcome, in drop-only and in full-corruption files.
    let mut seen = std::collections::BTreeMap::new();
    for seed in 0..30 {
        let node = Node::new(seed);
        node.run(|disk| async move {
            for f in 0..FILES {
                let file = disk.open(&format!("f{f}"), create()).await.unwrap();
                let (result, _) = file.write_at(vec![OLD; PAGES * PAGE], 0).await;
                result.unwrap();
                file.sync().await.unwrap();
                let (result, _) = file.write_at(vec![NEW; PAGES * PAGE], 0).await;
                result.unwrap();
            }
            disk.sync_dir("").await.unwrap();
        });
        node.restart(KillType::PowerCut);
        // Each file's corruption mode, from the trace. Each file here is written through one
        // handle, so the worst mode of its pages is the mode that handle was opened with.
        let modes: Vec<(String, String)> = node
            .sim
            .trace()
            .iter()
            .filter(|r| r.name == "DiskPowerCutFile")
            .map(|r| {
                let field = |k: &str| r.fields.iter().find(|(n, _)| *n == k).unwrap().1.clone();
                (
                    field("file").trim_matches('"').to_owned(),
                    field("worst_mode"),
                )
            })
            .collect();
        assert_eq!(modes.len(), FILES, "seed {seed}");
        let files = node.run(|disk| async move {
            let mut files = Vec::new();
            for f in 0..FILES {
                files.push(contents(&disk, &format!("f{f}")).await.unwrap());
            }
            files
        });
        for (f, bytes) in files.iter().enumerate() {
            assert_eq!(bytes.len(), PAGES * PAGE);
            let mode = &modes
                .iter()
                .find(|(name, _)| *name == format!("f{f}"))
                .unwrap()
                .1;
            for page in bytes.chunks(PAGE) {
                *seen.entry((mode.clone(), classify(page))).or_insert(0) += 1;
            }
        }
    }
    let count = |mode: &str, page| seen.get(&(mode.to_owned(), page)).copied().unwrap_or(0);
    for page in [Page::Kept, Page::Dropped] {
        assert!(count("DropOnly", page) > 0, "{seen:?}");
        assert!(count("Full", page) > 0, "{seen:?}");
    }
    assert_eq!(count("DropOnly", Page::Torn), 0, "{seen:?}");
    assert_eq!(count("DropOnly", Page::Corrupted), 0, "{seen:?}");
    assert!(count("Full", Page::Torn) > 0, "{seen:?}");
    assert!(count("Full", Page::Corrupted) > 0, "{seen:?}");
}

#[test]
fn a_full_disk_fails_with_enospc_until_restored() {
    let node = Node::new(1);
    let id = node.ctx().disks()[0];
    node.run(|disk| async move {
        write(&disk, "a", &[1; 100], false).await;
    });
    node.ctrl.fill_disk(id);
    node.run(|disk| async move {
        assert_eq!(disk.space().available, 0);
        let file = disk.open("a", create()).await.unwrap();
        // Overwriting needs no new space.
        let (result, _) = file.write_at(vec![2; 100], 0).await;
        result.unwrap();
        let (result, _) = file.write_at(vec![2; 1], 100).await;
        assert_eq!(kind(result), io::ErrorKind::StorageFull);
        assert_eq!(kind(file.set_len(200).await), io::ErrorKind::StorageFull);
        assert_eq!(
            kind(disk.open("b", create()).await),
            io::ErrorKind::StorageFull
        );
        assert_eq!(file.len(), 100);
    });
    node.ctrl.restore_disks();
    node.run(|disk| async move {
        assert!(disk.space().available > 0);
        let file = disk.open("a", create()).await.unwrap();
        let (result, _) = file.write_at(vec![2; 1], 100).await;
        result.unwrap();
        write(&disk, "b", b"fits", false).await;
        assert_eq!(disk.space().available, disk.space().total - 105);
    });
}

#[test]
fn io_errors_and_bit_flips_until_restored() {
    let node = Node::new(2);
    let id = node.ctx().disks()[0];
    let original = data(1000, 5);
    let o = original.clone();
    node.run(|disk| async move { write(&disk, "a", &o, true).await });

    node.ctrl.set_disk_errors(id, 1.0);
    node.run(|disk| async move {
        let file = disk.open("a", create()).await.unwrap();
        let (result, _) = file.read_at(vec![0; 10], 0).await;
        assert_eq!(result.unwrap_err().raw_os_error(), Some(5));
        let (result, _) = file.write_at(vec![0; 10], 0).await;
        assert_eq!(result.unwrap_err().raw_os_error(), Some(5));
    });
    node.ctrl.restore_disks();
    let o = original.clone();
    node.run(|disk| async move { assert_eq!(contents(&disk, "a").await.unwrap(), o) });

    node.ctrl.set_bit_flips(id, 1.0);
    let o = original.clone();
    node.run(|disk| async move {
        let read = contents(&disk, "a").await.unwrap();
        let flipped: u32 = read.iter().zip(&o).map(|(a, b)| (a ^ b).count_ones()).sum();
        assert_eq!(flipped, 1);
    });
    node.ctrl.restore_disks();
    node.run(|disk| async move { assert_eq!(contents(&disk, "a").await.unwrap(), original) });
}

#[test]
fn disk_operations_take_virtual_time_and_queue() {
    let node = Node::new(3);
    let ctx = node.ctx();
    node.run(|disk| async move {
        let file = disk.open("a", create()).await.unwrap();
        let start = ctx.now();
        let (result, _) = file.write_at(vec![0; 1 << 20], 0).await;
        result.unwrap();
        let big = ctx.now() - start;
        // 1 MiB at 1 GiB/s is about a millisecond, give or take half.
        assert!(big >= Duration::from_micros(400), "{big:?}");

        // Ten small writes at once finish one after another, not together.
        let file = Rc::new(file);
        let start = ctx.now();
        let tasks: Vec<_> = (0..10)
            .map(|i| {
                let file = file.clone();
                ctx.spawn("writer", Priority::Default, async move {
                    let (result, _) = file.write_at(vec![1; 10], i * 10).await;
                    result.unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await;
        }
        let all = ctx.now() - start;
        assert!(all >= Duration::from_micros(10 * 25), "{all:?}");
    });
}

#[test]
fn wipe_gives_new_empty_disks_and_disk_fail_leaves_none() {
    let node = Node::new(4);
    let before = node.ctrl.disks(node.machine);
    node.run(|disk| async move {
        write(&disk, "a", b"data", true).await;
        disk.sync_dir("").await.unwrap();
    });
    node.restart(KillType::Wipe);
    let after = node.ctrl.disks(node.machine);
    assert_eq!(after.len(), 1);
    assert_ne!(before, after);
    assert_eq!(node.ctx().disks(), after);
    node.run(|disk| async move {
        assert_eq!(disk.list("").await.unwrap(), Vec::<String>::new());
        write(&disk, "b", b"data", true).await;
    });
    node.restart(KillType::DiskFail);
    assert!(node.ctx().disks().is_empty());
    assert!(node.ctrl.disks(node.machine).is_empty());
    // A wipe replaces the failed disks.
    node.restart(KillType::Wipe);
    assert_eq!(node.ctx().disks().len(), 1);
}

#[test]
fn a_dead_processs_disk_and_files_fail() {
    let node = Node::new(5);
    let old = node.ctx();
    let disk = old.disk(old.disks()[0]);
    let file = node.run(|disk| async move {
        let file = disk.open("a", create()).await.unwrap();
        let (result, _) = file.write_at(b"before".to_vec(), 0).await;
        result.unwrap();
        file
    });
    node.restart(KillType::Crash);
    // The old incarnation's handles, used from the new one.
    node.run(move |_| async move {
        assert!(disk.open("a", read()).await.is_err());
        assert!(disk.list("").await.is_err());
        let (result, _) = file.read_at(vec![0; 6], 0).await;
        assert!(result.is_err());
        let (result, _) = file.write_at(b"after".to_vec(), 0).await;
        assert!(result.is_err());
        assert!(file.sync().await.is_err());
    });
    node.run(|disk| async move { assert_eq!(contents(&disk, "a").await.unwrap(), b"before") });
}

#[test]
fn the_same_seed_gives_the_same_disk_outcomes() {
    let run = |seed| {
        let node = Node::new(seed);
        node.run(|disk| async move {
            for f in 0..4 {
                write(&disk, &format!("d/{f}"), &data(20_000, f), f % 2 == 0).await;
            }
            disk.sync_dir("d").await.unwrap();
            disk.rename("d/0", "e/0").await.unwrap();
        });
        node.restart(KillType::PowerCut);
        let files = node.run(|disk| async move {
            let mut files = Vec::new();
            for dir in ["d", "e"] {
                for name in disk.list(dir).await.unwrap() {
                    let path = format!("{dir}/{name}");
                    files.push((path.clone(), contents(&disk, &path).await));
                }
            }
            files
        });
        (files, node.sim.unseed())
    };
    assert_eq!(run(7), run(7));
    assert_ne!(run(7).1, run(8).1);
}

/// The rules from `basalt_env::Disk`'s docs, as `basalt-rt`'s tests check them on a real disk.
#[test]
fn paths_and_directories_follow_the_disk_rules() {
    let node = Node::new(6);
    node.run(|disk| async move {
        let opts = OpenOptions::new().read(true).write(true).create_new(true);
        let file = disk.open("log/tmp", opts).await.unwrap();
        let (result, _) = file.write_at(b"tail".to_vec(), 20_000).await;
        result.unwrap();
        assert_eq!(file.len(), 20_004);
        // A short read only at the end of the file, and the gap reads as zeros.
        let (n, buf) = file.read_at(vec![9; 8], 19_998).await;
        assert_eq!(n.unwrap(), 6);
        assert_eq!(buf, [0, 0, b't', b'a', b'i', b'l', 9, 9]);
        let (n, _) = file.read_at(vec![9; 8], 30_000).await;
        assert_eq!(n.unwrap(), 0);
        assert_eq!(
            kind(disk.open("log/tmp", opts).await),
            io::ErrorKind::AlreadyExists
        );
        disk.rename("log/tmp", "log/current").await.unwrap();
        assert_eq!(disk.list("log").await.unwrap(), ["current"]);
        assert_eq!(disk.list("").await.unwrap(), Vec::<String>::new());
        assert_eq!(disk.list("missing").await.unwrap(), Vec::<String>::new());
        disk.sync_dir("missing").await.unwrap();
        disk.remove("log/current").await.unwrap();
        assert_eq!(
            kind(disk.remove("log/current").await),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            kind(disk.open("nope", read()).await),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            kind(disk.open("no/pe", read()).await),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            kind(disk.rename("nope", "x").await),
            io::ErrorKind::NotFound
        );
        for bad in ["/etc/passwd", "../x", "a//b", "a/./b", "", "a/"] {
            assert_eq!(
                kind(disk.open(bad, read()).await),
                io::ErrorKind::InvalidInput,
                "{bad:?}"
            );
        }
        // Options std rejects before opening anything.
        for bad in [
            OpenOptions::new(),
            OpenOptions::new().read(true).create(true),
            OpenOptions::new().read(true).truncate(true),
        ] {
            assert_eq!(kind(disk.open("x", bad).await), io::ErrorKind::InvalidInput);
        }

        let create = OpenOptions::new().write(true).create(true);
        for name in ["d/c", "d/a", "d/sub/x", "d/b"] {
            disk.open(name, create).await.unwrap();
        }
        disk.rename("d/b", "e/f/b").await.unwrap();
        assert_eq!(disk.list("d").await.unwrap(), ["a", "c"]);
        assert_eq!(disk.list("e/f").await.unwrap(), ["b"]);
        assert_eq!(disk.list("e").await.unwrap(), Vec::<String>::new());
        disk.sync_dir("e/f").await.unwrap();
        disk.sync_dir("d").await.unwrap();

        assert_eq!(
            kind(disk.open("d", read()).await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.open("e", create).await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.rename("d/a", "e/f").await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.rename("e/f", "x").await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.remove("d/sub").await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.open("d/a/x", create).await),
            io::ErrorKind::NotADirectory
        );
        assert_eq!(
            kind(disk.open("d/a/x", read()).await),
            io::ErrorKind::NotADirectory
        );
        assert_eq!(kind(disk.list("d/a").await), io::ErrorKind::NotADirectory);
        assert_eq!(
            kind(disk.sync_dir("d/a").await),
            io::ErrorKind::NotADirectory
        );
        assert_eq!(
            kind(disk.rename("d/c", "d/a/x").await),
            io::ErrorKind::NotADirectory
        );

        // Truncating on open, and a handle used for what it wasn't opened for.
        write(&disk, "t", b"12345", false).await;
        let t = disk
            .open("t", OpenOptions::new().write(true).truncate(true))
            .await
            .unwrap();
        assert_eq!(t.len(), 0);
        let (result, _) = t.read_at(vec![0; 1], 0).await;
        assert!(result.is_err());
        let r = disk.open("t", read()).await.unwrap();
        let (result, _) = r.write_at(vec![0; 1], 0).await;
        assert!(result.is_err());
        // An open file outlives its name, as on Linux.
        let kept = disk.open("t", create).await.unwrap();
        disk.remove("t").await.unwrap();
        let (result, _) = kept.write_at(b"still".to_vec(), 0).await;
        result.unwrap();
        assert_eq!(kept.len(), 5);
    });
}

#[test]
fn an_unsynced_remove_can_be_undone_by_a_power_cut() {
    let mut came_back = 0;
    for seed in 0..40 {
        for sync_after in [false, true] {
            let node = Node::new(seed);
            node.run(move |disk| async move {
                write(&disk, "d/f", b"value", true).await;
                disk.sync_dir("d").await.unwrap();
                disk.remove("d/f").await.unwrap();
                if sync_after {
                    disk.sync_dir("d").await.unwrap();
                }
            });
            node.restart(KillType::PowerCut);
            let back = node.run(|disk| async move { contents(&disk, "d/f").await });
            match back {
                None => {}
                Some(back) => {
                    assert!(!sync_after, "seed {seed}: a synced remove was undone");
                    assert_eq!(back, b"value");
                    came_back += 1;
                }
            }
        }
    }
    assert!(came_back > 0, "an unsynced remove always survived");
}

#[test]
fn an_unsynced_length_change_may_or_may_not_survive_a_power_cut() {
    let (mut kept, mut lost) = (0, 0);
    for seed in 0..40 {
        let node = Node::new(seed);
        node.run(|disk| async move {
            write(&disk, "f", &[7; 100], true).await;
            disk.sync_dir("").await.unwrap();
            let file = disk.open("f", create()).await.unwrap();
            file.set_len(50_000).await.unwrap();
        });
        node.restart(KillType::PowerCut);
        let after = node.run(|disk| async move { contents(&disk, "f").await.unwrap() });
        assert_eq!(after[..100], [7; 100], "seed {seed}");
        assert!(after[100..].iter().all(|&b| b == 0), "seed {seed}");
        match after.len() {
            100 => lost += 1,
            50_000 => kept += 1,
            len => panic!("seed {seed}: length {len}"),
        }
    }
    assert!(kept > 0 && lost > 0, "kept {kept}, lost {lost}");
}

#[test]
fn shrinking_then_growing_a_file_reads_zeros_and_syncs_them() {
    for seed in 0..20 {
        let node = Node::new(seed);
        node.run(|disk| async move {
            write(&disk, "f", &[0xaa; 10_000], true).await;
            disk.sync_dir("").await.unwrap();
            let file = disk.open("f", create()).await.unwrap();
            file.set_len(5_000).await.unwrap();
            file.set_len(10_000).await.unwrap();
            let read = contents(&disk, "f").await.unwrap();
            assert_eq!(read[..5_000], [0xaa; 5_000]);
            assert!(read[5_000..].iter().all(|&b| b == 0));
            file.sync().await.unwrap();
            // An unsynced write past what's checked, so the power cut rebuilds the file from
            // its durable image.
            let (result, _) = file.write_at(vec![1; 10], 20_000).await;
            result.unwrap();
        });
        node.restart(KillType::PowerCut);
        let after = node.run(|disk| async move { contents(&disk, "f").await.unwrap() });
        assert!(after.len() >= 10_000, "seed {seed}");
        assert_eq!(after[..5_000], [0xaa; 5_000], "seed {seed}");
        assert!(after[5_000..10_000].iter().all(|&b| b == 0), "seed {seed}");
    }
}

#[test]
fn a_sparse_file_costs_only_what_is_written() {
    let node = Node::new(8);
    node.run(|disk| async move {
        let file = disk.open("sparse", create()).await.unwrap();
        // 10 GiB long, with one page written: dense images would allocate it all.
        file.set_len(10 << 30).await.unwrap();
        let (result, _) = file.write_at(b"end".to_vec(), (10 << 30) - 3).await;
        result.unwrap();
        file.sync().await.unwrap();
        let (n, buf) = file.read_at(vec![9; 6], (10 << 30) - 6).await;
        assert_eq!(n.unwrap(), 6);
        assert_eq!(buf, [0, 0, 0, b'e', b'n', b'd']);
        assert_eq!(disk.space().total - disk.space().available, 10 << 30);
    });
}

/// A handle another process holds on a killed process's disk: an operation already waiting for
/// its turn when the kill comes fails, and changes nothing.
#[test]
fn an_operation_in_flight_when_its_process_dies_fails() {
    for seed in 0..10 {
        let node = Node::new(seed);
        let (file, disk) = node.run(|disk| async move {
            write(&disk, "a", b"before", true).await;
            disk.sync_dir("").await.unwrap();
            (disk.open("a", create()).await.unwrap(), disk)
        });
        let b = node.ctrl.add_machine("other", 0);
        let other = node.ctrl.add_process(b, "other", |_| Box::pin(async {}));
        let (ctrl, machine, c) = (node.ctrl.clone(), node.machine, other.clone());
        let killer = other.spawn("killer", Priority::Default, async move {
            // Every disk operation takes longer than this, so the kill lands mid-operation.
            c.sleep(Duration::from_micros(1)).await;
            ctrl.kill(machine, KillType::Crash);
        });
        let io = other.spawn("io", Priority::Default, async move {
            let (write, _) = file.write_at(b"after!".to_vec(), 0).await;
            let rename = disk.rename("a", "b").await;
            (write, rename)
        });
        node.sim.block_on(killer).unwrap();
        let (write, rename) = node.sim.block_on(io).unwrap();
        assert!(write.is_err(), "seed {seed}");
        assert!(rename.is_err(), "seed {seed}");
        node.ctrl.reboot(node.machine);
        node.run(|disk| async move {
            assert_eq!(disk.list("").await.unwrap(), ["a"]);
            assert_eq!(contents(&disk, "a").await.unwrap(), b"before");
        });
    }
}

#[test]
fn lift_faults_restores_every_disk() {
    let node = Node::new(9);
    let id = node.ctx().disks()[0];
    let original = data(1000, 6);
    let o = original.clone();
    node.run(|disk| async move { write(&disk, "a", &o, true).await });
    node.ctrl.fill_disk(id);
    node.ctrl.set_disk_errors(id, 1.0);
    node.ctrl.set_bit_flips(id, 1.0);
    node.ctrl.lift_faults();
    node.run(|disk| async move {
        assert_eq!(disk.space().available, disk.space().total - 1000);
        // Reads are neither failing nor flipped, and writes that grow files fit again.
        assert_eq!(contents(&disk, "a").await.unwrap(), original);
        write(&disk, "b", &[1; 10_000], true).await;
    });
}
