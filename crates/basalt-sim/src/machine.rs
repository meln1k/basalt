//! Machines, the processes on them, and which incarnations are alive.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;

use basalt_env::backend::LocalBoxFuture;
use basalt_env::{Ctx, DiskId, ProcessId};

use crate::time::{Clock, MachineClock};

/// Identifies a machine within a simulated world, from
/// [`SimControl::add_machine`](crate::SimControl::add_machine). Unlike a [`ProcessId`], it
/// stays the same across kills and reboots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MachineId(u32);

impl MachineId {
    pub const fn get(self) -> u32 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn for_tests(raw: u32) -> MachineId {
        MachineId(raw)
    }

    fn index(self) -> usize {
        self.0 as usize
    }
}

/// How [`SimControl::kill`](crate::SimControl::kill) takes a machine down. Every kind kills every
/// process on the machine at once; they differ in what happens to its disks, which come with the
/// simulated disks. Until then `PowerCut` and `DiskFail` act like `Crash`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KillType {
    /// A process crash or OOM kill. Unsynced writes survive, because the OS page cache outlives
    /// the process.
    Crash,
    /// The whole machine loses power. Each pending page is kept, dropped, torn, or corrupted.
    PowerCut,
    /// The machine's disks fail and are gone.
    DiskFail,
    /// The machine is replaced: it comes back with new, empty disks under new [`DiskId`]s.
    Wipe,
}

/// What a process runs each time it starts: once when it's added, then again on each reboot,
/// with the new incarnation's `Ctx`.
pub(crate) type EntryPoint = Rc<dyn Fn(Ctx) -> LocalBoxFuture<'static, ()>>;

/// Every machine and every incarnation of every process. A process is alive while its machine
/// is up and still on the boot the process started in, so a kill or reboot changes one field
/// of the machine and nothing else.
///
/// No method hands out a borrow or calls user code, so any of them may run from `Drop` code.
pub(crate) struct Machines {
    clock: Rc<Clock>,
    /// Indexed by `MachineId`.
    machines: RefCell<Vec<Machine>>,
    /// Dead incarnations stay, since a dead process's `Ctx` still reads its machine's clocks and
    /// disks.
    processes: RefCell<BTreeMap<ProcessId, Process>>,
    next_process: Cell<u32>,
    next_disk: Cell<u32>,
}

struct Machine {
    zone: Rc<str>,
    disks: Rc<[DiskId]>,
    /// False from a kill until the next reboot.
    up: bool,
    /// How many times the machine has rebooted.
    boot: u32,
    clock: Rc<MachineClock>,
    /// What runs on each boot.
    processes: Vec<(Rc<str>, EntryPoint)>,
    /// Whether faults on it are refused (`SimControl::protect`).
    protected: bool,
}

/// One incarnation of a process.
struct Process {
    label: Rc<str>,
    machine: MachineId,
    /// The boot of its machine it started in.
    boot: u32,
}

impl Machines {
    pub(crate) fn new(clock: Rc<Clock>) -> Machines {
        Machines {
            clock,
            machines: RefCell::default(),
            processes: RefCell::default(),
            next_process: Cell::new(0),
            next_disk: Cell::new(0),
        }
    }

    pub(crate) fn add_machine(&self, zone: &str, disks: usize) -> MachineId {
        let machine = Machine {
            zone: zone.into(),
            disks: self.new_disks(disks),
            up: true,
            boot: 0,
            clock: Rc::new(MachineClock::new(self.clock.clone())),
            processes: Vec::new(),
            protected: false,
        };
        let mut machines = self.machines.borrow_mut();
        let id = MachineId(u32::try_from(machines.len()).expect("too many machines"));
        machines.push(machine);
        id
    }

    /// Adds a process to a running machine and returns its first incarnation.
    pub(crate) fn add_process(
        &self,
        machine: MachineId,
        label: &str,
        entry: EntryPoint,
    ) -> ProcessId {
        let label: Rc<str> = label.into();
        let boot = {
            let mut machines = self.machines.borrow_mut();
            let m = &mut machines[machine.index()];
            assert!(
                m.up,
                "{machine:?} is down; reboot it before adding processes"
            );
            m.processes.push((label.clone(), entry));
            m.boot
        };
        self.incarnate(label, machine, boot)
    }

    /// Marks `machine` down, which kills every process on it, and applies what `kind` does to
    /// its disks.
    pub(crate) fn kill(&self, machine: MachineId, kind: KillType) {
        let wiped = (kind == KillType::Wipe).then(|| self.new_disks(self.disks(machine).len()));
        let mut machines = self.machines.borrow_mut();
        let m = &mut machines[machine.index()];
        m.up = false;
        if let Some(disks) = wiped {
            m.disks = disks;
        }
    }

    /// Brings a machine that's down up on a new boot, and returns a new incarnation of each of
    /// its processes, to start.
    pub(crate) fn reboot(&self, machine: MachineId) -> Vec<(ProcessId, EntryPoint)> {
        let (boot, processes) = {
            let mut machines = self.machines.borrow_mut();
            let m = &mut machines[machine.index()];
            assert!(!m.up, "{machine:?} is up; kill it before rebooting");
            m.up = true;
            m.boot += 1;
            (m.boot, m.processes.clone())
        };
        processes
            .into_iter()
            .map(|(label, entry)| (self.incarnate(label, machine, boot), entry))
            .collect()
    }

    pub(crate) fn is_up(&self, machine: MachineId) -> bool {
        self.machines.borrow()[machine.index()].up
    }

    pub(crate) fn protect(&self, machine: MachineId) {
        self.machines.borrow_mut()[machine.index()].protected = true;
    }

    pub(crate) fn is_protected(&self, machine: MachineId) -> bool {
        self.machines.borrow()[machine.index()].protected
    }

    /// Every machine, protected or not.
    pub(crate) fn ids(&self) -> Vec<MachineId> {
        let n = u32::try_from(self.machines.borrow().len()).expect("too many machines");
        (0..n).map(MachineId).collect()
    }

    pub(crate) fn zone(&self, machine: MachineId) -> Rc<str> {
        self.machines.borrow()[machine.index()].zone.clone()
    }

    pub(crate) fn clock(&self, machine: MachineId) -> Rc<MachineClock> {
        self.machines.borrow()[machine.index()].clock.clone()
    }

    fn disks(&self, machine: MachineId) -> Rc<[DiskId]> {
        self.machines.borrow()[machine.index()].disks.clone()
    }

    /// The machine with this index, if there is one.
    pub(crate) fn machine(&self, index: u32) -> Option<MachineId> {
        (index < self.machines.borrow().len() as u32).then_some(MachineId(index))
    }

    pub(crate) fn machine_of(&self, process: ProcessId) -> MachineId {
        self.process(process, |p| p.machine)
    }

    pub(crate) fn alive(&self, process: ProcessId) -> bool {
        let (machine, boot) = self.process(process, |p| (p.machine, p.boot));
        let machines = self.machines.borrow();
        let m = &machines[machine.index()];
        m.up && m.boot == boot
    }

    /// The error every I/O operation of a dead process returns, by the dead-process rules in
    /// `basalt_env::backend`.
    pub(crate) fn check_alive(&self, process: ProcessId) -> io::Result<()> {
        if self.alive(process) {
            Ok(())
        } else {
            Err(io::Error::other(format!("{process:?} is dead")))
        }
    }

    pub(crate) fn label(&self, process: ProcessId) -> Rc<str> {
        self.process(process, |p| p.label.clone())
    }

    pub(crate) fn clock_of(&self, process: ProcessId) -> Rc<MachineClock> {
        self.clock(self.machine_of(process))
    }

    /// The disks of the process's machine right now.
    pub(crate) fn disks_of(&self, process: ProcessId) -> Rc<[DiskId]> {
        self.disks(self.machine_of(process))
    }

    fn process<R>(&self, process: ProcessId, f: impl FnOnce(&Process) -> R) -> R {
        f(self
            .processes
            .borrow()
            .get(&process)
            .expect("every process id comes from this world"))
    }

    /// A fresh id for a new incarnation; ids are never reused.
    fn incarnate(&self, label: Rc<str>, machine: MachineId, boot: u32) -> ProcessId {
        let id = ProcessId::new(self.next_process.get());
        self.next_process.set(id.get() + 1);
        let process = Process {
            label,
            machine,
            boot,
        };
        self.processes.borrow_mut().insert(id, process);
        id
    }

    fn new_disks(&self, n: usize) -> Rc<[DiskId]> {
        (0..n)
            .map(|_| {
                let id = DiskId::new(self.next_disk.get());
                self.next_disk.set(id.get() + 1);
                id
            })
            .collect()
    }
}
