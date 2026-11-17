use basalt_env::Ctx;
use basalt_sim::Sim;

/// A process whose entry point does nothing, alone on its own machine with no disks.
pub fn process(sim: &Sim, label: &str) -> Ctx {
    let ctrl = sim.control();
    let machine = ctrl.add_machine(label, 0);
    ctrl.add_process(machine, label, |_| Box::pin(async {}))
}
