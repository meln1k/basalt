//! `SimControl`'s protected machines and `lift_faults`.

use basalt_env::{Duration, Priority};
use basalt_sim::{KillType, Sim};

mod common;

#[test]
fn lift_faults_reboots_down_machines_and_stops_clock_drift() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("m", 0);
    ctrl.add_process(m, "p", |_| Box::pin(async {}));
    ctrl.set_clock_drift(m, 500_000);
    ctrl.kill(m, KillType::Crash);
    ctrl.lift_faults();
    assert!(ctrl.is_up(m));

    // From now on, the machine's clock runs at the rate of virtual time.
    let timer = common::process(&sim, "timer");
    let (wall, now) = (ctrl.wall_clock(m), ctrl.now());
    let ctx = timer.clone();
    let sleep = timer.spawn("sleep", Priority::Default, async move {
        ctx.sleep(Duration::from_secs(1)).await;
    });
    sim.block_on(sleep).unwrap();
    assert_eq!(
        ctrl.wall_clock(m).duration_since(wall).unwrap(),
        ctrl.now() - now
    );
}

#[test]
fn a_protected_machine_is_hidden_from_fault_workloads() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let [a, b, c] = ["a", "b", "c"].map(|zone| ctrl.add_machine(zone, 0));
    ctrl.protect(b);
    assert_eq!(ctrl.machines(), [a, c]);
}

#[test]
#[should_panic(expected = "which is protected from faults")]
fn killing_a_protected_machine_panics() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("m", 0);
    ctrl.protect(m);
    ctrl.kill(m, KillType::Crash);
}
