//! `SimControl`'s `lift_faults` and the world clock it drives tests by.

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

/// `SimControl::sleep` runs on the world's clock: a drifting machine clock, or a kill, doesn't
/// move it, and a test driving the run with it outlives every machine.
#[test]
fn a_driver_sleeps_on_the_world_clock_and_outlives_every_machine() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let m = ctrl.add_machine("m", 0);
    let ctx = ctrl.add_process(m, "p", |_| Box::pin(async {}));
    ctrl.set_clock_drift(m, 500_000);
    let c = ctrl.clone();
    let start = ctrl.now();
    let elapsed = sim
        .block_on(async move {
            c.kill(m, KillType::Crash);
            c.sleep(Duration::from_secs(2)).await;
            c.now() - start
        })
        .unwrap();
    assert_eq!(elapsed, Duration::from_secs(2));
    // The dead process's clock ran 50% fast meanwhile; the driver's didn't.
    assert!(ctx.now() > start + Duration::from_secs(2));
}

#[test]
fn timeout_drops_the_future_when_virtual_time_runs_out() {
    let sim = Sim::new(0);
    let ctrl = sim.control();
    let c = ctrl.clone();
    let result = sim
        .block_on(async move {
            let forever = c.sleep(Duration::MAX);
            c.timeout(Duration::from_secs(1), forever).await
        })
        .unwrap();
    assert!(result.is_err());
    assert_eq!(
        ctrl.now(),
        basalt_env::Instant::ZERO + Duration::from_secs(1)
    );
}
