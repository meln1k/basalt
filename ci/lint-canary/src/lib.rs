//! Each line breaks determinism in a way clippy.toml must reject.

pub fn violations(ctx: &basalt_env::Ctx) {
    let _ = std::time::Instant::now();
    let _ = ctx.wall_clock().elapsed();
    let _: std::collections::HashMap<u8, u8> = Default::default();
    let _ = std::thread::spawn(|| {});
    let _ = std::fs::read("/etc/hostname");
    let _ = std::env::var("HOME");
    let _ = std::process::Command::new("true").status();
    let _ = std::os::unix::net::UnixStream::connect("/tmp/socket");
    thread_local! { static CANARY: u8 = const { 0 }; }
}
