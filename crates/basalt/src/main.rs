//! The one shipped binary: `basalt server` runs on `basalt-rt`, and `basalt sim` runs tests on
//! `basalt-sim`.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// Basalt: run a server, or run the deterministic simulation tests built into this binary.
#[derive(Parser)]
#[command(version = basalt_harness::VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a server on the real network and disks.
    Server,
    /// Run simulation tests against this exact build.
    #[command(subcommand)]
    Sim(SimCommand),
}

#[derive(Subcommand)]
enum SimCommand {
    /// List the tests built into this binary.
    List,
    /// Run one test with one seed and print its full trace.
    Run,
    /// Run a test over a range of seeds in parallel child processes.
    Sweep,
    /// Run each seed twice and check that both runs end with the same unseed.
    CheckDeterminism,
}

fn main() -> ExitCode {
    let name = match Cli::parse().command {
        Command::Server => "server",
        Command::Sim(SimCommand::List) => "sim list",
        Command::Sim(SimCommand::Run) => "sim run",
        Command::Sim(SimCommand::Sweep) => "sim sweep",
        Command::Sim(SimCommand::CheckDeterminism) => "sim check-determinism",
    };
    eprintln!("basalt {name}: not implemented yet");
    ExitCode::FAILURE
}
