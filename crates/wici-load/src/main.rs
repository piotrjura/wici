//! `wici-load` binary. See `wici-load --help` and `scripts/load.sh`.

use std::io::Write;
use std::process::ExitCode;

use wici_load::{ArgsError, parse, run};

#[tokio::main]
async fn main() -> ExitCode {
    let plan = match parse(std::env::args().skip(1)) {
        Ok(plan) => plan,
        Err(ArgsError::Help) => return say(&mut std::io::stdout(), &ArgsError::Help, true),
        Err(error) => return say(&mut std::io::stderr(), &error, false),
    };
    match run(&plan).await {
        Ok(report) => say(&mut std::io::stdout(), &report, report.passed()),
        Err(error) => say(
            &mut std::io::stderr(),
            &format!("load run failed: {error}"),
            false,
        ),
    }
}

fn say(out: &mut impl Write, text: &dyn std::fmt::Display, ok: bool) -> ExitCode {
    let _ = writeln!(out, "{text}");
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
