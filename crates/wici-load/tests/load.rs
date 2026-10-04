//! Load runs against a real server. Run through `scripts/with-postgres.sh`.

use std::time::Duration;

use wici_load::{LoadError, Plan, run};
use wici_testkit::TestServer;

fn small(url: &str) -> Plan {
    Plan {
        users: 3,
        active: 2,
        interval: Duration::from_millis(50),
        duration: Duration::from_millis(400),
        drain: Duration::from_secs(2),
        ..Plan::new(url)
    }
}

#[tokio::test]
async fn clean_run_delivers_everything() {
    let server = TestServer::start().await;
    let report = run(&small(&server.url)).await.unwrap();
    let tally = &report.tally;
    assert!(report.passed(), "{report}");
    assert!(tally.delivered >= 6, "{report}");
    assert_eq!(
        tally.accept.count(),
        usize::try_from(tally.accepted).unwrap()
    );
    assert!(tally.round_trip.count() > 0, "{report}");
    assert_eq!((report.users, report.active), (3, 2));
}

#[tokio::test]
async fn server_errors_fail_the_run() {
    let server = TestServer::with(|c| c.limits.max_sealed_bytes = 1024).await;
    let report = run(&small(&server.url)).await.unwrap();
    assert!(report.tally.errors > 0, "{report}");
    assert!(!report.passed());
}

#[tokio::test]
async fn lost_connections_fail_the_run() {
    let mut server = TestServer::start().await;
    let plan = Plan {
        duration: Duration::from_secs(2),
        ..small(&server.url)
    };
    let running = tokio::spawn(async move { run(&plan).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    server.stop().await;
    let report = running.await.unwrap().unwrap();
    assert_eq!(report.tally.disconnects, 6, "{report}");
    assert!(!report.passed());
}

#[tokio::test]
async fn setup_failures_are_returned() {
    let server = TestServer::with(|c| c.limits.store.max_pairs_per_device = 0).await;
    let refused = run(&small(&server.url)).await.unwrap_err();
    assert!(matches!(refused, LoadError::Rejected { .. }), "{refused}");

    let slow = Plan {
        setup_timeout: Duration::ZERO,
        ..small(&server.url)
    };
    assert!(matches!(run(&slow).await, Err(LoadError::Timeout)));

    let closed = small("ws://127.0.0.1:1/v1/ws");
    assert!(matches!(run(&closed).await, Err(LoadError::Connect(_))));
}

#[tokio::test]
async fn idle_users_stay_connected_past_the_idle_timeout() {
    let server = TestServer::with(|c| {
        c.timeouts.idle = Duration::from_millis(300);
        c.timeouts.ping = Duration::from_millis(50);
    })
    .await;
    let plan = Plan {
        active: 0,
        duration: Duration::from_secs(1),
        drain: Duration::ZERO,
        ..small(&server.url)
    };
    let report = run(&plan).await.unwrap();
    assert!(report.passed(), "{report}");
}

/// Runs the `wici-load` binary with `args`.
#[cfg(test)]
async fn binary(args: &[&str]) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_wici-load"))
        .args(args)
        .output()
        .await
        .unwrap()
}

#[tokio::test]
async fn binary_reports_and_sets_the_exit_code() {
    let help = binary(&["--help"]).await;
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).starts_with("usage: wici-load"));

    let bad = binary(&["--users", "x"]).await;
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).starts_with("invalid value for --users"));

    let closed = binary(&["--url", "ws://127.0.0.1:1/v1/ws", "--users", "1"]).await;
    assert!(!closed.status.success());
    assert!(String::from_utf8_lossy(&closed.stderr).starts_with("load run failed: "));

    let server = TestServer::start().await;
    let run = binary(&[
        "--url",
        &server.url,
        "--users",
        "2",
        "--active",
        "1",
        "--interval-ms",
        "50",
        "--seconds",
        "1",
        "--drain-seconds",
        "1",
    ])
    .await;
    let report = String::from_utf8_lossy(&run.stdout);
    assert!(run.status.success(), "{report}");
    assert!(report.trim_end().ends_with("PASS"), "{report}");
}
