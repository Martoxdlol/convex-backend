//! Automated smoke test exercising the `examples/worker` +
//! `examples/conductor` binaries together.
//!
//! Spawns them as subprocesses on an ephemeral port, waits for
//! the conductor to exit 0, and kills the worker. This is the
//! lowest-overhead way to ensure both binaries stay linked,
//! handle their env-var surface, and talk to each other
//! end-to-end over real gRPC. If any piece regresses — proto
//! drift, tonic-version mismatch, an unused import that CI
//! misses — this test catches it.
//!
//! Tests that run subprocesses are a bit heavier than normal
//! unit tests, but the alternative (either no smoke coverage,
//! or a shell script a human has to remember to run) is worse.

use std::{
    net::TcpListener,
    process::{
        Command,
        Stdio,
    },
    time::{
        Duration,
        Instant,
    },
};

/// Find an unused localhost port by briefly binding to 0 and
/// closing. There's a tiny TOCTOU window but it's the common
/// pattern and good enough for a smoke test that immediately
/// rebinds on the same port in a child process.
fn pick_ephemeral_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Poll a closure until it returns true or the timeout elapses.
/// Gives the spawned worker a moment to actually start listening.
fn wait_until<F: FnMut() -> bool>(deadline: Instant, mut f: F) -> bool {
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn worker_conductor_smoke() {
    // Binaries built by the integration-test target depend on
    // the example binaries, which cargo builds automatically
    // when the test references them via CARGO_BIN_EXE_* env vars.
    // Examples use a different env prefix (CARGO_BIN_EXE_ works
    // only for [[bin]] entries); use the target/debug/examples
    // path instead, which cargo guarantees for example binaries
    // of the current package.
    let exe_dir =
        std::path::PathBuf::from(std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
            // Default target dir relative to the workspace root.
            // CARGO_MANIFEST_DIR points at this crate, so go up
            // two levels to reach the workspace.
            let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            crate_dir
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("target")
                .to_string_lossy()
                .to_string()
        }));
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
    let examples_dir = exe_dir.join(&profile).join("examples");
    let worker_bin = examples_dir.join("worker");
    let conductor_bin = examples_dir.join("conductor");

    // If the binaries aren't there yet (fresh `cargo test` before
    // the examples have built), skip with a clear message. The
    // CI invocation should `cargo build --examples` first; the
    // local dev loop picks them up after any `cargo build`.
    if !worker_bin.exists() || !conductor_bin.exists() {
        eprintln!(
            "examples not built at {:?} — run `cargo build --examples -p \
             convex_native_distributed` first, skipping",
            examples_dir,
        );
        return;
    }

    let port = pick_ephemeral_port();
    let addr = format!("127.0.0.1:{port}");
    let endpoint = format!("http://{addr}");

    // Spawn worker.
    let mut worker = Command::new(&worker_bin)
        .env("CONVEX_MODE", "worker")
        .env("CONVEX_WORKER_BIND_ADDR", &addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker");

    // Wait until the worker is listening.
    let ready = wait_until(Instant::now() + Duration::from_secs(10), || {
        std::net::TcpStream::connect(&addr).is_ok()
    });
    assert!(ready, "worker never opened a listening socket on {addr}");

    // Run conductor and capture output + exit status.
    let conductor_output = Command::new(&conductor_bin)
        .env("CONVEX_MODE", "conductor")
        .env("CONVEX_WORKER_ENDPOINTS", &endpoint)
        .output()
        .expect("spawn conductor");

    let stdout = String::from_utf8_lossy(&conductor_output.stdout);
    let stderr = String::from_utf8_lossy(&conductor_output.stderr);

    // Clean up the worker regardless of how the assertions go.
    let _ = worker.kill();
    let _ = worker.wait();

    assert!(
        conductor_output.status.success(),
        "conductor exited {:?}\nstdout: {stdout}\nstderr: {stderr}",
        conductor_output.status,
    );
    assert!(
        stdout.contains(&endpoint),
        "conductor stdout missing worker endpoint {endpoint:?}\nstdout: {stdout}",
    );
    assert!(
        stdout.contains("traffic=true"),
        "conductor should report worker accepts_traffic=true\nstdout: {stdout}",
    );
    assert!(
        stderr.contains("1 healthy"),
        "conductor stderr should summarize 1 healthy worker\nstderr: {stderr}",
    );
}
