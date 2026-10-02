//! Regression tests for callback ownership during shutdown.
use std::sync::mpsc::{Sender, channel};

use super::*;

extern "C" fn collect(context: *mut c_void, text: *const c_char) {
    // SAFETY: each test keeps its sender alive until close returns.
    let sender = unsafe { &*context.cast::<Sender<Value>>() };
    // SAFETY: the callback supplies a live C string.
    let text = unsafe { CStr::from_ptr(text) }.to_str().unwrap();
    sender.send(serde_json::from_str(text).unwrap()).unwrap();
}

fn callback(sender: &Sender<Value>) -> Callback {
    Callback {
        function: collect,
        context: ptr::from_ref(sender).cast_mut().cast(),
    }
}

#[test]
fn reply_finishes_once_or_reports_interruption_on_drop() {
    let (sender, receiver) = channel();
    Reply(Some(callback(&sender))).finish(&json!({"ok": null}));
    assert_eq!(receiver.try_recv().unwrap(), json!({"ok": null}));
    assert!(receiver.try_recv().is_err());
    drop(Reply(Some(callback(&sender))));
    assert_eq!(
        receiver.try_recv().unwrap()["error"]["kind"],
        "outcome_unknown"
    );
    assert!(receiver.try_recv().is_err());
}

#[test]
fn close_resolves_unpolled_calls_and_bounds_pending_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = wici_client::ClientConfig::new("ws://127.0.0.1:9", dir.path().join("client.db"));
    let (client, _events) = runtime
        .block_on(Client::open(config, DeviceKeys::generate()))
        .unwrap();
    let events = runtime.spawn(std::future::pending());
    let handle = Box::into_raw(Box::new(WiciClient {
        runtime,
        client: Arc::new(client),
        calls: Mutex::new(JoinSet::new()),
        events,
    }));
    let (sender, receiver) = channel();
    let request = CString::new(r#"{"method":"pairs"}"#).unwrap();
    for _ in 0..257 {
        let callback = callback(&sender);
        // SAFETY: live handle, string and sender; close happens below.
        unsafe {
            wici_client_call(
                handle,
                request.as_ptr(),
                callback.function,
                callback.context,
            );
        };
    }
    assert_eq!(receiver.try_recv().unwrap()["error"]["kind"], "busy");
    // SAFETY: no concurrent entry and the handle is never used again.
    unsafe { wici_client_close(handle) };
    for _ in 0..256 {
        assert_eq!(
            receiver.try_recv().unwrap()["error"]["kind"],
            "outcome_unknown"
        );
    }
    assert!(receiver.try_recv().is_err());
}

#[test]
fn close_waits_for_running_callbacks() {
    let (entered, observed) = channel();
    let (release, released) = channel();
    let (finished, completion) = channel();
    let (closing, close_started) = channel();
    let closer = std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let (sender, _receiver) = channel();
        let config =
            json!({"server_url":"ws://127.0.0.1:9", "database":dir.path().join("client.db")});
        let mut handle = open(&config.to_string(), &[1; 64], callback(&sender)).unwrap();
        handle.events.abort();
        let _ = handle.runtime.block_on(&mut handle.events);
        handle.events = handle.runtime.spawn(async move {
            entered.send(()).unwrap();
            // A slow host callback already running when close starts.
            released.recv().unwrap();
        });
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
        let handle = Box::into_raw(Box::new(handle));
        closing.send(()).unwrap();
        // SAFETY: exclusive ownership; callbacks return when released below.
        unsafe { wici_client_close(handle) };
        finished.send(()).unwrap();
    });
    close_started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(()).unwrap();
    completion.recv_timeout(Duration::from_secs(5)).unwrap();
    closer.join().unwrap();
}

#[test]
fn close_interrupts_a_started_call() {
    let dir = tempfile::tempdir().unwrap();
    let (sender, receiver) = channel();
    let config = json!({"server_url":"ws://127.0.0.1:9", "database":dir.path().join("started.db")});
    let (events, _event_receiver) = channel();
    let handle = open(&config.to_string(), &[2; 64], callback(&events)).unwrap();
    let reply = Reply(Some(callback(&sender)));
    let (started, observed) = channel();
    handle.calls.lock().unwrap().spawn_on(
        async move {
            let _reply = reply;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        },
        handle.runtime.handle(),
    );
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    // SAFETY: exclusive ownership, contexts stay live through close.
    unsafe { wici_client_close(Box::into_raw(Box::new(handle))) };
    assert_eq!(
        receiver.try_recv().unwrap()["error"]["kind"],
        "outcome_unknown"
    );
    assert!(receiver.try_recv().is_err());
}
