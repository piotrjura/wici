//! Calls the C ABI the way a native app does, against a real server.
#![expect(unsafe_code, reason = "exercises the C ABI")]

#[cfg(test)]
mod abi {
    use std::ffi::{CStr, CString, c_char, c_void};
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::time::Duration;

    use serde_json::{Value, json};
    use wici_ffi::{
        WiciClient, wici_client_call, wici_client_close, wici_client_open, wici_device_id,
        wici_device_secret_generate, wici_string_free,
    };

    const WAIT: Duration = Duration::from_secs(5);

    /// Forwards callback JSON into a channel. The context is a leaked
    /// `Mutex<Sender<Value>>`, valid for the whole test.
    extern "C" fn collect(context: *mut c_void, json: *const c_char) {
        // SAFETY: tests pass a `Mutex<Sender<Value>>` context and a valid string.
        let (sender, text) = unsafe {
            (
                &*context.cast::<Mutex<Sender<Value>>>(),
                CStr::from_ptr(json),
            )
        };
        let value = serde_json::from_str(text.to_str().unwrap()).unwrap();
        let _ = sender.lock().unwrap().send(value);
    }

    fn sink() -> (*mut c_void, Receiver<Value>) {
        let (sender, receiver) = channel();
        let context = Box::into_raw(Box::new(Mutex::new(sender))).cast::<c_void>();
        (context, receiver)
    }

    struct Device {
        handle: *mut WiciClient,
        events: Receiver<Value>,
        _dir: tempfile::TempDir,
    }

    fn open(url: &str) -> Device {
        let dir = tempfile::tempdir().unwrap();
        let config = json!({
            "server_url": url,
            "database": dir.path().join("c.db"),
            "reconnect_min_ms": 20,
            "retry_interval_ms": 100,
        });
        let config = CString::new(config.to_string()).unwrap();
        let mut secret = [0; 64];
        // SAFETY: 64 writable bytes.
        assert!(unsafe { wici_device_secret_generate(secret.as_mut_ptr()) });
        let (context, events) = sink();
        let mut error = ptr::null_mut();
        // SAFETY: valid string, 64-byte secret, leaked thread-safe context.
        let handle = unsafe {
            wici_client_open(
                config.as_ptr(),
                secret.as_ptr(),
                collect,
                context,
                &raw mut error,
            )
        };
        assert!(!handle.is_null() && error.is_null());
        Device {
            handle,
            events,
            _dir: dir,
        }
    }

    impl Device {
        fn call(&self, request: &Value) -> Value {
            let (context, results) = sink();
            let request = CString::new(request.to_string()).unwrap();
            // SAFETY: live handle, valid string, leaked context.
            unsafe { wici_client_call(self.handle, request.as_ptr(), collect, context) };
            results.recv_timeout(WAIT).unwrap()
        }

        fn ok(&self, request: &Value) -> Value {
            let result = self.call(request);
            result
                .get("ok")
                .cloned()
                .unwrap_or_else(|| panic!("{result}"))
        }

        fn event(&self, kind: &str) -> Value {
            self.event_where(|event| event["type"] == kind)
        }

        fn event_where(&self, wanted: impl Fn(&Value) -> bool) -> Value {
            std::iter::repeat_with(|| self.events.recv_timeout(WAIT).unwrap())
                .find(|event| wanted(event))
                .unwrap()
        }

        fn close(self) {
            // SAFETY: handle from `wici_client_open`, not used afterwards.
            unsafe { wici_client_close(self.handle) };
        }
    }

    #[test]
    fn device_secret_and_id() {
        let mut secret = [0; 64];
        // SAFETY: null and valid pointers as documented.
        unsafe {
            assert!(!wici_device_secret_generate(ptr::null_mut()));
            assert!(wici_device_secret_generate(secret.as_mut_ptr()));
            assert!(wici_device_id(ptr::null()).is_null());
            let id = wici_device_id(secret.as_ptr());
            assert_eq!(CStr::from_ptr(id).to_str().unwrap().len(), 43);
            wici_string_free(id);
            wici_string_free(ptr::null_mut());
            wici_client_close(ptr::null_mut());
        }
    }

    #[test]
    fn open_reports_bad_input() {
        let secret = [1; 64];
        let config = CString::new("{}").unwrap();
        let (context, _) = sink();
        let mut error = ptr::null_mut();
        // SAFETY: valid pointers; error is writable.
        unsafe {
            let handle = wici_client_open(
                config.as_ptr(),
                secret.as_ptr(),
                collect,
                context,
                &raw mut error,
            );
            assert!(handle.is_null());
            assert!(
                CStr::from_ptr(error)
                    .to_str()
                    .unwrap()
                    .starts_with("invalid config")
            );
            wici_string_free(error);
            let missing = wici_client_open(
                ptr::null(),
                secret.as_ptr(),
                collect,
                context,
                ptr::null_mut(),
            );
            assert!(missing.is_null());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pairing_messages_and_artifacts_through_the_abi() {
        let server = wici_testkit::TestServer::start().await;
        let url = server.url.clone();
        tokio::task::spawn_blocking(move || {
            let a = open(&url);
            let b = open(&url);
            let invite = a.ok(&json!({"method": "invite"}));
            let joined = b.ok(&json!({"method": "join", "link": invite["link"]}));
            assert_eq!(joined["pair"], invite["pair"]);
            let pair = invite["pair"].clone();
            a.event("claimed");
            a.ok(&json!({"method": "approve", "pair": pair}));
            b.event_where(|e| e["type"] == "pair" && e["pair"]["state"] == "active");

            let body = json!({"type": "event", "stream": "01a0fd24-0000-7000-8000-000000000001", "data": "hi"});
            let sent = b.call(&json!({"method": "send", "pair": pair, "lane": "data", "body": body}));
            assert!(sent["ok"]["id"].is_string(), "{sent}");
            assert_eq!(a.event("message")["message"]["body"]["data"], "hi");
            let pending = a.ok(&json!({"method": "pending"}));
            assert_eq!(pending.as_array().unwrap().len(), 1);

            let artifact = a.ok(&json!({"method": "upload", "pair": pair, "data": "AQID", "media_type": "x/y", "name": null}));
            let data = b.ok(&json!({"method": "download", "pair": pair, "artifact": artifact}));
            assert_eq!(data["data"], "AQID");

            let bad = a.call(&json!({"method": "nope"}));
            assert_eq!(bad["error"]["kind"], "invalid_request");
            let pair_error = a.call(&json!({"method": "approve", "pair": "01a0fd24-0000-7000-8000-000000000009"}));
            assert_eq!(pair_error["error"]["kind"], "unknown_pair");
            assert_eq!(a.ok(&json!({"method": "pairs"})).as_array().unwrap().len(), 1);
            a.ok(&json!({"method": "reconnect_now"}));
            a.close();
            b.close();
        })
        .await
        .unwrap();
    }

    #[test]
    fn call_with_null_arguments_reports_errors() {
        let (context, results) = sink();
        let request = CString::new("{}").unwrap();
        // SAFETY: null client is documented to report an error.
        unsafe { wici_client_call(ptr::null(), request.as_ptr(), collect, context) };
        assert_eq!(
            results.recv_timeout(WAIT).unwrap()["error"]["kind"],
            "invalid_request"
        );
    }
}
