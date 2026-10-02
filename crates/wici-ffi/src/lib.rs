//! C ABI for the Wici client. See `include/wici.h` for the C declarations.
//!
//! Requests and events are JSON strings. Each client owns a small Tokio
//! runtime; callbacks run on its threads, never on the caller's thread.
//! No Rust panic crosses this boundary: every entry point catches it.
#![expect(
    unsafe_code,
    reason = "C ABI boundary; each block has a safety comment"
)]

mod config;
mod rpc;

use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::runtime::Runtime;
use wici_client::Client;
use wici_crypto::DeviceKeys;

/// Receives a JSON string. The string is valid only during the call.
pub type WiciCallback = extern "C" fn(context: *mut c_void, json: *const c_char);

/// A callback and its caller-owned context.
struct Callback {
    function: WiciCallback,
    context: *mut c_void,
}

// SAFETY: the C contract (wici.h) requires `context` to stay valid and to be
// usable from any thread until the callback is no longer called.
unsafe impl Send for Callback {}
// SAFETY: as above; Rust never dereferences `context`.
unsafe impl Sync for Callback {}

impl Callback {
    fn call(&self, value: &Value) {
        // JSON text never contains NUL: serde escapes it.
        let text = CString::new(value.to_string()).unwrap_or_default();
        (self.function)(self.context, text.as_ptr());
    }
}

/// A running client. Opaque to C.
pub struct WiciClient {
    runtime: Runtime,
    client: Arc<Client>,
}

impl std::fmt::Debug for WiciClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WiciClient({})", self.client.device_id())
    }
}

/// Copies a C string. `None` for null or invalid UTF-8.
///
/// # Safety
///
/// `text` is null or a valid NUL-terminated string.
unsafe fn read_str(text: *const c_char) -> Option<String> {
    if text.is_null() {
        return None;
    }
    // SAFETY: non-null and NUL-terminated per this function's contract.
    let text = unsafe { CStr::from_ptr(text) };
    text.to_str().ok().map(str::to_owned)
}

/// Reads a 64-byte device secret. `None` for null.
///
/// # Safety
///
/// `secret` is null or points to 64 readable bytes.
const unsafe fn read_secret(secret: *const u8) -> Option<[u8; 64]> {
    if secret.is_null() {
        return None;
    }
    let mut out = [0; 64];
    // SAFETY: 64 readable bytes per this function's contract; `out` is a
    // separate local buffer.
    unsafe { ptr::copy_nonoverlapping(secret, out.as_mut_ptr(), 64) };
    Some(out)
}

/// Returns a heap string the caller frees with [`wici_string_free`].
fn into_c_string(text: &str) -> *mut c_char {
    CString::new(text).map_or(ptr::null_mut(), CString::into_raw)
}

/// Writes 64 random secret bytes to `out`. Store them in the Keychain.
/// Returns `false` if `out` is null.
///
/// # Safety
///
/// `out` is null or points to 64 writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn wici_device_secret_generate(out: *mut u8) -> bool {
    if out.is_null() {
        return false;
    }
    let secret = DeviceKeys::generate().to_secret();
    // SAFETY: `out` has 64 writable bytes per the contract.
    unsafe { ptr::copy_nonoverlapping(secret.as_ptr(), out, 64) };
    true
}

/// Device ID (base64url public key) for a secret, or null. Free the result
/// with [`wici_string_free`].
///
/// # Safety
///
/// `secret` is null or points to 64 readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn wici_device_id(secret: *const u8) -> *mut c_char {
    // SAFETY: forwarded contract.
    let Some(secret) = (unsafe { read_secret(secret) }) else {
        return ptr::null_mut();
    };
    let id = catch_unwind(|| DeviceKeys::from_secret(&secret).device_id().to_string());
    id.map_or(ptr::null_mut(), |id| into_c_string(&id))
}

fn open(config: &str, secret: &[u8; 64], on_event: Callback) -> Result<WiciClient, String> {
    let config = config::parse(config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("wici")
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let keys = DeviceKeys::from_secret(secret);
    let (client, mut events) = runtime
        .block_on(Client::open(config, keys))
        .map_err(|e| e.to_string())?;
    runtime.spawn(async move {
        while let Some(event) = events.recv().await {
            on_event.call(&serde_json::to_value(&event).unwrap_or(Value::Null));
        }
    });
    Ok(WiciClient {
        runtime,
        client: Arc::new(client),
    })
}

/// Opens a client and starts connecting. Blocks while the local database
/// opens, so call it off the main thread. Events go to `on_event` as JSON.
///
/// Returns null on failure and, if `error` is not null, stores a message
/// there that the caller frees with [`wici_string_free`].
///
/// # Safety
///
/// `config_json` is a NUL-terminated string, `secret` points to 64 bytes,
/// `error` is null or writable. `event_context` stays valid and thread-safe
/// until [`wici_client_close`] returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn wici_client_open(
    config_json: *const c_char,
    secret: *const u8,
    on_event: WiciCallback,
    event_context: *mut c_void,
    error: *mut *mut c_char,
) -> *mut WiciClient {
    // SAFETY: forwarded contracts.
    let (config, secret) = unsafe { (read_str(config_json), read_secret(secret)) };
    let callback = Callback {
        function: on_event,
        context: event_context,
    };
    let result = match (config, secret) {
        (Some(config), Some(secret)) => {
            catch_unwind(AssertUnwindSafe(|| open(&config, &secret, callback)))
                .unwrap_or_else(|_| Err("internal error".to_owned()))
        }
        _ => Err("missing config or secret".to_owned()),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(client)),
        Err(message) => {
            if !error.is_null() {
                // SAFETY: `error` is writable per the contract.
                unsafe { *error = into_c_string(&message) };
            }
            ptr::null_mut()
        }
    }
}

/// Runs a JSON request (see `rpc.rs`). `done` is called once, on a client
/// thread, with `{"ok": value}` or `{"error": {"kind", "message"}}`.
///
/// # Safety
///
/// `client` comes from [`wici_client_open`] and is not closed; `request` is
/// a NUL-terminated string; `context` stays valid and thread-safe until
/// `done` runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn wici_client_call(
    client: *const WiciClient,
    request: *const c_char,
    done: WiciCallback,
    context: *mut c_void,
) {
    let callback = Callback {
        function: done,
        context,
    };
    // SAFETY: forwarded contract.
    let request = unsafe { read_str(request) };
    // SAFETY: `client` is live per the contract.
    let Some(handle) = (unsafe { client.as_ref() }) else {
        callback.call(&rpc::Failure::new("invalid_request", "null client").to_json());
        return;
    };
    let Some(request) = request else {
        callback.call(&rpc::Failure::new("invalid_request", "null or non-UTF-8 request").to_json());
        return;
    };
    let client = Arc::clone(&handle.client);
    handle.runtime.spawn(async move {
        // The inner task turns a panic into a join error.
        let call = tokio::spawn(async move { rpc::call(&client, &request).await });
        let value = match call.await {
            Ok(Ok(value)) => json!({ "ok": value }),
            Ok(Err(failure)) => failure.to_json(),
            Err(_) => rpc::Failure::new("internal", "internal error").to_json(),
        };
        callback.call(&value);
    });
}

/// Stops and frees a client. Waits up to two seconds for work in progress.
/// Do not call from a Wici callback.
///
/// # Safety
///
/// `client` comes from [`wici_client_open`] and is not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn wici_client_close(client: *mut WiciClient) {
    if client.is_null() {
        return;
    }
    // SAFETY: ownership returns from `wici_client_open` per the contract.
    let handle = unsafe { Box::from_raw(client) };
    let _ = catch_unwind(AssertUnwindSafe(move || {
        let WiciClient { runtime, client } = *handle;
        drop(client);
        runtime.shutdown_timeout(Duration::from_secs(2));
    }));
}

/// Frees a string returned by this library. Null is ignored.
///
/// # Safety
///
/// `text` is null or came from this library and is not freed twice.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn wici_string_free(text: *mut c_char) {
    if !text.is_null() {
        // SAFETY: allocated by `CString::into_raw` in this library.
        drop(unsafe { CString::from_raw(text) });
    }
}
