use std::panic::PanicHookInfo;
use std::thread;

/// Panic hook that uses tracing to display panics. This can provide extra context when an async
/// task panics.
pub fn panic_hook(panic_info: &PanicHookInfo) {
    let location = panic_info
        .location()
        .map(|l| l.to_string())
        .unwrap_or_else(|| "?".to_string());
    let message = panic_info.payload_as_str().unwrap_or("?");

    let thread = thread::current();
    let thread_name = thread.name().unwrap_or("?");
    let thread_id = thread.id();

    tracing::error!(
        panic.location = %location,
        panic.message = %message,
        panic.thread.name = %thread_name,
        panic.thread.id = ?thread_id,
        "thread paniced"
    );
}
