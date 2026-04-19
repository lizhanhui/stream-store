// Re-export the proc macro so users only need to depend on this crate
pub use snafu_virtstack_macro::stack_trace_debug;

/// Core trait for virtual stack trace functionality.
///
/// This trait is automatically implemented by the [`stack_trace_debug`] proc macro attribute.
/// It provides access to the virtual stack trace showing the error propagation path.
pub trait VirtualStackTrace {
    /// Returns a virtual stack trace showing error propagation path.
    ///
    /// Each [`StackFrame`] in the returned vector represents one step in the error
    /// propagation chain, from the outermost error context down to the root cause.
    fn virtual_stack(&self) -> Vec<StackFrame>;
}

/// Represents a single frame in the virtual stack trace.
///
/// Each frame captures the location where an error was propagated and the
/// associated error message.
#[derive(Debug, Clone)]
pub struct StackFrame {
    /// Location where the error occurred or was propagated
    pub location: &'static std::panic::Location<'static>,
    /// Error message for this frame
    pub message: String,
}

impl StackFrame {
    /// Creates a new stack frame with the given location and message.
    pub fn new(location: &'static std::panic::Location<'static>, message: String) -> Self {
        Self { location, message }
    }
}

impl std::fmt::Display for StackFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at {}:{}:{}",
            self.message,
            self.location.file(),
            self.location.line(),
            self.location.column()
        )
    }
}
