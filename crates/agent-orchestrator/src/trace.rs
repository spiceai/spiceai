use serde::Serialize;
use tracing::Span;

/// [`trace_and_return_tool_result`] provides a standard way to add the output or error message from [`Serialize`]-able result to tracing, so as to collect into `task_history` table.
pub(crate) fn trace_and_return_result<T>(
    result: Result<T, Box<dyn std::error::Error + Send + Sync>>,
    task: &str,
    span: &Span,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: Sized + Serialize,
{
    match result {
        Ok(value) => {
            match serde_json::to_string(&value) {
                Ok(value) => {
                    tracing::info!(target: "task_history", parent: span, captured_output = %value);
                }
                Err(e) => {
                    tracing::warn!("Failed to record output of '{task}' to 'runtime.task_history'. This is unexpected. Error: {e}")
                }
            }
            Ok(value)
        }
        Err(e) => {
            tracing::error!(target: "task_history", parent: span, "{e}");
            Err(e)
        }
    }
}
