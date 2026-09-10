//! Loop termination reasons — engine-agnostic.
//!
//! Both the in-house event loop and the autoloop engine path produce a
//! [`TerminationReason`]; the coordination layer (summary, history, merge queue,
//! exit codes) consumes it. Defined outside the engine module so it survives the
//! in-house engine's deletion.

/// Reason the event loop terminated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminationReason {
    /// Completion promise was detected in output.
    CompletionPromise,
    /// Maximum iterations reached.
    MaxIterations,
    /// Maximum runtime exceeded.
    MaxRuntime,
    /// Maximum cost exceeded.
    MaxCost,
    /// Too many consecutive failures.
    ConsecutiveFailures,
    /// Loop thrashing detected (repeated blocked events).
    LoopThrashing,
    /// Stale loop detected (same topic emitted 3+ times consecutively).
    LoopStale,
    /// Too many consecutive malformed JSONL lines in events file.
    ValidationFailure,
    /// Manually stopped.
    Stopped,
    /// Interrupted by signal (SIGINT/SIGTERM).
    Interrupted,
    /// Restart requested via Telegram `/restart` command.
    RestartRequested,
    /// Workspace directory (worktree) was removed externally.
    WorkspaceGone,
    /// Loop was cancelled gracefully via loop.cancel event (human rejection, timeout).
    Cancelled,
}

impl TerminationReason {
    /// Returns the exit code for this termination reason per spec.
    ///
    /// Per spec "Loop Termination" section:
    /// - 0: Completion promise detected (success)
    /// - 1: Consecutive failures or unrecoverable error (failure)
    /// - 2: Max iterations, max runtime, or max cost exceeded (limit)
    /// - 130: User interrupt (SIGINT = 128 + 2)
    pub fn exit_code(&self) -> i32 {
        match self {
            TerminationReason::CompletionPromise => 0,
            TerminationReason::ConsecutiveFailures
            | TerminationReason::LoopThrashing
            | TerminationReason::LoopStale
            | TerminationReason::ValidationFailure
            | TerminationReason::Stopped
            | TerminationReason::WorkspaceGone => 1,
            TerminationReason::MaxIterations
            | TerminationReason::MaxRuntime
            | TerminationReason::MaxCost => 2,
            TerminationReason::Interrupted => 130,
            // Restart uses exit code 3 to signal the caller to exec-replace
            TerminationReason::RestartRequested => 3,
            // Cancelled is a clean exit (0) — the loop stopped intentionally
            TerminationReason::Cancelled => 0,
        }
    }

    /// Returns the reason string for use in loop.terminate event payload.
    ///
    /// Per spec event payload format:
    /// `completed | max_iterations | max_runtime | consecutive_failures | interrupted | error`
    pub fn as_str(&self) -> &'static str {
        match self {
            TerminationReason::CompletionPromise => "completed",
            TerminationReason::MaxIterations => "max_iterations",
            TerminationReason::MaxRuntime => "max_runtime",
            TerminationReason::MaxCost => "max_cost",
            TerminationReason::ConsecutiveFailures => "consecutive_failures",
            TerminationReason::LoopThrashing => "loop_thrashing",
            TerminationReason::LoopStale => "loop_stale",
            TerminationReason::ValidationFailure => "validation_failure",
            TerminationReason::Stopped => "stopped",
            TerminationReason::Interrupted => "interrupted",
            TerminationReason::RestartRequested => "restart_requested",
            TerminationReason::WorkspaceGone => "workspace_gone",
            TerminationReason::Cancelled => "cancelled",
        }
    }

    /// Returns true if this is a successful completion (not an error or limit).
    pub fn is_success(&self) -> bool {
        matches!(self, TerminationReason::CompletionPromise)
    }

    /// Machine-readable label recorded in loop history on completion
    /// (`loop_history.record_completed`). Note the `CompletionPromise` arm is
    /// `"completion_promise"` (not `as_str`'s `"completed"`): the history keeps
    /// the raw variant name, while `as_str` is the loop.terminate event payload
    /// vocabulary.
    pub fn history_label(&self) -> &'static str {
        match self {
            TerminationReason::CompletionPromise => "completion_promise",
            TerminationReason::MaxIterations => "max_iterations",
            TerminationReason::MaxRuntime => "max_runtime",
            TerminationReason::MaxCost => "max_cost",
            TerminationReason::ConsecutiveFailures => "consecutive_failures",
            TerminationReason::LoopThrashing => "loop_thrashing",
            TerminationReason::LoopStale => "loop_stale",
            TerminationReason::ValidationFailure => "validation_failure",
            TerminationReason::Stopped => "stopped",
            TerminationReason::Interrupted => "interrupted",
            TerminationReason::RestartRequested => "restart_requested",
            TerminationReason::WorkspaceGone => "workspace_gone",
            TerminationReason::Cancelled => "cancelled",
        }
    }

    /// Human-readable description recorded when a non-completing merge loop
    /// needs review.
    pub fn review_description(&self) -> &'static str {
        match self {
            TerminationReason::CompletionPromise => "completed",
            TerminationReason::MaxIterations => "max iterations reached",
            TerminationReason::MaxRuntime => "max runtime exceeded",
            TerminationReason::MaxCost => "max cost exceeded",
            TerminationReason::ConsecutiveFailures => "consecutive failures",
            TerminationReason::LoopThrashing => "loop thrashing detected",
            TerminationReason::LoopStale => "stale loop detected",
            TerminationReason::ValidationFailure => "validation failure",
            TerminationReason::Stopped => "manually stopped",
            TerminationReason::Interrupted => "interrupted by signal",
            TerminationReason::RestartRequested => "restart requested",
            TerminationReason::WorkspaceGone => "workspace directory removed",
            TerminationReason::Cancelled => "cancelled by human",
        }
    }
}

/// Human-readable rendering of a termination reason (e.g. for logging fields).
impl std::fmt::Display for TerminationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.review_description())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_label_matches_variant_names() {
        // Everything except CompletionPromise mirrors `as_str` (snake_case
        // variant name); CompletionPromise is spelled out as
        // `completion_promise` in the history (see the method doc).
        for r in [
            TerminationReason::MaxIterations,
            TerminationReason::MaxRuntime,
            TerminationReason::MaxCost,
            TerminationReason::ConsecutiveFailures,
            TerminationReason::LoopThrashing,
            TerminationReason::LoopStale,
            TerminationReason::ValidationFailure,
            TerminationReason::Stopped,
            TerminationReason::Interrupted,
            TerminationReason::RestartRequested,
            TerminationReason::WorkspaceGone,
            TerminationReason::Cancelled,
        ] {
            assert_eq!(r.history_label(), r.as_str());
        }
        assert_eq!(
            TerminationReason::CompletionPromise.history_label(),
            "completion_promise"
        );
    }

    #[test]
    fn review_description_and_display_cover_all_reasons() {
        let max_iter = TerminationReason::MaxIterations.review_description();
        assert_eq!(max_iter, "max iterations reached");
        assert_eq!(
            TerminationReason::MaxIterations.to_string(),
            "max iterations reached",
            "Display delegates to review_description"
        );
        assert_eq!(
            TerminationReason::CompletionPromise.to_string(),
            "completed"
        );
        // No reason should be a blank description.
        for r in [
            TerminationReason::CompletionPromise,
            TerminationReason::MaxIterations,
            TerminationReason::MaxRuntime,
            TerminationReason::MaxCost,
            TerminationReason::ConsecutiveFailures,
            TerminationReason::LoopThrashing,
            TerminationReason::LoopStale,
            TerminationReason::ValidationFailure,
            TerminationReason::Stopped,
            TerminationReason::Interrupted,
            TerminationReason::RestartRequested,
            TerminationReason::WorkspaceGone,
            TerminationReason::Cancelled,
        ] {
            assert!(!r.review_description().is_empty());
        }
    }
}
