//! v3 cutover: drive the autoloop runtime as ralph's orchestration engine.
//!
//! When `core.engine = "autoloop"`, `ralph run` spawns `autoloop run` as a
//! subprocess via [`AutoloopRunner`] instead of the in-house event loop,
//! consumes its structured `--events` LoopEvent stream, and maps the terminal
//! result onto ralph's [`TerminationReason`]. This is the thin-layer engine swap
//! at the heart of v3: autoloop owns loop execution; ralph coordinates.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use ralph_adapters::{AutoloopEventTailer, AutoloopRpcMapper, AutoloopRunner, events_run_result, parse_events};
use ralph_core::{LoopContext, RalphConfig, RunStats, TerminationReason};

use crate::completion_coord::coordinate_completion;

/// Map an autoloop `stopReason` onto ralph's [`TerminationReason`].
fn map_stop_reason(reason: &str) -> TerminationReason {
    match reason {
        "completed" | "completion_event" | "completion_promise" | "verdict_exit" => {
            TerminationReason::CompletionPromise
        }
        "max_iterations" => TerminationReason::MaxIterations,
        "max_runtime" => TerminationReason::MaxRuntime,
        "cost_budget" => TerminationReason::MaxCost,
        "stalled" => TerminationReason::LoopStale,
        "interrupted" => TerminationReason::Interrupted,
        "backend_failed" | "backend_timeout" | "verdict_takeover" => {
            TerminationReason::ValidationFailure
        }
        _ => TerminationReason::Stopped,
    }
}

/// Resolve `p` against `workspace` when relative.
fn resolve(workspace: &Path, p: &str) -> PathBuf {
    let path = PathBuf::from(p);
    if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    }
}

/// Drive the configured autoloop preset as ralph's engine, returning the mapped
/// [`TerminationReason`].
///
/// After the subprocess terminates, runs the engine-agnostic completion
/// coordination ([`coordinate_completion`]) so parallel-loop bookkeeping
/// (merge queue, loop registry, landing, summary, history) matches the in-house
/// engine. `context` carries the loop identity; `None` means an ad-hoc run with
/// no merge-queue / registry participation.
pub async fn run_autoloop_engine(
    config: RalphConfig,
    context: Option<LoopContext>,
    auto_merge_override: Option<bool>,
    loop_id: Option<String>,
    use_colors: bool,
    tui: bool,
    rpc: bool,
) -> Result<TerminationReason> {
    let workspace = config.core.workspace_root.clone();

    // Use an explicit preset if configured; otherwise generate one from ralph's
    // native hats topology so existing ralph configs run on the autoloop engine
    // without a hand-authored preset.
    let preset = match config.core.autoloop_preset.as_deref() {
        Some(p) => {
            let preset = resolve(&workspace, p);
            if !preset.join("autoloops.toml").is_file() {
                bail!(
                    "autoloop preset not found (no autoloops.toml): {}",
                    preset.display()
                );
            }
            preset
        }
        None => {
            let preset = workspace.join(".ralph").join("autoloop-preset");
            crate::autoloop_preset_gen::generate_preset(&config, &preset)
                .context("generating an autoloop preset from the hats topology")?;
            tracing::info!(preset = %preset.display(), "engine=autoloop: generated preset from hats config");
            preset
        }
    };

    // The prompt comes from the canonical field: an inline prompt (from `-p`,
    // held in `config.event_loop.prompt` by `run_command`) takes precedence over
    // a prompt file. This matters for `--rpc`, where `LoopStarted.prompt` is
    // protocol-visible — dropping an inline `-p` would surface an empty prompt.
    let prompt = if let Some(p) = config.event_loop.prompt.clone() {
        p
    } else {
        let pf = config.event_loop.prompt_file.clone();
        if pf.trim().is_empty() {
            String::new()
        } else {
            let path = resolve(&workspace, &pf);
            std::fs::read_to_string(&path)
                .with_context(|| format!("reading prompt file {}", path.display()))?
        }
    };

    // Structured event sink under .ralph/ — the preferred observability channel.
    let events_path = workspace.join(".ralph").join("autoloop-events.ndjson");
    if let Some(parent) = events_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&events_path);

    tracing::info!(
        preset = %preset.display(),
        "engine=autoloop: driving the autoloop runtime as a subprocess"
    );

    // Backend selection is not yet mapped from ralph's backend names to
    // autoloop's backend kinds (#347), so the subprocess uses autoloop's default
    // backend. Surface that explicitly rather than silently ignoring the config.
    if config.cli.backend != "auto" && !config.cli.backend.is_empty() {
        tracing::warn!(
            backend = %config.cli.backend,
            "engine=autoloop: ralph backend selection is not forwarded to autoloop yet; using autoloop's default backend (see #347)"
        );
    }

    let runner = AutoloopRunner::new(preset, prompt.clone(), workspace.clone())
        .events_path(events_path.clone());

    let start = Instant::now();
    let summary = if rpc {
        // RPC mode (#343): emit ralph's JSON-RPC `RpcEvent` stream on stdout by
        // live-tailing the same --events file and translating it through
        // `AutoloopRpcMapper`. stdout is kept protocol-clean (logs go to stderr,
        // the human-readable summary println below is suppressed).
        let max_iterations = Some(config.event_loop.max_iterations);
        run_autoloop_with_rpc(
            runner,
            events_path.clone(),
            prompt.clone(),
            rpc_backend_label(&config),
            max_iterations,
        )
        .await
        .context("autoloop RPC run failed")?
    } else if tui {
        // In-process TUI: render the autoloop run live by tailing its --events
        // file, concurrent with the subprocess. Resolves Ctrl+C by killing the
        // child (see run_autoloop_with_tui).
        run_autoloop_with_tui(runner, events_path.clone(), workspace.clone())
            .await
            .context("autoloop TUI run failed")?
    } else {
        // Headless: AutoloopRunner::run blocks on the subprocess; keep the async
        // runtime free. Unchanged from the pre-TUI path.
        tokio::task::spawn_blocking(move || runner.run())
            .await
            .context("autoloop run task panicked")?
            .context("autoloop run failed")?
    };

    // The human-readable summary line would corrupt the RPC stdout stream, so
    // emit it only outside RPC mode. RPC callers get the terminal LoopTerminated.
    if !rpc {
        if let Ok(content) = std::fs::read_to_string(&events_path) {
            if let Some(result) = events_run_result(&parse_events(&content)) {
                println!(
                    "autoloop engine: run_id={} iterations={} stop_reason={}",
                    result.run_id, result.iterations, result.stop_reason
                );
            }
        }
    }

    let reason = map_stop_reason(&summary.stop_reason);

    // Mirror the in-house engine's completion bookkeeping so parallel-loop
    // coordination (merge queue, registry, landing) works under the autoloop
    // engine. autoloop owns iteration/timing; we surface what the summary gives.
    let state = RunStats {
        iterations: summary.iterations,
        elapsed: start.elapsed(),
        cost_usd: summary.cost_usd,
    };

    let auto_merge = auto_merge_override.unwrap_or(config.features.auto_merge);
    let loop_id = loop_id
        .or_else(|| {
            context
                .as_ref()
                .and_then(|c| c.loop_id().map(|s| s.to_string()))
        })
        .unwrap_or_else(|| "primary".to_string());

    coordinate_completion(
        &reason,
        &state,
        context.as_ref(),
        &config.core.scratchpad.path,
        &prompt,
        auto_merge,
        &loop_id,
        use_colors,
        // In RPC mode stdout is the protocol channel — the banner's content is
        // already delivered as the LoopTerminated event.
        !rpc,
    );

    Ok(reason)
}

/// Run the autoloop subprocess with the in-process live TUI.
///
/// The TUI renders inside this (parent) process, concurrent with the `autoloop
/// run` subprocess, fed by live-tailing the `--events` file. tokio is
/// multi-threaded (`#[tokio::main]`), so the blocking subprocess wait
/// (`spawn_blocking`), the async TUI render loop, and the async event-reader
/// poll task coexist. The subprocess's stdout/stderr are piped (see
/// `AutoloopRunner::spawn`), so they never corrupt the ratatui tty.
///
/// ## Ctrl+C behavior (FIX gap#2)
///
/// `AutoloopRunner` exposes [`AutoloopRunner::spawn`], so on Ctrl+C the TUI
/// signals via its interrupt channel and we **kill the autoloop child**
/// (SIGTERM, then SIGKILL) — a clean teardown, NOT a blank-terminal hang. The
/// blocking wait then returns the child's (killed) result and we fall through
/// to the shared post-run path. This is the proper refactor the design calls
/// for, not the `exit(130)` fallback.
async fn run_autoloop_with_tui(
    runner: AutoloopRunner,
    events_path: PathBuf,
    workspace: PathBuf,
) -> Result<ralph_adapters::AutoloopRunSummary> {
    use ralph_tui::Tui;
    use tokio::sync::watch;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Termination drives BOTH the TUI shutdown and the event reader's final
    // drain. App needs an interrupt sink (Ctrl+C), but we don't watch it — the
    // TUI returning (on q OR Ctrl+C) is what triggers the kill below.
    let (terminated_tx, terminated_rx) = watch::channel(false);
    let (interrupt_tx, _interrupt_rx) = watch::channel(false);

    let tui = Tui::new()
        .with_termination_signal(terminated_rx.clone())
        .with_interrupt_tx(interrupt_tx)
        .with_export_workspace_root(workspace);
    let state = tui.state();

    // Mark the source so the footer suppresses guidance/steer affordances that
    // have no back-channel to the autoloop child (FIX gap#6).
    if let Ok(mut s) = state.lock() {
        s.autoloop_source = true;
    }

    // Live-tail the --events file into the TUI state until cancelled.
    let reader_handle = {
        let reader_state = Arc::clone(&state);
        let cancel_rx = terminated_rx.clone();
        tokio::spawn(async move {
            ralph_tui::run_autoloop_event_reader(events_path, reader_state, cancel_rx).await;
        })
    };

    // Spawn autoloop as its OWN process-group leader (piped stdio) so that on a
    // user quit we can kill the whole tree (autoloop + its backend agent) and
    // not orphan the agent. Headless keeps the child in ralph's group.
    let runner = runner.own_process_group(true);
    let child = runner.spawn().context("spawning the autoloop subprocess")?;
    let child_pid = child.id();

    // Block on the subprocess in a worker thread, freeing the async runtime for
    // the TUI + reader. wait_with_summary mirrors run()'s success/error contract.
    // On natural exit set `completed` and signal the TUI to drop — this is what
    // unblocks `tui.run()` when the run finishes on its own.
    let completed = Arc::new(AtomicBool::new(false));
    let wait_handle = {
        let terminated_tx = terminated_tx.clone();
        let completed = Arc::clone(&completed);
        tokio::spawn(async move {
            let summary = tokio::task::spawn_blocking(move || runner.wait_with_summary(child))
                .await
                .context("autoloop wait task panicked")?;
            completed.store(true, Ordering::SeqCst);
            let _ = terminated_tx.send(true);
            summary.context("autoloop run failed")
        })
    };

    // Run the TUI render/input loop concurrently with the subprocess. It returns
    // on natural completion (terminated_tx) OR on q / Ctrl+C.
    let tui_result = tui.run().await;

    // If the subprocess is still running (user quit via q or Ctrl+C — neither
    // exits autoloop), stop the whole process group so the backend isn't
    // orphaned. No-op if the run already completed naturally.
    if !completed.load(Ordering::SeqCst) {
        kill_autoloop_group(child_pid);
    }
    // Ensure the reader does its final drain even if the TUI exited first (q).
    let _ = terminated_tx.send(true);

    // Collect the subprocess result, then tear down the auxiliary tasks.
    let summary = wait_handle.await.context("autoloop wait join failed")?;
    let _ = reader_handle.await;
    tui_result.context("TUI render loop failed")?;

    summary
}

/// Backend label placed on the RPC `LoopStarted`/`IterationStart` events.
///
/// ralph's backend selection is not yet forwarded to autoloop (#347), so the
/// subprocess uses autoloop's default backend. Report that honestly as
/// `"autoloop"` when ralph's own selection is unset/auto; otherwise echo the
/// configured name so RPC consumers see what the user asked for.
fn rpc_backend_label(config: &RalphConfig) -> String {
    let backend = &config.cli.backend;
    if backend.is_empty() || backend == "auto" {
        "autoloop".to_string()
    } else {
        backend.clone()
    }
}

/// Current wall-clock time as Unix milliseconds.
fn now_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Serialize an [`RpcEvent`] as a JSON line to stdout and flush so RPC consumers
/// see each event promptly. stdout is the protocol channel in `--rpc` mode
/// (logs are routed to stderr at startup), so nothing else writes here.
fn emit_rpc(event: &ralph_proto::json_rpc::RpcEvent) {
    use std::io::Write;
    let line = ralph_proto::json_rpc::emit_event_line(event);
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(line.as_bytes());
    let _ = out.flush();
}

/// Run the autoloop subprocess in RPC mode: translate its `--events` stream into
/// ralph's JSON-RPC [`RpcEvent`](ralph_proto::json_rpc::RpcEvent) contract on
/// stdout (#343).
///
/// This is the `--rpc` counterpart to [`run_autoloop_with_tui`]: instead of
/// rendering the tailed `--events` file into a TUI, it maps each event through
/// [`AutoloopRpcMapper`] and emits the resulting `RpcEvent`s as JSON lines. A
/// leading `LoopStarted` frames the run (prompt/backend/max-iterations are
/// engine-side knowledge absent from the coarse `--events` stream); the mapper
/// supplies everything derivable from the stream through the terminal
/// `LoopTerminated`.
///
/// Mirrors the TUI reader's cancel/final-drain discipline: autoloop writes the
/// terminal `loop.finish` synchronously just before exit, so after the wait task
/// signals completion the reader performs one final `poll()` (plus a
/// `finalize()`) to capture it.
///
/// ## Cancel / control
///
/// The child is spawned as its own process-group leader. A cancel — SIGINT,
/// SIGTERM, or an `abort` RpcCommand on stdin — kills the whole autoloop
/// subtree (`kill_autoloop_group`: SIGTERM, then SIGKILL) so the backend agent
/// is not orphaned, and if autoloop never wrote an authoritative terminal a
/// synthesized `LoopTerminated { reason: Interrupted, ... }` closes the stream
/// so the RPC protocol always ends well-formed. Other RpcCommand variants
/// (guidance / steer / follow-up / get-state / set-hat) are consumed and
/// ignored for now: they require an in-loop agent channel that the autoloop
/// engine path does not expose yet (#345).
async fn run_autoloop_with_rpc(
    runner: AutoloopRunner,
    events_path: PathBuf,
    prompt: String,
    backend: String,
    max_iterations: Option<u32>,
) -> Result<ralph_adapters::AutoloopRunSummary> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use ralph_proto::json_rpc::{RpcCommand, RpcEvent, parse_command};
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::sync::watch;

    let started_at = now_unix_millis();
    emit_rpc(&RpcEvent::LoopStarted {
        prompt,
        max_iterations,
        backend: backend.clone(),
        workspace_root: None,
        started_at,
    });

    // Signals subprocess completion so the reader stops tailing and does its
    // final drain.
    let (done_tx, mut done_rx) = watch::channel(false);
    // Signals a cancel (SIGINT/SIGTERM or an RPC `abort` command on stdin) so we
    // kill the whole autoloop subtree, mirroring `run_autoloop_with_tui`.
    let (cancel_tx, mut cancel_rx) = watch::channel(false);

    // Spawn as its own process-group leader so a cancel can kill the entire tree
    // (autoloop + backend agent) rather than orphaning the agent. Rebind `runner`
    // (mirroring `run_autoloop_with_tui`) so `wait_with_summary` below uses the
    // process-grouped instance.
    let runner = runner.own_process_group(true);
    let child = runner
        .spawn()
        .context("spawning the autoloop subprocess")?;
    let child_pid = child.id();

    // SIGINT/SIGTERM → cancel. RPC mode is a protocol channel, not a human tty,
    // so the default "die on SIGINT" is replaced by a graceful kill of the
    // subtree + a terminal event (the TUI path does the same via q/Ctrl+C).
    #[cfg(unix)]
    {
        let tx = cancel_tx.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigint =
                signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = sigint.recv() => {
                    let _ = tx.send(true);
                }
                _ = sigterm.recv() => {
                    let _ = tx.send(true);
                }
            }
        });
    }
    // JSON-RPC commands arrive on stdin as one JSON object per line. Only
    // `abort` drives cancel here; the other variants (guidance/steer/follow-up/
    // get-state/set-hat) require an active in-loop agent channel that the
    // autoloop engine path does not expose yet (see #345) — they are consumed
    // (acked) and otherwise ignored.
    let tx = cancel_tx.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut lines = BufReader::new(stdin).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match parse_command(line) {
                Ok(RpcCommand::Abort { .. }) => {
                    let _ = tx.send(true);
                }
                Ok(_) => {
                    tracing::debug!(command = line, "RPC command consumed (no live channel; ignored)");
                }
                Err(e) => {
                    tracing::debug!(error = %e, line, "malformed RPC command line; ignoring");
                }
            }
        }
    });

    let completed = Arc::new(AtomicBool::new(false));
    let wait_handle = {
        let done_tx = done_tx.clone();
        let completed = Arc::clone(&completed);
        tokio::spawn(async move {
            let summary = tokio::task::spawn_blocking(move || runner.wait_with_summary(child))
                .await
                .context("autoloop wait task panicked")?;
            completed.store(true, Ordering::SeqCst);
            let _ = done_tx.send(true);
            summary.context("autoloop run failed")
        })
    };

    // Reader: tail the --events file, translate to RpcEvents, emit to stdout.
    let mut tailer = AutoloopEventTailer::new(&events_path);
    let mut mapper = AutoloopRpcMapper::new(started_at, backend);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker.tick().await; // first tick completes immediately; skip it
    let cancelled = loop {
        tokio::select! {
            biased;

            _ = done_rx.changed() => {
                if *done_rx.borrow() {
                    break false;
                }
            }

            _ = cancel_rx.changed() => {
                if *cancel_rx.borrow() {
                    break true;
                }
            }

            _ = ticker.tick() => {
                drain_rpc_events(&mut tailer, &mut mapper);
            }
        }
    };

    // If the run was cancelled before autoloop wrote its own terminal, kill the
    // whole subtree so the backend agent is not orphaned. (No-op if autoloop
    // already exited on its own.)
    if cancelled && !mapper.saw_terminal() {
        kill_autoloop_group(child_pid);
    }

    // Final drain: capture the terminal loop.finish written just before exit,
    // then flush a summary-only terminal if no loop.finish ever arrived.
    drain_rpc_events(&mut tailer, &mut mapper);
    if let Some(terminal) = mapper.finalize() {
        emit_rpc(&terminal);
    }
    // On cancel where autoloop didn't emit an authoritative terminal of its own,
    // synthesize one so the RPC stream always ends well-formed.
    if cancelled && !mapper.saw_terminal() {
        let terminated_at = now_unix_millis();
        emit_rpc(&RpcEvent::LoopTerminated {
            reason: ralph_proto::json_rpc::TerminationReason::Interrupted,
            total_iterations: 0,
            duration_ms: terminated_at.saturating_sub(started_at),
            total_cost_usd: 0.0,
            terminated_at,
        });
    }

    wait_handle
        .await
        .context("autoloop wait join failed")?
}

/// Poll the tailer once and emit every translated [`RpcEvent`] to stdout.
fn drain_rpc_events(tailer: &mut AutoloopEventTailer, mapper: &mut AutoloopRpcMapper) {
    match tailer.poll() {
        Ok(events) => {
            for event in &events {
                for rpc in mapper.map(event) {
                    emit_rpc(&rpc);
                }
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, "autoloop RPC reader poll failed");
        }
    }
}

/// Stop the autoloop subprocess tree: SIGTERM the whole process group, then
/// escalate to SIGKILL after a short grace so autoloop and its backend agent can
/// exit cleanly (flush, release locks) first. `pid` is the group leader's pid
/// (the child was spawned with [`AutoloopRunner::own_process_group`]). Off Unix
/// this is a best-effort no-op. The blocking wait reaps the child once it exits.
fn kill_autoloop_group(pid: u32) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::Pid;
        let pgid = Pid::from_raw(pid as i32);
        let _ = killpg(pgid, Signal::SIGTERM);
        // Detached escalation: hard-kill the group if it ignores SIGTERM. A
        // SIGKILL to an already-dead group is ESRCH and harmless.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let _ = killpg(pgid, Signal::SIGKILL);
        });
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
}

/// Start a headless orchestration loop for the Telegram bot daemon.
///
/// This is the daemon's [`ralph_proto::StartLoopFn`] target: it loads config,
/// applies the supplied prompt, forces autonomous/headless mode, acquires the
/// primary loop lock, and drives the autoloop engine.
///
/// Note: human-in-the-loop robot wiring under the autoloop engine is descoped
/// to #345. The loop still runs; in-loop Telegram interaction is not yet routed.
pub async fn start_loop(
    prompt: String,
    workspace_root: PathBuf,
    config_path: Option<PathBuf>,
) -> Result<TerminationReason> {
    use crate::{ConfigSource, load_config_with_overrides};

    // Load config from file or defaults.
    let config_source = config_path.unwrap_or_else(|| workspace_root.join("ralph.yml"));
    let sources = vec![ConfigSource::File(config_source)];
    let mut config = load_config_with_overrides(&sources)?;

    // Set workspace root to the provided path.
    config.core.workspace_root = workspace_root.clone();

    // Apply the prompt.
    config.event_loop.prompt = Some(prompt);
    config.event_loop.prompt_file = String::new();

    // Force autonomous headless mode (no TUI, no interactive).
    config.cli.default_mode = "autonomous".to_string();

    // Normalize and validate.
    config.normalize();
    let warnings = config
        .validate()
        .context("Configuration validation failed")?;
    for warning in &warnings {
        tracing::warn!("{}", warning);
    }

    // Auto-detect backend if needed.
    if config.cli.backend == "auto" {
        let priority = config.get_agent_priority();
        let detected = ralph_adapters::detect_backend(&priority, |backend| {
            config.adapter_settings(backend).enabled
        });
        match detected {
            Ok(backend) => {
                tracing::info!("Auto-detected backend: {}", backend);
                config.cli.backend = backend;
            }
            Err(e) => return Err(anyhow::Error::new(e)),
        }
    }

    // Ensure scratchpad directory exists.
    crate::ensure_scratchpad_directory(&config)?;

    // Acquire the loop lock (primary loop).
    let prompt_summary = config.event_loop.prompt.as_deref().unwrap_or("[daemon]");
    let prompt_summary = ralph_core::truncate_with_ellipsis(prompt_summary, 100);

    let _lock_guard = ralph_core::LoopLock::try_acquire(&workspace_root, &prompt_summary)
        .context("Failed to acquire loop lock — another loop may be running")?;

    let loop_context = ralph_core::LoopContext::primary(workspace_root);

    // Drive the loop headlessly via the autoloop engine (daemon: never a TUI,
    // never RPC).
    run_autoloop_engine(config, Some(loop_context), None, None, false, false, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_autoloop_stop_reasons_to_termination() {
        assert!(matches!(
            map_stop_reason("completed"),
            TerminationReason::CompletionPromise
        ));
        assert!(matches!(
            map_stop_reason("completion_event"),
            TerminationReason::CompletionPromise
        ));
        assert!(matches!(
            map_stop_reason("completion_promise"),
            TerminationReason::CompletionPromise
        ));
        assert!(matches!(
            map_stop_reason("max_iterations"),
            TerminationReason::MaxIterations
        ));
        assert!(matches!(
            map_stop_reason("cost_budget"),
            TerminationReason::MaxCost
        ));
        assert!(matches!(
            map_stop_reason("interrupted"),
            TerminationReason::Interrupted
        ));
        assert!(matches!(
            map_stop_reason("backend_failed"),
            TerminationReason::ValidationFailure
        ));
        // Unknown reasons fall back to a generic stop.
        assert!(matches!(
            map_stop_reason("something_new"),
            TerminationReason::Stopped
        ));
    }
}
