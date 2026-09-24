# Broadexec execution engine archaeology

Status: second excavation pass. Historical reconstruction, not pybroadexec architecture.

This document reconstructs how the original Broadexec execution engine actually behaves. The purpose is to separate useful execution semantics from Bash-era implementation accidents.

## The high-level pipeline

A normal script run follows roughly this sequence:

1. initialize a unique run ID;
2. load plugins and run initialization hooks;
3. parse options and reject conflicting modes;
4. resolve host lists/direct targets and exclusions;
5. choose/validate the operation script;
6. run pre-script plugin hooks;
7. apply host filters;
8. preflight/repair SSH known-host entries, concurrently;
9. in interactive mode, gather script-declared questions and run embedded local preparation;
10. detect script-declared custom credentials;
11. construct a temporary executable payload, including required helper libraries;
12. initialize external progress state and write an access log;
13. create per-target stdout/stderr temporary files;
14. launch one background SSH process per target;
15. map each SSH PID to its target's stdout/stderr files;
16. poll all PIDs, displaying completed results as they arrive;
17. classify connection failures, unsupported-OS results and timeouts;
18. kill unfinished SSH processes after the global execution timeout;
19. generate/display consolidated errors and reports;
20. perform failsafe cleanup.

The important observation is that Broadexec already has an implicit **run lifecycle** and **per-target lifecycle**, even though Bash variables, PIDs and temporary files represent the state.

## Concurrency model: eager fan-out

Broadexec loops over the complete selected target list and starts the SSH command in the background for every target. It records each shell PID and associates that PID with per-target output/error files.

There is no bounded worker pool in the normal execution path.

This means the historical concurrency model is effectively:

> resolve every target -> launch every target -> then wait for completion

This is simple and maximizes immediate fan-out, but a modern implementation should not blindly preserve unlimited process creation. The behavioral requirement is **concurrent fleet execution**, not "one local OS process for every host with no fan-out limit."

A pybroadexec design should probably make concurrency/fan-out explicit and configurable while choosing a sensible default.

## Per-target state is encoded indirectly

There is no HostExecution object, but one effectively exists.

For each launched target Broadexec retains:

- target/server identity;
- local SSH PID;
- stdout temporary file;
- stderr temporary file;
- whether the result has already been displayed;
- connection-error classification;
- timeout classification;
- unsupported-OS classification.

The PID is the primary correlation key.

This maps naturally to a future explicit per-target result/state model. We should preserve the semantics while eliminating the parallel arrays and filesystem-as-state-machine implementation.

## Payload transport and remote lifecycle

For normal key-based execution, Broadexec pipes the prepared script into SSH. The remote command writes it to a unique file under `/tmp`, executes it through the configured run shell, and removes the temporary script afterwards.

One execution mode creates a dedicated temporary directory, stores parameters separately, invokes a secured-sudo helper, then removes the script, parameter file and directory.

The normal path also runs `uname -n` before the payload. Broadexec uses the resulting output as part of its host/result handling.

The useful requirement is:

> A remote operation has an explicit staging -> execute -> cleanup lifecycle, and remote temporary artifacts should be removed.

The literal shell command composition should not survive.

## SSH host-key behavior: historically proactive, security-sensitive

Before normal execution, Broadexec starts a known-host repair/check operation for every selected target in parallel and waits for those preflight jobs before continuing.

The helper attempts to resolve aliases through:
- Broadexec's hosts mapping;
- `~/.ssh/config`;
- system host lookup.

If a key is missing it uses `ssh-keyscan` and appends the result to `~/.ssh/known_hosts`.

The actual normal SSH execution then uses strict host-key checking.

This reveals a good usability goal — don't discover missing SSH trust halfway through a 500-host run — but the automatic trust/bootstrap mechanism deserves a fresh security design. Automatically accepting a newly scanned key is not equivalent to verifying host identity.

pybroadexec should separate:
- host-key policy;
- preflight diagnostics;
- optional bootstrap behavior.

## Credential paths expose historical compromises

Normal execution uses batch-mode SSH and key authentication.

A separate custom-credential path can read a username/password declared inside the operation script, construct an SSH_ASKPASS helper, copy the script with SCP, execute it using password authentication, retrieve remote stdout/stderr files, and clean them up.

That mechanism should **not** be copied.

In particular, storing plaintext passwords in operation scripts is incompatible with a modern security model.

The historical requirement underneath it is still useful:

> Different operations/targets may need different SSH identities or privilege-escalation contexts.

pybroadexec should solve identity selection without embedding secrets in scripts.

## Completion detection: poll the local SSH processes

After launching all targets, Broadexec enters a polling loop.

Approximately every 0.2 seconds it examines every not-yet-displayed PID. When the local SSH process disappears, Broadexec treats that target as completed and processes its captured stdout/stderr.

This has an interesting UX consequence:

> Results are displayed in completion order, not inventory order.

That is probably worth preserving as a streaming/default terminal behavior. A saved report may choose deterministic ordering separately.

## Output is buffered per target, not streamed line-by-line

Although the wait loop displays results while the fleet is still running, each host's main output is displayed only after that target's SSH process has completed.

So historical Broadexec is **completion-streaming**, not true stdout streaming.

That is a useful distinction for pybroadexec design. Interleaving live lines from hundreds of machines can become unreadable. The old behavior gives users progressive feedback while preserving each target's output as one coherent unit.

Possible future modes:
- completion-streamed coherent host blocks (likely default);
- deterministic report ordering;
- optional true live streaming for diagnostics.

## Connection failure classification is text-driven

If a completed target has no substantive stdout, Broadexec inspects SSH stderr text. It distinguishes at least:

- SSH connection timeout;
- missing/unknown host key;
- generic inaccessible/login failure.

This is fragile because semantics are inferred from OpenSSH message strings.

A modern engine should obtain structured transport outcomes where possible and preserve raw stderr separately.

The conceptual distinction remains important:
- transport could not start the operation;
- operation ran and returned a result;
- operation exceeded execution timeout.

## A notable historical blind spot: remote exit status

The execution engine is strongly oriented around stdout/stderr content and whether the SSH process completed. In this excavation pass, there is no clear first-class per-host model preserving the remote script's exit code independently as a result field.

This is a major area for pybroadexec to improve.

A future target result should explicitly preserve at least:
- transport outcome;
- remote exit code when execution started;
- stdout;
- stderr;
- timing;
- timeout/cancellation reason.

This would let pybroadexec report facts without guessing semantics from output text.

## Timeout semantics: one whole-run execution deadline

The execution timeout begins after all SSH commands have been launched and Broadexec enters the wait function.

The polling loop continues until either:
- all PIDs have completed; or
- the configured script-run timeout expires.

If the deadline expires, every unfinished target is classified. SSH connection-timeout text is treated specially; otherwise the host is considered to have timed out during script execution.

Broadexec then kills the corresponding local SSH process.

This is therefore historically a **global run deadline for unfinished targets**, not an independently measured per-target runtime starting at each target's launch.

pybroadexec should explicitly decide whether it wants:
- connection timeout;
- per-target execution timeout;
- whole-run deadline;
- or combinations of all three.

Those are different concepts and should not be conflated.

## Cancellation and timeout try to clean both sides

On timeout Broadexec kills the local SSH process, waits/checks whether it died, and then attempts another SSH connection to remove the remote temporary script.

On Ctrl+C it:

1. announces interruption;
2. walks all known SSH PIDs;
3. kills each active session;
4. removes its local stdout/stderr files;
5. removes partially generated reports;
6. checks/report PIDs that could not be killed;
7. kills outstanding known-host preflight jobs;
8. removes remaining temporary files/old lock state;
9. sets external run status to `INTERRUPTED`;
10. exits non-zero.

This is one of the strongest pieces of execution-engine intent in the old project:

> Cancellation is an operation with cleanup semantics, not merely process termination.

That should absolutely survive.

However, killing the local SSH process does not necessarily prove that an already-started remote process stopped. pybroadexec must be precise about what cancellation guarantees.

## Remote cancellation is a hard problem the old code only partly solves

Broadexec attempts cleanup after killing SSH, but its follow-up primarily removes the staged script. Depending on how the remote shell/process was launched, terminating SSH may or may not terminate descendants already executing remotely.

The old code deserves credit for recognizing the problem — it even reports local PIDs it could not kill — but pybroadexec should explicitly document its remote-process cancellation semantics.

Potential design questions:
- Does pybroadexec merely cancel transport?
- Can it wrap operations in a remotely identifiable process group?
- Should it support a best-effort remote termination token/run ID?
- What happens when the network disappears during cancellation?
- How do we avoid claiming "cancelled" when the remote process may still be running?

This should eventually become a dedicated design issue.

## Run IDs are already important

Broadexec creates a run ID from timestamp + local PID and uses it for remote temporary paths and logging.

That idea should survive in stronger form. A unique run ID is useful for:
- local correlation;
- remote staging;
- reports;
- structured events;
- cleanup;
- diagnostics;
- external callers.

A modern run ID should not depend on PID uniqueness alone.

## Progress API was integrated into the engine

The unfinished external-stats feature is not bolted entirely outside the engine.

The run can maintain states:

- INIT
- RUNNING
- FINISHED
- ERROR
- INTERRUPTED

and progress fields:

- completed count;
- total targets;
- percentage;
- elapsed seconds.

The engine updates progress as completed target results are processed.

This is strong evidence for an internal event/state model in pybroadexec. Terminal rendering, JSON event output, reports and an embedding API could all consume the same execution events rather than each inventing their own state tracking.

That does **not** imply a daemon or monitoring system.

## Reports are produced as execution artifacts

The wait phase initializes report files. Results are appended while completed targets are processed. Separate error and grep/list reports may also be produced.

External mode can print report paths for the caller.

This reinforces an architectural distinction worth keeping:

- **execution state** exists only for the active run;
- **execution artifacts** may survive afterwards.

That remains consistent with "pybroadexec executes work across fleets; it does not own the fleet."

## Error blacklisting is evidence of noisy real-world SSH/script output

Before generating the consolidated error log, Broadexec can remove configured unwanted stderr phrases.

The literal blacklist approach may be too blunt, but the feature reveals a real operational need: some environments/tools emit known harmless noise to stderr.

pybroadexec should preserve raw stderr somewhere if it supports suppression in presentation. Presentation filters must not silently destroy the factual result record.

## What should be preserved from this engine

### KEEP

- concurrent execution as a core behavior;
- explicit run identity;
- independent per-target execution state;
- coherent per-target stdout/stderr capture;
- progressive display as targets finish;
- distinction between transport failure and execution timeout;
- run-level progress;
- reports as optional execution artifacts;
- deliberate Ctrl+C/timeout cleanup;
- remote temporary-artifact cleanup;
- ability to be driven non-interactively by another program.

### IMPROVE

- bounded/configurable fan-out;
- structured target result objects;
- first-class remote exit codes;
- structured transport failure reasons;
- explicit connection/per-target/global timeout semantics;
- safer host-key handling;
- safe credential/identity selection;
- deterministic machine-readable output;
- cancellation semantics that admit uncertainty about remote state;
- preservation of raw output alongside presentation filtering.

### DO NOT PORT

- PID-indexed Bash arrays as the state model;
- temporary files as the primary internal message bus;
- polling every PID from a Bash loop;
- parsing OpenSSH English stderr to determine all failure classes;
- passwords embedded in operation scripts;
- SSH_ASKPASS helper generation as designed here;
- huge composed remote shell strings;
- automatic `ssh-keyscan` trust as an implicit prerequisite;
- unconditional one-process-per-host fan-out.

## Emerging execution-engine principle

The original implementation suggests a useful modern principle:

> **A pybroadexec run is a finite collection of independently tracked target executions, coordinated by one ephemeral run lifecycle.**

This gives us a clean way to reason about concurrency without drifting into monitoring.

A target can have a state such as queued, connecting, running, completed, transport-failed, timed-out, cancelled or uncertain. Those states describe one run only. When the run is over, pybroadexec exits; the result/report may remain.

## One particularly good old behavior

The old completion-streaming model deserves special attention.

Instead of spraying interleaved stdout from 200 hosts across the terminal, Broadexec waits until an individual host finishes and then displays that host's coherent result while other hosts continue running.

That may still be an excellent default in 2026.

It is simple, readable and naturally communicates progress.

## Next excavation

The next pass should reconstruct **targeting anatomy**:

- exact host-file formats;
- IP/hostname mapping;
- user@host and port handling;
- direct hosts;
- exclusions;
- duplicate handling;
- multilevel filters;
- multiple simultaneous filter branches;
- team versus custom host lists;
- batch-mode behavior;
- invalid/empty selection semantics.

After that, the operation/script contract deserves its own excavation.

No pybroadexec implementation should begin yet.
