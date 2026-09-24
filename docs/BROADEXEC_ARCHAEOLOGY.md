# Broadexec archaeology — first excavation

Status: initial survey. This is a design/research document, not an implementation plan.

The purpose of this excavation is to understand what the original Broadexec actually became, including partially implemented and abandoned ideas, before designing pybroadexec. The old repository is evidence. It is not a codebase to port line-for-line.

See also [PROJECT_PHILOSOPHY.md](PROJECT_PHILOSOPHY.md).

## First surprise: Broadexec was already more than parallel SSH

At its simplest, Broadexec runs a selected script concurrently across a selected set of hosts. But the repository shows that the project had already grown a substantial execution environment around that operation.

The important archaeological finding is therefore:

> The useful inheritance is probably not "parallel SSH". It is the machinery around making an ad-hoc fleet execution controlled, inspectable, reusable, and safer.

That distinction should guide the rest of the excavation.

## Feature map found in the repository

### 1. Concurrent fleet execution — KEEP

The core behavior is explicit: prepare a script, launch SSH executions in the background for all selected targets, retain per-process/per-host output files, wait for the executions, then build reports and error output.

Concurrency is therefore original Broadexec behavior, not a new pybroadexec feature.

Questions for pybroadexec:
- bounded concurrency/fan-out versus launching every target immediately;
- streaming versus buffered output;
- deterministic cancellation;
- per-host and whole-run timeouts;
- resource limits for very large target sets.

### 2. Host inventories and multilevel filters — KEEP / IMPROVE

Host files are more than lists. Additional columns act as hierarchical filter dimensions. A filter such as `test.pilot` applies successive filter levels, and multiple filters can select several branches from the same inventory.

This is one of the more distinctive Broadexec ideas and deserves careful preservation at the behavioral level.

The old implementation also contains evidence that interactive filter selection became difficult: the filter-menu code was temporarily disabled as "quite buggy on more complicated hostlists" and marked for total rework.

That makes this a strong **KEEP the capability / REDESIGN the interface and implementation** candidate.

### 3. Direct target selection — KEEP

Broadexec supports bypassing host lists and specifying hosts directly. Multiple direct-host arguments were supported.

The new CLI should retain an easy ad-hoc path without requiring users to construct inventory files for every operation.

### 4. Script library and interactive discovery — KEEP / REDESIGN

Broadexec discovers predefined scripts, excludes scripts marked disabled, and can offer interactive script selection when the user does not provide one.

This reveals an important distinction from a generic parallel shell: Broadexec considered scripts first-class reusable execution units.

The CLI-only requirement does not forbid an interactive terminal workflow, but pybroadexec should decide whether menus remain valuable or whether excellent discovery commands and shell completion are enough.

### 5. Script-declared behavior — IMPORTANT, INVESTIGATE DEEPLY

Scripts can communicate metadata/requirements back to Broadexec. Examples found include:

- disabling a script;
- declaring supported OS/version ranges;
- declaring questions to ask before execution;
- specifying custom execution credentials;
- embedded local pre-execution code;
- parameters subsequently passed to the remote script.

This may be one of the most important pieces of Broadexec's identity.

The old Bash mechanism is unsafe/fragile in places and should not simply be reproduced. But the underlying idea — **an executable can describe how Broadexec should prepare and validate its run** — deserves serious design work.

### 6. Questions framework — FINISH / REDESIGN

The manual documents script variables such as `BRDEXEC_SCRIPT_QUESTION_...` that cause Broadexec to gather required, optional, or flag-like values before execution and convert the answers into script arguments.

The manual itself marks this area FIXME/incomplete.

The user need is still interesting: reusable operational scripts often require a small amount of validated input before being sent to hundreds of hosts.

Do not preserve the Bash-variable protocol automatically. Preserve the problem it solved.

### 7. Embedded pre-execution framework — REDESIGN

Broadexec can extract a specially delimited section of a remote script, execute that section once on the jump host, use it to gather/compute parameters or abort the operation, remove it from the payload, and then execute the remaining script remotely.

This is clever and powerful, but also tightly coupled to shell parsing and arbitrary local execution.

The capability appears to mean:

> A fleet operation may have a local preparation/validation phase before its per-host remote phase.

That is worth preserving as a design concept. The literal embedded-code mechanism should be reconsidered from scratch.

### 8. Script parameter forwarding / batch use — KEEP

Broadexec supports forwarding parameters directly to the selected script so callers can bypass interactive questions. This was specifically useful when Broadexec itself was called by another script.

pybroadexec should remain composable from shell scripts and automation. Interactive convenience must never be mandatory.

### 9. OS/platform gating — KEEP CONCEPT / REDESIGN

Broadexec contains an OS-release library and scripts can declare supported operating systems and minimum/maximum releases. The library had grown to detect platform families including Linux, HP-UX, Solaris and AIX. A FIXME proposed hardware/platform detection via dmidecode.

The useful concept is precondition protection: do not blindly run an operation on targets it declares unsupported.

However, pybroadexec must be careful not to become a configuration-management fact database. Compatibility checks should remain execution preconditions, not persistent infrastructure state.

### 10. Output modes and fleet-result search — KEEP / IMPROVE

Broadexec supports:

- compact one-line-per-host output;
- human-readable multiline output;
- quiet and very-quiet modes;
- grep-like matching of execution output that returns matching hostnames;
- case-insensitive matching;
- error-message blacklisting.

This is more significant than cosmetic formatting. At fleet scale, **finding which hosts produced a particular result** is part of the tool's job.

The old grep-like behavior may evolve into a more general result-query/grouping model without becoming monitoring.

### 11. Reports — KEEP / IMPROVE

Each execution can produce a normal report, separate error report, and — for grep-style operations — a host-list report. Reports can be retained indefinitely or automatically cleaned after a configured age.

This is compatible with the project philosophy: reports are records of executions, not persistent health state.

pybroadexec should later distinguish clearly between:
- terminal presentation;
- machine-readable result output;
- optional durable run reports.

### 12. Error taxonomy — KEEP CONCEPT

The manual defines many specific error codes: invalid filters, empty inventories, unsigned scripts, missing reports, incomplete batch input, unsafe copy destinations, missing host folders, conflicting options, and more.

The exact historical numbers probably do not deserve preservation unless compatibility demands it, but structured failure categories absolutely do.

A future CLI should make it possible for another program to distinguish:
- invalid invocation;
- target-selection failure;
- transport failure;
- remote execution failure;
- timeout/cancellation;
- local preparation failure;
- reporting/internal failure.

### 13. File distribution — INVESTIGATE

Broadexec can distribute a file to multiple hosts and deliberately restricts destinations to safer locations such as `/tmp` and home-directory subpaths. The manual says transfers are not all initiated simultaneously in order to avoid overloading the jump server uplink.

This shows thoughtful fleet behavior rather than a raw `scp` loop.

Open question: is file distribution part of pybroadexec's core execution mission, a useful companion primitive, or scope creep now that excellent alternatives exist?

### 14. Cleanup and interruption handling — KEEP, CORE QUALITY

Broadexec tracks temporary files and attempts cleanup even on Ctrl+C. During interruption it attempts to stop active SSH work before cleaning local state.

This is not merely housekeeping. Reliable cancellation across concurrent remote operations should be a first-class requirement in pybroadexec.

### 15. Security through script integrity/signatures — INVESTIGATE / REDESIGN

Broadexec contains GPG signature verification for executable scripts and public-key handling. The stated intent was to prevent accidental or unauthorized modification of centrally supplied operational scripts.

The main script currently has signature verification disabled/commented while the plugin remains in the repository, and the plugin itself contains a TODO concerning different GPG versions.

The security requirement may still be valuable, particularly in shared operational environments. The exact GPG workflow should not be inherited without a modern threat-model discussion.

### 16. Team configuration and updates — INVESTIGATE

Broadexec supported team-specific configuration, scripts and host lists, with central/team updates so an administrator could prepare shared material for other users.

The main script also contains disabled update-check behavior.

This is historical evidence of a real multi-user distribution problem. We should decide whether modern Git/package-management workflows solve enough of it that pybroadexec does not need an updater of its own.

A self-updater should be viewed skeptically.

### 17. Plugin and hook system — INVESTIGATE CAREFULLY

Broadexec has a plugin loader, dependency declarations, disabled-plugin tracking, and named hooks such as initialization and pre-script-manipulation hooks. Existing plugins include installation, host-list menus, dialog UI, report cleanup, and signature verification.

The loader itself contains unfinished dependency/error handling.

This is evidence that Broadexec needed extension points. It is not proof that pybroadexec needs a general plugin framework in v1.

First identify stable extension boundaries. Only then decide whether plugins are warranted.

### 18. Automated self-testing framework — KEEP THE INTENT

Broadexec acquired its own scenario-testing library because combinations of arguments, settings and execution behavior became too complex for manual regression checking. Release notes describe a second generation capable of automatically checking outputs for consistency.

This is a strong historical warning: pybroadexec needs tests from the beginning, especially for concurrency, filtering, output classification, cancellation, and CLI combinations.

We should use normal modern Python testing rather than recreate a custom framework unless there is a specific need for end-to-end scenario fixtures.

### 19. External progress/status interface — FINISH IDEA, REDESIGN MECHANISM

The 0.9 development notes and `stats_lib.sh` reveal a particularly interesting unfinished feature: another script could invoke Broadexec, receive a stats-file location, and observe state/progress while the execution ran.

The pilot use case checked whether a proposed UID was unique across all hosts before creating a user.

This is **not monitoring**. It is programmatic observation of a currently running execution.

The old shared-file polling mechanism should not dictate the Python design, but this is a potentially valuable capability:
- stable machine-readable progress;
- embedding pybroadexec in a larger shell/program workflow;
- separation of execution output from execution lifecycle events.

This deserves a dedicated design issue.

### 20. Admin mode — LIKELY SPLIT/DROP, INVESTIGATE FIRST

Historical admin functions included distributing SSH keys, filling known_hosts/SSH configuration, checking connectivity, checking/fixing password expiration, and removing keys for unused accounts.

The main entry point currently says admin functions are temporarily disabled, and the release notes list limitations.

Much of this feels adjacent to fleet execution rather than intrinsic to it. Before carrying any of it forward, determine which pieces are prerequisites/setup helpers and which belong to other tools entirely.

### 21. Dialog GUI — DROP unless evidence says otherwise

A dialog-based UI plugin exists and depends on the host-list menu plugin.

For the new explicitly CLI-only project, this looks like historical baggage unless "CLI-only" is later interpreted to include terminal dialog interfaces. Plain terminal UX should be preferred.

## Explicit unfinished / abandoned evidence found so far

The first pass found these notable archaeological markers:

- update checking disabled with a TODO to make it work;
- admin functions temporarily disabled;
- signature-verification plugin present but disabled in the main path;
- test-library verification/loading TODOs;
- interactive host-filter menu disabled because complicated inventories made it buggy and it needed a total rework;
- placeholder SSH temporary-file cleanup;
- several cleanup/process-kill placement FIXMEs;
- OS-release library inclusion marked FIXME;
- insecure/fragile line-loading code marked for improvement;
- plugin dependency failures missing proper warnings/exits;
- old lockfile-related compatibility cleanup;
- an unfinished replacement script-execution mechanism;
- config template containing old variables questioned for deletion;
- OS/hardware detection expansion ideas;
- documentation sections for admin functions and script signing left TODO;
- questions framework documentation marked FIXME;
- OS-release documentation marked FIXME;
- 0.9 getopts library marked WIP;
- 0.9 external progress/stats feature marked WIP.

These should not all become pybroadexec features. They are leads for investigation.

## What looks genuinely distinctive after the first pass

The combination worth studying is not merely:

`parallel SSH + host list`

It is closer to:

`reusable operation + rich target selection + local preparation + declared preconditions + concurrent execution + per-target lifecycle + fleet-scale output analysis + durable execution report`

while remaining ephemeral and agentless.

That is a much more interesting product boundary.

## Risks visible in the old design

Broadexec's history also shows how easily the project can expand sideways.

The repository accumulated:
- installation/update management;
- SSH account administration;
- terminal dialog UI;
- its own test framework;
- plugin infrastructure;
- OS detection;
- file distribution;
- configuration inheritance;
- security/signing;
- progress APIs.

Many solved real problems, but together they made a Bash program into a small platform.

pybroadexec should not automatically rebuild the platform.

For each inherited feature we should ask:

> Does this make an ephemeral fleet execution safer, clearer, more reusable, or more composable?

If not, it needs a very strong reason to exist.

## Preliminary classification

| Area | Initial classification |
| --- | --- |
| Concurrent execution | KEEP |
| Per-target lifecycle/results | KEEP |
| Host inventories | KEEP |
| Multilevel filtering | KEEP / IMPROVE |
| Direct hosts | KEEP |
| Script library/discovery | KEEP / REDESIGN |
| Script metadata | INVESTIGATE DEEPLY |
| Questions | FINISH / REDESIGN |
| Local pre-execution phase | REDESIGN |
| Parameter forwarding | KEEP |
| OS compatibility guards | KEEP CONCEPT / REDESIGN |
| Output formatting/search | KEEP / IMPROVE |
| Reports | KEEP / IMPROVE |
| Structured errors | KEEP CONCEPT |
| File distribution | INVESTIGATE |
| Ctrl+C / timeout cleanup | KEEP |
| Script signatures | INVESTIGATE / REDESIGN |
| Team configs | INVESTIGATE |
| Self-update | LIKELY DROP |
| Plugins/hooks | INVESTIGATE |
| Custom test framework | DROP implementation; KEEP testing goal |
| External live progress API | FINISH IDEA / REDESIGN |
| Admin mode | LIKELY SPLIT/DROP |
| Dialog UI | DROP |

These are not decisions yet.

## Next excavation passes

The next research should be narrower and evidence-driven:

1. **Execution engine anatomy** — reconstruct exactly how concurrency, SSH, PID tracking, timeout, cancellation, stdout/stderr and cleanup interact.
2. **Targeting anatomy** — reconstruct host-file syntax, multilevel filtering, direct hosts, aliases/IP behavior, and edge cases.
3. **Operation/script contract** — inventory every magic variable, marker and hook a script can use to influence Broadexec.
4. **Output/reporting anatomy** — reconstruct what users actually see and what external consumers can parse.
5. **Unfinished 0.9 design** — inspect getopts, stats/progress, testing v2, disabled replacements and abandoned code to infer intended direction.
6. **Competitor pressure test** — only after we understand Broadexec itself, compare these concrete capabilities against clush/ClusterShell, Salt-SSH, Ansible ad-hoc and pssh.

The goal of archaeology is not nostalgia. It is to find the requirements that survived contact with real operations, then decide which still deserve to exist in pybroadexec.
