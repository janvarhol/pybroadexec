# pybroadexec project philosophy and scope

## Purpose

pybroadexec is intended to be a CLI-only tool for **ephemeral, concurrent execution across many remote Linux hosts over SSH**.

The original Broadexec project is not merely code to translate. It is a source of proven behavior, ideas, unfinished work, and operational lessons. pybroadexec should be designed from the ground up in Python while preserving the useful identity and capabilities of Broadexec.

This document defines the project's boundary before implementation begins. Its purpose is to stop pybroadexec from slowly becoming a monitoring system, configuration-management platform, or general automation framework simply because those features are adjacent to remote execution.

A useful short description is:

> I have a script or command. I have many Linux machines. Run it concurrently, with useful safety and control, tell me precisely what happened on every target, and do not require me to deploy an agent or automation infrastructure first.

## Core identity

pybroadexec should remain:

- CLI-only.
- Agentless/clientless on target hosts.
- SSH-oriented.
- Concurrent by design.
- Focused on executing commands and scripts across many hosts.
- Capable of rich host selection and filtering.
- Explicit about execution lifecycle, failures, stdout, stderr, exit status, timeouts, and reporting.
- Extensible where extension serves fleet execution.
- Useful without requiring a persistent server, daemon, database, web UI, or control plane.

Concurrency is not an optional optimization. It is part of Broadexec's identity. Executing against hundreds of hosts should be treated as one concurrent operation with independently tracked per-target executions.

## The central boundary

> **pybroadexec knows about jobs and executions, not infrastructure state.**

During an invocation pybroadexec must know what is happening to each target. For example:

- waiting to start;
- connecting;
- executing;
- completed;
- remote process exited non-zero;
- SSH connection failed;
- authentication failed;
- timed out;
- execution was cancelled.

Those are facts about **this execution**.

pybroadexec should not turn them into persistent conclusions such as:

- host is healthy/unhealthy;
- service is degraded;
- fleet health is 93%;
- host needs attention;
- disk is critical;
- infrastructure is compliant/non-compliant.

A user-provided script may make those judgments and print them. pybroadexec may faithfully capture, group, filter, report, or return that output. It should not invent infrastructure semantics of its own.

For example, this is within scope:

```text
Targets: 247

SUCCESS             238
REMOTE EXIT != 0      6
SSH FAILED             2
TIMEOUT                1
```

This is not a direction for pybroadexec itself:

```text
Fleet health: 96%
WARNING: web17 is unhealthy
CRITICAL: web22 requires attention
```

unless those messages are simply output produced by the executed script.

## Why this is not "Nagios with bad SSH"

Monitoring systems answer questions over time:

- Is this host or service healthy?
- Has a metric crossed a threshold?
- Has something changed since the previous check?
- Should somebody be alerted?
- What has the state of this system been for the last week?

pybroadexec answers a different question:

> What happened when I executed this operation against these targets?

A monitoring system normally requires concepts such as persistent checks, schedules, state transitions, metrics/history, alerting, acknowledgement, health models, and long-running infrastructure.

pybroadexec needs none of them.

It may execute a monitoring script, just as it may execute a patch script, diagnostic script, grep, package query, filesystem check, or arbitrary shell command. The meaning belongs to the script/operator. pybroadexec owns the execution.

Therefore pybroadexec should not grow:

- a persistent monitoring daemon;
- a time-series database;
- host/service health state;
- alert rules;
- alert delivery;
- monitoring dashboards;
- check scheduling intended to turn it into a monitoring platform.

Historical execution reports may be useful, but storing an execution record is not the same thing as maintaining infrastructure health state.

## Why this is not a wannabe Salt or Ansible

Configuration-management and orchestration systems solve a broader problem. They commonly model desired state, resources, reusable automation units, dependency relationships, convergence, idempotency, configuration inventories, and long-lived automation workflows.

pybroadexec should not attempt to become a smaller Salt or Ansible.

Its fundamental abstraction is intentionally simpler:

> **Select targets -> execute something concurrently -> collect exact results -> report them -> exit.**

The executed script can be sophisticated. It can even be idempotent or perform configuration management. But pybroadexec itself does not need a desired-state language or its own universe of package/service/file/user modules.

This distinction is important. If pybroadexec gains playbooks, roles, resources, desired-state declarations, convergence logic, and hundreds of built-in infrastructure modules, we have stopped improving Broadexec and started rebuilding existing configuration-management software.

The absence of those concepts is a feature, not an unfinished part of the product.

## Ephemeral by design

A normal pybroadexec invocation should have a clear lifecycle:

1. Start.
2. Load configuration and target definitions.
3. Resolve and filter targets.
4. Prepare and validate the requested operation.
5. Execute concurrently.
6. Capture per-target results.
7. Present and/or save useful execution reports.
8. Clean up.
9. Exit.

Nothing needs to remain running afterwards.

This gives the project a strong architectural test:

> **If a proposed feature requires pybroadexec to remain alive after the requested execution has finished, it deserves special scrutiny.**

That does not automatically make the feature wrong, but it may indicate that pybroadexec is drifting into another product category.

## What should survive from Broadexec

The original Broadexec repository should be investigated systematically before pybroadexec's implementation architecture is finalized.

We should preserve **capabilities and useful behavior**, not necessarily Bash implementation structures.

Areas to investigate include:

- concurrent SSH execution;
- host lists and multilevel filtering;
- script discovery and selection;
- script metadata and embedded options/questions;
- script validation and verification;
- stdout/stderr processing;
- per-host execution status;
- result grouping;
- reporting;
- access/execution logging;
- file-copy behavior;
- custom users/authentication behavior;
- plugins and hooks;
- test scenarios and test libraries;
- team configuration;
- safety checks;
- interruption, timeout, cleanup, and failure behavior.

The original repository should also be searched for TODOs, disabled code, commented-out functionality, incomplete plugins, documentation promises, and partially implemented ideas. Unfinished work should be treated as design evidence rather than automatically discarded.

Each discovered capability can later be classified as:

- **KEEP** — preserve its behavior.
- **IMPROVE** — preserve the purpose but redesign the experience or implementation.
- **FINISH** — an unfinished Broadexec idea worth completing.
- **REDESIGN** — the requirement remains useful but the old model no longer makes sense.
- **DROP** — historical behavior that no longer belongs in pybroadexec.

## Competitors are design constraints, not enemies

pybroadexec exists in a world with mature tools including Salt/Salt-SSH, Ansible, ClusterShell/clush, pssh and others.

We should not recreate functionality merely because Broadexec once contained it.

Before major implementation work, Broadexec's behavior should be compared with existing tools. For every major proposed feature we should ask:

1. Does an established tool already solve this well?
2. If yes, why would somebody reasonably prefer pybroadexec for this task?
3. Is our version materially simpler, safer, faster to deploy, or better suited to ephemeral fleet execution?
4. Does the feature strengthen pybroadexec's core identity, or merely make the feature list longer?

If an existing tool completely solves the intended problem with comparable simplicity, that is evidence against spending development effort reproducing it.

The goal is not to prove that pybroadexec must exist. The design process should be allowed to discover that some old Broadexec ideas are no longer worth rebuilding.

## Deliberate non-goals

Unless future evidence gives us a compelling reason to reconsider, pybroadexec is **not** intended to become:

- a monitoring platform;
- a configuration-management system;
- a desired-state engine;
- a replacement for Salt or Ansible;
- a persistent agent/controller architecture;
- a web application;
- a fleet dashboard;
- a metrics platform;
- an alert manager;
- an infrastructure database;
- a workflow/orchestration language;
- a scheduler whose purpose is continuous infrastructure management.

It may interoperate with such systems and execute scripts used by them. It simply does not need to become them.

## Design questions still intentionally open

This document defines boundaries, not the final architecture. Important questions remain open and should be investigated rather than guessed:

- What should the modern concurrency model be?
- What SSH implementation/transport strategy gives the best balance of performance, compatibility, security, and dependencies?
- How should inventories and multilevel filters work?
- What should constitute a "script" or execution unit?
- Which original embedded-script features remain useful?
- How much result persistence is useful without becoming monitoring?
- How should identical/similar output from large fleets be grouped?
- What reporting formats should exist for humans and automation?
- How should authentication and privilege escalation be handled?
- Is a plugin system still justified, and where should extension boundaries exist?
- Which unfinished Broadexec features deserve completion?
- Where exactly does pybroadexec offer something preferable to clush, Salt-SSH, Ansible ad-hoc execution, and pssh?

These should become research/design issues before implementation issues.

## Development principle

For the initial project phase:

> **Do not port code yet. Understand the old system, define the new system, and document the decisions.**

The Bash Broadexec repository is reference material. pybroadexec is the new project.

Implementation should begin only after we have enough documented understanding to explain:

- what pybroadexec is;
- what it deliberately is not;
- which Broadexec behaviors matter;
- which unfinished ideas matter;
- how its scope differs from established alternatives;
- and what the first coherent version should contain.

This document should evolve as those decisions are made, but the core distinction should remain difficult to erode accidentally:

> **pybroadexec executes work across fleets. It does not own the fleet.**
