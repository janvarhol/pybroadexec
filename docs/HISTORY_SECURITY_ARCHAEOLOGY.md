# Broadexec history archaeology: security, abandoned paths, and why history matters

Status: historical side-tunnel. This document deliberately reads commit/branch history rather than treating current `master` as the whole design.

## Why this pass exists

The original Broadexec repository contains ideas that were:
- implemented and later disabled;
- moved into plugins;
- rewritten after portability problems;
- abandoned because Bash made them too complex;
- left on development branches;
- merged with comments explaining the operational reason.

Therefore:

> Current master is evidence, not the complete specification.

For future archaeology, branch names, commit messages, removed code and abandoned experiments are first-class evidence. A feature being absent from current master does **not** automatically mean the idea was rejected.

Likewise, a feature still present does not automatically mean it was considered good.

## Historical branches still visible

The repository still exposes branches including:

- `0.1beta`
- `development`
- `development_fix_grep_bugs`
- `development_move_error_output_to_STDERR`
- `development_proxy_broadexec`
- `dev_fix_os_release_library`
- `dev_disown_sush_fix`
- `dev_add_logname_to_reportname`

Even this surviving branch list tells us where pain accumulated: output/error semantics, proxying, OS detection, Ctrl+C/process ownership and reporting/audit identity.

Deleted/merged branches also remain visible through commit history and merge messages, including work around questions, GPG verification, secured sudo, host-list layout, plugins and performance.

## Secured sudo was a deliberate operational solution

Commit `866a742` ("Added secure sudo") introduced `BRDEXEC_SECURED_SUDO_SCRIPT`, a dedicated execution path and a helper template.

Its own comment explains the use case:

> run scripts in case sudo is allowed only to run one command/script

The configuration documentation says the helper should be installed on every intended host and allowed passwordlessly through sudo. Broadexec then creates a unique temporary directory, places exactly one script and parameter file there, changes into it, and invokes the fixed sudo-approved helper.

This confirms the earlier hypothesis.

The point was **not** simply another spelling of `sudo sh`.

The goal was:

> Give the Broadexec SSH user permission to invoke one fixed privileged gateway, while allowing that gateway to execute the staged operation.

That is a real security/delegation requirement.

## But secured sudo was not actually a strong authorization boundary

The helper checks that the working directory contains one `*.sh` file, then executes that file as privileged shell code.

If the unprivileged Broadexec user controls the staged script, the fixed sudo command effectively grants privilege to arbitrary script content.

So the mechanism restricts the *sudo command name*, but by itself does not restrict the *privileged action*.

This is a critical distinction.

The historical design becomes much more meaningful when paired with script trust/signature verification. If only an approved, integrity-verified script can reach the privileged helper, the architecture could become a controlled privileged-operation gateway.

But the current helper alone does not establish that property.

Therefore:

- preserve the **delegated privileged operation** requirement;
- do not describe the old secured-sudo helper as cryptographically/authorization secure;
- do not port it;
- investigate whether signing was intended to close this gap.

## Signing history confirms trust was not decorative

The GPG feature had a long and somewhat painful life.

History includes:
- moving script verification into a plugin (`57e9500`);
- later explicitly re-enabling GPG verification (`7e4bca5`);
- a dedicated `development_enable_gpg_again` branch;
- fixes for GPG verification/error handling;
- commit `3ca6f7e` adding a trusted-key allowlist and stating: **"Scripts will be run only when signed with key in etc/hush"**;
- later portability work to support enterprise Linux and Debian GPG variants.

That wording matters.

The intended policy was stronger than "show a warning if signature verification is available." At least in that stage of development, managed scripts were intended to be executable only when signed by an explicitly trusted key.

## The trust mechanism evolved

Earlier verification logic mixed:
- a custom script hash marker;
- a local `etc/hush` list;
- GPG fallback.

Later work simplified toward detached GPG signatures plus an explicit allowed signing-key list.

This is exactly the sort of evolution that current-source-only archaeology misses.

The abandoned custom-hash path should not be resurrected merely because code remains commented out. History suggests it was superseded by a clearer signer-trust model.

## GPG pain was implementation/portability pain, not necessarily rejection of trust

Commit history repeatedly mentions different GPG versions and distro compatibility.

The plugin itself carried a TODO about GPG-version differences. Later commits specifically fixed verification across enterprise Linux and Debian environments.

So we should distinguish:

> "GPG integration was troublesome"

from:

> "Operation trust was a bad idea."

The first has evidence. The second does not.

For pybroadexec, we should revisit the trust requirement independently of GPG.

Git provenance, signed commits/tags, package signatures, filesystem policy, detached signatures or hashes may all be candidates depending on the threat model.

Do not select one yet.

## A possible historical security architecture

The evidence now supports, but does not conclusively prove, this intended direction:

```
shared managed operation library
           |
           v
verify artifact integrity
           |
           v
verify signer is trusted
           |
           v
stage operation on target
           |
           v
invoke one sudo-approved privileged gateway
           |
           v
execute approved operation as root
```

If that was the intended combination, it is one of the most interesting abandoned Broadexec ideas.

It provides a possible answer to:

> How can many operators run a curated set of privileged fleet operations without giving every operator arbitrary root SSH access?

That problem is still valid in 2026.

But pybroadexec must not claim to solve it until a proper threat model and end-to-end authorization design exist.

## The current secured-sudo helper has concrete weaknesses

The helper:
- trusts the current working directory;
- finds files by wildcard;
- reads arguments from a plaintext file;
- reconstructs shell arguments through command substitution;
- executes whichever single `*.sh` file is present;
- does not itself verify signature/hash/ownership;
- does not bind the staged payload to a run identity cryptographically;
- does not robustly preserve argument boundaries;
- does not establish that the caller could not alter the script after any earlier verification.

These are reasons to redesign the mechanism, not reasons to discard delegated privilege as a requirement.

## TOCTOU is the elephant in the crypt

Even if Broadexec verifies a script locally, then separately transports it and invokes a privileged helper remotely, there is a potential time-of-check/time-of-use problem.

A strong future design would need to establish what exactly is trusted:
- the local source file?
- its content digest?
- the transported bytes?
- a signed immutable bundle?
- a repository revision?
- the privileged helper's own policy?

This is why "just bring GPG back" is not an architecture.

## History also validates the 'keep it dumb' design philosophy

Commit `7e644ae` changed enhanced-variable loading specifically to remove an expensive nested loop and make embedded processing much faster.

The replacement still remained shell-oriented and understandable rather than introducing a more elaborate framework.

This is consistent with the project's development philosophy: solve real operational pain, but keep machinery approachable.

For pybroadexec:

> Prefer explicit boring data structures and small functions over clever framework machinery.

Python gives us permission to make the implementation safer without making it mysterious.

## Historical evidence changes our archaeology method

From this point onward, each major subsystem excavation should inspect:

1. current `master`;
2. surviving branches relevant to the subsystem;
3. commit messages containing subsystem terms;
4. merge commits revealing old/deleted branch names;
5. commits that removed or disabled functionality;
6. comments/FIXMEs/TODOs around the change;
7. templates/manual/release notes from the same period when available.

We should explicitly record **why** something disappeared whenever history provides evidence.

Possible classifications become:

- abandoned because requirement disappeared;
- abandoned because implementation became too complex;
- superseded by a better design;
- disabled because of portability;
- incomplete/WIP;
- regression/lost during refactor;
- unclear — do not guess.

## Notes from the project's original author for future discovery

The author specifically remembers that commit/branch history contains useful abandoned ideas, including cases where work was stopped because of complexity or a dead end and explanations were recorded for why it should not be done.

Therefore future archaeology must not stop at current source.

When a strange FIXME, disabled feature or suspicious current implementation appears, history lookup is now the default response.

This note is part of the archaeology record so the instruction survives future sessions.

## Immediate consequences for pybroadexec

### Preserve as requirements to investigate

- curated reusable operations;
- artifact provenance/integrity;
- ability to define trusted operation sources;
- delegated privilege distinct from SSH login identity;
- possible policy that only trusted operations may use elevated execution;
- clear audit identity for who ran what against which targets.

### Do not preserve as mechanisms

- `etc/hush` as designed;
- parsing GPG human output;
- global key import side effects;
- fixed sudo helper executing arbitrary staged shell;
- plaintext parameter file semantics;
- local verification with no strong binding to remote bytes;
- wildcard selection of privileged payloads.

## Security principle emerging from history

A useful principle to carry into design:

> **Privilege should attach to an explicitly trusted operation, not merely to the fact that pybroadexec can SSH to the target.**

This is substantially better than treating `sudo` as a string prepended to every remote command.

It is not yet a feature commitment. It is a design requirement worth preserving for the security/threat-model phase.

## What this changes about the remaining excavation

We should now use history aggressively for:
- output/error/reporting;
- plugins/config/team architecture;
- testing;
- the abandoned 0.9 progress/getopts work;
- targeting/filter branches where complexity caused features to be disabled.

We should also revisit earlier archaeology findings when history exposes missing intent.

Archaeology is no longer "read old master."

It is:

> **reconstruct the decisions.**

That is a much better foundation for pybroadexec.
