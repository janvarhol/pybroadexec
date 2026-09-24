# Broadexec targeting archaeology

Status: third excavation pass. Historical reconstruction, not final pybroadexec architecture.

Target selection is one of the parts of old Broadexec that looks deceptively simple until the requirements are reconstructed. It was not merely "read hosts from a file." It had grown into a compact inventory/query system built from positional columns, multiple host-list sources, direct targets, exclusions, aliases, interactive discovery and repeatable CLI filters.

This document records the useful semantics and the skeletons.

## Historical targeting inputs

Broadexec can obtain targets from several paths:

- one selected host-list file (`-h` / `--hostslist`, also historical aliases);
- direct hosts supplied with `-H` / `--hosts`, repeatable;
- a configured default host list;
- an interactive host-list selection when no explicit list is supplied;
- team-specific and custom host-list locations.

Supplying a host list and direct hosts together is explicitly rejected.

That is a good rule to preserve initially: one targeting source per invocation keeps the final target set explainable.

## Host-list rows are more than hosts

The first field is the execution target. Additional fields form ordered filter dimensions.

The manual's simple example:

```
host1 prod
host2 test pilot
host3 test
host4 prod
```

Here:
- column 1 = target;
- column 2 = first filter level;
- column 3 = second filter level.

`-f prod` selects host1 + host4.

`-f test.pilot` selects host2.

The crucial point is that the dot is not an arbitrary tag expression. It represents **positional hierarchy**.

A filter `A.B.C` means:

> field 2 must equal A AND field 3 must equal B AND field 4 must equal C.

This explains why the interactive selector was such a nightmare: valid choices at the next level depend on choices made at every previous level.

## Multiple filters are OR branches

Broadexec also supports repeated `-f` arguments.

The documented example:

```
host1 customer1 prod
host2 customer1 test pilot
host3 customer1 test
host4 customer2 test
host5 customer2 test pilot
host6 customer2 prod
```

with:

```
-f customer1.test.pilot -f customer2.test.pilot
```

selects host2 + host5.

Semantically:

```
(customer1 AND test AND pilot)
OR
(customer2 AND test AND pilot)
```

where each AND term applies to the next positional column.

This is much more useful than treating all metadata as unordered tags. It can represent a hierarchy such as:

```
customer -> environment -> ring
```

or:

```
region -> role -> deployment-wave
```

without requiring a separate inventory language.

## The hidden elegance: host files are tiny schemas

The old format has no headers, but its semantics amount to:

```
target dimension_1 dimension_2 dimension_3 ...
```

The meaning of dimensions is organizational convention rather than file metadata.

That is both the power and weakness of the design.

Power:
- trivial to type;
- trivial to inspect;
- grep/awk friendly;
- no parser dependency;
- unlimited depth;
- very fast to teach when a team already knows what each column means.

Weakness:
- a file cannot explain what column 3 means;
- changing hierarchy requires coordination;
- values cannot naturally contain whitespace;
- typo detection is limited;
- two host lists may use incompatible implicit schemas.

For pybroadexec we should preserve the ability to use **ridiculously simple inventory files**. We should not force YAML onto somebody who only wants ten hostnames.

But richer inventory may eventually deserve optional named dimensions.

## Filters are exact by level, but host lookup is less exact

Filter values are compared against specific columns. However, the implementation repeatedly locates a host's source row with grep and often takes the first match.

That creates ambiguity when one target appears multiple times with different metadata.

The normal target list is deduplicated before filtering, so duplicate execution is intentionally prevented. There is even a self-test asserting that a duplicated hostname executes once.

This historical behavior gives us two useful requirements:

1. final execution targets are unique;
2. duplicate inventory definitions need explicit semantics.

The first should survive. The second should be redesigned rather than inheriting "grep first matching row."

pybroadexec should probably reject conflicting duplicate definitions or define a documented merge rule.

## Deduplication is deliberate behavior

Before filtering, Broadexec constructs its execution target list from non-comment lines, sorts it, applies `uniq`, and takes field one.

After combining multiple filter branches, it sorts uniquely again.

So repeated targets — whether accidentally duplicated or selected by multiple OR branches — execute once.

This is worth preserving.

A target set should have set semantics even if multiple selectors matched it. We may still want to retain *why* a target matched for diagnostics.

## Empty filter results are errors

If filtering produces zero targets, Broadexec stops with error 110 rather than quietly performing a successful no-op.

This is a safety feature disguised as input validation.

For fleet execution, this is sensible:

> "I intended to patch prod and my filter matched nothing" should not look like success.

That should probably remain the default, perhaps with an explicit future opt-in if no-op targeting is useful for automation.

## Interactive filtering is contextual

The interactive selector does not simply list every value from each column.

It progressively narrows the temporary inventory based on previous selections and derives the valid values for the next column from the remaining rows.

So for:

```
customer1 prod
customer1 test pilot
customer2 prod blue
customer2 test green
```

after selecting `customer1`, the next menu should contain only values that exist beneath customer1. Selecting a value then constrains the next level again.

Choosing `ALL` terminates deeper traversal.

This behavior is genuinely useful and worth preserving conceptually.

The Bash implementation, however, is exactly the sort of code archaeology was invented to prevent us from translating. :D

## Interactive selection produces a reusable CLI expression

After menu-based filter selection, Broadexec prints enough information for the user to repeat the same run non-interactively, including the resulting `-f` expression.

This is an excellent old UX principle:

> Interactive discovery should teach the deterministic CLI form.

We should keep this.

A user can explore interactively once and then paste the resulting command into documentation, cron, a wrapper or their shell history.

## Batch mode refuses ambiguity

Batch mode cannot fall back to menus.

If Broadexec needs an interactive host-list or filter selection while in batch mode, it errors.

That is exactly right.

Non-interactive mode should never guess which targets the operator meant.

pybroadexec should make target resolution deterministic before execution begins and should be able to show the resolved target set without executing anything.

A future `--dry-run` / `targets` / `plan` concept is worth discussing later, not implementing yet.

## Direct hosts are intentionally lightweight

Repeated `-H` values are collected and converted into a temporary host list. This lets the rest of the program treat direct targets similarly to inventory-derived targets.

This is a nice architectural idea hidden behind temporary files:

> Different input methods should normalize into one target representation before execution.

That should absolutely survive.

In Python the normalized representation should be an in-memory target/inventory model rather than a generated temp file.

## Exclusions happen before final filtering

`--exclude` accepts one or more hosts. Broadexec creates a temporary copy of the selected host list and removes matching entries before later target/filter processing.

The intent is useful:

```
all prod web servers
MINUS
host-that-is-under-maintenance
```

Exclusions are operationally important because they let a broad reusable selector be safely narrowed for one run.

In pybroadexec the ordering must be explicit and documented. A candidate model is:

```
source -> include/query -> exclude -> unique final target set
```

but we should decide this deliberately rather than blindly reproduce the historical call order.

## Host identity and connection identity are mixed together

Old Broadexec permits target strings carrying connection information such as:

```
user@hostname
hostname:port
user@hostname:port
```

The execution code extracts user and port from the target string.

It also builds a temporary hosts mapping file and can use a mapping from hostname to another connection address/IP. The shipped templates show both:

```
host_1
host_2
```

and:

```
192.168.1.1 host_3
192.168.1.2 host_4
```

This exposes an important distinction that the Bash representation blurs:

- **target identity**: the machine name Broadexec/user thinks about;
- **connection endpoint**: what SSH connects to;
- **SSH user**;
- **SSH port**;
- potentially SSH config alias.

pybroadexec should model those separately.

For example, conceptually:

```
Target
  name
  endpoint
  user
  port
  metadata
```

Again, this is a conceptual data model, not an implementation decision.

## There is a suspicious hosts-file skeleton

The old `check_and_generate_hosts_file` implementation currently checks the same default `hosts` path twice and appends it twice.

Given the surrounding comments and team/custom configuration concepts, this looks like either a regression, unfinished refactor or lost team-hosts branch.

We should **not** infer intended semantics from the duplicated code alone.

This is exactly the kind of skeleton archaeology should mark for historical investigation rather than preserve.

Before defining pybroadexec inventory precedence, we should inspect older commits/tags/branches if available and see whether this function previously combined team and default host mappings differently.

## Port parsing contains another skeleton

The historical parser recognizes one colon in a target as a port separator and turns it into OpenSSH `-p` syntax.

This predates normal concern for IPv6 literals. A raw colon-count parser cannot safely distinguish IPv6 addresses from `host:port`.

pybroadexec must use an unambiguous target syntax/parser and support IPv6 deliberately.

Also, the old SCP port construction appears suspicious: it builds the SSH port option first and then embeds that expanded value while constructing the SCP option. This deserves a bug note, not preservation.

## Alias resolution is pragmatic but fuzzy

For SSH execution, Broadexec may replace a selected hostname with an address found in its hosts mapping.

Known-host preflight separately attempts resolution through:
- Broadexec host mappings;
- OpenSSH config;
- system resolver.

This shows that integration with the user's existing OpenSSH world was important.

That argues strongly for pybroadexec respecting OpenSSH configuration rather than inventing an isolated SSH universe.

However, endpoint resolution should be one well-defined subsystem, not slightly different logic in targeting, known-host repair and execution.

## Sorting changes inventory order

Because Broadexec frequently uses `sort | uniq`, final execution order is not necessarily source-file order.

With eager concurrency this mattered less, and completion output was already nondeterministic.

For pybroadexec, we should decide whether target resolution preserves inventory order while deduplicating. Stable first-seen order is probably friendlier for planning/debug output, even if completion-streaming remains naturally asynchronous.

This is a design choice, not yet a decision.

## Host-list selection has team/custom concepts

The host-list menu gathers lists from:
- a team-specific subdirectory when configured;
- the general hosts directory;
- excluding the special `hosts` mapping file.

This indicates two different historical concepts:

1. **inventory lists** — sets of targets to operate on;
2. **host mappings** — connection/address aliases.

Those should remain distinct in the new design.

The old filesystem layout also attempted to support shared/team configuration without requiring every list to be global.

We should revisit the exact team/workspace model later when configuration archaeology is done.

## One host list per run was intentional

Broadexec rejects more than one selected host-list file.

At the same time, multiple filter branches can combine subsets within that list.

That suggests an old preference for keeping the source inventory understandable:

> choose one universe, then express subsets of that universe.

This is probably why complex filtering became so valuable.

We should not automatically replace this with arbitrary inventory unions. Multiple inventory sources might be useful, but they complicate provenance, duplicate resolution and metadata conflicts.

## A modern interpretation without becoming Ansible inventory

We can preserve Broadexec's simplicity while making targeting safer.

The conceptual pipeline could be:

```
Inventory source
      |
      v
parse + validate
      |
      v
normalized Target records
      |
      v
selector/filter expression
      |
      v
exclusions
      |
      v
deduplicate / conflict check
      |
      v
resolved execution target set
```

The important boundary is that pybroadexec is selecting **where this finite operation runs**. It is not maintaining an authoritative model of infrastructure.

No dynamic desired state, host facts database or long-lived inventory service is implied.

## What should be preserved

### KEEP

- plain host-list files as a first-class simple input;
- direct one-off hosts;
- unique final targets;
- exclusions;
- hierarchical positional filtering;
- multiple filter branches with OR semantics;
- unlimited practical filter depth;
- contextual interactive filter discovery;
- deterministic CLI equivalent after interactive selection;
- hard failure when a requested filter matches nothing;
- no interactive fallback in batch/non-interactive mode;
- connection user/port overrides;
- integration with OpenSSH aliases/configuration;
- distinction between inventory lists and host/address mappings.

### IMPROVE

- normalize every source into explicit Target objects;
- stable and documented duplicate handling;
- clear target identity versus connection endpoint;
- proper IPv6-aware parsing;
- explicit selector/exclusion ordering;
- inventory validation with useful error messages;
- target provenance ("why is this host in my run?");
- previewing the resolved target set before execution;
- named metadata/dimensions as an optional richer format;
- one endpoint-resolution path used everywhere.

### DO NOT PORT

- grep-first-row semantics for duplicate metadata;
- repeated whole-file scans for every host/filter level;
- positional state spread through global shell variables;
- temporary files as the filter engine;
- colon counting as endpoint parsing;
- duplicated/ambiguous hosts mapping generation;
- accidental sorting as the definition of target order.

## The feature that looked insane but should survive

The multilevel filter itself.

It is easy to look at the Bash implementation and conclude that this feature was overengineered. The opposite is true.

The **implementation** became painful because Bash is a terrible place to implement a hierarchical query engine.

The user-facing idea is compact:

```
-f customer.prod.web
-f customer2.prod.web
```

and the interactive selector can guide somebody through valid branches without requiring them to remember the hierarchy.

Python should make this feature boring internally.

That is exactly the sort of thing a rewrite is for.

## Questions to answer during design, not archaeology

- Should positional filters remain the canonical syntax or a compatibility/simple mode?
- Should richer inventories support headers/named dimensions?
- Should selector values support globbing/regex, or is exact matching a safety feature?
- Should exclusions accept the same selector language as inclusions?
- Should direct hosts be allowed together with an inventory as an explicit union?
- Should inventory order be preserved?
- How should conflicting duplicate target definitions be handled?
- How much of `~/.ssh/config` should be delegated directly to OpenSSH?
- What syntax should represent IPv6 + explicit port cleanly?

These are design decisions. Archaeology only tells us what old Broadexec meant.

## Next excavation

The next pass should reconstruct the **operation/script contract**:

- script metadata;
- supported-OS declarations;
- questions;
- parameter forwarding;
- embedded local preparation;
- included libraries;
- disabled operations;
- custom credentials;
- run-shell / sudo modes;
- signing/trust;
- what an operation is expected to print/return;
- which pieces were implemented, partial or merely planned.

That area may tell us whether "script" is still the right word for pybroadexec's core executable unit — or whether the modern concept is an **operation backed by a script**.

Still no implementation.
