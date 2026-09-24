# Broadexec operation / script-contract archaeology

Status: fourth excavation pass. Historical reconstruction, not final pybroadexec design.

This pass asks a deceptively important question:

> What was the thing Broadexec executed?

At first glance the answer is "a shell script." The repository says otherwise. A Broadexec script could declare inputs, alter execution settings, declare compatibility requirements, run a local preparation phase, select credentials, disable itself, consume injected helper libraries, and participate in trust/signature checks.

So the historical executable was already halfway between a plain script and a small operation definition.

That distinction matters for pybroadexec.

## Three historical classes of executable

The old repository effectively supports three levels.

### 1. Plain script

A normal shell script can be sent and run without Broadexec-specific metadata.

This is essential to preserve.

A user should never need to package a five-line diagnostic script into a framework-specific object merely to run it across hosts.

### 2. Enhanced script

A script may contain recognized `BRDEXEC_...` declarations that alter Broadexec behavior.

The temporary-script builder scans declarations at the start of lines and only sources names present in `etc/enhanced_script_supported_variables.db`.

The allowlist includes settings such as:

- human-readable output;
- batch mode;
- run shell;
- output hostname delimiter;
- SSH connection timeout;
- script run timeout;
- human-readable report mode.

This means an operation could carry some of its execution policy with it.

### 3. Script with local preparation / metadata contract

Questions, embedded local code, OS compatibility declarations, custom credentials, disable markers and signatures extend the script further.

At that point the file is not merely remote shell code. It is simultaneously:

- remote executable payload;
- Broadexec metadata source;
- local input specification;
- local preparation program;
- execution-policy override source;
- compatibility declaration;
- potentially a signed operational artifact.

That is powerful, but the concerns are badly entangled.

## Plain scripts must remain first-class

The strongest requirement from this excavation is simplicity.

pybroadexec should support:

```
pybroadexec ... ./check_something.sh
```

without requiring a manifest.

The richer operation concept should be optional.

If pybroadexec requires metadata files, schemas or packaging for every execution, we will have lost one of Broadexec's most useful characteristics.

## Broadexec tells the payload that it owns the run

When preparing the temporary remote script, Broadexec inserts:

```sh
BRDEXEC_RUNBY=true
```

immediately after the shebang.

That lets a script change behavior depending on whether it is run directly or through Broadexec.

This is a useful contract.

A modern version might expose execution context through environment variables rather than rewriting the user's source, for example conceptually:

```
PYBROADEXEC_RUN=1
PYBROADEXEC_RUN_ID=...
PYBROADEXEC_TARGET=...
```

Names are not being decided here.

The important requirement is:

> An operation may know that it is being executed under pybroadexec and receive a small, documented execution context.

## Enhanced-script settings are operation-local policy

The allowlisted embedded settings show that the old design already distinguished global configuration from per-operation requirements.

A script could effectively say things such as:

- this operation needs a longer timeout;
- this operation expects a particular run shell;
- display/report this operation in a particular form.

This is valuable, but policy precedence needs redesign.

A future system must answer clearly:

```
built-in default
< config
< operation metadata
< explicit CLI override
```

or whatever precedence we deliberately choose.

The old system accumulated precedence organically.

pybroadexec should document it before implementation.

## Questions framework: operations declare their inputs

A script can declare lines such as:

```sh
BRDEXEC_SCRIPT_QUESTION_r_u="Enter user name to be unlocked:"
```

Broadexec interprets the declaration, asks the question locally, and converts the answer into remote script arguments:

```
-u <answer>
```

Three input modes are documented:

- `r` — required;
- `o` — optional;
- `b` — boolean/flag-like parameter without a value.

Broadexec then shows the resulting command and asks for confirmation.

This is a genuinely useful abstraction:

> A reusable fleet operation can declare the input it requires without implementing its own interactive prompting on every remote host.

The old encoding inside Bash variable names is not worth preserving.

The requirement is.

## Questions and non-interactive execution were intentionally compatible

The same operation can receive arguments via repeated `-p` parameters instead of prompting.

This matters because Broadexec was intended to be callable by another program/script.

So an operation's input model had two front ends:

```
human -> prompt -> validated operation arguments
automation -> explicit CLI arguments -> same operation arguments
```

That is excellent design intent.

pybroadexec should never create an operation system that only works interactively.

Every interactive input should have a deterministic non-interactive representation.

This mirrors the targeting principle already found:

> Interactive discovery should teach deterministic CLI usage.

## The question parser contains unfinished bones

The implementation contains evidence of intended default values, but the parsing logic appears inconsistent: it checks one underscore-field count and then separately contains a branch for another count that does not line up cleanly with the preceding validation.

The manual labels the questions framework `FIXME`.

This should be classified **FINISH THE IDEA / REDESIGN THE FORMAT**, not port.

A modern input definition could eventually support:

- required/optional;
- flag;
- default;
- type;
- validation;
- secret input;
- allowed values;
- help text.

But we should resist turning this into a form-builder DSL. Only add capabilities justified by real fleet operations.

## Local embedded preparation is the most interesting feature in this excavation

A script can contain:

```
###BRDEXEC_EMBEDED START
...
###BRDEXEC_EMBEDED STOP
```

The enclosed code is extracted and sourced **locally on the Broadexec machine**, once.

It can:

- prompt the operator;
- inspect local state;
- calculate arguments;
- set `BRDEXEC_EMBEDED_PARAMETERS`;
- abort the run through `BRDEXEC_EMBEDED_ERROR`.

Broadexec then constructs a remote payload with that local section removed.

This is conceptually powerful:

> One operation can have a local preparation phase followed by N concurrent remote execution phases.

That is much more interesting than arbitrary "embedded Bash."

Examples of legitimate preparation:
- validate an operator-supplied username once;
- obtain/derive an identifier once;
- check a local prerequisite;
- calculate common parameters;
- require confirmation before touching hundreds of machines.

The old implementation literally sources code extracted from the file into Broadexec's own shell process. That is an enormous trust boundary.

The capability should survive only after a clean security/design discussion.

## A skeleton in embedded preparation

The implementation around `BRDEXEC_EMBEDED_ERROR` appears suspicious.

The example says the variable should be set to `true` to cancel execution. The function's conditional/error path appears inconsistent with that documented meaning.

This may be a bug, inversion, unfinished refactor, or historical code path that needs commit archaeology.

Do not derive pybroadexec semantics from the current conditional.

Derive them from the intended contract:

```
prepare -> either produce valid parameters or abort before fleet execution
```

## OS compatibility declarations are operation preconditions

A script can declare:

```sh
BRDEXEC_SUPPORTED_OS="sles"
BRDEXEC_SUPPORTED_OS_VERSION_MIN[sles]=10.4
BRDEXEC_SUPPORTED_OS_VERSION_MAX[sles]=11.3

osrelease_check
```

Broadexec injects its OS-release helper library into the temporary remote script.

The helper detects the remote OS/version and emits `BRD_UNSUPPORTED ...` before exiting when the target falls outside the declared range.

The execution engine then recognizes that marker specially.

This is another sign that a Broadexec script was already an operation with preconditions.

The useful requirement:

> An operation may declare conditions under which it is safe/supported to execute on a target.

But this should not become persistent host-fact collection or configuration management.

The check belongs to **this execution**.

## OS compatibility is currently cooperative, not truly declarative

An important wrinkle: declaring `BRDEXEC_SUPPORTED_...` values is not sufficient.

The script must also call `osrelease_check`.

Broadexec explicitly detects declarations without the call and errors.

So the old contract is half declarative and half imperative.

A modern design should probably avoid that split. If compatibility metadata is declared, the execution engine should enforce it automatically.

That would make the safety property harder for script authors to accidentally bypass.

## The OS library itself shows why policy and payload should be separated

The old helper contains distribution-specific parsing for Oracle Linux, Red Hat, CentOS, SLES, Manjaro and `/etc/os-release`, plus FIXME ideas for hardware/platform detection.

This is where scope can explode.

If pybroadexec owns an ever-growing fact-detection library, we are walking toward configuration management.

A better principle is likely:

> pybroadexec may provide a small precondition mechanism, but should not aspire to maintain a universal infrastructure-facts database.

Exactly how compatibility checks work remains a design question.

## Disabled scripts are first-class operational governance

A file containing:

```
BRDEXEC_SCRIPT_DISABLED
```

is hidden from predefined-script discovery and cannot be selected normally.

This is simple but useful.

Operational repositories often contain:
- retired operations kept for reference;
- work in progress;
- scripts temporarily forbidden;
- scripts awaiting review.

A future operation metadata model should probably retain an enabled/disabled state with an optional reason.

A plain arbitrary script supplied explicitly from outside the managed operation library is a separate trust/use case and should be considered separately.

## Run-shell / privilege mode belongs to execution policy

Broadexec supports run-shell modes that normalize roughly into:

- plain shell;
- `sudo sh -c`;
- secured-sudo helper;
- `sudo su - -c`.

This solves a real requirement:

> Transport identity and remote execution privilege are not necessarily the same thing.

That concept should survive.

But pybroadexec should not make arbitrary shell-string templates the core privilege model.

Connection identity, remote execution identity and privilege escalation should be explicit concepts.

## Secured-sudo is evidence of a stronger security model that was emerging

The special `secured_sudo` path stages the script and parameters in a dedicated temporary directory and invokes a configured privileged helper.

That suggests the old project was trying to solve an important problem:

> Allow operators to execute approved fleet operations with elevated privilege without granting unrestricted remote root shell access.

This deserves separate archaeology before deciding whether it belongs in pybroadexec.

It may connect strongly to script signing/trust.

Potentially, the combination was intended to become:

```
trusted/signed operation
        +
restricted sudo entry point
        =
controlled privileged fleet execution
```

That is substantially more sophisticated than "sudo the script."

We should inspect the secured-sudo helper/config and history before classifying it.

## Custom username/password in the script: requirement valid, mechanism DROP

The historical enhanced script may contain:

```sh
BRDEXEC_SCRIPT_USER=admin_user
BRDEXEC_SCRIPT_PWD=secret
```

Broadexec detects these values, warns the operator, then takes a separate SCP/SSH path using password authentication and generated askpass machinery.

This must not survive as designed.

Secrets do not belong in operation source files.

However, the underlying requirement remains:

> An operation may need a different connection identity than the user's normal default.

Modern solutions may include:
- OpenSSH config aliases;
- SSH agent;
- explicit identity selection;
- secret-provider integration much later, if justified.

The operation may reference an identity/profile. It should not contain the secret.

## There is an especially ugly security asymmetry in the custom-password path

The normal execution path uses strict host-key checking.

The password path disables strict host-key checking and points SSH at `/dev/null` known-host files.

So the path that transmits a password is also the path that deliberately gives up host identity verification.

That is historical debt, not behavior to preserve.

It is exactly why archaeology precedes implementation.

## Script signing reveals a trust model, not merely a GPG feature

A signature-verification plugin expects detached `.asc` signatures, imports supplied public keys, verifies the script, extracts the signing key ID and checks it against an allowed/trusted list.

The code contains TODOs around GPG-version differences and older commented signature/hash logic.

The interesting requirement is not "pybroadexec needs GPG."

It is:

> In a shared operational environment, Broadexec wanted to distinguish an approved operation from a locally modified arbitrary file.

This becomes especially meaningful when combined with privileged execution.

Questions for future design:
- Are arbitrary local scripts allowed?
- Are managed library operations trusted differently?
- Can privilege escalation require an approved operation?
- Is trust provided by Git/repository provenance, signatures, filesystem ownership, package signatures, explicit hashes, or something else?
- Should pybroadexec itself enforce trust at all?

We need a threat-model document before selecting a mechanism.

## Script source is also configuration source — dangerous but instructive

Broadexec scans `BRDEXEC_...` lines from the temporary script, checks variable names against an allowlist, writes each allowed line to a temporary file, and **sources it into the Broadexec process**.

There is even a FIXME saying to secure the loaded line better.

The allowlist limits names, but sourcing a shell assignment is still executable shell syntax.

This is a classic example where the old requirement is good and the mechanism is not:

> Operation-local metadata should be data, not executable syntax.

pybroadexec should parse metadata as data.

This also means arbitrary operation metadata must never become implicit Python code execution.

## The script is modified before execution

Broadexec does not simply upload the original file.

It builds a temporary payload by:

1. copying the shebang;
2. injecting `BRDEXEC_RUNBY=true`;
3. injecting the OS-release helper library;
4. appending the remainder of the script, or a transformed embedded-preparation version;
5. reading selected operation settings from the result.

This means "what the user wrote" and "what the target executes" are not identical.

That complicates:
- auditing;
- signatures;
- debugging;
- reproducibility.

For pybroadexec, payload transformation should be minimized and observable.

If pybroadexec must construct a wrapper, it should ideally preserve the original operation artifact and make the wrapper behavior deterministic.

## Parameter handling needs a real argument model

Questions, `-p`, and embedded preparation all ultimately concatenate shell argument strings.

That creates quoting and injection hazards and makes whitespace awkward.

A modern design should carry arguments as structured argument arrays for as long as possible.

The old user manual itself warns that whitespace or leading `-` handling can cause incorrect forwarding.

This is implementation debt we can eliminate without changing the product philosophy.

## What does the operation promise about output?

Very little formally.

Broadexec captures stdout/stderr and recognizes a few special strings such as `BRD_UNSUPPORTED`, but otherwise the script owns its output semantics.

That is good.

pybroadexec should not require scripts to emit a framework-specific result schema merely to be useful.

Optional structured-result conventions might be interesting someday, but plain stdout/stderr + exit code must remain the universal contract.

And unlike old Broadexec, remote exit code should become first-class.

## Emerging model: script versus operation

Archaeology now suggests two concepts rather than one.

### Script

An arbitrary executable payload.

Properties:
- minimal ceremony;
- may know nothing about pybroadexec;
- receives arguments;
- produces stdout/stderr;
- returns an exit code.

### Operation

A reusable, managed execution definition whose payload may be a script.

It may additionally declare:
- name/description;
- enabled/disabled status;
- inputs;
- timeout expectations;
- privilege requirements;
- compatibility/preconditions;
- local preparation;
- trust/provenance requirements;
- output presentation hints.

This distinction would let pybroadexec remain excellent at:

```
run this script on these hosts
```

without giving up the useful richer behavior old Broadexec accumulated.

**This is not yet a naming or architecture decision.** It is the strongest conceptual finding from this excavation.

## Keep the operation definition boring

If we adopt an operation concept later, it must not become a playbook language.

An operation should describe **one finite fleet execution**.

It should not contain:
- multi-stage desired-state convergence;
- dependency graphs;
- handlers;
- roles;
- persistent scheduling;
- infrastructure resources;
- a general workflow language.

Otherwise we have recreated the configuration-management systems that PROJECT_PHILOSOPHY.md explicitly excludes.

A useful test:

> Can I explain this operation as "prepare once, execute this thing independently on selected targets, collect results"?

If yes, it probably belongs.

If it becomes "step 7 runs only after resource X converges and triggers handler Y," we have left Broadexec territory.

## Preliminary classification

| Historical capability | Initial classification |
| --- | --- |
| Arbitrary plain scripts | KEEP, fundamental |
| Broadexec execution context | KEEP / IMPROVE |
| Per-script execution settings | KEEP CONCEPT / REDESIGN |
| Questions / declared inputs | FINISH IDEA / REDESIGN |
| Non-interactive parameter forwarding | KEEP / IMPROVE |
| Local embedded preparation | KEEP CONCEPT / SECURITY REDESIGN |
| OS compatibility declarations | KEEP CONCEPT / REDESIGN |
| Injected OS helper library | DROP mechanism |
| Disabled managed scripts | KEEP |
| Run-shell / privilege modes | KEEP requirement / REDESIGN |
| Secured-sudo path | INVESTIGATE DEEPLY |
| Custom connection user | KEEP requirement |
| Password stored in script | DROP |
| Askpass/password SSH mechanism | DROP |
| Signature/trust verification | INVESTIGATE / THREAT MODEL |
| GPG implementation | undecided; do not inherit automatically |
| Sourcing metadata as shell | DROP |
| Payload rewriting | MINIMIZE / REDESIGN |
| stdout/stderr contract | KEEP |
| remote exit code | ADD as first-class result |

## Skeletons requiring history excavation

Before closing this area, Git history should be consulted for at least:

1. the apparently inverted/inconsistent `BRDEXEC_EMBEDED_ERROR` logic;
2. the questions/default-value parser;
3. the old versus plugin-based signature verification;
4. secured-sudo design and helper scripts;
5. why OS support remained partly declarative and partly an explicit `osrelease_check` call;
6. any abandoned enhanced-script metadata format.

Those may reveal intent that the current master no longer expresses clearly.

## The strongest design lesson from this pass

Broadexec's evolution was trying to solve two conflicting needs:

> **Let me run any dumb shell script immediately.**

and:

> **Let me turn important recurring scripts into safer, self-describing fleet operations.**

Those are not mutually exclusive.

pybroadexec should probably preserve both layers.

The mistake would be forcing every script to become an operation, or refusing richer operation semantics because plain scripts must stay simple.

## Next excavation

Before output/reporting, one small historical side-tunnel is now justified:

**secured-sudo + trust/signing history.**

Those features appear connected and may reveal an unfinished security architecture that is easy to misunderstand from current master.

After that, continue with:
- output/error/reporting archaeology;
- plugin/config/team archaeology;
- testing archaeology;
- then freeze archaeology and write the new design.

Still no Python implementation.
