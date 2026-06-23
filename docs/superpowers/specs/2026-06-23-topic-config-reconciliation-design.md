# Topic cleanup.policy reconciliation — design spec

**Date:** 2026-06-23
**Status:** approved (design); pending implementation
**Author:** Ryan (with Claude)
**Revision:** v4 — folds in round-2 deep-review findings plus a richer result
type. Headline (round 2): `create_topics` does **not** raise on an
already-existing topic (it returns an in-band `error_code=36`), which is both a
blocker for this feature *and* a pre-existing bug in `ensure_topic`. v3 also:
preserved broker `error_message` when raising, stripped whitespace in policy
comparison, interpolated `expected` into messages, defined the absent-entry case,
corrected the v0-branch rationale, raised `ValueError` for the inert combo,
renamed the `error` action to `raise`. v4: `EnsureTopicResult` is now a single
discriminated `outcome` enum (+ observed `policy`) instead of three correlated
booleans. v5 (round-3 polish): renamed the two near-synonym outcomes for their
cause (`unverified`→`unreadable`, `unchecked`→`skipped`); moved `start()` inside
`try/finally` to repair a second latent leak; factored the in-band-error check into
one helper across create/describe/alter; and expanded the test plan
(`start()`-forwarding, faithful-mock, no-CREATE-ACL, admin-connect-failure). v6
(round-4 DX review): **dropped** the `KTablesError` base (it would over-promise
against a codebase that raises stdlib exceptions — `TopicConfigMismatchError` now
stands alone; the absent-policy case raises `RuntimeError`); **dropped** the
`EnsureTopicResult.__post_init__` guard (the result is a plain frozen carrier like
`ViewStats`; the outcome↔policy coupling is a unit-tested invariant, since ktables
is the only constructor); accepted the `bool`→`EnsureTopicResult` return as a hard
break (no `__bool__`/`is_compacting` mitigation); and added doc requirements
(constructor-docstring rationale, teaching `ValueError` message, documented
raises-set, changelog disclosure of the new default-path describe/WARNING).

## 1. Problem

`ensure_topic()` (`ktables/kafka_table.py:81`) creates a topic with an explicit
`cleanup.policy=compact` only on the **create** path. When the topic already
exists it relies on a `TopicAlreadyExistsError` that — empirically (§12,
`proof_double_create.py`) — **a real broker never raises**: a second
`create_topics` on a fully-propagated topic returns normally with
`topic_errors = [(name, 36, 'The topic has already been created')]`. aiokafka's
`create_topics`/`_send_to_controller` return that response **without inspecting
the per-topic error code**.

Two consequences:

1. **Pre-existing bug:** today `ensure_topic` returns `True` and logs "created
   topic" for topics that already existed; its `except TopicAlreadyExistsError`
   branch is dead against a real broker (only the unit tests' fake admin, which
   manually raises, exercise it — see the comment at
   `tests/test_kafka_table.py:510`). This is repaired as part of this work.
2. **The silent footgun this feature targets:** a topic auto-created with
   `cleanup.policy=delete` is accepted as-is. ktables' last-write-wins correctness
   does not depend on compaction (README "Consistency contract" §4), but `delete`
   retention **evicts** any key not re-written within the retention window, so a
   fresh consumer materializes a table silently **missing entries** — real data
   loss for a registry/table.

The fix: determine already-exists from the **in-band** create response, then
check the policy and — per an explicit caller choice — ignore, warn, raise, or
safely reconcile it.

## 2. Goals / non-goals

**Goals**
- Correctly detect an already-existing topic from the create response (repairing
  the pre-existing bug), and detect when its `cleanup.policy` isn't compacting.
- Give the caller a single, ergonomic knob to choose the response.
- Make reconciliation *safe* — flip `cleanup.policy` without clobbering other
  operator-tuned configs.
- Backward compatible behavior: an explicit `ignore` matches the *intended*
  pre-feature behavior (create-or-accept); the default never mutates broker state
  and never converts a working startup into a hard failure.

**Non-goals (this iteration)**
- Enforcing the *full* declared `topic_configs` against an existing topic. v1
  checks/reconciles **only** `cleanup.policy`. See §11.
- A destructive "hard reset" wiping config to defaults. Rejected (strictly
  dominated by the safe reconcile). See ADR.
- Deleting/recreating the topic (destroys the materialized data).
- Preventing retention-based eviction under `compact,delete`. v1 only ensures the
  topic *compacts*; retention is the operator's concern (§5.3).

## 3. Developer surface (the ergonomics)

One new keyword-only parameter on the four constructors and the `ensure_topic()`
primitive, placed immediately after `topic_configs`:

```python
on_policy_mismatch: PolicyMismatchAction = "warn"
```

with a module-level type alias mirroring the existing `TableStatus`
(`kafka_table.py:64`):

```python
PolicyMismatchAction = Literal["ignore", "warn", "raise", "reconcile"]
"""What to do when an EXISTING topic's cleanup.policy isn't compacting:
``ignore``: do nothing (no describe call) — intended pre-feature behavior.
``warn`` (default): describe, log loudly on a confirmed mismatch, change nothing.
``raise``: raise TopicConfigMismatchError. ``reconcile``: safely flip to compact,
preserving other configs."""
```

**Four imperative-verb values.** `ignore` / `warn` / `raise` / `reconcile` each
name the action taken, on a passive→aggressive axis. A boolean can't express the
"tell me loudly but don't touch my broker" middle ground. `ignore` is a distinct,
legitimate state ("create if missing, but don't verify the existing policy") —
orthogonal to `ensure_topic=False` ("don't manage the topic at all").

**Why `on_policy_mismatch`, not `on_config_mismatch`.** v1 governs exactly one
config — `cleanup.policy`. The narrower name is honest about scope, matches the
`expected = (...).get("cleanup.policy")` reality and the codebase's precise-naming
idiom, and frees `on_config_mismatch` for the §11 "enforce all declared configs"
generalization.

**Why string literals.** Matches the `TableStatus = Literal[...]` convention; no
import needed.

**Touch points** (forward the parameter exactly as `ensure_topic`/`topic_configs`
are already forwarded):

| Symbol | File:line | Change |
| --- | --- | --- |
| `ensure_topic()` | `kafka_table.py:81` | New param; in-band create-result parsing (§4.1); runs the check; returns `EnsureTopicResult`. |
| `KafkaTable.__init__` | `kafka_table.py:188` | New param → `self._on_policy_mismatch`; validate (incl. the inert-combo `ValueError`); pass at `start()` (`:430`). |
| `KafkaTableWriter.__init__` | `kafka_table.py:648` | New param → field; same validation; pass at `start()` (`:691`). |
| `GroupedKafkaTable.__init__` | `grouped_table.py:115` | Pure pass-through. |
| `GroupedKafkaTableWriter.__init__` | `grouped_table.py:317` | Pure pass-through. |

**Validation (eager, at construction).**
- Unknown `on_policy_mismatch` string → `ValueError`.
- **Inert-combo guard:** `on_policy_mismatch in {"raise", "reconcile"}` together
  with `ensure_topic=False` → `ValueError`. A caller asking for a guarantee the
  configuration cannot deliver (ktables makes no admin calls when
  `ensure_topic=False`) fails fast, instead of a silent no-op. `ignore`/`warn` +
  `ensure_topic=False` stay legal (both mean "do nothing"). This also makes the
  precondition self-documenting and removes the last silent no-op.
- Lives in `KafkaTable`/`KafkaTableWriter.__init__` (`:203-210`); the grouped
  wrappers inherit it via the inner class.

**Exports.** Add `PolicyMismatchAction`, `EnsureTopicOutcome`,
`EnsureTopicResult`, and `TopicConfigMismatchError` to `ktables/__init__.py`
`__all__`.

## 4. Behavior

### 4.1 Determining already-exists, and where the check runs (the create-path fix)

`create_topics` returns a `CreateTopicsResponse` whose `topic_errors` is a list of
`(name, error_code[, error_message])` (arity varies by version; index by
position). `_try_create_topic` inspects it **in-band** — it does not rely on an
exception:

- `error_code == 0` → created → `True`.
- `error_code == 36` (TOPIC_ALREADY_EXISTS) → already exists → `False`.
- any other non-zero → `raise aiokafka.errors.for_code(code)(message)` (§5.2).

The per-entry `error_message` is extracted defensively
(`message = entry[2] if len(entry) > 2 else ""`) since the CreateTopics v0
`topic_errors` tuple is `(name, code)` with no message (modern brokers negotiate
v2+/v3, which carry it) — the same v0/v1 arity care §5.1 takes for describe.
Defensively also catch `TopicAlreadyExistsError` (in case a broker/version *does*
raise it) and treat it as `False`; in-band-return and raise are mutually exclusive
per call, so this never double-handles. This repairs the pre-existing bug:
"created topic" is logged only when `error_code == 0`.

The check runs **only when** `ensure_topic=True`, `on_policy_mismatch != "ignore"`,
the topic already existed (create returned `False`), and the declared policy
requires compaction (§4.2). Otherwise no `describe_configs` call is made.

**Control flow** (single admin client, check in normal flow so its exceptions
aren't chained under or misattributed by the create path). Note `start()` moves
**inside** the `try` so a failed connect still hits `finally: close()` — repairing
a latent leak in today's `ensure_topic`, where `start()` precedes the `try`:

```
admin = AIOKafkaAdminClient(...)
try:
    await admin.start()
    created = await _try_create_topic(admin, ...)       # in-band parse; False on already-exists
    if created:
        return EnsureTopicResult(outcome="created", policy=None)
    if on_policy_mismatch == "ignore" or not _requires_compaction(expected):
        return EnsureTopicResult(outcome="skipped", policy=None)
    # _check_topic_policy returns outcome ∈ {verified, reconciled, mismatch, unreadable}
    # (or raises in 'raise' mode, or on can't-verify under 'raise'/'reconcile').
    return await _check_topic_policy(admin, topic, expected, on_policy_mismatch)
finally:
    await admin.close()   # idempotent; safe on an unstarted/partially-started client
```

### 4.2 The expected (declared) policy

Derived with the **same predicate the create path uses** (`is not None`):

```python
effective = topic_configs if topic_configs is not None else DEFAULT_TOPIC_CONFIGS
expected = effective.get("cleanup.policy")            # str | None
```

| `topic_configs` | create path makes | `expected` | check fires? |
| --- | --- | --- | --- |
| `None` (default) | compact topic | `"compact"` | yes |
| `{"cleanup.policy": "compact", ...}` | compact topic | `"compact"` | yes |
| `{"cleanup.policy": "delete"}` | delete topic | `"delete"` | no |
| `{}` (empty) | delete topic (broker default) | `None` | no |
| `{"retention.ms": "…"}` (no policy) | delete topic (broker default) | `None` | no |

`_requires_compaction(expected)` ≡ `expected is not None and "compact" in
_split(expected)`.

### 4.3 What counts as a mismatch

Read the existing topic's **effective** `cleanup.policy` (§5). Compare as
**whitespace-stripped** comma-split sets — `_split(p) = {x.strip() for x in
p.split(",")}` — so `"delete, compact"` / `"delete,compact"` (reordered/spaced)
classify correctly and never false-positive:

- `compact` in the set → **satisfied** (no action). If the set also contains
  `delete`, log the §5.3 retention-eviction `INFO`.
- `compact` absent → **confirmed mismatch** → apply the action (§4.4).
- **Unverifiable** — describe raised, returned an in-band `error_code != 0`, or
  returned *no* `cleanup.policy` entry / empty resources → the "could not verify"
  path (§4.4), distinct from a confirmed mismatch.

The mismatch keys on the policy *value* (independent of `config_source`), so a
correctly-compacting topic reported via a broker default is **not** misread.

### 4.4 Action dispatch

| Mode | Confirmed mismatch | Could not verify | Mutates? |
| --- | --- | --- | --- |
| `ignore` | — (no describe) | — | no |
| `warn` (default) | `WARNING` (§4.5), continue | `INFO` "could not verify", continue | no |
| `raise` | raise `TopicConfigMismatchError` | re-raise the cause (see below) | no |
| `reconcile` | safe describe-then-merge (§5); `INFO` on success | re-raise the cause | yes (on mismatch) |

The three "could not verify" causes resolve as: **describe raised** (an exception);
**in-band `error_code != 0`** → `raise for_code(code)(message)` (§5.2), also an
exception; **absent `cleanup.policy` entry** (describe succeeded, code 0, no policy)
→ a `None` policy reading (not an exception). Under `warn`: the two raising causes
are caught (only `KafkaError`/`asyncio.TimeoutError` — never bare `Exception`,
mirroring `barrier()` at `kafka_table.py:389`) and a `None` reading is detected;
all log at `INFO` and yield `outcome="unreadable"`. Under `raise`/`reconcile`: a
raising cause propagates, and a `None` reading raises
`RuntimeError("could not read cleanup.policy for topic '…'")` (no ktables base
exception — see §6).

**Guiding principle:** `warn` is advisory — never blocks startup or mutates, and a
*can't-verify* condition is `INFO`, not `WARNING`, so a DESCRIBE-denied
locked-down cluster doesn't get recurring un-actionable startup noise (and should
prefer `ensure_topic=False` — §10). `raise`/`reconcile` carry stronger contracts
and fail startup when they cannot fulfill them.

### 4.5 Messages (DX) — interpolate `expected`, never a literal

The confirmed-mismatch `WARNING` (and the `raise`-mode exception `str()`) name the
actual values, with the remedy built from `expected` (not a hardcoded `'compact'`,
since `expected` may be `compact,delete`):

```
WARNING ktables: topic 'my-topic' already exists with cleanup.policy='delete',
which does not satisfy the required '{expected}'. Keys not re-written within the
topic's retention window will be EVICTED, so a fresh consumer may materialize a
table missing entries. Fix the topic's cleanup.policy to '{expected}', or
construct with on_policy_mismatch='reconcile' (or 'raise' to fail fast).
```

## 5. The safe reconcile algorithm (empirically proven — §12)

aiokafka exposes only the classic full-replace `alter_configs` (no
`incremental_alter_configs`): any config not in the request resets to broker
default (proven). So reconcile is describe-then-merge.

### 5.1 Read + merge

1. `describe_configs([ConfigResource(TOPIC, topic)])` — **without**
   `include_synonyms` (the merge never reads the `synonyms` field, so requesting
   it only narrows broker compatibility for no benefit).
2. Keep only **writable, explicitly-set topic overrides**: `read_only == False`
   and the value is a topic-level override. Both protocol shapes are handled
   defensively — DescribeConfigs v1+ exposes `config_source` (`TOPIC_CONFIG == 1`);
   v0 exposes an `is_default` boolean. **Note on reachability:** aiokafka
   negotiates the *highest* mutually-supported response version, so against any
   modern broker (Kafka, Redpanda v25) the v1+ (`config_source`) shape is always
   used; the v0 `is_default` branch is **unreachable via the integration broker**
   and is covered solely by a synthetic-tuple **unit** test (§9). Dropping
   `include_synonyms` does *not* change this (version negotiation does) — it is
   dropped only because synonyms are unused.
3. Merge `cleanup.policy = expected` (the declared policy) over that set.
4. Submit the merged set via `alter_configs`.

### 5.2 Inspect results — in-band error codes (the C1 fix)

**Broker rejection is reported IN-BAND, not by raising** — empirically confirmed
(§12, `proof_inband_errors.py`): an invalid alter returned `error_code=40`, a
missing-topic describe/alter returned `error_code=3`, none raised. So every
per-resource `error_code` of both responses must be inspected:

```python
for resource in flatten(responses):          # each call returns list[Response]; each has .resources
    code, message = resource[0], resource[1]  # error_code, error_message
    if code != 0:
        logger.debug("broker error_code=%d on %s: %s", code, topic, message)
        raise aiokafka.errors.for_code(code)(message or "")
```

- `for_code(code)` returns the exception **class** for the wire code (verified:
  `40 → InvalidConfigurationError`, `29 → TopicAuthorizationFailedError`,
  `3 → UnknownTopicOrPartitionError`; unmapped → `UnknownError`, never `None`).
- Pass the broker's `error_message` (resource index 1) so the raised exception is
  actionable — without it, `str()` is a bare `[Error 29]`, losing the broker's
  reason. Log the raw `code` too, in case it's unmapped.
- Guard on `code != 0` — `for_code(0)` is `NoError` and would itself raise.
- `reconcile` logs `INFO` success **only after** confirming `error_code == 0` on
  the alter — never before (else a denied reconcile reports false success).
- Factor this into one helper (e.g. `_raise_on_inband_error(responses, topic)`)
  reused by the describe read, the alter, **and** the create-result parse (§4.1) —
  all three response shapes put `error_code`/`error_message` at indices 0/1, so a
  single helper avoids a divergent re-implementation. `message or ""` is
  load-bearing (the broker may return `error_message=None`).

### 5.3 `compact,delete`

Satisfies the compaction check (it *does* compact), but its `delete` component
still applies `retention.ms` eviction. Whenever the **effective post-action**
policy contains `delete` (an existing `compact,delete` topic, *or* a reconcile
whose `expected` is `compact,delete`), log one `INFO`: "topic uses
cleanup.policy='…'; retention-based eviction still applies — un-refreshed keys may
be evicted." Documented v1 boundary (§2): we ensure compaction, not no-eviction.

### 5.4 Known limitations (for the ADR)

- describe→alter is two calls, not atomic — a **third-party** config change
  interleaving them could be lost under full-replace. Reader+writer both
  reconciling is benign (identical merged sets converge). aiokafka lacks
  `incremental_alter_configs`.
- Full-replace re-submission risk: if the broker rejects any *preserved* override
  on write (e.g. a value valid-on-read but invalid-on-write), §5.2 raises and the
  whole reconcile fails even though `cleanup.policy` alone would have succeeded.
  Named honestly; tested (§9).
- Requires `ALTER_CONFIGS` ACL (distinct from `CREATE`/`DESCRIBE`); a denial
  raises (§5.2), never swallowed.
- `alter_configs` carries only configs; partitions/RF are untouched.

## 6. Types & errors

```python
EnsureTopicOutcome = Literal[
    "created",     # this call created the topic (compacted)
    "verified",    # existed, confirmed compacting (policy contains 'compact')
    "reconciled",  # existed, was non-compacting, this call flipped it to compact
    "mismatch",    # existed, non-compacting, left as-is (warn mode)
    "unreadable",  # existed, tried but could not read the policy (warn mode)
    "skipped",     # existed, but we chose not to look: ignore mode OR the declared
                   # policy needs no compaction (two causes, deliberately merged)
]

@dataclass(frozen=True, slots=True)
class EnsureTopicResult:
    """Outcome of ensure_topic(). A single discriminated `outcome` (not correlated
    booleans), plus the observed cleanup.policy. `policy` is the value read from the
    existing topic for verified/reconciled/mismatch (for `reconciled` it is the
    PRE-reconcile value, e.g. 'delete'), and None for created/skipped/unreadable
    (nothing was read). A `compact,delete` topic surfaces as outcome='verified',
    policy='compact,delete'. Plain frozen carrier, matching `ViewStats` — ktables is
    the only constructor and only ever produces a valid outcome↔policy pair; that
    invariant is asserted by a unit test, not a runtime guard."""
    outcome: EnsureTopicOutcome
    policy: str | None

class TopicConfigMismatchError(Exception):
    """Existing topic's cleanup.policy isn't compacting and the caller chose
    on_policy_mismatch='raise'. Raised ONLY on a confirmed mismatch, so all fields
    are populated. The mismatch rule is set-containment of 'compact' (not string
    equality), so do not re-derive `actual != expected`."""
    def __init__(self, topic: str, expected: str, actual: str) -> None: ...
```

No ktables-wide base exception is introduced: the codebase raises stdlib
`ValueError`/`RuntimeError`/`TypeError` and lets aiokafka `KafkaError` propagate,
so a base that parented only this feature's errors would over-promise (`except
<base>` would miss most ktables raises). `TopicConfigMismatchError` stands alone;
the rare absent-`cleanup.policy` case raises `RuntimeError` (matching how
`start()` already signals "couldn't establish the expected state"). A library-wide
hierarchy, if ever wanted, is a separate coherent refactor.

- `ensure_topic()` now returns `EnsureTopicResult` (was `bool`) — a **hard breaking
  change** to the standalone deploy primitive; internal `start()` callers discard
  the return, so they're unaffected. Outcome map: fresh create → `created`;
  existing & compacting → `verified` (policy carries `compact` or `compact,delete`);
  existing `delete` + `warn` → `mismatch`; existing `delete` + `reconcile` →
  `reconciled`; `ignore` or no-compaction-needed → `skipped`; existing but policy
  unreadable + `warn` → `unreadable`. (`raise` mode raises instead of returning
  `mismatch`/`unreadable`.) Each reachable state is exactly one enum value; the
  outcome↔policy coupling (e.g. `created` always `policy=None`, `verified` always
  set) is a documented + unit-tested invariant of what ktables constructs.
- `actual` is non-optional `str`: `TopicConfigMismatchError` is raised only on a
  confirmed mismatch (describe succeeded, real policy lacks `compact`). Every
  unverifiable case raises the underlying broker exception or `RuntimeError`
  instead — never `TopicConfigMismatchError`.
- Not parented under `KafkaError` — a ktables policy assertion, not a broker error.

## 7. Observability

- `reconcile` success `INFO`: "reconciled topic 'X' cleanup.policy 'delete' →
  '{expected}'; preserved N operator overrides (retention.ms, segment.bytes)."
- Confirmed mismatch under `warn`: the §4.5 `WARNING`.
- Could-not-verify under `warn`: `INFO` (asserted to **not** be `WARNING`).
- `compact,delete` accepted/produced: the §5.3 `INFO`.
- Deliberately **not** in `ViewStats`/`status`: a one-shot startup concern, the
  writer has no `ViewStats`, and the new `EnsureTopicResult`/logs are the audit
  surface. Recorded as a conscious rejection in the ADR.

## 8. Permissions matrix (ACL rows verified by mocked in-band codes — §9, not by an ACL broker)

| ACL state | `ignore` | `warn` | `raise` | `reconcile` |
| --- | --- | --- | --- | --- |
| CREATE only (no DESCRIBE) | no-op | describe in-band `code≠0`/raises → `INFO`, continue | raises (can't verify) | raises (can't read) |
| CREATE + DESCRIBE (no ALTER) | no-op | verify; warn on mismatch | raise on mismatch | mismatch → alter denied (code 29) → raises (§5.2) |
| CREATE + DESCRIBE + ALTER | no-op | verify; warn | raise on mismatch | reconcile succeeds |
| no CREATE | (`_try_create_topic` raises the create error via `for_code` before any check) | same | same | same |

## 9. Testing strategy (TDD)

Unit (mocked admin; extends `tests/test_kafka_table.py:507`). The existing
`_FakeAdmin` (whose `create_topics` returns `None`) **must be replaced** by a fake
that mirrors the real shape: each admin call returns a **list of response
objects**, each with a `.resources` list; describe resources are 5-tuples
`(code, msg, type, name, entries)`, alter resources 4-tuples
`(code, msg, type, name)`, and `create_topics` returns a response with
`topic_errors = [(name, code[, msg])]`. Include a test where a flat-list mock is
fed and the consumer **fails** (asserting `.resources`/`topic_errors` is indexed),
so the real-shape contract is enforced, not assumed.

- Invalid `on_policy_mismatch` string → `ValueError` at construction.
- `ensure_topic=False` + `raise`/`reconcile` → `ValueError` at construction;
  `ensure_topic=False` + `warn`/`ignore` → constructs cleanly, **no** warning.
- **`start()` forwards the param (both classes):** monkeypatch
  `ktables.kafka_table.ensure_topic`; construct `KafkaTable`/`KafkaTableWriter`
  with `on_policy_mismatch="raise"`, call `start()`, assert the patched
  `ensure_topic` received `on_policy_mismatch="raise"`. Without this, a forgotten
  forward silently degrades every table/writer to the `warn` default.
- **`start()` tolerates the new return type:** the patched `ensure_topic` returns
  `EnsureTopicResult(outcome="mismatch", policy="delete")`; `start()` completes
  normally (never branches on the value — it's discarded).
- **Create-path regression (the pre-existing bug):** `create_topics` returns
  `topic_errors=[(t,36,…)]` → `_try_create_topic` returns `False`, no "created"
  log, the policy check **fires** (for the already-exists path the function returns
  `outcome="skipped"`/`verified`/… per mode, never `created`). `code==0` →
  `outcome="created"`, check skipped, `describe_configs.call_count == 0`. A
  CREATE-denial code (29) → raises `TopicAuthorizationFailedError` (broker message
  in `str()`) **before** any describe (`describe_configs.call_count == 0`),
  covering the §8 "no CREATE" row.
- `ignore` → `describe_configs` **never called** (`call_count == 0`).
- `expected is None`/non-compacting → `describe_configs` **never called**.
- **Default args (omit the param)** + mismatch → exactly one `WARNING`,
  `alter_configs.call_count == 0`.
- `warn`: describe raises `KafkaError` → `INFO`, no raise; a **non-`KafkaError`**
  (e.g. `ValueError`) during the check **propagates** even under `warn` (guards
  against over-broad except).
- In-band `error_code` parametrized over {29, 3, 40} on describe and alter:
  `warn` → `INFO`; `raise`/`reconcile` → raise the matching `for_code` type **with
  the broker `error_message` in `str()`**.
- `for_code(0)` happy path: `error_code == 0` alter → logs success, does **not**
  raise; describe `code==0` → proceeds to merge.
- Absent `cleanup.policy` entry (code 0, empty/no policy): `warn` → `INFO` +
  `outcome="unreadable"`; `raise`/`reconcile` → `RuntimeError` (not
  `TopicConfigMismatchError`).
- `raise`: confirmed mismatch → `TopicConfigMismatchError` with all fields;
  `str()` names topic/expected/actual.
- `reconcile`: mismatch → `alter_configs` called **once** with merged set
  (preserved overrides + `cleanup.policy=expected`); success `INFO` names the
  preserved count; an alter response with a **preserved-override** rejection
  (code≠0) → raises.
- Override extraction helper: synthetic **v0** entry (`is_default` bool@3) and
  **v1** entries with `config_source` ∈ {1 (override→kept), 4/5 (default→dropped)}
  → classified correctly (covers the v0 branch + the non-override v1 arm).
- `_split` fed `"compact"`, `"compact,delete"`, `"delete,compact"`,
  `"delete, compact"`, `" compact "` → all classify correctly.
- Mid-check `describe`/`alter` raising `KafkaError` **and** `asyncio.TimeoutError`
  (both arms of the except tuple) across all modes (`warn`→INFO; `raise`/
  `reconcile`→propagate); assert `admin.close()` still ran (mirror `:549/:559`).
- **Admin `start()` failure:** the fake admin's `start()` raises
  `KafkaConnectionError` → propagates in all modes, and `admin.close()` still runs
  (covers the `start()`-inside-`try` leak fix, §4.1).
- `EnsureTopicResult.outcome`/`policy` assertions for **every** §6 outcome:
  `created`, `verified` with `policy="compact"` (**and assert the §5.3 INFO is
  absent**) vs `verified` with `policy="compact,delete"` (**INFO present**),
  `reconciled` (`policy` = pre-reconcile value), `mismatch`, `unreadable` (`warn`
  describe-failure, `policy=None`), and `skipped` from **both** sub-causes —
  `ignore` mode and non-compacting `expected` — each asserting
  `outcome="skipped", policy=None` (covers both operands of the §4.1 `or`).
- Coupling invariant: assert that across all branches ktables only ever returns a
  valid outcome↔policy pair (e.g. `created`/`skipped`/`unreadable` ⇒ `policy is
  None`; `verified`/`reconciled`/`mismatch` ⇒ `policy` set) — the invariant is
  test-enforced, not a runtime guard.
- Five `caplog` level+substring assertions (§7): reconcile-success INFO (**names
  the preserved-override count**), mismatch `WARNING`, **can't-verify is `INFO`,
  not `WARNING`**, `compact,delete` `INFO`, and `verified`-plain-`compact` emits
  **no** retention INFO.

Integration (real Redpanda; auto-marked `integration`):
- Pre-create `delete` + custom config; `reconcile` flips to `compact`, **preserves**
  the custom config; returns `EnsureTopicResult(outcome="reconciled",
  policy="delete")` (the observed pre-reconcile policy).
- **Idempotency:** re-run on the now-`compact` topic → `describe.call_count==1`,
  `alter.call_count==0`, clean start.
- Pre-create `compact,delete` → all active modes accept it (no warn/raise/alter);
  **capture the broker's literal returned policy string** and assert `_split`
  classifies it.
- Pre-create `delete`; `raise` raises; `warn` logs and the table still starts.
- Concurrent reader+writer both `reconcile` a `delete` topic → final `compact`,
  overrides survive.

Coverage target 100% on new lines (`/pytest-coverage`).

## 10. Docs & ADR impact

- **Constructor docstrings (all four classes):** carry a 2–3 line summary of
  `on_policy_mismatch` including the **why** ("an existing topic with
  `cleanup.policy=delete` silently loses table entries to retention; this controls
  the response") and that `reconcile` *mutates broker config* and needs an
  `ALTER_CONFIGS` ACL. Users read the constructor, not the `Literal` alias.
- **Inert-combo `ValueError` message must teach:** state *why* (with
  `ensure_topic=False` ktables makes no admin calls, so `raise`/`reconcile` can't
  act) and *what to do* (`ensure_topic=True` to verify, or manage policy
  out-of-band). A locked-down-cluster user will hit this combination.
- **Document the raises-set** on `ensure_topic`/`start()`: `TopicConfigMismatchError`
  on a confirmed mismatch; `aiokafka.KafkaError` on ACL denial / broker errors;
  `RuntimeError` on an unreadable policy. The disjoint hierarchies are a debugging
  trap otherwise.
- **README** "Locked-down clusters" (~line 150): the knob **requires
  `ensure_topic=True`**; on a `DESCRIBE`-denied locked-down cluster prefer
  `ensure_topic=False` (no describe attempted, no recurring `INFO`) and manage
  `cleanup.policy` out-of-band. State the **default is `warn`** but production
  registries should opt into `raise`/`reconcile` (a deliberate
  default-vs-recommendation split, not drift).
- **`docs/API.md`**: add `on_policy_mismatch` to the four constructor rows and the
  `ensure_topic` row; change `ensure_topic`'s return to `EnsureTopicResult`; add
  the four reconciliation symbols (`PolicyMismatchAction`, `EnsureTopicOutcome`,
  `EnsureTopicResult`, `TopicConfigMismatchError`) under a dedicated
  "Topic-config reconciliation" sub-heading rather than the flat module-level
  table, so the high-traffic rows (`ensure_topic`, `ViewStats`) stay scannable.
- **CHANGELOG:** note (1) the **hard breaking** `ensure_topic` return change
  (`bool` → `EnsureTopicResult`; `result.outcome == "created"` replaces the old
  `True`); (2) the **new default-path behavior**: on upgrade, a table/writer with
  an existing topic now issues one `describe_configs` call at startup and a
  non-compacting topic logs a `WARNING` — no code change required to trigger this;
  and (3) the two pre-existing-bug fixes: in-band already-exists detection
  (previously `ensure_topic` mis-reported existing topics as created), and the
  admin client now closes even when `start()` fails (`start()` moved inside
  `try/finally`).
- **ADR** (`docs/adr/0001-topic-config-reconciliation.md`; create `docs/adr/`):
  warn-by-default + opt-in safe reconcile via describe-then-merge; **rejected**
  blind full-replace and destructive hard-reset; the in-band error-code handling
  (create + describe + alter); the non-atomic describe→alter and
  `compact,delete`-still-evicts limitations; the inert-combo `ValueError`; the
  no-ViewStats-signal decision.
- **aiokafka floor compatibility (verified during implementation):** the project
  declares `aiokafka>=0.13` and CI runs the **full integration suite against
  `aiokafka==0.13.0`**. All four proof scripts were re-run on 0.13.0 and every
  load-bearing behavior is identical to 0.14.0 (in-band `create_topics`
  `topic_errors` code 36; in-band describe/alter `error_code`; `for_code` map;
  describe entry `config_source==1` shape; safe describe-then-merge; no
  `incremental_alter_configs`). **No code change or floor bump is needed.** Note:
  the `kafka_table.py:8` "aiokafka 0.13.0" reference is the *supported floor*, not
  stale drift — the earlier "out of scope drift" framing was imprecise.

## 11. Out of scope / future extensions

- **Enforce all declared `topic_configs`** on an existing topic (IaC-style),
  still non-destructive; the generalization of §5; would adopt the freed-up
  `on_config_mismatch` name. Deferred to keep v1 focused and avoid false-positive
  mismatches from broker value normalization.

## 12. Empirical backing

Reproducible proof scripts under `notes/`, run against Redpanda `v25.3.15`:

- `proof_alter_configs.py` — `alter_configs` flips an existing topic to `compact`
  and is a destructive full-replace (custom `retention.ms` reset).
- `proof_safe_reconcile.py` — describe-then-merge flips to `compact` while
  **preserving** custom `retention.ms`/`segment.bytes`.
- `proof_inband_errors.py` — alter/describe broker rejection is reported via
  in-band `error_code` (40, 3) with **no raise**; basis for §5.2.
- `proof_double_create.py` — a steady-state second `create_topics` returns
  `topic_errors=[(name, 36, …)]` with **no raise**; basis for §4.1 and the
  pre-existing-bug finding.

Preserve these as the rationale record for the ADR.
