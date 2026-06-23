# Reconcile an existing topic's cleanup.policy via an opt-in `on_policy_mismatch` knob

**Status:** accepted

`ensure_topic` only set `cleanup.policy=compact` when it *created* a topic; an
existing topic (e.g. broker-auto-created as `delete`) was accepted as-is, and a
`delete` topic silently evicts un-refreshed keys so a fresh consumer materializes
a table missing entries. We add `on_policy_mismatch` (`ignore`/`warn`/`raise`/
`reconcile`, default `warn`) on every constructor and `ensure_topic`: it detects a
non-compacting existing topic and warns (default), fails, or safely reconciles it.
`reconcile` uses **describe-then-merge** — aiokafka exposes only the full-replace
`alter_configs` (no `incremental_alter_configs`), which would reset every omitted
config to default, so the existing explicit overrides are resubmitted alongside
`cleanup.policy=compact`.

## Considered options

- **Blind `alter_configs({"cleanup.policy": "compact"})`** — rejected: empirically
  a full-replace that wipes operator-tuned configs (e.g. `retention.ms`) back to
  default.
- **A destructive "hard reset"** — rejected: strictly dominated by the safe
  reconcile (identical result on a default topic, data-losing on a tuned one).
- **A boolean `reconcile=True/False`** — rejected: can't express the
  "warn-but-don't-mutate" middle ground; replaced by the four-value enum.
- **A `KTablesError` base / a runtime `EnsureTopicResult.__post_init__` guard** —
  rejected after a DX review: the codebase raises stdlib exceptions and uses plain
  frozen result types (`ViewStats`), so both would be inconsistent. `EnsureTopicResult`
  is a plain frozen carrier; `TopicConfigMismatchError` stands alone on `Exception`.

## Consequences

- **Two pre-existing bugs fixed along the way:** (1) already-exists is now read
  from the broker's IN-BAND `create_topics` `topic_errors` (code 36) — a real
  KRaft broker never raises `TopicAlreadyExistsError`, so `ensure_topic` had been
  mis-reporting existing topics as "created"; (2) the admin client is now closed
  even when `start()` fails (`start()` moved inside `try/finally`).
- **Breaking:** `ensure_topic()` returns `EnsureTopicResult` instead of `bool`
  (`result.outcome == "created"` replaces the old `True`). Internal `start()`
  callers discard it, so tables/writers are unaffected.
- **New default-path behavior:** with `ensure_topic=True` (default), an existing
  topic now incurs one `describe_configs` call at startup, and a non-compacting
  topic logs a `WARNING`.
- **Known limitations:** describe→alter is two non-atomic calls (a third-party
  config change between them could be lost under full-replace; concurrent ktables
  reconcilers converge because they compute identical merged sets);
  `reconcile` needs the `ALTER_CONFIGS` ACL; `compact,delete` is accepted as
  compacting although its `delete` component still applies retention eviction.
- **Verified against the `aiokafka>=0.13` floor:** the in-band create/describe/
  alter behavior, response shapes, and `for_code` mapping are identical on 0.13.0
  and 0.14.0.
- Deliberately **not** surfaced in `ViewStats`/`status`: a one-shot startup
  concern; `EnsureTopicResult` and the logs are the audit surface.
