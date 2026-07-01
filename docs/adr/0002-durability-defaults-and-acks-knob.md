# At-least-once producer default with an opt-in `acks` knob, decoupling durability from idempotence

**Status:** accepted

`KafkaTableWriter` and `GroupedKafkaTableWriter` previously defaulted to
`enable_idempotence=True`, which aiokafka implements by forcing `acks=all` and
bootstrapping an idempotent producer session via an `InitProducerId` request.
That default assumes the broker supports the idempotent producer. Several
Kafka-API-compatible brokers do not: against Tansu 0.6.0, for example,
`InitProducerId` fails with `UnknownError`, so a writer with the old default
cannot even start. We flip the default to `enable_idempotence=False`
(at-least-once) and add an independent `acks` knob (`0`/`1`/`"all"`, default
unset) so ack durability can be chosen separately from idempotence.

## Considered options

- **Keep `enable_idempotence=True` as the default** — rejected: makes the
  library fail to start out of the box on brokers without idempotent-producer
  support, and couples "I want durable acks" to "I want the idempotent
  producer", which not every broker offers.
- **Flip the default to `False` but expose no `acks` control** — rejected: an
  operator on a non-idempotent broker would then have no way to ask for
  in-sync-replica acks at all; `acks=all` durability would be unreachable.
- **Add `acks` but validate the `enable_idempotence=True` + `acks in {0,1}`
  contradiction in ktables** — rejected for now: aiokafka already raises a clear
  error for that combination at producer construction, so a passthrough knob
  avoids duplicating (and drifting from) that validation. `acks` unset is
  omitted from the producer call so aiokafka's own default still applies.

## Consequences

- **Behavior change:** `enable_idempotence` now defaults to `False`. Producers
  are at-least-once unless the caller opts in; retries may duplicate or reorder
  and a leader failover can drop an acked write. Restore the old behavior with
  `enable_idempotence=True`. Shipped as a minor release: the surface is additive
  and the previous durability level stays one keyword away.
- **New `acks` knob:** `acks="all"` gives in-sync-replica durability *without*
  the idempotent producer — the durability level idempotence could not reach on
  brokers lacking `InitProducerId` support. Left unset, behavior is unchanged
  from aiokafka's defaults (`acks=1`, or `acks=all` when idempotence is on).
- **Passthrough semantics:** ktables performs no `acks` validation of its own;
  invalid values and the idempotence contradiction surface as aiokafka errors at
  `start()`.
- **Verified against a real broker:** on Tansu 0.6.0 the new default and
  `acks=0`/`1`/`"all"` (idempotence off) all round-trip, while
  `enable_idempotence=True` fails at `InitProducerId` — the behavior that
  motivated both changes.
