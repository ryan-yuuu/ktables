# Changelog

## [1.0.0](https://github.com/ryan-yuuu/ktables/compare/v0.4.0...v1.0.0) (2026-06-23)


### ⚠ BREAKING CHANGES

* reconcile existing topic cleanup.policy via on_policy_mismatch ([#25](https://github.com/ryan-yuuu/ktables/issues/25))

### Features

* reconcile existing topic cleanup.policy via on_policy_mismatch ([#25](https://github.com/ryan-yuuu/ktables/issues/25)) ([769820a](https://github.com/ryan-yuuu/ktables/commit/769820ad1ee84a987697400ca123f12e34a2ffa4))

## [0.4.0](https://github.com/ryan-yuuu/ktables/compare/v0.3.0...v0.4.0) (2026-06-20)


### Features

* expose fetch_max_wait_ms to tune barrier latency ([#23](https://github.com/ryan-yuuu/ktables/issues/23)) ([46d0b08](https://github.com/ryan-yuuu/ktables/commit/46d0b08975f5237dd9296d09a1a46c8a695169f9))

## [0.3.0](https://github.com/ryan-yuuu/ktables/compare/v0.2.0...v0.3.0) (2026-06-17)


### Features

* grouped/multimap table (group -&gt; {member -&gt; value}) over a compacted topic ([#17](https://github.com/ryan-yuuu/ktables/issues/17)) ([0b53f8b](https://github.com/ryan-yuuu/ktables/commit/0b53f8bbab56cd4c91561faede5e2257e5e119b8))


### Documentation

* fix the CI badge URL after the Tests-to-CI rename ([#19](https://github.com/ryan-yuuu/ktables/issues/19)) ([655cd7d](https://github.com/ryan-yuuu/ktables/commit/655cd7db2d61e99257c24de5ba9bf46da32bba19))
* update README ([cd54e92](https://github.com/ryan-yuuu/ktables/commit/cd54e929845f1a1d3cdd7206113cd505c9e3f678))

## [0.2.0](https://github.com/ryan-yuuu/ktables/compare/v0.1.2...v0.2.0) (2026-06-13)


### Features

* add KafkaTable.barrier() — on-demand read-your-own-writes ([#8](https://github.com/ryan-yuuu/ktables/issues/8)) ([7a46433](https://github.com/ryan-yuuu/ktables/commit/7a464331d9430a9657535cccbdf484fec04d0c85))

## [0.1.2](https://github.com/ryan-yuuu/ktables/compare/v0.1.1...v0.1.2) (2026-06-10)


### Bug Fixes

* ship py.typed so type checkers use the package's inline annotations ([#6](https://github.com/ryan-yuuu/ktables/issues/6)) ([e556fb4](https://github.com/ryan-yuuu/ktables/commit/e556fb414b9a9ec723a18deeea5a16fa9f382006))

## [0.1.1](https://github.com/ryan-yuuu/ktables/compare/v0.1.0...v0.1.1) (2026-06-10)


### Documentation

* update README for published package (pip install, PyPI badge, summary sync) ([#4](https://github.com/ryan-yuuu/ktables/issues/4)) ([466e676](https://github.com/ryan-yuuu/ktables/commit/466e67693caf9a72057e7684314355d300d4a70a))

## 0.1.0 (2026-06-10)


### Features

* KafkaTable and KafkaTableWriter — a GlobalKTable for asyncio Python ([#1](https://github.com/ryan-yuuu/ktables/issues/1)) ([4a2b0b9](https://github.com/ryan-yuuu/ktables/commit/4a2b0b9ecfea398d6c8bdcfcfba23f828c1566b0))
