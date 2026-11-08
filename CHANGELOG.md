# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.1] - 2026-11-08

### Security

- Hardened handling of untrusted data: a payload key named `__proto__` is now copied as data by
  `resolvePayload` instead of replacing the copy's prototype (which also dropped the key), and
  persisted mutation records can no longer re-point a live mutation's prototype in cross-tab mode.
  Global `Object.prototype` pollution was never possible.

### Changed

- Development tooling: Jest 30, tsup 9, GitHub Actions v7. No change to the library's behaviour.
- README: static license badge.

## [1.0.0] - first public release

### Added

- `OutboxQueue` with per-mutation idempotency keys, FIFO execution and coalesced `flush()`.
- Dependency chaining via `dependsOn`, with temporary-ID mapping through an explicit `tempId`
  option (or `payload.tempId`) and a configurable `getServerId`.
- `context.resolvePayload`, `context.resolveId` and an `AbortSignal` (`context.signal`) for executors.
- Dead-letter queue with machine-readable codes: `NON_RETRYABLE`, `MAX_ATTEMPTS`, `PARENT_FAILED`,
  `PARENT_REMOVED`. Failures cascade to dependents; `retryDeadLetter` revives the cascade.
- Exponential backoff with full jitter; server-provided `retryAfterMs` is honoured.
- Status codes read from `statusCode`, `status` or axios-style `response.status`.
- `executorTimeoutMs`, `isOnline` (offline time never consumes attempts) and `destroy()`.
- `startAutoFlush()`: interval, flush-on-enqueue, `online` event, and a timer for the exact moment
  the earliest backoff expires.
- Lifecycle events via `on()`: `enqueued`, `completed`, `retry`, `dead_letter`, `storage_error`.
- Configurable `logger` (or `null` to silence).
- **Multi-tab support** via `crossTab: true`: every change is a locked read-modify-write of
  storage (Web Locks), tabs elect a single leader that executes mutations, leadership fails over
  when the leader's tab closes or crashes (in-flight work is retried with the same idempotency
  key), and all tabs stay in sync over `BroadcastChannel`. `flush()` in a follower delegates to the
  leader. Pluggable through the `OutboxCoordinator` interface; `MemoryCoordinator` is included.
- Warning when two instances share a storage key without `crossTab`.
- `snapshot.isLeader`.
- End-to-end tests in real Chromium tabs (`npm run test:e2e`).
- React: `useOutbox` (re-renders only when counts change, stable actions) and `useOutboxMutations`.
- Adapters: `MemoryStorageAdapter`, `GenericStorageAdapter` (AsyncStorage, localStorage) and
  `MMKVStorageAdapter` (react-native-mmkv v3 and v4).
- Dual ESM/CJS build with correct types for every resolution mode, including legacy `node10`
  (older Metro, `moduleResolution: "node"`).

### Durability

- State, including the temp-ID map, is persisted after every mutation; writes are serialized and
  coalesced.
- All async operations hydrate first, so early `enqueue()` calls cannot overwrite persisted state.
- A failed storage read blocks writes until a later read succeeds and is merged; corrupt data is
  backed up to `<key>:corrupt`.
- Temp-ID mappings expire after `idMapTtlMs` (default 7 days) unless still referenced.
- Legacy array-format storage is read transparently.

### Changed (relative to the pre-release snapshot)

- `payload.id` is no longer treated as a temporary ID; use `tempId` instead. Mapping any payload `id`
  that differed from the response `id` produced false mappings for ordinary updates.
- `501` and `505` are now permanent errors; `408` and `425` are retryable.
- A mutation that is mid-request can no longer be removed.

[Unreleased]: https://github.com/yazanTah/resilient-outbox/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/yazanTah/resilient-outbox/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/yazanTah/resilient-outbox/releases/tag/v1.0.0
