# resilient-outbox

[![CI](https://github.com/yazanTah/resilient-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/yazanTah/resilient-outbox/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/resilient-outbox.svg)](https://www.npmjs.com/package/resilient-outbox)
[![types](https://img.shields.io/npm/types/resilient-outbox.svg)](https://www.npmjs.com/package/resilient-outbox)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

A durable offline mutation queue for **React Native, Expo and the web**. It gives every write an idempotency key, runs dependent writes in order while swapping temporary client IDs for server IDs, and moves permanent failures into a dead-letter queue so they never block anything else.

Zero runtime dependencies. Typed end to end. Under 1,900 lines of heavily commented source you can read in an afternoon.

---

## Why

Offline writes fail in four ways that naive retry loops get wrong:

1. **Duplicates on flaky networks.** The request reaches the server, the response is lost, the client retries, and the user is charged twice. → Every mutation carries a stable `idempotencyKey` across retries and app restarts.
2. **Writes that depend on each other.** A comment is created offline for a post that was _also_ created offline, so it references `temp_123`. → `dependsOn` holds the comment until the post succeeds, then `resolvePayload` rewrites `temp_123` to the real server ID — even if the app was killed in between.
3. **Two tabs, one queue.** Two open tabs that each keep their own copy of the queue silently overwrite each other's writes and send everything twice. → `crossTab: true` makes every change an atomic, locked read-modify-write and elects one tab to send, with automatic failover when it closes.
4. **Poison pills.** A `422` will never succeed, and a queue that retries it forever blocks every action behind it. → Permanent errors go straight to the dead-letter queue, together with anything that depended on them, and can be retried later in one call.

## Install

```bash
npm install resilient-outbox
# or
npx expo install resilient-outbox
```

Runtime requirements: `Promise`, `AbortController` (React Native ≥ 0.60, every modern browser, Node ≥ 16). `crypto.randomUUID` / `crypto.getRandomValues` are used when present; see [Limitations](#limitations).

---

## Quick start

```ts
import { OutboxQueue } from 'resilient-outbox';
import { GenericStorageAdapter } from 'resilient-outbox/adapters';
import AsyncStorage from '@react-native-async-storage/async-storage';

class HttpError extends Error {
  constructor(
    readonly statusCode: number,
    readonly retryAfterMs?: number
  ) {
    super(`HTTP ${statusCode}`);
  }
}

export const outbox = new OutboxQueue({
  name: 'api',
  storage: new GenericStorageAdapter(AsyncStorage),
  executorTimeoutMs: 15_000,
  executor: async (mutation, { resolvePayload, signal }) => {
    const res = await fetch(`https://api.example.com/${mutation.type}`, {
      method: 'POST',
      signal, // aborted on timeout or queue.destroy()
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': mutation.idempotencyKey
      },
      body: JSON.stringify(resolvePayload(mutation.payload))
    });

    if (!res.ok) {
      const retryAfter = Number(res.headers.get('Retry-After'));
      throw new HttpError(res.status, Number.isFinite(retryAfter) ? retryAfter * 1000 : undefined);
    }
    return res.json();
  }
});

outbox.startAutoFlush();
```

There is no need to call `hydrate()`: every async method loads persisted state first. Call it yourself only if you want to read state synchronously at startup.

### Dependent mutations and temporary IDs

```ts
const post = await outbox.enqueue({
  type: 'posts',
  tempId: 'temp_post_123', // mapped to the server's `id` once this succeeds
  payload: { id: 'temp_post_123', title: 'Offline-first architecture' }
});

await outbox.enqueue({
  type: 'comments',
  dependsOn: post.id, // waits for the post; dead-lettered if the post fails
  payload: { postId: 'temp_post_123', body: 'Great post!' } // rewritten by resolvePayload
});
```

By default the server ID is read from `result.id`. Pass `getServerId: (result) => result.data.uuid` for other response shapes. `context.resolveId(tempId)` resolves a single value.

### React

```tsx
import { useOutbox, useOutboxMutations } from 'resilient-outbox/react';

export function SyncStatus() {
  const { pending, deadLetter, isSyncing, flush } = useOutbox(outbox);
  const failed = useOutboxMutations(outbox, 'dead_letter');

  return (
    <View>
      {isSyncing && <Text>Syncing…</Text>}
      {pending > 0 && <Text>{pending} change(s) waiting to sync</Text>}
      {failed.map(m => (
        <View key={m.id}>
          <Text>
            {m.type} failed: {m.error?.message}
          </Text>
          <Button title="Retry" onPress={() => outbox.retryDeadLetter(m.id)} />
          <Button title="Discard" onPress={() => outbox.remove(m.id)} />
        </View>
      ))}
      <Button title="Sync now" onPress={flush} disabled={isSyncing || pending === 0} />
    </View>
  );
}
```

Both hooks use `useSyncExternalStore`, so they don't tear under concurrent rendering. `useOutbox` re-renders only when a count or flag changes, and its actions are referentially stable. Tested on React 18 and 19.

---

## Running automatically

`startAutoFlush()` flushes immediately, after every enqueue, on an interval (default 15s), exactly when the earliest backoff expires, and on the `online` event where one exists.

`flush()` does nothing while `isOnline()` returns `false`, so time spent offline never uses up retry attempts. On the web it defaults to `navigator.onLine`. React Native has no reliable equivalent, so wire up NetInfo:

```ts
import NetInfo from '@react-native-community/netinfo';
import { AppState } from 'react-native';

const outbox = new OutboxQueue({
  name: 'api',
  storage,
  executor,
  isOnline: async () => (await NetInfo.fetch()).isConnected !== false
});

outbox.startAutoFlush();
NetInfo.addEventListener(state => state.isConnected && outbox.flush());
AppState.addEventListener('change', state => state === 'active' && outbox.flush());
```

---

## Error handling

The executor signals failure by throwing. The status code is read from `err.statusCode`, `err.status`, or `err.response.status` (axios), and `err.retryAfterMs` overrides the backoff delay.

| Error                              | Default behaviour                                                      |
| :--------------------------------- | :--------------------------------------------------------------------- |
| `408`, `425`, `429`                | Retried with exponential backoff and full jitter                       |
| `409` with `retryAfterMs`          | Retried after that delay (server: "this key is still in progress")     |
| Other `4xx`, `501`, `505`          | Dead-lettered immediately (`NON_RETRYABLE`). Does not block the queue. |
| Other `5xx`                        | Retried up to `maxAttempts` (default 5), then dead-lettered            |
| No status (network error, timeout) | Retried up to `maxAttempts`, then dead-lettered (`MAX_ATTEMPTS`)       |

Override with `retryConfig.isRetryable(error, statusCode)`.

Every dead-lettered mutation records why in `mutation.error.code`:

| Code             | Meaning                                                | `retryDeadLetter(parentId)`                |
| :--------------- | :----------------------------------------------------- | :----------------------------------------- |
| `NON_RETRYABLE`  | The executor threw a permanent error                   | —                                          |
| `MAX_ATTEMPTS`   | Transient failures exhausted `maxAttempts`             | —                                          |
| `PARENT_FAILED`  | A mutation it depends on was dead-lettered             | Revived automatically with its parent      |
| `PARENT_REMOVED` | A mutation it depends on was removed before completing | Not revived; retry or remove it explicitly |

---

## Guarantees

- **At-least-once delivery.** A mutation is marked completed only after the executor resolves. If the app dies mid-request, the mutation is retried on next launch with the **same idempotency key**. Exactly-once _effects_ therefore depend on your server honouring that key (see below).
- **Ordering.** Mutations are executed one at a time in FIFO order. A failing mutation doesn't block later _independent_ ones; use `dependsOn` wherever order matters.
- **Durability.** State is saved after every mutation finishes. Writes are serialized and coalesced, so storage I/O stays bounded however fast state changes, and a write can never land out of order.
- **No silent overwrite.** If storage can't be read at startup, nothing is written until a later read succeeds and is merged in. Unparseable data is backed up to `<key>:corrupt` before the queue continues.
- **Multi-tab safety** (with `crossTab`). Changes from any tab are atomic, exactly one tab sends at a time, and leadership fails over when that tab dies.
- **Single flight.** Concurrent `flush()` calls share one run. A call made during a run schedules one more pass, so work enqueued mid-flush is picked up.

### Server contract

The client keeps the key stable; the server has to make the check-and-write **atomic**. "Look up the key, do the work, then save the key" in separate steps has two holes: two concurrent requests can both pass the lookup, and a crash after the work but before saving the key lets the retry do it again.

**When the side effect is a write to your own database**, put the work and the key in the same transaction, with the key under a unique constraint:

```sql
BEGIN;
-- Unique key: a concurrent duplicate blocks here until we commit, then fails, and reads our result
INSERT INTO idempotency_keys (key, status, body) VALUES ($1, 0, '{}');
INSERT INTO orders (...) VALUES (...) RETURNING *;
UPDATE idempotency_keys SET status = 201, body = $order_json WHERE key = $1;
COMMIT;
-- On a unique violation: SELECT status, body FROM idempotency_keys WHERE key = $1 and return it
```

A crash anywhere before `COMMIT` leaves neither the order nor the key, so the retry starts clean. [`examples/server/idempotency.mjs`](./examples/server/idempotency.mjs) is a runnable version (Node's built-in SQLite), with tests that fire concurrent requests from separate threads and kill the process between the write and saving the key.

**When the side effect is an external call** (charging a card through a payment provider), it can't join your transaction:

1. Insert the key with status `in_progress` (unique constraint). If it exists and is complete, return the stored result. If it is still in progress, respond `409` with a `Retry-After` header; resilient-outbox retries a 409 that carries `retryAfterMs` instead of dead-lettering it.
2. Call the provider and **pass the same idempotency key through** (Stripe, Adyen and most payment APIs accept one). If you crash after the charge, the retry's call is deduplicated by the provider.
3. Save the result and mark the key complete. Treat `in_progress` rows older than your request timeout as abandoned and re-run step 2; the provider's key makes that safe.

---

## Multiple tabs

Open the same app in two tabs and, by default, each tab keeps its own copy of the queue in memory: their writes overwrite each other, and both send the same mutations. Turn on `crossTab` in every tab:

```ts
const outbox = new OutboxQueue({
  name: 'api',
  storage: new GenericStorageAdapter(localStorage),
  crossTab: true,
  executor
});
outbox.startAutoFlush(); // in every tab: whichever tab leads keeps the queue moving
```

- **No lost writes.** Every change — from any tab — is a read-modify-write of storage under a [Web Lock](https://developer.mozilla.org/docs/Web/API/Web_Locks_API), so tabs can never overwrite each other.
- **One sender.** Tabs elect a leader, and only the leader executes mutations. `flush()` in another tab asks the leader to flush and resolves with its count.
- **Failover.** When the leader tab closes or crashes, the browser releases its lock and the next tab takes over within milliseconds, retrying whatever was in flight with the **same idempotency key**.
- **Every tab stays live.** Tabs are notified over `BroadcastChannel`, so `useOutbox` in any tab reflects enqueues, progress and failures from all of them. `snapshot.isLeader` tells you which tab is sending.
- **Misconfiguration is caught.** If two instances share a storage key without `crossTab`, both log a warning.

Execution events (`completed`, `retry`, `dead_letter`) fire in the leader's tab. This behaviour is verified end to end in real Chromium tabs in CI (`npm run test:e2e`).

Running several queues in one JavaScript context, or on a platform without Web Locks? Share a `MemoryCoordinator` between them, or implement the three-method `OutboxCoordinator` interface yourself.

## Storage adapters

```ts
import { GenericStorageAdapter, MMKVStorageAdapter, MemoryStorageAdapter } from 'resilient-outbox/adapters';

new GenericStorageAdapter(AsyncStorage); // @react-native-async-storage/async-storage
new GenericStorageAdapter(window.localStorage); // web
new MMKVStorageAdapter(createMMKV({ id: 'outbox' })); // react-native-mmkv v3 or v4
new MemoryStorageAdapter(); // default; tests and ephemeral queues
```

Any object with async `getItem` / `setItem` / `removeItem` works as an adapter.

---

## Observability

```ts
outbox.on('completed', ({ mutation }) => analytics.track('sync_ok', { type: mutation.type }));
outbox.on('retry', ({ mutation, delayMs }) => log.debug('retrying', mutation.id, delayMs));
outbox.on('dead_letter', ({ mutation, error }) => Sentry.captureMessage(`${mutation.type}: ${error.code}`));
outbox.on('storage_error', ({ operation, error }) =>
  Sentry.captureException(error, { extra: { operation } })
);
```

Events: `enqueued`, `completed`, `retry`, `dead_letter`, `storage_error`. Each `on()` returns an unsubscribe function. A throwing listener is logged and never breaks the queue. Pass `logger: null` to silence internal logging, or any `{ warn, error }` object to redirect it.

---

## API

### `new OutboxQueue(options)`

| Option              | Default                | Description                                                                                                                   |
| :------------------ | :--------------------- | :---------------------------------------------------------------------------------------------------------------------------- |
| `name`              | _required_             | Storage namespace (`@resilient-outbox:<name>`). One instance per name.                                                        |
| `executor`          | _required_             | `(mutation, context) => Promise<result>`. Throw to signal failure.                                                            |
| `storage`           | `MemoryStorageAdapter` | Where state is persisted.                                                                                                     |
| `retryConfig`       | see below              | `maxAttempts` (5), `initialBackoffMs` (1000), `maxBackoffMs` (30000), `backoffMultiplier` (2), `jitter` (true), `isRetryable` |
| `executorTimeoutMs` | none                   | Aborts `context.signal` and fails the attempt (retryably) after this long.                                                    |
| `isOnline`          | `navigator.onLine`     | `flush()` is a no-op while this returns `false`.                                                                              |
| `getServerId`       | `result => result.id`  | Extracts the server ID that a `tempId` maps to.                                                                               |
| `idMapTtlMs`        | 7 days                 | Unused temp-ID mappings are dropped after this, unless a queued mutation still references them.                               |
| `autoPersist`       | `true`                 | Set `false` to keep state in memory only.                                                                                     |
| `crossTab`          | `false`                | `true` to coordinate browser tabs (see [Multiple tabs](#multiple-tabs)), or a custom `OutboxCoordinator`.                     |
| `logger`            | `console`              | `{ warn, error }`, or `null` to silence.                                                                                      |

The executor `context` has `resolvePayload(payload)`, `resolveId(id)`, a read-only `idMap`, and an `AbortSignal` in `signal`.

### Methods

| Method                                                                           | Description                                                                                               |
| :------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------- |
| `enqueue({ type, payload, tempId?, dependsOn?, idempotencyKey?, maxAttempts? })` | Adds a mutation and persists it. Resolves with the `Mutation`.                                            |
| `flush()`                                                                        | Runs every due mutation. Resolves with the number completed.                                              |
| `startAutoFlush(options?)`                                                       | `{ intervalMs = 15000, flushOnReconnect = true, flushOnEnqueue = true }`. Returns a stop function.        |
| `stopAutoFlushing()`                                                             | Stops auto-flush.                                                                                         |
| `retryDeadLetter(id)`                                                            | Moves a mutation, and the descendants it took down with it, back to pending.                              |
| `retryAllDeadLetter()`                                                           | Moves every dead-lettered mutation back to pending. Resolves with the count.                              |
| `remove(id)`                                                                     | Removes a mutation (`false` if it is mid-request). Dependents get `PARENT_REMOVED`.                       |
| `clear()`                                                                        | Removes everything, including temp-ID mappings.                                                           |
| `get(id)`, `getAll()`, `getPending()`, `getDeadLetter()`                         | Synchronous reads.                                                                                        |
| `on(event, listener)`                                                            | Subscribes to a lifecycle event.                                                                          |
| `subscribe(listener)`                                                            | Low-level change notifications (used by the hooks).                                                       |
| `getSnapshot()`                                                                  | `{ total, pending, syncing, deadLetter, isSyncing, isHydrated, isLeader }`, stable until a value changes. |
| `getMutationsSnapshot()`                                                         | Shallow copies of all mutations, stable until the queue changes.                                          |
| `hydrate()`                                                                      | Loads persisted state. Idempotent; called automatically.                                                  |
| `destroy()`                                                                      | Stops timers, aborts the in-flight request without using up an attempt, and drops listeners.              |

Also exported: `MemoryCoordinator`, `createWebCoordinator`, `isWebCoordinatorSupported`, `MutationErrorCode`, `OutboxTimeoutError`, `OutboxDestroyedError`, `calculateBackoffMs`, `isDefaultRetryable`, `getErrorStatusCode`, `generateUUID`, and every type.

---

## Limitations

- **Use `crossTab: true` on the web.** Without it, two instances sharing a storage key — the same app in two tabs — overwrite each other (the library warns in the console when it detects this). With it, `crossTab` needs the Web Locks API and `BroadcastChannel` (Chrome/Edge 69+, Firefox 96+, Safari 15.4+); elsewhere it logs a warning and runs as a single instance.
- **Cross-tab writes cost a storage round-trip each.** Every change is a locked read-modify-write of the whole queue, which is instant for `localStorage` and typical queue sizes, but not designed for tens of thousands of queued mutations.
- **Payloads and results must be JSON-serializable**, because that is how they are persisted.
- **Everything is held in memory.** Fine for thousands of mutations, not millions.
- **Idempotency keys need good randomness.** On runtimes without Web Crypto (e.g. older Hermes builds) IDs fall back to `Math.random`. Install `react-native-get-random-values` there.

## When to use something else

If you already use TanStack Query and only need mutations to be replayed after a restart, its persisted mutations may be enough. If you need two-way sync of a local database, use a sync engine such as PowerSync, Replicache or WatermelonDB. `resilient-outbox` fits in between: a REST or RPC backend, writes that must not be duplicated or lost, and no appetite for a full sync framework.

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Report security issues as described in [SECURITY.md](./SECURITY.md).

## License

MIT © 2026 [Yazan Taha](https://github.com/yazanTah)
