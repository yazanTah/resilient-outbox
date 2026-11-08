# resilient-outbox

[![CI](https://github.com/yazanTah/resilient-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/yazanTah/resilient-outbox/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/resilient-outbox.svg)](https://www.npmjs.com/package/resilient-outbox)
[![types](https://img.shields.io/npm/types/resilient-outbox.svg)](https://www.npmjs.com/package/resilient-outbox)
[![license](https://img.shields.io/npm/l/resilient-outbox.svg)](./LICENSE)

A durable offline mutation queue for **React Native, Expo and the web**. It gives every write an idempotency key, runs dependent writes in order while swapping temporary client IDs for server IDs, and moves permanent failures into a dead-letter queue so they never block anything else.

Zero runtime dependencies. Typed end to end. About 1,300 lines of heavily commented source you can read in one sitting.

---

## Why

Offline writes fail in three ways that naive retry loops get wrong:

1. **Duplicates on flaky networks.** The request reaches the server, the response is lost, the client retries, and the user is charged twice. → Every mutation carries a stable `idempotencyKey` across retries and app restarts.
2. **Writes that depend on each other.** A comment is created offline for a post that was _also_ created offline, so it references `temp_123`. → `dependsOn` holds the comment until the post succeeds, then `resolvePayload` rewrites `temp_123` to the real server ID — even if the app was killed in between.
3. **Poison pills.** A `422` will never succeed, and a queue that retries it forever blocks every action behind it. → Permanent errors go straight to the dead-letter queue, together with anything that depended on them, and can be retried later in one call.

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
- **Single flight.** Concurrent `flush()` calls share one run. A call made during a run schedules one more pass, so work enqueued mid-flush is picked up.

### Server contract

Store the `Idempotency-Key` with the result of the first request, and return that stored result for any repeat:

```ts
app.post('/:resource', async (req, res) => {
  const key = req.header('Idempotency-Key');
  const previous = key && (await idempotency.get(key));
  if (previous) return res.status(previous.status).json(previous.body);

  const body = await create(req.params.resource, req.body);
  if (key) await idempotency.set(key, { status: 201, body }, { ttlDays: 30 });
  res.status(201).json(body);
});
```

---

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
| `logger`            | `console`              | `{ warn, error }`, or `null` to silence.                                                                                      |

The executor `context` has `resolvePayload(payload)`, `resolveId(id)`, a read-only `idMap`, and an `AbortSignal` in `signal`.

### Methods

| Method                                                                           | Description                                                                                        |
| :------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------- |
| `enqueue({ type, payload, tempId?, dependsOn?, idempotencyKey?, maxAttempts? })` | Adds a mutation and persists it. Resolves with the `Mutation`.                                     |
| `flush()`                                                                        | Runs every due mutation. Resolves with the number completed.                                       |
| `startAutoFlush(options?)`                                                       | `{ intervalMs = 15000, flushOnReconnect = true, flushOnEnqueue = true }`. Returns a stop function. |
| `stopAutoFlushing()`                                                             | Stops auto-flush.                                                                                  |
| `retryDeadLetter(id)`                                                            | Moves a mutation, and the descendants it took down with it, back to pending.                       |
| `retryAllDeadLetter()`                                                           | Moves every dead-lettered mutation back to pending. Resolves with the count.                       |
| `remove(id)`                                                                     | Removes a mutation (`false` if it is mid-request). Dependents get `PARENT_REMOVED`.                |
| `clear()`                                                                        | Removes everything, including temp-ID mappings.                                                    |
| `get(id)`, `getAll()`, `getPending()`, `getDeadLetter()`                         | Synchronous reads.                                                                                 |
| `on(event, listener)`                                                            | Subscribes to a lifecycle event.                                                                   |
| `subscribe(listener)`                                                            | Low-level change notifications (used by the hooks).                                                |
| `getSnapshot()`                                                                  | `{ total, pending, syncing, deadLetter, isSyncing, isHydrated }`, stable until a value changes.    |
| `getMutationsSnapshot()`                                                         | Shallow copies of all mutations, stable until the queue changes.                                   |
| `hydrate()`                                                                      | Loads persisted state. Idempotent; called automatically.                                           |
| `destroy()`                                                                      | Stops timers, aborts the in-flight request without using up an attempt, and drops listeners.       |

Also exported: `MutationErrorCode`, `OutboxTimeoutError`, `OutboxDestroyedError`, `calculateBackoffMs`, `isDefaultRetryable`, `getErrorStatusCode`, `generateUUID`, and every type.

---

## Limitations

- **One writer per storage key.** Two `OutboxQueue` instances with the same `name` — including the same queue open in two browser tabs — will overwrite each other's state. On the web, use a single tab or give each tab its own `name`.
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
