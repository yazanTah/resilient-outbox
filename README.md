# resilient-outbox

Durable, transactional offline mutation queue with client-side idempotency keys, cascading dependency chaining, and Dead-Letter Queue (DLQ) quarantine for React Native, Expo, and Web.

---

## The Problem

Handling offline user actions in mobile apps is prone to subtle failure modes:

1. **The Flaky Network Trap (Duplicate Mutations)**: A user taps "Submit" on a spotty cellular connection. The request reaches the server, but the network drops before the client receives the `200 OK`. The client assumes failure and replays the request, resulting in duplicate orders or charges.
2. **Cascading Dependencies (Temporary IDs)**: A user creates a Post offline (assigned temporary ID `temp_101`), then immediately comments on that Post (referencing `temp_101`). If mutations execute concurrently or without ID resolution, the comment request fails on the server.
3. **The Poison Pill (Unrecoverable Errors)**: If an offline mutation fails with a client error (`400 Bad Request`, `401 Unauthorized`, `422 Unprocessable Entity`), naive queues retry indefinitely, blocking all subsequent user actions.

`resilient-outbox` solves these problems with a focused, zero-dependency transactional queue.

---

## Features

- **Client-Side Idempotency**: Automatically attaches RFC4122 v4 UUID idempotency keys to every mutation to prevent duplicate server writes.
- **Dependency Chaining & Temporary ID Mapping**: Enforces sequential execution for dependent mutations and dynamically maps client temporary IDs to server-generated IDs.
- **Dead-Letter Queue (DLQ)**: Quarantines permanent `4xx` client errors immediately so they never block the queue.
- **Exponential Backoff with Full Jitter**: Retries transient `5xx` and network drops with exponential backoff and randomized jitter to prevent server thundering herds.
- **Concurrency Lock**: Coalesces concurrent `flush()` calls into a single in-flight promise to avoid race conditions.
- **Zero Dependencies**: Pure TypeScript with a minimal footprint (`dependencies: {}`).
- **Pluggable Storage**: Works out-of-the-box with in-memory storage, `AsyncStorage`, `MMKV`, or browser `localStorage`.
- **Modern React Hook**: `useOutbox` hook built with `useSyncExternalStore` for zero-tearing React 18 & 19 concurrent rendering.

---

## Installation

```bash
npm install resilient-outbox
# or
yarn add resilient-outbox
# or
npx expo install resilient-outbox
```

---

## Quick Start

### 1. Initialize the Queue

```typescript
import { OutboxQueue } from 'resilient-outbox';
import { GenericStorageAdapter } from 'resilient-outbox/adapters';
import AsyncStorage from '@react-native-async-storage/async-storage';

export const outbox = new OutboxQueue({
  name: 'api-outbox',
  storage: new GenericStorageAdapter(AsyncStorage),
  executor: async (mutation, context) => {
    // Resolve any client temporary IDs in the payload from parent mutations
    const resolvedPayload = context.resolvePayload(mutation.payload);

    const response = await fetch(`https://api.example.com/${mutation.type}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // Send the idempotency key to prevent duplicate writes on server
        'Idempotency-Key': mutation.idempotencyKey
      },
      body: JSON.stringify(resolvedPayload)
    });

    if (!response.ok) {
      const err = new Error(`Request failed with status ${response.status}`);
      (err as any).statusCode = response.status;
      throw err;
    }

    return response.json();
  }
});

// Hydrate persisted state on app launch
await outbox.hydrate();
```

---

### 2. Enqueue Mutations (with Dependencies)

```typescript
// 1. Create a Post with a temporary client ID
const postMutation = await outbox.enqueue({
  type: 'posts',
  payload: {
    tempId: 'temp_post_123',
    title: 'Offline First Architecture'
  }
});

// 2. Create a Comment that depends on the Post succeeding first
await outbox.enqueue({
  type: 'comments',
  payload: {
    postId: 'temp_post_123', // Will be swapped for the real server post ID upon sync
    content: 'Great post!'
  },
  dependsOn: postMutation.id // Won't execute until postMutation completes
});

// 3. Flush pending mutations when connected
await outbox.flush();
```

---

### 3. React Integration

```tsx
import React from 'react';
import { View, Text, Button } from 'react-native';
import { useOutbox } from 'resilient-outbox/react';
import { outbox } from './outbox';

export function SyncStatusBar() {
  const { pending, deadLetter, isSyncing, flush, retryAllDeadLetter } = useOutbox(outbox);

  if (pending === 0 && deadLetter === 0 && !isSyncing) {
    return null;
  }

  return (
    <View style={{ padding: 12, backgroundColor: '#f0f0f0' }}>
      {isSyncing && <Text>Syncing mutations with server...</Text>}
      {pending > 0 && <Text>{pending} action(s) pending offline sync.</Text>}
      {deadLetter > 0 && (
        <View>
          <Text style={{ color: 'red' }}>{deadLetter} action(s) failed validation.</Text>
          <Button title="Retry Failed" onPress={() => retryAllDeadLetter()} />
        </View>
      )}
      <Button title="Sync Now" onPress={() => flush()} disabled={isSyncing} />
    </View>
  );
}
```

---

## Error Handling & Dead-Letter Queue

| Error Category | HTTP Status | Behavior |
| :--- | :--- | :--- |
| **Client Error (Permanent)** | `400`, `401`, `403`, `404`, `422` | Quarantined to Dead-Letter Queue immediately. Does not block the queue. |
| **Rate Limit** | `429` | Retried using exponential backoff with jitter. |
| **Server Error (Transient)** | `500`, `502`, `503`, `504` | Retried up to `maxAttempts` (default: 5) with exponential backoff. |
| **Network Failure** | `TypeError: Failed to fetch`, timeout | Retried up to `maxAttempts` with backoff. |

```typescript
// Inspect quarantined mutations:
const failed = outbox.getDeadLetter();

// Retry a quarantined mutation manually:
await outbox.retryDeadLetter(failed[0].id);
```

---

## API Reference

### `new OutboxQueue(options)`
- `name: string`: Identifier used as storage namespace (`@resilient-outbox:<name>`).
- `storage?: OutboxStorageAdapter`: Storage backend (`MemoryStorageAdapter` or `GenericStorageAdapter`).
- `executor: MutationExecutor`: Async callback that performs the actual network request.
- `retryConfig?: RetryConfig`:
  - `maxAttempts?: number` (default: `5`)
  - `initialBackoffMs?: number` (default: `1000`)
  - `maxBackoffMs?: number` (default: `30000`)
  - `backoffMultiplier?: number` (default: `2`)
  - `jitter?: boolean` (default: `true`)
  - `isRetryable?: (error: unknown, statusCode?: number) => boolean`

### Methods
- `enqueue(options)`: Add a mutation to the queue. Returns the created `Mutation`.
- `flush()`: Process pending mutations. Coalesces concurrent calls into a single execution. Returns number of synced mutations.
- `hydrate()`: Restore persisted state from storage.
- `get(id)`: Get a specific mutation by ID.
- `getAll()`: Get all mutations in the queue.
- `getPending()`: Get all mutations with `status === 'pending'`.
- `getDeadLetter()`: Get all mutations quarantined in the Dead-Letter Queue.
- `retryDeadLetter(id)`: Reset a dead-letter mutation to `pending`.
- `retryAllDeadLetter()`: Reset all dead-letter mutations to `pending`.
- `startAutoFlush({ intervalMs?, flushOnReconnect? })`: Flush pending mutations on an interval (default 15s) and on the browser `online` event. Returns a stop function. In React Native, also call `flush()` from your AppState / NetInfo listeners.
- `stopAutoFlushing()`: Stop auto-flush.

### Durability

Queue state **and** the temp-ID map are persisted after every mutation, and writes are serialized, so a crash mid-flush or an app restart between a parent and its dependent child still resolves temp IDs correctly. Legacy (array-format) storage is migrated automatically on hydrate.
- `remove(id)`: Remove a mutation from the queue.
- `clear()`: Clear all mutations and cached ID mappings.
- `subscribe(listener)`: Subscribe to state changes. Returns an unsubscribe function.
- `getSnapshot()`: Returns referentially stable `{ total, pending, syncing, deadLetter, isSyncing }`.

---

## License

MIT © 2026 [Yazan Taha (yazanTah)](https://github.com/yazanTah)
