import { useMemo, useSyncExternalStore } from 'react';
import type { OutboxQueue } from '../core/queue.js';
import type { Mutation, MutationStatus, QueueStateSnapshot } from '../core/types.js';

export interface UseOutboxReturn extends QueueStateSnapshot {
  flush: () => Promise<number>;
  retryDeadLetter: (id: string) => Promise<boolean>;
  retryAllDeadLetter: () => Promise<number>;
  remove: (id: string) => Promise<boolean>;
  getPending: () => ReturnType<OutboxQueue['getPending']>;
  getDeadLetter: () => ReturnType<OutboxQueue['getDeadLetter']>;
}

/**
 * Subscribes to aggregate queue state (counts, `isSyncing`, `isHydrated`).
 * Built on `useSyncExternalStore`, so it is tear-free under React 18/19 concurrent rendering.
 * Re-renders only when a count or flag changes. Returned actions are referentially stable.
 */
export function useOutbox(queue: OutboxQueue): UseOutboxReturn {
  const store = useMemo(
    () => ({
      subscribe: (onChange: () => void) => queue.subscribe(onChange),
      getSnapshot: () => queue.getSnapshot()
    }),
    [queue]
  );
  const snapshot = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);

  const actions = useMemo(
    () => ({
      flush: () => queue.flush(),
      retryDeadLetter: (id: string) => queue.retryDeadLetter(id),
      retryAllDeadLetter: () => queue.retryAllDeadLetter(),
      remove: (id: string) => queue.remove(id),
      getPending: () => queue.getPending(),
      getDeadLetter: () => queue.getDeadLetter()
    }),
    [queue]
  );

  return useMemo(() => ({ ...snapshot, ...actions }), [snapshot, actions]);
}

/**
 * Subscribes to the mutations themselves, optionally filtered by status — e.g. to render a
 * "failed to sync" list with retry buttons. Returns shallow copies that are referentially
 * stable until the queue changes.
 */
export function useOutboxMutations(
  queue: OutboxQueue,
  status?: MutationStatus | ReadonlyArray<MutationStatus>
): ReadonlyArray<Mutation> {
  const store = useMemo(
    () => ({
      subscribe: (onChange: () => void) => queue.subscribe(onChange),
      getSnapshot: () => queue.getMutationsSnapshot()
    }),
    [queue]
  );
  const all = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);

  const key = status === undefined ? '' : typeof status === 'string' ? status : status.join('|');
  return useMemo(() => {
    if (!key) return all;
    const wanted = new Set(key.split('|'));
    return all.filter(m => wanted.has(m.status));
  }, [all, key]);
}
