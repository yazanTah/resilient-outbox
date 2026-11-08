import { useSyncExternalStore } from 'react';
import type { OutboxQueue } from '../core/queue.js';
import type { QueueStateSnapshot } from '../core/types.js';

export interface UseOutboxReturn extends QueueStateSnapshot {
  flush: () => Promise<number>;
  retryDeadLetter: (id: string) => Promise<boolean>;
  retryAllDeadLetter: () => Promise<number>;
  getPending: () => ReturnType<OutboxQueue['getPending']>;
  getDeadLetter: () => ReturnType<OutboxQueue['getDeadLetter']>;
}

/**
 * Modern React hook for consuming Outbox queue state.
 * Built with useSyncExternalStore to eliminate tearing in React 18 & 19 concurrent mode.
 */
export function useOutbox(queue: OutboxQueue): UseOutboxReturn {
  const snapshot = useSyncExternalStore(
    (onStoreChange) => queue.subscribe(onStoreChange),
    () => queue.getSnapshot(),
    () => queue.getSnapshot()
  );

  return {
    ...snapshot,
    flush: () => queue.flush(),
    retryDeadLetter: (id: string) => queue.retryDeadLetter(id),
    retryAllDeadLetter: () => queue.retryAllDeadLetter(),
    getPending: () => queue.getPending(),
    getDeadLetter: () => queue.getDeadLetter()
  };
}
