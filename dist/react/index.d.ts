import { O as OutboxQueue } from '../queue-L_9YRjmi.js';
import { Q as QueueStateSnapshot } from '../types-BRkMBWU8.js';

interface UseOutboxReturn extends QueueStateSnapshot {
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
declare function useOutbox(queue: OutboxQueue): UseOutboxReturn;

export { type UseOutboxReturn, useOutbox };
