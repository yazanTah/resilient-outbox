import { O as OutboxQueueOptions, E as EnqueueOptions, M as Mutation, Q as QueueStateSnapshot } from './types-BRkMBWU8.js';

declare class OutboxQueue {
    readonly name: string;
    private storage;
    private executor;
    private retryConfig;
    private autoPersist;
    private mutations;
    private idMap;
    private listeners;
    private isSyncing;
    private activeFlushPromise;
    private isHydrated;
    private persistChain;
    private stopAutoFlush;
    private currentSnapshot;
    constructor(options: OutboxQueueOptions);
    get storageKey(): string;
    get hydrated(): boolean;
    hydrate(): Promise<void>;
    /**
     * Writes are serialized so concurrent enqueue/flush calls can never land in
     * storage out of order. The state is serialized at write time, so the latest
     * write always reflects the latest in-memory state.
     */
    private persist;
    enqueue<TPayload = unknown, TResult = unknown>(options: EnqueueOptions<TPayload>): Promise<Mutation<TPayload, TResult>>;
    get(id: string): Mutation | undefined;
    getAll(): ReadonlyArray<Mutation>;
    getPending(): Mutation[];
    getDeadLetter(): Mutation[];
    remove(id: string): Promise<boolean>;
    clear(): Promise<void>;
    retryDeadLetter(id: string): Promise<boolean>;
    retryAllDeadLetter(): Promise<number>;
    private createSyncContext;
    flush(): Promise<number>;
    private executeFlush;
    /**
     * Periodically flushes the queue and flushes immediately when the device
     * comes back online (where `online` events exist, e.g. web / React Native Web).
     * Returns a function that stops auto-flushing. Calling it again replaces the
     * previous configuration. For React Native, call `queue.flush()` from an
     * AppState / NetInfo listener in addition.
     */
    startAutoFlush(options?: {
        intervalMs?: number;
        flushOnReconnect?: boolean;
    }): () => void;
    stopAutoFlushing(): void;
    subscribe(listener: () => void): () => void;
    private updateSnapshot;
    private notify;
    getSnapshot(): QueueStateSnapshot;
}

export { OutboxQueue as O };
