import { O as OutboxQueueOptions, E as EnqueueOptions, M as Mutation, Q as QueueStateSnapshot } from './types-BRkMBWU8.mjs';

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
    private currentSnapshot;
    constructor(options: OutboxQueueOptions);
    get storageKey(): string;
    get hydrated(): boolean;
    hydrate(): Promise<void>;
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
    subscribe(listener: () => void): () => void;
    private updateSnapshot;
    private notify;
    getSnapshot(): QueueStateSnapshot;
}

export { OutboxQueue as O };
