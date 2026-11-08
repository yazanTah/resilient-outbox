type MutationStatus = 'pending' | 'syncing' | 'dead_letter' | 'completed';
interface MutationError {
    message: string;
    statusCode?: number;
    timestamp: number;
}
interface Mutation<TPayload = unknown, TResult = unknown> {
    id: string;
    idempotencyKey: string;
    type: string;
    payload: TPayload;
    dependsOn?: string;
    createdAt: number;
    attempts: number;
    maxAttempts: number;
    nextAttemptAt: number;
    status: MutationStatus;
    error?: MutationError;
    result?: TResult;
}
interface SyncContext {
    idMap: Map<string, unknown>;
    resolvePayload: <T = unknown>(payload: T) => T;
}
type MutationExecutor<TPayload = unknown, TResult = unknown> = (mutation: Mutation<TPayload, TResult>, context: SyncContext) => Promise<TResult>;
interface RetryConfig {
    maxAttempts?: number;
    initialBackoffMs?: number;
    maxBackoffMs?: number;
    backoffMultiplier?: number;
    jitter?: boolean;
    isRetryable?: (error: unknown, statusCode?: number) => boolean;
}
interface OutboxStorageAdapter {
    getItem(key: string): Promise<string | null>;
    setItem(key: string, value: string): Promise<void>;
    removeItem(key: string): Promise<void>;
}
interface OutboxQueueOptions {
    name: string;
    storage?: OutboxStorageAdapter;
    executor: MutationExecutor;
    retryConfig?: RetryConfig;
    autoPersist?: boolean;
}
interface EnqueueOptions<TPayload = unknown> {
    type: string;
    payload: TPayload;
    idempotencyKey?: string;
    dependsOn?: string;
    maxAttempts?: number;
}
interface QueueStateSnapshot {
    total: number;
    pending: number;
    syncing: number;
    deadLetter: number;
    isSyncing: boolean;
}

export type { EnqueueOptions as E, Mutation as M, OutboxQueueOptions as O, QueueStateSnapshot as Q, RetryConfig as R, SyncContext as S, MutationError as a, MutationExecutor as b, MutationStatus as c, OutboxStorageAdapter as d };
