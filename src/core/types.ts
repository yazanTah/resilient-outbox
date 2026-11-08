export type MutationStatus = 'pending' | 'syncing' | 'dead_letter' | 'completed';

export interface MutationError {
  message: string;
  statusCode?: number;
  timestamp: number;
}

export interface Mutation<TPayload = unknown, TResult = unknown> {
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

export interface SyncContext {
  idMap: Map<string, unknown>;
  resolvePayload: <T = unknown>(payload: T) => T;
}

export type MutationExecutor<TPayload = unknown, TResult = unknown> = (
  mutation: Mutation<TPayload, TResult>,
  context: SyncContext
) => Promise<TResult>;

export interface RetryConfig {
  maxAttempts?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  backoffMultiplier?: number;
  jitter?: boolean;
  isRetryable?: (error: unknown, statusCode?: number) => boolean;
}

export interface OutboxStorageAdapter {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
  removeItem(key: string): Promise<void>;
}

export interface OutboxQueueOptions {
  name: string;
  storage?: OutboxStorageAdapter;
  executor: MutationExecutor;
  retryConfig?: RetryConfig;
  autoPersist?: boolean;
}

export interface EnqueueOptions<TPayload = unknown> {
  type: string;
  payload: TPayload;
  idempotencyKey?: string;
  dependsOn?: string;
  maxAttempts?: number;
}

export interface QueueStateSnapshot {
  total: number;
  pending: number;
  syncing: number;
  deadLetter: number;
  isSyncing: boolean;
}
