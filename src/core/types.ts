export type MutationStatus = 'pending' | 'syncing' | 'dead_letter' | 'completed';

/**
 * Machine-readable reason a mutation ended up in the Dead-Letter Queue.
 *
 * - `NON_RETRYABLE`: the executor threw an error classified as permanent (e.g. 4xx).
 * - `MAX_ATTEMPTS`: transient failures exhausted `maxAttempts`.
 * - `PARENT_FAILED`: the mutation it depends on was dead-lettered.
 * - `PARENT_REMOVED`: the mutation it depends on was removed before completing.
 */
export type MutationErrorCode = 'NON_RETRYABLE' | 'MAX_ATTEMPTS' | 'PARENT_FAILED' | 'PARENT_REMOVED';

export interface MutationError {
  message: string;
  code?: MutationErrorCode;
  statusCode?: number;
  timestamp: number;
}

export interface Mutation<TPayload = unknown, TResult = unknown> {
  id: string;
  idempotencyKey: string;
  type: string;
  payload: TPayload;
  /** Client-generated temporary ID that will be mapped to the server ID on success. */
  tempId?: string;
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
  /** Read-only view of resolved `tempId -> serverId` mappings. */
  idMap: ReadonlyMap<string, unknown>;
  /** Returns the server ID for a temporary ID, or the input unchanged if it is not mapped. */
  resolveId: (id: unknown) => unknown;
  /** Deep-copies the payload, replacing every string that is a known temporary ID with its server ID. */
  resolvePayload: <T = unknown>(payload: T) => T;
  /** Aborted when `executorTimeoutMs` elapses or the queue is destroyed. Pass it to `fetch`. */
  signal: AbortSignal;
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

export interface OutboxLogger {
  warn: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
}

export interface OutboxQueueOptions {
  /** Unique name; also the storage key namespace. Use one queue instance per name. */
  name: string;
  storage?: OutboxStorageAdapter;
  executor: MutationExecutor;
  retryConfig?: RetryConfig;
  autoPersist?: boolean;
  /**
   * Extracts the server-generated ID from an executor result, used to map `tempId`.
   * Defaults to `result.id`.
   */
  getServerId?: (result: unknown, mutation: Mutation) => unknown;
  /**
   * How long an unused `tempId -> serverId` mapping is kept (measured from its last use).
   * Defaults to 7 days. Set to `Infinity` to keep mappings forever.
   */
  idMapTtlMs?: number;
  /** Aborts and fails (retryably) an executor call that takes longer than this. Default: no timeout. */
  executorTimeoutMs?: number;
  /**
   * Connectivity probe. While it returns `false`, `flush()` is a no-op so offline time does not
   * burn retry attempts. Defaults to `navigator.onLine` when available, otherwise always online.
   */
  isOnline?: () => boolean | Promise<boolean>;
  /** Logger for internal warnings. Pass `null` to silence. Defaults to `console`. */
  logger?: OutboxLogger | null;
}

export interface EnqueueOptions<TPayload = unknown> {
  type: string;
  payload: TPayload;
  idempotencyKey?: string;
  /**
   * Temporary client ID for the entity this mutation creates. Once it succeeds, the ID is
   * mapped to the server ID and substituted into dependent payloads by `resolvePayload`.
   * If omitted, `payload.tempId` is used for backwards compatibility.
   */
  tempId?: string;
  dependsOn?: string;
  maxAttempts?: number;
}

export interface AutoFlushOptions {
  /** Periodic flush interval. Default 15000ms. */
  intervalMs?: number;
  /** Flush when the `online` event fires (web / React Native Web). Default true. */
  flushOnReconnect?: boolean;
  /** Flush right after each enqueue. Default true. */
  flushOnEnqueue?: boolean;
}

export interface QueueStateSnapshot {
  total: number;
  pending: number;
  syncing: number;
  deadLetter: number;
  isSyncing: boolean;
  isHydrated: boolean;
}

export interface OutboxEventMap {
  enqueued: { mutation: Mutation };
  completed: { mutation: Mutation; result: unknown };
  retry: { mutation: Mutation; error: unknown; delayMs: number };
  dead_letter: { mutation: Mutation; error: MutationError };
  storage_error: { operation: 'hydrate' | 'persist'; error: unknown };
}

export type OutboxEventName = keyof OutboxEventMap;
