export { E as EnqueueOptions, M as Mutation, a as MutationError, b as MutationExecutor, c as MutationStatus, O as OutboxQueueOptions, d as OutboxStorageAdapter, Q as QueueStateSnapshot, R as RetryConfig, S as SyncContext } from './types-BRkMBWU8.js';
export { O as OutboxQueue } from './queue-8KhTXxYg.js';
export { GenericKeyValueStorage, GenericStorageAdapter, MemoryStorageAdapter } from './adapters/index.js';

/**
 * Calculates exponential backoff delay with optional full jitter.
 * Formula: min(maxMs, initialMs * (multiplier ^ attempt)) * (jitter ? random() : 1)
 */
declare function calculateBackoffMs(attempt: number, initialMs?: number, maxMs?: number, multiplier?: number, useJitter?: boolean): number;
/**
 * Standard classification for HTTP and network errors:
 * - 4xx errors are client errors and non-retryable (quarantined to Dead-Letter Queue).
 * - 429 Too Many Requests is retryable.
 * - 5xx errors and network failures (status 0 / undefined) are retryable.
 */
declare function isDefaultRetryable(error: unknown, statusCode?: number): boolean;

/**
 * Generates a standard RFC4122 v4 UUID with zero dependencies.
 * Uses globalThis.crypto.randomUUID when available, with a cryptographically secure fallback.
 */
declare function generateUUID(): string;

export { calculateBackoffMs, generateUUID, isDefaultRetryable };
