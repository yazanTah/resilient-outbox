/**
 * Calculates exponential backoff delay with optional full jitter.
 * Formula: min(maxMs, initialMs * multiplier^(attempt - 1)) * (jitter ? random() : 1)
 */
export function calculateBackoffMs(
  attempt: number,
  initialMs: number = 1000,
  maxMs: number = 30000,
  multiplier: number = 2,
  useJitter: boolean = true
): number {
  if (attempt <= 0) return 0;
  const rawBackoff = Math.min(maxMs, initialMs * Math.pow(multiplier, attempt - 1));
  if (!useJitter) {
    return Math.round(rawBackoff);
  }
  // Full jitter: uniformly distributed between 0 and rawBackoff
  return Math.round(Math.random() * rawBackoff);
}

/** HTTP status codes that are 4xx/5xx but worth retrying. */
const RETRYABLE_4XX = new Set([408, 425, 429]);
/** 5xx codes that will not succeed on retry. */
const PERMANENT_5XX = new Set([501, 505]);

/**
 * Standard classification for HTTP and network errors:
 * - 408 Request Timeout, 425 Too Early and 429 Too Many Requests are retryable.
 * - Other 4xx errors are client errors and non-retryable (quarantined to the Dead-Letter Queue).
 * - 5xx errors are retryable, except 501 Not Implemented and 505 HTTP Version Not Supported.
 * - Errors without a status code (network drops, timeouts, aborted connections) are retryable.
 */
export function isDefaultRetryable(_error: unknown, statusCode?: number): boolean {
  if (statusCode === undefined) return true;
  if (RETRYABLE_4XX.has(statusCode)) return true;
  if (statusCode >= 400 && statusCode < 500) return false;
  if (PERMANENT_5XX.has(statusCode)) return false;
  return true;
}

/**
 * Extracts an HTTP status code from common error shapes:
 * `err.statusCode`, `err.status` (fetch wrappers, ky) and `err.response.status` (axios).
 */
export function getErrorStatusCode(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const e = error as { statusCode?: unknown; status?: unknown; response?: { status?: unknown } };
  const candidate = e.statusCode ?? e.status ?? e.response?.status;
  return typeof candidate === 'number' && Number.isFinite(candidate) ? candidate : undefined;
}

/**
 * Reads a server-requested retry delay from `err.retryAfterMs`, if the executor provided one
 * (e.g. parsed from a `Retry-After` header).
 */
export function getErrorRetryAfterMs(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const value = (error as { retryAfterMs?: unknown }).retryAfterMs;
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined;
}
