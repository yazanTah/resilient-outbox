/**
 * Calculates exponential backoff delay with optional full jitter.
 * Formula: min(maxMs, initialMs * (multiplier ^ attempt)) * (jitter ? random() : 1)
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

/**
 * Standard classification for HTTP and network errors:
 * - 4xx errors are client errors and non-retryable (quarantined to Dead-Letter Queue).
 * - 429 Too Many Requests is retryable.
 * - 5xx errors and network failures (status 0 / undefined) are retryable.
 */
export function isDefaultRetryable(error: unknown, statusCode?: number): boolean {
  if (statusCode !== undefined) {
    if (statusCode === 429) return true; // Rate limit
    if (statusCode >= 400 && statusCode < 500) return false; // Client errors -> DLQ
    if (statusCode >= 500) return true; // Server errors -> retry
  }

  // Network drops, timeout errors, connection aborts
  if (error instanceof Error) {
    const msg = error.message.toLowerCase();
    if (
      msg.includes('network') ||
      msg.includes('timeout') ||
      msg.includes('failed to fetch') ||
      msg.includes('econnreset') ||
      msg.includes('econnrefused')
    ) {
      return true;
    }
  }

  return true;
}
