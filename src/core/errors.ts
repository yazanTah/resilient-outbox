/** Thrown into the executor's flow when a call exceeds `executorTimeoutMs`. Retryable by default. */
export class OutboxTimeoutError extends Error {
  readonly timeoutMs: number;

  constructor(timeoutMs: number) {
    super(`Executor timed out after ${timeoutMs}ms`);
    this.name = 'OutboxTimeoutError';
    this.timeoutMs = timeoutMs;
  }
}

/** Thrown when using a queue after `destroy()` has been called. */
export class OutboxDestroyedError extends Error {
  constructor(name: string) {
    super(`[resilient-outbox:${name}] Queue has been destroyed`);
    this.name = 'OutboxDestroyedError';
  }
}
