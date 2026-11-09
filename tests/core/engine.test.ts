import { OutboxQueue } from '../../src/core/queue.js';
import { OutboxDestroyedError, OutboxTimeoutError } from '../../src/core/errors.js';
import type { Mutation, OutboxEventName, SyncContext } from '../../src/core/types.js';
import { httpError, silentLogger } from '../helpers/index.js';

describe('Construction & validation', () => {
  test('rejects invalid options', () => {
    expect(() => new OutboxQueue({ name: '', executor: async () => ({}) })).toThrow(TypeError);
    expect(() => new OutboxQueue({ name: 'x' } as never)).toThrow(TypeError);
  });

  test('rejects an empty mutation type', async () => {
    const queue = new OutboxQueue({ name: 'v', executor: async () => ({}) });
    await expect(queue.enqueue({ type: '', payload: {} })).rejects.toThrow(TypeError);
  });
});

describe('Error classification', () => {
  const run = async (error: unknown, options: { maxAttempts?: number } = {}) => {
    const queue = new OutboxQueue({
      name: 'cls',
      retryConfig: { jitter: false, initialBackoffMs: 1000, ...options },
      executor: async () => {
        throw error;
      }
    });
    const m = await queue.enqueue({ type: 'A', payload: {} });
    await queue.flush();
    return m;
  };

  test('reads axios-style err.response.status', async () => {
    const m = await run(Object.assign(new Error('bad'), { response: { status: 400 } }));
    expect(m.error).toMatchObject({ code: 'NON_RETRYABLE', statusCode: 400 });
  });

  test('reads err.status', async () => {
    const m = await run(Object.assign(new Error('gone'), { status: 410 }));
    expect(m.status).toBe('dead_letter');
  });

  test('501 is permanent; 408 and 429 retry', async () => {
    expect((await run(httpError(501))).status).toBe('dead_letter');
    expect((await run(httpError(408))).status).toBe('pending');
    expect((await run(httpError(429))).status).toBe('pending');
  });

  test('non-Error throwables are recorded', async () => {
    const m = await run('boom', { maxAttempts: 1 });
    expect(m.error).toMatchObject({ code: 'MAX_ATTEMPTS', message: 'Exceeded max attempts (1): boom' });
  });

  test('a 409 for a key still in progress is retried after Retry-After, a plain 409 is dead-lettered', async () => {
    const inProgress = await run(Object.assign(httpError(409), { retryAfterMs: 2000 }));
    expect(inProgress.status).toBe('pending');
    expect(inProgress.attempts).toBe(1);
    const conflict = await run(httpError(409));
    expect(conflict.error?.code).toBe('NON_RETRYABLE');
  });

  test('honors a server-provided retryAfterMs', async () => {
    const before = Date.now();
    const m = await run(Object.assign(httpError(429), { retryAfterMs: 60_000 }));
    expect(m.nextAttemptAt).toBeGreaterThanOrEqual(before + 60_000);
  });

  test('a custom isRetryable overrides the default', async () => {
    const queue = new OutboxQueue({
      name: 'custom',
      retryConfig: { isRetryable: () => false },
      executor: async () => {
        throw httpError(503);
      }
    });
    const m = await queue.enqueue({ type: 'A', payload: {} });
    await queue.flush();
    expect(m.error?.code).toBe('NON_RETRYABLE');
  });
});

describe('Connectivity', () => {
  test('flush is a no-op while offline, so offline time burns no attempts', async () => {
    let online = false;
    const executor = jest.fn(async () => ({}));
    const queue = new OutboxQueue({ name: 'off', executor, isOnline: async () => online });
    const m = await queue.enqueue({ type: 'A', payload: {} });

    expect(await queue.flush()).toBe(0);
    expect(executor).not.toHaveBeenCalled();
    expect(m.attempts).toBe(0);

    online = true;
    expect(await queue.flush()).toBe(1);
  });

  test('a throwing isOnline probe is treated as online', async () => {
    const queue = new OutboxQueue({
      name: 'probe',
      isOnline: () => {
        throw new Error('probe broke');
      },
      executor: async () => ({})
    });
    await queue.enqueue({ type: 'A', payload: {} });
    expect(await queue.flush()).toBe(1);
  });

  test('defaults to navigator.onLine when present', async () => {
    const original = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
    Object.defineProperty(globalThis, 'navigator', { value: { onLine: false }, configurable: true });
    try {
      const queue = new OutboxQueue({ name: 'nav', executor: async () => ({}) });
      await queue.enqueue({ type: 'A', payload: {} });
      expect(await queue.flush()).toBe(0);
    } finally {
      if (original) Object.defineProperty(globalThis, 'navigator', original);
      else delete (globalThis as { navigator?: unknown }).navigator;
    }
  });
});

describe('Timeouts & cancellation', () => {
  test('executorTimeoutMs aborts the signal and schedules a retry', async () => {
    let signal!: AbortSignal;
    const retries: unknown[] = [];
    const queue = new OutboxQueue({
      name: 'timeout',
      executorTimeoutMs: 20,
      executor: (_m: Mutation, ctx: SyncContext) => {
        signal = ctx.signal;
        return new Promise(() => {}); // hangs forever
      }
    });
    queue.on('retry', e => retries.push(e.error));
    const m = await queue.enqueue({ type: 'A', payload: {} });
    await queue.flush();

    expect(signal.aborted).toBe(true);
    expect(m.status).toBe('pending');
    expect(m.attempts).toBe(1);
    expect(retries[0]).toBeInstanceOf(OutboxTimeoutError);
    expect((retries[0] as OutboxTimeoutError).timeoutMs).toBe(20);
  });

  test('destroy() aborts in-flight work without consuming an attempt, then rejects calls', async () => {
    const queue = new OutboxQueue({
      name: 'destroy',
      executor: (_m: Mutation, ctx: SyncContext) =>
        new Promise((_, reject) => ctx.signal.addEventListener('abort', () => reject(new Error('aborted'))))
    });
    const m = await queue.enqueue({ type: 'A', payload: {} });
    const flushing = queue.flush();
    await new Promise(r => setTimeout(r, 0));
    queue.destroy();
    queue.destroy(); // idempotent
    await flushing;

    expect(m.status).toBe('pending');
    expect(m.attempts).toBe(0);
    await expect(queue.enqueue({ type: 'B', payload: {} })).rejects.toBeInstanceOf(OutboxDestroyedError);
    await expect(queue.flush()).rejects.toBeInstanceOf(OutboxDestroyedError);
    expect(() => queue.startAutoFlush()).toThrow(OutboxDestroyedError);
  });
});

describe('Flush scheduling', () => {
  test('a flush requested during a flush runs another pass for newly enqueued work', async () => {
    const seen: string[] = [];
    const queue: OutboxQueue = new OutboxQueue({
      name: 'rerun',
      executor: async m => {
        seen.push(m.type);
        if (m.type === 'FIRST') {
          await queue.enqueue({ type: 'SECOND', payload: {} });
          void queue.flush();
        }
        return {};
      }
    });
    await queue.enqueue({ type: 'FIRST', payload: {} });
    expect(await queue.flush()).toBe(2);
    expect(seen).toEqual(['FIRST', 'SECOND']);
  });

  test('retryAllDeadLetter revives everything and reports the count', async () => {
    let fail = true;
    const queue = new OutboxQueue({
      name: 'all',
      executor: async () => {
        if (fail) throw httpError(400);
        return {};
      }
    });
    await queue.enqueue({ type: 'A', payload: {} });
    await queue.enqueue({ type: 'B', payload: {} });
    await queue.flush();
    expect(queue.getDeadLetter()).toHaveLength(2);
    fail = false;
    expect(await queue.retryAllDeadLetter()).toBe(2);
    expect(await queue.retryAllDeadLetter()).toBe(0);
    expect(await queue.flush()).toBe(2);
  });
});

describe('Events & listeners', () => {
  test('emits lifecycle events and supports unsubscribe', async () => {
    const events: OutboxEventName[] = [];
    const queue = new OutboxQueue({
      name: 'events',
      retryConfig: { maxAttempts: 2, jitter: false, initialBackoffMs: 0 },
      executor: async (m: Mutation) => {
        if (m.type === 'FAIL') throw httpError(500);
        return {};
      }
    });
    const names: OutboxEventName[] = ['enqueued', 'completed', 'retry', 'dead_letter'];
    const offs = names.map(n => queue.on(n, () => events.push(n)));

    await queue.enqueue({ type: 'OK', payload: {} });
    const fail = await queue.enqueue({ type: 'FAIL', payload: {} });
    await queue.flush();
    fail.nextAttemptAt = 0;
    await queue.flush();
    expect(events).toEqual(['enqueued', 'enqueued', 'completed', 'retry', 'dead_letter']);

    offs.forEach(off => off());
    await queue.enqueue({ type: 'OK', payload: {} });
    expect(events).toHaveLength(5);
  });

  test('throwing listeners are isolated and logged', async () => {
    const logger = { warn: jest.fn(), error: jest.fn() };
    const queue = new OutboxQueue({ name: 'iso', logger, executor: async () => ({}) });
    queue.subscribe(() => {
      throw new Error('ui');
    });
    queue.on('enqueued', () => {
      throw new Error('event');
    });
    const m = await queue.enqueue({ type: 'A', payload: {} });
    await queue.flush();
    expect(m.status).toBe('completed');
    expect(logger.error).toHaveBeenCalled();
  });

  test('logger: null silences output', async () => {
    const spy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const queue = new OutboxQueue({ name: 'quiet', logger: null, executor: async () => ({}) });
    queue.subscribe(() => {
      throw new Error('ui');
    });
    await queue.enqueue({ type: 'A', payload: {} });
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  test('getSnapshot is referentially stable until counts change', async () => {
    const queue = new OutboxQueue({ name: 'snap', logger: silentLogger, executor: async () => ({}) });
    await queue.hydrate();
    const a = queue.getSnapshot();
    await queue.retryAllDeadLetter();
    expect(queue.getSnapshot()).toBe(a);
    await queue.enqueue({ type: 'A', payload: {} });
    expect(queue.getSnapshot()).not.toBe(a);

    const ms = queue.getMutationsSnapshot();
    expect(queue.getMutationsSnapshot()).toBe(ms);
    expect(queue.get(ms[0]!.id)).toBeDefined();
  });
});
