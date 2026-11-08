import { OutboxQueue } from '../../src/core/queue.js';
import { httpError } from '../helpers/index.js';

describe('Auto-flush', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  test('flushes on start and on an interval, and stops cleanly', async () => {
    const executor = jest.fn(async () => ({}));
    const queue = new OutboxQueue({ name: 'interval', executor });
    await queue.enqueue({ type: 'A', payload: {} });

    const stop = queue.startAutoFlush({ intervalMs: 1000, flushOnReconnect: false, flushOnEnqueue: false });
    await jest.advanceTimersByTimeAsync(0);
    expect(executor).toHaveBeenCalledTimes(1);

    await queue.enqueue({ type: 'B', payload: {} });
    await jest.advanceTimersByTimeAsync(1000);
    expect(executor).toHaveBeenCalledTimes(2);

    stop();
    await queue.enqueue({ type: 'C', payload: {} });
    await jest.advanceTimersByTimeAsync(5000);
    expect(executor).toHaveBeenCalledTimes(2);
  });

  test('flushOnEnqueue sends new mutations immediately', async () => {
    const executor = jest.fn(async () => ({}));
    const queue = new OutboxQueue({ name: 'enq', executor });
    queue.startAutoFlush({ intervalMs: 60_000, flushOnReconnect: false });
    await queue.enqueue({ type: 'A', payload: {} });
    await jest.advanceTimersByTimeAsync(0);
    expect(executor).toHaveBeenCalledTimes(1);
    queue.destroy();
  });

  test('wakes exactly when a backoff expires instead of waiting for the interval', async () => {
    let calls = 0;
    const queue = new OutboxQueue({
      name: 'wake',
      retryConfig: { jitter: false, initialBackoffMs: 2000 },
      executor: async () => {
        if (++calls === 1) throw httpError(503);
        return {};
      }
    });
    queue.startAutoFlush({ intervalMs: 60_000, flushOnReconnect: false });
    const m = await queue.enqueue({ type: 'A', payload: {} });
    await jest.advanceTimersByTimeAsync(0);
    expect(m.status).toBe('pending');

    await jest.advanceTimersByTimeAsync(1999);
    expect(calls).toBe(1);
    await jest.advanceTimersByTimeAsync(1);
    expect(calls).toBe(2);
    expect(m.status).toBe('completed');
    queue.stopAutoFlushing();
  });

  test('flushes when the online event fires, and restarting replaces the previous config', async () => {
    const handlers = new Map<string, () => void>();
    const g = globalThis as { addEventListener?: unknown; removeEventListener?: unknown };
    const origAdd = g.addEventListener;
    const origRemove = g.removeEventListener;
    g.addEventListener = (type: string, fn: () => void) => handlers.set(type, fn);
    g.removeEventListener = (type: string) => handlers.delete(type);

    try {
      let online = false;
      const executor = jest.fn(async () => ({}));
      const queue = new OutboxQueue({ name: 'online', executor, isOnline: () => online });
      queue.startAutoFlush({ intervalMs: 60_000 });
      queue.startAutoFlush({ intervalMs: 60_000 }); // replaces, does not double-register
      await queue.enqueue({ type: 'A', payload: {} });
      await jest.advanceTimersByTimeAsync(0);
      expect(executor).not.toHaveBeenCalled();

      online = true;
      handlers.get('online')!();
      await jest.advanceTimersByTimeAsync(0);
      expect(executor).toHaveBeenCalledTimes(1);

      queue.stopAutoFlushing();
      expect(handlers.has('online')).toBe(false);
    } finally {
      g.addEventListener = origAdd;
      g.removeEventListener = origRemove;
    }
  });

  test('auto-flush failures are logged, not thrown', async () => {
    const logger = { warn: jest.fn(), error: jest.fn() };
    const queue = new OutboxQueue({
      name: 'logfail',
      logger,
      storage: {
        getItem: async () => {
          throw new Error('nope');
        },
        setItem: async () => {},
        removeItem: async () => {}
      },
      executor: async () => ({})
    });
    queue.startAutoFlush({ flushOnReconnect: false });
    await jest.advanceTimersByTimeAsync(0);
    queue.destroy();
    await jest.advanceTimersByTimeAsync(20_000);
    expect(logger.error).toHaveBeenCalled();
  });
});
