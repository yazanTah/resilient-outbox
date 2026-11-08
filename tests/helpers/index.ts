import type { OutboxStorageAdapter } from '../../src/core/types.js';

export function httpError(status: number, message = `HTTP ${status}`): Error & { statusCode: number } {
  return Object.assign(new Error(message), { statusCode: status });
}

export const silentLogger = { warn: jest.fn(), error: jest.fn() };

/** Storage whose operations can be made to fail on demand, recording every write. */
export class FlakyStorage implements OutboxStorageAdapter {
  data = new Map<string, string>();
  failReads = 0;
  failWrites = 0;
  writes: string[] = [];

  async getItem(key: string): Promise<string | null> {
    if (this.failReads > 0) {
      this.failReads--;
      throw new Error('read failed');
    }
    return this.data.get(key) ?? null;
  }

  async setItem(key: string, value: string): Promise<void> {
    if (this.failWrites > 0) {
      this.failWrites--;
      throw new Error('write failed');
    }
    this.writes.push(value);
    this.data.set(key, value);
  }

  async removeItem(key: string): Promise<void> {
    this.data.delete(key);
  }
}

type LockCallback = () => Promise<unknown>;

/**
 * Minimal Web Locks (exclusive mode) implementation for runtimes that lack `navigator.locks`
 * (Node < 24). Requests for a name are granted in FIFO order; a request aborted while still
 * queued rejects with an AbortError, as the spec requires.
 */
export class FakeLockManager {
  private queues = new Map<string, { start: () => void; started: boolean }[]>();

  request(name: string, a: LockCallback | { signal?: AbortSignal }, b?: LockCallback): Promise<unknown> {
    const callback = typeof a === 'function' ? a : b!;
    const signal = typeof a === 'function' ? undefined : a.signal;
    let queue = this.queues.get(name);
    if (!queue) {
      queue = [];
      this.queues.set(name, queue);
    }
    const q = queue;

    return new Promise((resolve, reject) => {
      const entry = {
        started: false,
        start: () => {
          entry.started = true;
          callback()
            .then(resolve, reject)
            .finally(() => {
              q.shift();
              q[0]?.start();
            });
        }
      };
      signal?.addEventListener('abort', () => {
        if (entry.started) return;
        q.splice(q.indexOf(entry), 1);
        reject(Object.assign(new Error('Lock request aborted'), { name: 'AbortError' }));
      });
      q.push(entry);
      if (q.length === 1) entry.start();
    });
  }
}
