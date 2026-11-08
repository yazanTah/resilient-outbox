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
