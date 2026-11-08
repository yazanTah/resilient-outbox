import { OutboxQueue } from '../../src/core/queue.js';
import { MemoryStorageAdapter } from '../../src/adapters/index.js';

describe('Durability & auto-flush', () => {
  test('idMap survives a restart so children resolve temp IDs', async () => {
    const storage = new MemoryStorageAdapter();
    const seen: unknown[] = [];
    const executor = async (m: any, ctx: any) => {
      seen.push(ctx.resolvePayload(m.payload));
      return m.type === 'CREATE_POST' ? { id: 'srv_1' } : { ok: true };
    };

    const q1 = new OutboxQueue({ name: 'r', storage, executor });
    const parent = await q1.enqueue({ type: 'CREATE_POST', payload: { tempId: 'temp_1' } });
    await q1.flush();
    await q1.enqueue({ type: 'COMMENT', payload: { postId: 'temp_1' }, dependsOn: parent.id });

    // "Restart": a fresh queue over the same storage
    const q2 = new OutboxQueue({ name: 'r', storage, executor });
    await q2.hydrate();
    await q2.flush();
    expect(seen[1]).toEqual({ postId: 'srv_1' });
  });

  test('checkpoints after each mutation (crash mid-flush keeps progress)', async () => {
    const storage = new MemoryStorageAdapter();
    let calls = 0;
    const q = new OutboxQueue({
      name: 'c',
      storage,
      executor: async () => {
        if (++calls === 2) {
          const stored = JSON.parse((await storage.getItem(q.storageKey))!);
          // first mutation already removed from storage as completed
          expect(stored.mutations).toHaveLength(1);
        }
        return {};
      }
    });
    await q.enqueue({ type: 'A', payload: {} });
    await q.enqueue({ type: 'B', payload: {} });
    await q.flush();
    expect(calls).toBe(2);
  });

  test('loads legacy array-format storage', async () => {
    const storage = new MemoryStorageAdapter();
    await storage.setItem('@resilient-outbox:l', JSON.stringify([
      { id: '1', idempotencyKey: 'k', type: 'T', payload: {}, createdAt: 0, attempts: 0, maxAttempts: 5, nextAttemptAt: 0, status: 'syncing' }
    ]));
    const q = new OutboxQueue({ name: 'l', storage, executor: async () => ({}) });
    await q.hydrate();
    expect(q.getPending()).toHaveLength(1);
  });

  test('writes never overlap and the last write reflects latest state', async () => {
    let active = 0;
    let maxActive = 0;
    let last = 0;
    const slow = {
      getItem: async () => null,
      removeItem: async () => {},
      setItem: async (_k: string, v: string) => {
        active++;
        maxActive = Math.max(maxActive, active);
        await new Promise(r => setTimeout(r, 10));
        last = JSON.parse(v).mutations.length;
        active--;
      }
    };
    const q = new OutboxQueue({ name: 's', storage: slow, executor: async () => ({}) });
    await Promise.all([q.enqueue({ type: 'A', payload: {} }), q.enqueue({ type: 'B', payload: {} })]);
    expect(maxActive).toBe(1);
    expect(last).toBe(2);
  });

  test('startAutoFlush flushes pending mutations on an interval and can stop', async () => {
    jest.useFakeTimers();
    try {
      const executor = jest.fn(async () => ({}));
      const q = new OutboxQueue({ name: 'a', executor });
      await q.enqueue({ type: 'A', payload: {} });
      const stop = q.startAutoFlush({ intervalMs: 1000, flushOnReconnect: false });
      await jest.advanceTimersByTimeAsync(1100);
      expect(executor).toHaveBeenCalledTimes(1);
      stop();
      await q.enqueue({ type: 'B', payload: {} });
      await jest.advanceTimersByTimeAsync(5000);
      expect(executor).toHaveBeenCalledTimes(1);
    } finally {
      jest.useRealTimers();
    }
  });
});
