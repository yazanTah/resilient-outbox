import { OutboxQueue } from '../../src/core/queue.js';
import { MemoryStorageAdapter } from '../../src/adapters/index.js';
import type { Mutation, SyncContext } from '../../src/core/types.js';
import { httpError } from '../helpers/index.js';

describe('Durability across restarts', () => {
  test('idMap survives a restart so children resolve temp IDs', async () => {
    const storage = new MemoryStorageAdapter();
    const seen: unknown[] = [];
    const executor = async (m: Mutation, ctx: SyncContext) => {
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

  test('dead-letter state and error codes survive a restart', async () => {
    const storage = new MemoryStorageAdapter();
    const q1 = new OutboxQueue({
      name: 'dlq-restart',
      storage,
      executor: async () => {
        throw httpError(403);
      }
    });
    const parent = await q1.enqueue({ type: 'P', payload: {} });
    await q1.enqueue({ type: 'C', payload: {}, dependsOn: parent.id });
    await q1.flush();

    const q2 = new OutboxQueue({ name: 'dlq-restart', storage, executor: async () => ({}) });
    await q2.hydrate();
    expect(q2.getDeadLetter().map(m => m.error?.code)).toEqual(['NON_RETRYABLE', 'PARENT_FAILED']);
    await q2.retryDeadLetter(parent.id);
    expect(await q2.flush()).toBe(2);
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
});
