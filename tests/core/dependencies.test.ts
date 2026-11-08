import { OutboxQueue } from '../../src/core/queue.js';
import type { Mutation, SyncContext } from '../../src/core/types.js';
import { httpError, silentLogger } from '../helpers/index.js';

describe('Dependency chains & dead-letter cascading', () => {
  test('a failing parent dead-letters the whole chain, and retrying it revives the chain', async () => {
    let parentFails = true;
    const queue = new OutboxQueue({
      name: 'chain',
      logger: silentLogger,
      executor: async (m: Mutation) => {
        if (m.type === 'PARENT' && parentFails) throw httpError(401);
        return { ok: true };
      }
    });

    const parent = await queue.enqueue({ type: 'PARENT', payload: {} });
    const child = await queue.enqueue({ type: 'CHILD', payload: {}, dependsOn: parent.id });
    const grandchild = await queue.enqueue({ type: 'GRANDCHILD', payload: {}, dependsOn: child.id });
    const unrelated = await queue.enqueue({ type: 'OTHER', payload: {} });

    await queue.flush();
    expect(parent.error?.code).toBe('NON_RETRYABLE');
    expect(child.error?.code).toBe('PARENT_FAILED');
    expect(grandchild.error?.code).toBe('PARENT_FAILED');
    expect(unrelated.status).toBe('completed');

    parentFails = false;
    expect(await queue.retryDeadLetter(parent.id)).toBe(true);
    expect([parent.status, child.status, grandchild.status]).toEqual(['pending', 'pending', 'pending']);

    expect(await queue.flush()).toBe(3);
    expect(grandchild.status).toBe('completed');
  });

  test('retryDeadLetter does not revive descendants that failed for their own reasons', async () => {
    const queue = new OutboxQueue({
      name: 'chain-own',
      logger: silentLogger,
      executor: async () => {
        throw httpError(422);
      }
    });
    const parent = await queue.enqueue({ type: 'P', payload: {} });
    await queue.flush();
    const child = await queue.enqueue({ type: 'C', payload: {}, dependsOn: parent.id });
    await queue.retryDeadLetter(parent.id);
    // child had never failed because of the parent; it is still a plain pending item
    expect(child.status).toBe('pending');
    expect(await queue.retryDeadLetter('missing')).toBe(false);
  });

  test('removing an uncompleted parent dead-letters its dependents with PARENT_REMOVED', async () => {
    const executor = jest.fn(async () => ({}));
    const queue = new OutboxQueue({ name: 'remove', executor });
    const parent = await queue.enqueue({ type: 'P', payload: {} });
    const child = await queue.enqueue({ type: 'C', payload: {}, dependsOn: parent.id });

    expect(await queue.remove(parent.id)).toBe(true);
    expect(child.status).toBe('dead_letter');
    expect(child.error?.code).toBe('PARENT_REMOVED');
    expect(await queue.remove(parent.id)).toBe(false);

    await queue.flush();
    expect(executor).not.toHaveBeenCalled();
  });

  test('removing a completed parent leaves dependents alone', async () => {
    const queue = new OutboxQueue({ name: 'remove-done', executor: async () => ({}) });
    const parent = await queue.enqueue({ type: 'P', payload: {} });
    await queue.flush();
    const child = await queue.enqueue({ type: 'C', payload: {}, dependsOn: parent.id });
    await queue.remove(parent.id);
    expect(child.status).toBe('pending');
  });

  test('a syncing mutation cannot be removed', async () => {
    let release!: () => void;
    const queue = new OutboxQueue({
      name: 'remove-syncing',
      executor: () => new Promise(resolve => (release = () => resolve({})))
    });
    const m = await queue.enqueue({ type: 'A', payload: {} });
    const flushing = queue.flush();
    await new Promise(r => setTimeout(r, 0));
    expect(m.status).toBe('syncing');
    expect(await queue.remove(m.id)).toBe(false);
    release();
    await flushing;
  });

  test('a mutation removed during a flush is skipped', async () => {
    const seen: string[] = [];
    const ids: { second?: string } = {};
    const queue: OutboxQueue = new OutboxQueue({
      name: 'remove-during',
      executor: async m => {
        seen.push(m.type);
        if (m.type === 'A') await queue.remove(ids.second!);
        return {};
      }
    });
    await queue.enqueue({ type: 'A', payload: {} });
    ids.second = (await queue.enqueue({ type: 'B', payload: {} })).id;
    await queue.flush();
    expect(seen).toEqual(['A']);
  });

  test('a child whose parent is no longer in the queue (purged after completing) runs', async () => {
    const queue = new OutboxQueue({ name: 'orphan', executor: async () => ({}) });
    const child = await queue.enqueue({ type: 'C', payload: {}, dependsOn: 'completed-long-ago' });
    await queue.flush();
    expect(child.status).toBe('completed');
  });

  test('a child waits while its parent is backing off', async () => {
    const queue = new OutboxQueue({
      name: 'wait',
      retryConfig: { jitter: false, initialBackoffMs: 10_000 },
      executor: async (m: Mutation) => {
        if (m.type === 'P') throw httpError(503);
        return {};
      }
    });
    const parent = await queue.enqueue({ type: 'P', payload: {} });
    const child = await queue.enqueue({ type: 'C', payload: {}, dependsOn: parent.id });
    await queue.flush();
    expect(parent.status).toBe('pending');
    expect(child.status).toBe('pending');
    expect(child.attempts).toBe(0);
  });
});

describe('Temporary ID mapping', () => {
  test('explicit tempId with a custom getServerId', async () => {
    const received: unknown[] = [];
    const queue = new OutboxQueue({
      name: 'tempid',
      getServerId: result => (result as { data: { uuid: string } }).data.uuid,
      executor: async (m: Mutation, ctx: SyncContext) => {
        received.push(ctx.resolvePayload(m.payload));
        return m.type === 'CREATE' ? { data: { uuid: 'srv-7' } } : {};
      }
    });
    const create = await queue.enqueue({ type: 'CREATE', payload: { title: 'x' }, tempId: 'tmp-7' });
    expect(create.tempId).toBe('tmp-7');
    await queue.enqueue({ type: 'LINK', payload: { ids: ['tmp-7', 'other'] }, dependsOn: create.id });
    await queue.flush();
    expect(received[1]).toEqual({ ids: ['srv-7', 'other'] });
  });

  test('resolveId and resolvePayload handle primitives, nesting and non-plain objects', async () => {
    let ctx!: SyncContext;
    const queue = new OutboxQueue({
      name: 'resolve',
      executor: async (m: Mutation, c: SyncContext) => {
        ctx = c;
        return m.type === 'CREATE' ? { id: 42 } : {};
      }
    });
    await queue.enqueue({ type: 'CREATE', payload: { tempId: 't1' } });
    await queue.enqueue({ type: 'NOOP', payload: {} });
    await queue.flush();

    const date = new Date(0);
    expect(ctx.resolveId('t1')).toBe(42);
    expect(ctx.resolveId('unknown')).toBe('unknown');
    expect(ctx.resolveId(5)).toBe(5);
    expect(ctx.resolvePayload('t1')).toBe(42);
    expect(ctx.resolvePayload(null)).toBeNull();
    const out = ctx.resolvePayload({ a: { b: ['t1'] }, when: date, n: 1 });
    expect(out).toEqual({ a: { b: [42] }, when: date, n: 1 });
    expect(out.when).toBe(date); // passed through, not turned into {}
    expect(ctx.idMap.get('t1')).toBe(42);
  });

  test('no mapping is recorded when the server returns no id or the same id', async () => {
    let ctx!: SyncContext;
    const queue = new OutboxQueue({
      name: 'nomap',
      executor: async (m: Mutation, c: SyncContext) => {
        ctx = c;
        if (m.type === 'SAME') return { id: 'same' };
        return null;
      }
    });
    await queue.enqueue({ type: 'NONE', payload: { tempId: 'a' } });
    await queue.enqueue({ type: 'SAME', payload: {}, tempId: 'same' });
    await queue.flush();
    expect(ctx.idMap.size).toBe(0);
  });
});
