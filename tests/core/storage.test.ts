import { OutboxQueue } from '../../src/core/queue.js';
import { MemoryStorageAdapter } from '../../src/adapters/index.js';
import { MemoryCoordinator } from '../../src/core/coordinator.js';
import type { Mutation, SyncContext } from '../../src/core/types.js';
import { FlakyStorage, silentLogger } from '../helpers/index.js';

const KEY = (name: string) => `@resilient-outbox:${name}`;

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- test helper inspecting raw JSON
function storedState(storage: FlakyStorage, name: string): any {
  return JSON.parse(storage.data.get(KEY(name))!);
}

describe('Hydration', () => {
  test('enqueue before an explicit hydrate() does not clobber persisted mutations', async () => {
    const storage = new FlakyStorage();
    const q1 = new OutboxQueue({ name: 'h', storage, executor: async () => ({}) });
    await q1.enqueue({ type: 'OLD', payload: {} });

    const q2 = new OutboxQueue({ name: 'h', storage, executor: async () => ({}) });
    await q2.enqueue({ type: 'NEW', payload: {} }); // no hydrate() call
    expect(q2.getAll().map(m => m.type)).toEqual(['OLD', 'NEW']);
    expect(storedState(storage, 'h').mutations).toHaveLength(2);
  });

  test('hydrate() is idempotent and flips isHydrated', async () => {
    const getItem = jest.fn(async () => null);
    const queue = new OutboxQueue({
      name: 'idem',
      storage: { getItem, setItem: async () => {}, removeItem: async () => {} },
      executor: async () => ({})
    });
    expect(queue.hydrated).toBe(false);
    expect(queue.getSnapshot().isHydrated).toBe(false);
    await Promise.all([queue.hydrate(), queue.hydrate(), queue.flush()]);
    expect(getItem).toHaveBeenCalledTimes(1);
    expect(queue.hydrated).toBe(true);
    expect(queue.getSnapshot().isHydrated).toBe(true);
  });

  test('a failed read never lets a later write overwrite the unread data', async () => {
    const storage = new FlakyStorage();
    const q1 = new OutboxQueue({ name: 'r', storage, executor: async () => ({}) });
    await q1.enqueue({ type: 'PRECIOUS', payload: {} });

    storage.failReads = 1;
    const errors: unknown[] = [];
    const q2 = new OutboxQueue({ name: 'r', storage, logger: silentLogger, executor: async () => ({}) });
    q2.on('storage_error', e => errors.push(e.operation));
    await q2.hydrate();
    expect(errors).toEqual(['hydrate']);
    expect(q2.getAll()).toHaveLength(0);

    // The next write re-reads storage first and merges what it finds
    await q2.enqueue({ type: 'NEW', payload: {} });
    expect(q2.getAll().map(m => m.type)).toEqual(['PRECIOUS', 'NEW']);
    expect(storedState(storage, 'r').mutations.map((m: Mutation) => m.type)).toEqual(['PRECIOUS', 'NEW']);
  });

  test('a write is skipped while storage is still unreadable', async () => {
    const storage = new FlakyStorage();
    const q1 = new OutboxQueue({ name: 'r2', storage, executor: async () => ({}) });
    await q1.enqueue({ type: 'PRECIOUS', payload: {} });
    const before = storage.data.get(KEY('r2'));

    storage.failReads = 2;
    const q2 = new OutboxQueue({ name: 'r2', storage, logger: silentLogger, executor: async () => ({}) });
    await q2.enqueue({ type: 'NEW', payload: {} });
    expect(storage.data.get(KEY('r2'))).toBe(before);
  });

  test('corrupt data is backed up and the queue starts empty', async () => {
    const storage = new FlakyStorage();
    storage.data.set(KEY('c'), '{not json');
    const queue = new OutboxQueue({ name: 'c', storage, logger: silentLogger, executor: async () => ({}) });
    await queue.hydrate();
    expect(queue.getAll()).toHaveLength(0);
    expect(storage.data.get(`${KEY('c')}:corrupt`)).toBe('{not json');
  });

  test('unrecognized shapes are treated as corrupt; malformed entries are dropped', async () => {
    const storage = new FlakyStorage();
    storage.data.set(KEY('shape'), JSON.stringify({ hello: 'world' }));
    const q1 = new OutboxQueue({ name: 'shape', storage, logger: silentLogger, executor: async () => ({}) });
    await q1.hydrate();
    expect(storage.data.has(`${KEY('shape')}:corrupt`)).toBe(true);

    storage.data.set(
      KEY('partial'),
      JSON.stringify({
        version: 2,
        mutations: [null, { id: 'x' }, { id: 'ok', type: 'T', status: 'syncing', payload: {} }],
        idMap: [['t', 's'], 'junk']
      })
    );
    const q2 = new OutboxQueue({ name: 'partial', storage, executor: async () => ({}) });
    await q2.hydrate();
    const [m] = q2.getAll();
    expect(q2.getAll()).toHaveLength(1);
    expect(m).toMatchObject({ id: 'ok', status: 'pending', attempts: 0, maxAttempts: 5, nextAttemptAt: 0 });
  });

  test('loads legacy v1 array storage', async () => {
    const storage = new MemoryStorageAdapter();
    await storage.setItem(
      KEY('legacy'),
      JSON.stringify([
        {
          id: '1',
          idempotencyKey: 'k',
          type: 'T',
          payload: { tempId: 'tmp' },
          createdAt: 0,
          attempts: 2,
          maxAttempts: 5,
          nextAttemptAt: 0,
          status: 'syncing'
        }
      ])
    );
    let ctx!: SyncContext;
    const queue = new OutboxQueue({
      name: 'legacy',
      storage,
      executor: async (_m: Mutation, c: SyncContext) => {
        ctx = c;
        return { id: 'srv' };
      }
    });
    await queue.hydrate();
    expect(queue.getPending()[0]?.attempts).toBe(2);
    await queue.flush();
    // legacy payload.tempId still maps
    expect(ctx.idMap.get('tmp')).toBe('srv');
  });
});

describe('Persistence', () => {
  test('rapid changes coalesce into a bounded number of writes', async () => {
    const storage = new FlakyStorage();
    const queue = new OutboxQueue({ name: 'co', storage, executor: async () => ({}) });
    await queue.hydrate();
    await Promise.all(Array.from({ length: 50 }, (_, i) => queue.enqueue({ type: `T${i}`, payload: {} })));
    expect(storage.writes.length).toBeLessThanOrEqual(3);
    expect(storedState(storage, 'co').mutations).toHaveLength(50);
  });

  test('write failures are reported and do not break the queue', async () => {
    const storage = new FlakyStorage();
    storage.failWrites = 1;
    const errors: string[] = [];
    const queue = new OutboxQueue({ name: 'wf', storage, logger: silentLogger, executor: async () => ({}) });
    queue.on('storage_error', e => errors.push(e.operation));
    const m = await queue.enqueue({ type: 'A', payload: {} });
    expect(errors).toEqual(['persist']);
    await queue.flush();
    expect(m.status).toBe('completed');
    expect(storedState(storage, 'wf').mutations).toHaveLength(0);
  });

  test('autoPersist: false never writes', async () => {
    const storage = new FlakyStorage();
    const queue = new OutboxQueue({ name: 'np', storage, autoPersist: false, executor: async () => ({}) });
    await queue.enqueue({ type: 'A', payload: {} });
    await queue.flush();
    expect(storage.writes).toHaveLength(0);
  });

  test('clear() empties memory, storage and the id map', async () => {
    const storage = new FlakyStorage();
    const queue = new OutboxQueue({ name: 'clr', storage, executor: async () => ({ id: 's' }) });
    await queue.enqueue({ type: 'A', payload: {}, tempId: 't' });
    await queue.flush();
    await queue.enqueue({ type: 'B', payload: {} });
    await queue.clear();
    expect(queue.getAll()).toHaveLength(0);
    expect(storedState(storage, 'clr')).toEqual({ version: 2, mutations: [], idMap: [] });
  });
});

describe('Temp-ID map retention', () => {
  const DAY = 24 * 60 * 60 * 1000;

  test('unused mappings expire after idMapTtlMs, but referenced ones are kept', async () => {
    const storage = new FlakyStorage();
    const now = Date.now();
    storage.data.set(
      KEY('ttl'),
      JSON.stringify({
        version: 2,
        mutations: [
          {
            id: 'm',
            type: 'T',
            status: 'dead_letter',
            payload: { ref: 'kept' },
            attempts: 0,
            maxAttempts: 5,
            nextAttemptAt: 0
          }
        ],
        idMap: [
          ['old', 's1', now - 8 * DAY],
          ['kept', 's2', now - 8 * DAY],
          ['fresh', 's3', now - DAY],
          ['nostamp', 's4']
        ]
      })
    );
    const queue = new OutboxQueue({ name: 'ttl', storage, executor: async () => ({}) });
    await queue.enqueue({ type: 'TRIGGER_WRITE', payload: {} });
    const keys = storedState(storage, 'ttl').idMap.map((e: unknown[]) => e[0]);
    expect(keys.sort()).toEqual(['fresh', 'kept', 'nostamp']);
  });

  test('idMapTtlMs: Infinity keeps everything', async () => {
    const storage = new FlakyStorage();
    storage.data.set(KEY('inf'), JSON.stringify({ version: 2, mutations: [], idMap: [['old', 's', 0]] }));
    const queue = new OutboxQueue({ name: 'inf', storage, idMapTtlMs: Infinity, executor: async () => ({}) });
    await queue.enqueue({ type: 'A', payload: {} });
    expect(storedState(storage, 'inf').idMap).toHaveLength(1);
  });
});

describe('Untrusted persisted data', () => {
  const evil = JSON.stringify({ version: 2, idMap: [], mutations: [] }).replace(
    '"mutations":[]',
    '"mutations":[{"id":"m","type":"T","status":"pending","attempts":0,"maxAttempts":5,"nextAttemptAt":0,' +
      '"payload":{"__proto__":{"polluted":"yes"},"keep":"me"},"__proto__":{"hacked":true}}]'
  );

  test.each([
    ['single instance', false],
    ['cross-tab', true]
  ] as const)('%s: __proto__ keys never change prototypes', async (_label, crossTab) => {
    const storage = new FlakyStorage();
    storage.data.set(KEY('evil'), evil);
    const resolved: unknown[] = [];
    const queue = new OutboxQueue({
      name: 'evil',
      storage,
      crossTab: crossTab ? new MemoryCoordinator() : false,
      executor: async (m: Mutation, ctx: SyncContext) => {
        resolved.push(ctx.resolvePayload(m.payload));
        return {};
      }
    });
    await queue.hydrate();
    // A second sync merges the stored record into the live object (the cross-tab path)
    await queue.enqueue({ type: 'NEXT', payload: {} });
    const m = queue.get('m')!;
    expect(Object.getPrototypeOf(m)).toBe(Object.prototype);
    expect((m as unknown as { hacked?: unknown }).hacked).toBeUndefined();

    expect(await queue.flush()).toBeGreaterThanOrEqual(1);
    const payload = resolved[0] as Record<string, unknown>;
    expect(Object.getPrototypeOf(payload)).toBe(Object.prototype);
    expect(Object.prototype.hasOwnProperty.call(payload, '__proto__')).toBe(true); // kept as data
    expect(payload.keep).toBe('me');
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    queue.destroy();
  });
});
