import { OutboxQueue } from '../../src/core/queue.js';
import {
  MemoryCoordinator,
  createWebCoordinator,
  isWebCoordinatorSupported
} from '../../src/core/coordinator.js';
import type { OutboxCoordinator } from '../../src/core/coordinator.js';
import type { Mutation, OutboxQueueOptions } from '../../src/core/types.js';
import { FakeLockManager, FlakyStorage, httpError, silentLogger } from '../helpers/index.js';

/** Waits until all queued timers/microtasks (channel deliveries, leader handoffs) have run. */
const settle = async (rounds = 5) => {
  for (let i = 0; i < rounds; i++) await new Promise(r => setTimeout(r, 0));
};

type Executor = OutboxQueueOptions['executor'];

/** Simulates several browser tabs: separate queue instances sharing storage and a coordinator. */
function browser(coordinator: OutboxCoordinator = new MemoryCoordinator()) {
  const storage = new FlakyStorage();
  const sent: { tab: string; type: string; key: string }[] = [];
  const tabs: OutboxQueue[] = [];
  const open = (tab: string, executor?: Executor, extra: Partial<OutboxQueueOptions> = {}) => {
    const queue = new OutboxQueue({
      name: 'app',
      storage,
      crossTab: coordinator,
      logger: silentLogger,
      executor:
        executor ??
        (async (m: Mutation) => {
          sent.push({ tab, type: m.type, key: m.idempotencyKey });
          return { id: `srv-${m.type}` };
        }),
      ...extra
    });
    tabs.push(queue);
    return queue;
  };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- inspects raw persisted JSON
  const stored = (): any => JSON.parse(storage.data.get('@resilient-outbox:app') ?? '{"mutations":[]}');
  return { storage, sent, open, stored, closeAll: () => tabs.forEach(t => t.destroy()) };
}

describe('Cross-tab coordination', () => {
  test('exactly one tab becomes leader', async () => {
    const b = browser();
    const a = b.open('A');
    const c = b.open('B');
    await settle();
    expect(a.getSnapshot().isLeader).toBe(true);
    expect(c.getSnapshot().isLeader).toBe(false);
    b.closeAll();
  });

  test('concurrent enqueues from different tabs are never lost', async () => {
    const b = browser();
    const a = b.open('A');
    const c = b.open('B');
    await Promise.all([
      ...Array.from({ length: 20 }, (_, i) => a.enqueue({ type: `A${i}`, payload: {} })),
      ...Array.from({ length: 20 }, (_, i) => c.enqueue({ type: `B${i}`, payload: {} }))
    ]);
    expect(b.stored().mutations).toHaveLength(40);
    await settle();
    expect(a.getAll()).toHaveLength(40);
    expect(c.getAll()).toHaveLength(40);
    b.closeAll();
  });

  test('without crossTab the same race loses writes (the bug this fixes)', async () => {
    const storage = new FlakyStorage();
    const opts = { name: 'app', storage, executor: async () => ({}) };
    const a = new OutboxQueue(opts);
    const c = new OutboxQueue(opts);
    await Promise.all([a.enqueue({ type: 'A', payload: {} }), c.enqueue({ type: 'B', payload: {} })]);
    const stored = JSON.parse(storage.data.get('@resilient-outbox:app')!);
    expect(stored.mutations.length).toBeLessThan(2);
  });

  test('only the leader executes, and each mutation is sent exactly once', async () => {
    const b = browser();
    const a = b.open('A');
    const c = b.open('B');
    a.startAutoFlush({ flushOnReconnect: false });
    c.startAutoFlush({ flushOnReconnect: false });
    await settle();

    await c.enqueue({ type: 'FROM_B', payload: {} });
    await a.enqueue({ type: 'FROM_A', payload: {} });
    await settle(20);

    expect(b.sent.map(s => s.tab)).toEqual(['A', 'A']);
    expect(b.sent.map(s => s.type).sort()).toEqual(['FROM_A', 'FROM_B']);
    expect(b.stored().mutations).toHaveLength(0);
    b.closeAll();
  });

  test('a follower sees the leader’s progress and its flush() delegates to the leader', async () => {
    const b = browser();
    const a = b.open('A');
    const c = b.open('B');
    await settle();
    const renders: number[] = [];
    c.subscribe(() => renders.push(c.getSnapshot().pending));

    await c.enqueue({ type: 'X', payload: {} });
    expect(c.getSnapshot().pending).toBe(1);
    expect(await c.flush()).toBe(1);
    await settle();

    expect(b.sent).toEqual([expect.objectContaining({ tab: 'A', type: 'X' })]);
    expect(c.getSnapshot().pending).toBe(0);
    expect(a.getSnapshot().pending).toBe(0);
    expect(renders).toContain(0);
    b.closeAll();
  });

  test('when the leader closes, a follower takes over and finishes the work', async () => {
    const b = browser();
    const a = b.open('A');
    const c = b.open('B');
    await settle();
    await a.enqueue({ type: 'LATER', payload: {} });

    a.destroy();
    await settle(10);
    expect(c.getSnapshot().isLeader).toBe(true);
    expect(await c.flush()).toBe(1);
    expect(b.sent.map(s => s.tab)).toEqual(['B']);
    b.closeAll();
  });

  test('a leader that dies mid-request has its in-flight mutation retried with the same key', async () => {
    const coordinator = new MemoryCoordinator();
    // Wrap the coordinator so the test can yank leadership like a crashed tab would
    const releases: (() => void)[] = [];
    const crashable: OutboxCoordinator = {
      withLock: (n, fn) => coordinator.withLock(n, fn),
      openChannel: (n, cb) => coordinator.openChannel(n, cb),
      requestLeadership: (n, cb) => {
        const release = coordinator.requestLeadership(n, cb);
        releases.push(release);
        return release;
      }
    };
    const b = browser(crashable);
    const keys: string[] = [];
    const a = b.open('A', (m: Mutation) => {
      keys.push(m.idempotencyKey);
      return new Promise(() => {}); // the request never returns: the tab "crashes"
    });
    const c = b.open('B', async (m: Mutation) => {
      keys.push(m.idempotencyKey);
      return {};
    });
    await settle();

    const m = await a.enqueue({ type: 'PAY', payload: { amount: 10 } });
    void a.flush();
    await settle();
    expect(b.stored().mutations[0].status).toBe('syncing');

    releases[0]!(); // tab A crashes: the browser releases its lock
    await settle(10);
    expect(c.getSnapshot().isLeader).toBe(true);
    expect(await c.flush()).toBe(1);
    expect(keys).toEqual([m.idempotencyKey, m.idempotencyKey]);
    expect(b.stored().mutations).toHaveLength(0);
    b.closeAll();
  });

  test('a follower cannot remove a mutation the leader is sending', async () => {
    const b = browser();
    let release!: () => void;
    const a = b.open('A', () => new Promise(r => (release = () => r({}))));
    const c = b.open('B');
    await settle();

    const m = await c.enqueue({ type: 'X', payload: {} });
    const flushing = a.flush();
    await settle();
    expect(await c.remove(m.id)).toBe(false);
    release();
    expect(await flushing).toBe(1);
    b.closeAll();
  });

  test('a mutation removed in a follower is never sent by the leader', async () => {
    const b = browser();
    const a = b.open('A');
    const c = b.open('B');
    await settle();
    const m = await a.enqueue({ type: 'DISCARD', payload: {} });
    await settle();
    expect(await c.remove(m.id)).toBe(true);
    expect(await a.flush()).toBe(0);
    expect(b.sent).toHaveLength(0);
    b.closeAll();
  });

  test('temp IDs resolved by the leader are visible to every tab', async () => {
    const b = browser();
    const seen: unknown[] = [];
    const a = b.open('A', async (m: Mutation, ctx) => {
      seen.push(ctx.resolvePayload(m.payload));
      return m.type === 'POST' ? { id: 'srv-1' } : {};
    });
    const c = b.open('B');
    await settle();

    const post = await c.enqueue({ type: 'POST', payload: {}, tempId: 'tmp-1' });
    await c.enqueue({ type: 'COMMENT', payload: { postId: 'tmp-1' }, dependsOn: post.id });
    expect(await a.flush()).toBe(2);
    expect(seen[1]).toEqual({ postId: 'srv-1' });
    expect(b.stored().idMap[0].slice(0, 2)).toEqual(['tmp-1', 'srv-1']);
    b.closeAll();
  });

  test('dead letters and retries work across tabs', async () => {
    const b = browser();
    let fail = true;
    const a = b.open('A', async () => {
      if (fail) throw httpError(422);
      return {};
    });
    const c = b.open('B');
    await settle();

    await c.enqueue({ type: 'X', payload: {} });
    await a.flush();
    await settle();
    expect(c.getDeadLetter()).toHaveLength(1);

    fail = false;
    expect(await c.retryAllDeadLetter()).toBe(1);
    expect(await c.flush()).toBe(1);
    b.closeAll();
  });

  test('a failed write rejects the operation instead of pretending it is durable', async () => {
    const b = browser();
    const a = b.open('A');
    await settle();
    b.storage.failWrites = 1;
    await expect(a.enqueue({ type: 'X', payload: {} })).rejects.toThrow('write failed');
    expect(a.getAll()).toHaveLength(0);
    b.closeAll();
  });

  test('a follower flush() resolves even if no leader answers', async () => {
    jest.useFakeTimers();
    try {
      const silent: OutboxCoordinator = {
        withLock: (_n, fn) => fn(),
        requestLeadership: () => () => {},
        openChannel: () => ({ post: () => {}, close: () => {} })
      };
      const q = new OutboxQueue({ name: 'lonely', crossTab: silent, executor: async () => ({}) });
      const result = q.flush();
      expect(q.flush()).toBe(result); // coalesced
      await jest.advanceTimersByTimeAsync(30_000);
      expect(await result).toBe(0);
      q.destroy();
    } finally {
      jest.useRealTimers();
    }
  });

  test('a follower waiting on flush() is served if it becomes leader itself', async () => {
    const b = browser();
    const a = b.open('A', () => new Promise(() => {}));
    const c = b.open('B');
    await settle();
    await c.enqueue({ type: 'X', payload: {} });

    const pending = c.flush(); // A never answers: it is stuck
    await settle();
    a.destroy(); // destroy aborts A's request; leadership moves to B
    expect(await pending).toBeGreaterThanOrEqual(0);
    await settle(10);
    expect(c.getSnapshot().isLeader).toBe(true);
    b.closeAll();
  });

  test('ignores foreign and malformed channel messages', async () => {
    const coordinator = new MemoryCoordinator();
    const b = browser(coordinator);
    const a = b.open('A');
    await settle();
    const intruder = coordinator.openChannel('@resilient-outbox:app', () => {});
    intruder.post(null);
    intruder.post({ source: 'other-lib', type: 'changed' });
    intruder.post({ source: 'resilient-outbox', from: 'x', type: 'flush-result', id: 'unknown' });
    intruder.post({ source: 'resilient-outbox', from: 'x', type: 'flush-result' });
    await settle();
    expect(a.getSnapshot().isLeader).toBe(true);
    intruder.close();
    b.closeAll();
  });

  test('warns when a peer without crossTab says hello', async () => {
    const coordinator = new MemoryCoordinator();
    const logger = { warn: jest.fn(), error: jest.fn() };
    const q = new OutboxQueue({ name: 'app', crossTab: coordinator, logger, executor: async () => ({}) });
    const peer = coordinator.openChannel('@resilient-outbox:app', () => {});
    peer.post({ source: 'resilient-outbox', from: 'legacy', type: 'hello' });
    peer.post({ source: 'resilient-outbox', from: 'legacy', type: 'hello-ack' });
    await settle();
    expect(logger.warn).toHaveBeenCalledTimes(1);
    peer.close();
    q.destroy();
  });

  test('crossTab: true falls back to a single instance, with a warning, when unsupported', () => {
    const nav = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
    Object.defineProperty(globalThis, 'navigator', { value: {}, configurable: true });
    try {
      const logger = { warn: jest.fn(), error: jest.fn() };
      const q = new OutboxQueue({ name: 'fallback', crossTab: true, logger, executor: async () => ({}) });
      expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('crossTab requires'));
      expect(q.getSnapshot().isLeader).toBe(true);
      q.destroy();
    } finally {
      if (nav) Object.defineProperty(globalThis, 'navigator', nav);
    }
  });
});

describe('Duplicate-instance detection without crossTab', () => {
  test('warns in browsers when another instance shares the storage key', async () => {
    const g = globalThis as { document?: unknown };
    const hadDocument = 'document' in g;
    g.document ??= {};
    try {
      const storage = new FlakyStorage();
      const logA = { warn: jest.fn(), error: jest.fn() };
      const logB = { warn: jest.fn(), error: jest.fn() };
      const a = new OutboxQueue({ name: 'dup', storage, logger: logA, executor: async () => ({}) });
      const b = new OutboxQueue({ name: 'dup', storage, logger: logB, executor: async () => ({}) });
      await new Promise(r => setTimeout(r, 50));
      expect(logA.warn).toHaveBeenCalledWith(expect.stringContaining('crossTab: true'));
      expect(logB.warn).toHaveBeenCalledWith(expect.stringContaining('crossTab: true'));
      a.destroy();
      b.destroy();
      await new Promise(r => setTimeout(r, 0));
    } finally {
      if (!hadDocument) delete g.document;
    }
  });
});

// Native Web Locks on Node >= 24 (and in CI); a spec-faithful stand-in on older Node
const nativeLocks = isWebCoordinatorSupported();

describe(`createWebCoordinator (${nativeLocks ? 'native' : 'polyfilled'} Web Locks + BroadcastChannel)`, () => {
  const original = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
  beforeAll(() => {
    if (nativeLocks) return;
    const nav = (globalThis as { navigator?: object }).navigator ?? {};
    Object.defineProperty(globalThis, 'navigator', {
      value: Object.assign(Object.create(nav) as object, { locks: new FakeLockManager() }),
      configurable: true
    });
  });
  afterAll(() => {
    if (!nativeLocks && original) Object.defineProperty(globalThis, 'navigator', original);
  });

  test('is detected as supported', () => {
    expect(isWebCoordinatorSupported()).toBe(true);
  });

  test('two instances coordinate through the platform APIs', async () => {
    const storage = new FlakyStorage();
    const sent: string[] = [];
    const make = (tab: string) =>
      new OutboxQueue({
        name: 'web',
        storage,
        crossTab: true,
        logger: silentLogger,
        executor: async (m: Mutation) => {
          sent.push(`${tab}:${m.type}`);
          return {};
        }
      });
    const a = make('A');
    const b = make('B');
    await settle(10);
    expect([a.getSnapshot().isLeader, b.getSnapshot().isLeader].filter(Boolean)).toHaveLength(1);

    await Promise.all([a.enqueue({ type: 'one', payload: {} }), b.enqueue({ type: 'two', payload: {} })]);
    const follower = a.getSnapshot().isLeader ? b : a;
    expect(await follower.flush()).toBe(2);
    expect(new Set(sent.map(s => s[0])).size).toBe(1); // a single tab sent everything

    const leader = follower === a ? b : a;
    leader.destroy();
    await settle(10);
    expect(follower.getSnapshot().isLeader).toBe(true);
    follower.destroy();
    await settle();
  });

  test('withLock serializes callers and posting on a closed channel is harmless', async () => {
    const coordinator = createWebCoordinator()!;
    const order: string[] = [];
    await Promise.all([
      coordinator.withLock('mutex-test', async () => {
        order.push('1 start');
        await new Promise(r => setTimeout(r, 10));
        order.push('1 end');
      }),
      coordinator.withLock('mutex-test', async () => {
        order.push('2');
      })
    ]);
    expect(order).toEqual(['1 start', '1 end', '2']);

    const received: unknown[] = [];
    const a = coordinator.openChannel('chan-test', () => {});
    const b = coordinator.openChannel('chan-test', m => received.push(m));
    a.post({ hello: 1 });
    await settle();
    expect(received).toEqual([{ hello: 1 }]);
    a.close();
    b.close();
    expect(() => {
      a.post('after close');
    }).not.toThrow();
  });

  test('releasing leadership while still queued does not acquire it later', async () => {
    const coordinator = createWebCoordinator()!;
    const first = jest.fn();
    const second = jest.fn();
    const releaseFirst = coordinator.requestLeadership('lead-test', first);
    const releaseSecond = coordinator.requestLeadership('lead-test', second);
    await settle();
    releaseSecond();
    releaseFirst();
    await settle();
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).not.toHaveBeenCalled();
  });
});

describe('createWebCoordinator', () => {
  test('returns undefined when the platform APIs are missing', () => {
    const nav = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
    Object.defineProperty(globalThis, 'navigator', { value: {}, configurable: true });
    try {
      expect(createWebCoordinator()).toBeUndefined();
      expect(isWebCoordinatorSupported()).toBe(false);
    } finally {
      if (nav) Object.defineProperty(globalThis, 'navigator', nav);
    }
  });
});
