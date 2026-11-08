/**
 * Coordinates several `OutboxQueue` instances that share one storage key — typically the same
 * app open in several browser tabs. It provides three primitives:
 *
 * - an exclusive lock, held around every read-modify-write of storage, so no instance can
 *   overwrite another's changes;
 * - leader election, so exactly one instance executes mutations at a time, and leadership moves
 *   to another instance automatically when the leader's tab closes or crashes;
 * - a broadcast channel, so other instances refresh as soon as storage changes.
 */
export interface OutboxCoordinator {
  /** Runs `fn` while holding a lock that is exclusive across every context sharing `name`. */
  withLock<T>(name: string, fn: () => Promise<T>): Promise<T>;
  /**
   * Waits in line to become leader of `name` and calls `onAcquire` once this context is leader.
   * Leadership lasts until the returned function is called or the context dies.
   */
  requestLeadership(name: string, onAcquire: () => void): () => void;
  /** Opens a channel that delivers messages to every *other* context that opened `name`. */
  openChannel(name: string, onMessage: (message: unknown) => void): OutboxChannel;
}

export interface OutboxChannel {
  post(message: unknown): void;
  close(): void;
}

interface LockManagerLike {
  request(name: string, callback: () => Promise<unknown>): Promise<unknown>;
  request(
    name: string,
    options: { signal?: AbortSignal },
    callback: () => Promise<unknown>
  ): Promise<unknown>;
}

interface BroadcastChannelLike {
  onmessage: ((event: { data: unknown }) => void) | null;
  postMessage(message: unknown): void;
  close(): void;
}

type BroadcastChannelCtor = new (name: string) => BroadcastChannelLike;

function webApis(): { locks: LockManagerLike; Channel: BroadcastChannelCtor } | undefined {
  const g = globalThis as {
    navigator?: { locks?: Partial<LockManagerLike> };
    BroadcastChannel?: BroadcastChannelCtor;
  };
  const locks = g.navigator?.locks;
  if (typeof locks?.request !== 'function' || typeof g.BroadcastChannel !== 'function') return undefined;
  return { locks: locks as LockManagerLike, Channel: g.BroadcastChannel };
}

/** Whether the Web Locks API and `BroadcastChannel` are available (all modern browsers, Node ≥ 24). */
export function isWebCoordinatorSupported(): boolean {
  return webApis() !== undefined;
}

/**
 * Coordinator built on the Web Locks API and `BroadcastChannel`. Locks held by a tab are released
 * by the browser when the tab closes or crashes, which is what makes leader failover reliable.
 * Returns `undefined` when either API is missing.
 */
export function createWebCoordinator(): OutboxCoordinator | undefined {
  const apis = webApis();
  if (!apis) return undefined;
  const { locks, Channel } = apis;

  return {
    withLock: <T>(name: string, fn: () => Promise<T>) => locks.request(name, fn) as Promise<T>,

    requestLeadership(name, onAcquire) {
      const controller = new AbortController();
      let release: (() => void) | undefined;
      locks
        .request(name, { signal: controller.signal }, () => {
          if (controller.signal.aborted) return Promise.resolve();
          onAcquire();
          return new Promise<void>(resolve => {
            release = resolve;
          });
        })
        .catch(() => undefined); // AbortError when released while still waiting
      return () => {
        controller.abort();
        release?.();
      };
    },

    openChannel(name, onMessage) {
      const channel = new Channel(name);
      channel.onmessage = event => {
        onMessage(event.data);
      };
      return {
        post: message => {
          try {
            channel.postMessage(message);
          } catch {
            // Posting on a closed channel throws InvalidStateError; nothing is listening anyway
          }
        },
        close: () => {
          channel.close();
        }
      };
    }
  };
}

const clone: (value: unknown) => unknown =
  typeof structuredClone === 'function'
    ? value => structuredClone(value)
    : value => JSON.parse(JSON.stringify(value)) as unknown;

/**
 * Coordinator for instances living in the same JavaScript context: share one `MemoryCoordinator`
 * between them. Useful for tests and for runtimes without Web Locks.
 */
export class MemoryCoordinator implements OutboxCoordinator {
  private locks = new Map<string, Promise<unknown>>();
  private leaders = new Map<
    string,
    { current?: symbol; queue: { token: symbol; onAcquire: () => void }[] }
  >();
  private channels = new Map<string, Set<{ deliver: (message: unknown) => void }>>();

  withLock<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.locks.get(name) ?? Promise.resolve();
    const run = previous.then(fn, fn);
    this.locks.set(
      name,
      run.catch(() => undefined)
    );
    return run;
  }

  requestLeadership(name: string, onAcquire: () => void): () => void {
    let state = this.leaders.get(name);
    if (!state) {
      state = { queue: [] };
      this.leaders.set(name, state);
    }
    const token = Symbol('leader');
    state.queue.push({ token, onAcquire });
    this.promote(name);
    return () => {
      const s = this.leaders.get(name)!;
      s.queue = s.queue.filter(entry => entry.token !== token);
      if (s.current === token) {
        s.current = undefined;
        this.promote(name);
      }
    };
  }

  private promote(name: string): void {
    const state = this.leaders.get(name)!;
    if (state.current) return;
    const next = state.queue.shift();
    if (!next) return;
    state.current = next.token;
    queueMicrotask(next.onAcquire);
  }

  openChannel(name: string, onMessage: (message: unknown) => void): OutboxChannel {
    let set = this.channels.get(name);
    if (!set) {
      set = new Set();
      this.channels.set(name, set);
    }
    const members = set;
    const self = { deliver: onMessage };
    members.add(self);
    return {
      post: message => {
        const copy = clone(message);
        for (const member of members) {
          if (member !== self) {
            setTimeout(() => {
              if (members.has(member)) member.deliver(copy);
            }, 0);
          }
        }
      },
      close: () => {
        members.delete(self);
      }
    };
  }
}
