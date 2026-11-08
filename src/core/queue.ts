import type {
  AutoFlushOptions,
  EnqueueOptions,
  Mutation,
  MutationError,
  MutationErrorCode,
  MutationExecutor,
  OutboxEventMap,
  OutboxEventName,
  OutboxLogger,
  OutboxQueueOptions,
  OutboxStorageAdapter,
  QueueStateSnapshot,
  RetryConfig,
  SyncContext
} from './types.js';
import type { OutboxChannel, OutboxCoordinator } from './coordinator.js';
import { createWebCoordinator } from './coordinator.js';
import { OutboxDestroyedError, OutboxTimeoutError } from './errors.js';
import { generateUUID } from '../utils/id.js';
import {
  calculateBackoffMs,
  getErrorRetryAfterMs,
  getErrorStatusCode,
  isDefaultRetryable
} from '../utils/backoff.js';
import { MemoryStorageAdapter } from '../adapters/index.js';

const STORAGE_VERSION = 2;
const DEFAULT_ID_MAP_TTL_MS = 7 * 24 * 60 * 60 * 1000;
/** How long a follower waits for the leader to answer a `flush()` request. */
const FOLLOWER_FLUSH_TIMEOUT_MS = 30_000;
const MESSAGE_SOURCE = 'resilient-outbox';

type IdMapEntry = [tempId: string, serverId: unknown, lastUsedAt?: number];

interface PersistedState {
  version: number;
  mutations: Mutation[];
  idMap: IdMapEntry[];
}

interface AutoFlushState {
  options: Required<AutoFlushOptions>;
  stop: () => void;
  wakeTimer?: ReturnType<typeof setTimeout>;
}

type ExecutionOutcome = { ok: true; result: unknown } | { ok: false; error: unknown };
type ClaimResult = 'claimed' | 'skipped' | 'cascaded';

interface ChannelMessage {
  source: typeof MESSAGE_SOURCE;
  from: string;
  type: 'hello' | 'hello-ack' | 'changed' | 'flush-request' | 'flush-result';
  id?: string;
  count?: number;
}

interface TransactOptions<T> {
  /** Whether the change must be written to storage. Default `true`. */
  write?: boolean | ((result: T) => boolean);
  /** Allowed after `destroy()`, to record the outcome of an aborted in-flight request. */
  internal?: boolean;
}

function isMutationLike(value: unknown): value is Mutation {
  if (!value || typeof value !== 'object') return false;
  const m = value as Partial<Mutation>;
  return typeof m.id === 'string' && typeof m.type === 'string' && typeof m.status === 'string';
}

/** Parses both the v2 `{ version, mutations, idMap }` format and the legacy v1 `Mutation[]` format. */
function parseState(raw: string): Pick<PersistedState, 'mutations' | 'idMap'> {
  const parsed: unknown = JSON.parse(raw);
  const state = parsed as Partial<PersistedState> | null;
  const list: unknown = Array.isArray(parsed) ? parsed : state?.mutations;
  if (!Array.isArray(list)) {
    throw new Error('Unrecognized storage format');
  }
  const idMap = !Array.isArray(parsed) && Array.isArray(state?.idMap) ? state.idMap : [];
  return {
    mutations: list.filter(isMutationLike),
    idMap: idMap.filter((e): e is IdMapEntry => Array.isArray(e) && typeof e[0] === 'string')
  };
}

function legacyTempId(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') return undefined;
  const tempId = (payload as { tempId?: unknown }).tempId;
  return typeof tempId === 'string' || typeof tempId === 'number' ? String(tempId) : undefined;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object') return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function defaultGetServerId(result: unknown): unknown {
  return result && typeof result === 'object' ? (result as { id?: unknown }).id : undefined;
}

function defaultIsOnline(): boolean {
  const nav = (globalThis as { navigator?: { onLine?: unknown } }).navigator;
  return typeof nav?.onLine === 'boolean' ? nav.onLine : true;
}

function unref(timer: unknown): void {
  (timer as { unref?: () => void } | undefined)?.unref?.();
}

export class OutboxQueue {
  readonly name: string;
  private readonly storage: OutboxStorageAdapter;
  private readonly executor: MutationExecutor;
  private readonly retryConfig: Required<RetryConfig>;
  private readonly autoPersist: boolean;
  private readonly getServerId: (result: unknown, mutation: Mutation) => unknown;
  private readonly idMapTtlMs: number;
  private readonly executorTimeoutMs: number | undefined;
  private readonly isOnline: () => boolean | Promise<boolean>;
  private readonly logger: OutboxLogger | null;

  private mutations: Mutation[] = [];
  private index: Map<string, Mutation> | null = null;
  /** Mutations removed while a flush may still hold a reference to them. */
  private removed = new WeakSet<Mutation>();
  private idMap = new Map<string, unknown>();
  private idMapLastUsed = new Map<string, number>();

  private listeners = new Set<() => void>();
  private eventListeners = new Map<OutboxEventName, Set<(payload: unknown) => void>>();

  private isSyncing = false;
  private activeFlushPromise: Promise<number> | null = null;
  private rerunRequested = false;

  private hydratePromise: Promise<void> | null = null;
  private isHydrated = false;
  /** Set when storage could not be read; the next write re-reads and merges before writing. */
  private needsRehydrate = false;
  private persistChain: Promise<void> = Promise.resolve();
  private writeQueued = false;

  // Cross-tab coordination
  private readonly instanceId = generateUUID();
  private readonly coordinator: OutboxCoordinator | undefined;
  private channel: OutboxChannel | undefined;
  private releaseLeadership: (() => void) | undefined;
  private isLeader: boolean;
  private flushRequests = new Map<string, (count: number) => void>();
  private followerFlush: Promise<number> | null = null;
  private warnedAboutInstances = false;

  private autoFlush: AutoFlushState | null = null;
  private destroyed = false;
  private readonly destroyController = new AbortController();

  private version = 0;
  private mutationsSnapshot: { version: number; value: ReadonlyArray<Mutation> } = {
    version: -1,
    value: []
  };
  private currentSnapshot: QueueStateSnapshot;

  constructor(options: OutboxQueueOptions) {
    // Runtime validation for JavaScript callers
    const raw = options as Partial<OutboxQueueOptions> | undefined;
    if (typeof raw?.name !== 'string' || raw.name.length === 0) {
      throw new TypeError('[resilient-outbox] `name` must be a non-empty string');
    }
    if (typeof raw.executor !== 'function') {
      throw new TypeError('[resilient-outbox] `executor` must be a function');
    }

    this.name = options.name;
    this.storage = options.storage ?? new MemoryStorageAdapter();
    this.executor = options.executor;
    this.autoPersist = options.autoPersist ?? true;
    this.getServerId = options.getServerId ?? defaultGetServerId;
    this.idMapTtlMs = options.idMapTtlMs ?? DEFAULT_ID_MAP_TTL_MS;
    this.executorTimeoutMs = options.executorTimeoutMs;
    this.isOnline = options.isOnline ?? defaultIsOnline;
    this.logger = options.logger === null ? null : (options.logger ?? console);

    this.retryConfig = {
      maxAttempts: options.retryConfig?.maxAttempts ?? 5,
      initialBackoffMs: options.retryConfig?.initialBackoffMs ?? 1000,
      maxBackoffMs: options.retryConfig?.maxBackoffMs ?? 30000,
      backoffMultiplier: options.retryConfig?.backoffMultiplier ?? 2,
      jitter: options.retryConfig?.jitter ?? true,
      isRetryable: options.retryConfig?.isRetryable ?? isDefaultRetryable
    };

    const crossTab = options.crossTab ?? false;
    this.coordinator = crossTab === true ? createWebCoordinator() : crossTab || undefined;
    if (crossTab === true && !this.coordinator) {
      this.logger?.warn(
        `[resilient-outbox:${this.name}] crossTab requires the Web Locks API and BroadcastChannel, ` +
          'which this runtime lacks. Running as a single instance.'
      );
    }
    this.isLeader = !this.coordinator;
    this.currentSnapshot = {
      total: 0,
      pending: 0,
      syncing: 0,
      deadLetter: 0,
      isSyncing: false,
      isHydrated: false,
      isLeader: this.isLeader
    };

    if (this.coordinator) {
      this.channel = this.coordinator.openChannel(this.storageKey, message => {
        this.onChannelMessage(message);
      });
      this.releaseLeadership = this.coordinator.requestLeadership(`${this.storageKey}:leader`, () => {
        void this.onBecomeLeader();
      });
    } else {
      this.detectOtherInstances(options.storage !== undefined);
    }
  }

  get storageKey(): string {
    return `@resilient-outbox:${this.name}`;
  }

  get hydrated(): boolean {
    return this.isHydrated;
  }

  private get mutexName(): string {
    return `${this.storageKey}:mutex`;
  }

  // --- Hydration & Persistence ---

  /**
   * Loads persisted state. Idempotent, and called automatically by every async operation,
   * so calling it explicitly is only needed to read state synchronously at startup.
   */
  hydrate(): Promise<void> {
    if (!this.hydratePromise) {
      this.hydratePromise = this.loadFromStorage();
    }
    return this.hydratePromise;
  }

  private async ready(): Promise<void> {
    this.assertNotDestroyed();
    await this.hydrate();
    this.assertNotDestroyed();
  }

  private async loadFromStorage(): Promise<void> {
    try {
      if (this.coordinator) {
        await this.coordinator.withLock(this.mutexName, () => this.syncFromStorage());
      } else {
        await this.mergeFromStorage();
      }
    } catch (err) {
      this.reportStorageError('hydrate', err);
    } finally {
      this.isHydrated = true;
      this.notify();
    }
  }

  /**
   * Reads and parses storage. Throws if the read fails. Corrupt data is backed up to
   * `<storageKey>:corrupt`, reported, and treated as an empty queue.
   */
  private async readState(): Promise<Pick<PersistedState, 'mutations' | 'idMap'> | null> {
    const raw = await this.storage.getItem(this.storageKey);
    if (!raw) return null;
    try {
      return parseState(raw);
    } catch (err) {
      await this.storage.setItem(`${this.storageKey}:corrupt`, raw).catch(() => undefined);
      this.reportStorageError('hydrate', err);
      return null;
    }
  }

  private normalize(m: Mutation, resetSyncing: boolean): Mutation {
    // Persisted data is untrusted: drop keys that could re-point an object's prototype when the
    // record is later merged into a live mutation with Object.assign
    const safe = Object.fromEntries(
      Object.entries(m).filter(([key]) => key !== '__proto__' && key !== 'constructor' && key !== 'prototype')
    ) as unknown as Mutation;
    return {
      ...safe,
      attempts: typeof safe.attempts === 'number' ? safe.attempts : 0,
      maxAttempts: typeof safe.maxAttempts === 'number' ? safe.maxAttempts : this.retryConfig.maxAttempts,
      nextAttemptAt: typeof safe.nextAttemptAt === 'number' ? safe.nextAttemptAt : 0,
      // If the app was killed mid-request, the request may or may not have reached the server.
      // Replaying it is safe because it carries the same idempotency key.
      status: resetSyncing && safe.status === 'syncing' ? 'pending' : safe.status
    };
  }

  /**
   * Single-instance mode: merges storage into memory. Throws only if the read itself fails, in
   * which case writes are held back until a later read succeeds, so unread data is never lost.
   */
  private async mergeFromStorage(): Promise<void> {
    let state: Pick<PersistedState, 'mutations' | 'idMap'> | null;
    try {
      state = await this.readState();
    } catch (err) {
      this.needsRehydrate = true;
      throw err;
    }
    this.needsRehydrate = false;
    if (!state) return;

    const known = new Set(this.mutations.map(m => m.id));
    const restored = state.mutations.filter(m => !known.has(m.id)).map(m => this.normalize(m, true));
    this.mutations = [...restored, ...this.mutations];
    this.index = null;

    const now = Date.now();
    for (const [tempId, serverId, lastUsedAt] of state.idMap) {
      if (!this.idMap.has(tempId)) {
        this.idMap.set(tempId, serverId);
        this.idMapLastUsed.set(tempId, typeof lastUsedAt === 'number' ? lastUsedAt : now);
      }
    }
  }

  /**
   * Cross-tab mode: storage is the source of truth, so memory is replaced by it. Objects that
   * already exist are updated in place, so references returned by `enqueue()` and `get()` stay
   * live. Must be called while holding the mutex.
   */
  private async syncFromStorage(): Promise<void> {
    const state = (await this.readState()) ?? { mutations: [], idMap: [] };

    const local = new Map(this.mutations.map(m => [m.id, m]));
    this.mutations = state.mutations.map(stored => {
      const next = this.normalize(stored, false);
      const existing = local.get(next.id);
      if (!existing) return next;
      local.delete(next.id);
      for (const key of Object.keys(existing)) {
        if (!(key in next)) Reflect.deleteProperty(existing, key);
      }
      return Object.assign(existing, next);
    });
    for (const gone of local.values()) this.removed.add(gone);
    this.index = null;

    const now = Date.now();
    const previousLastUsed = this.idMapLastUsed;
    this.idMap = new Map();
    this.idMapLastUsed = new Map();
    for (const [tempId, serverId, lastUsedAt] of state.idMap) {
      this.idMap.set(tempId, serverId);
      const stored = typeof lastUsedAt === 'number' ? lastUsedAt : now;
      this.idMapLastUsed.set(tempId, Math.max(stored, previousLastUsed.get(tempId) ?? 0));
    }
  }

  /**
   * The single entry point for every state change.
   *
   * - Single instance: applies `fn` to memory, notifies, then schedules a coalesced write.
   * - Cross-tab: under the shared mutex, re-reads storage, applies `fn`, writes, and tells the
   *   other instances to refresh. A failed write rejects and memory is resynced from storage, so
   *   a change is never reported as durable when it is not.
   */
  private async transact<T>(fn: () => T, options: TransactOptions<T> = {}): Promise<T> {
    if (options.internal) await this.hydrate();
    else await this.ready();

    const shouldWrite = (result: T): boolean =>
      typeof options.write === 'function' ? options.write(result) : options.write !== false;

    const coordinator = this.coordinator;
    if (!coordinator) {
      const result = fn();
      this.notify();
      if (shouldWrite(result)) await this.persist();
      return result;
    }

    return coordinator.withLock(this.mutexName, async () => {
      await this.syncFromStorage();
      const result = fn();
      if (shouldWrite(result) && this.autoPersist) {
        try {
          await this.writeState();
        } catch (err) {
          this.reportStorageError('persist', err);
          await this.syncFromStorage().catch(() => undefined);
          this.notify();
          throw err;
        }
        this.channel?.post(this.message('changed'));
      }
      this.notify();
      return result;
    });
  }

  /**
   * Single-instance writes are serialized, and while one write is waiting its turn, further
   * calls share it: the state is serialized when the write starts, so it always contains every
   * change made before it. This bounds storage I/O to one in-flight and one queued write.
   */
  private persist(): Promise<void> {
    if (!this.autoPersist) return Promise.resolve();
    if (this.writeQueued) return this.persistChain;
    this.writeQueued = true;
    this.persistChain = this.persistChain.then(async () => {
      this.writeQueued = false;
      await this.writeNow();
    });
    return this.persistChain;
  }

  private async writeNow(): Promise<void> {
    try {
      if (this.needsRehydrate) {
        await this.mergeFromStorage();
        this.notify();
      }
      await this.writeState();
    } catch (err) {
      this.reportStorageError('persist', err);
    }
  }

  private async writeState(): Promise<void> {
    this.pruneIdMap();
    const state: PersistedState = {
      version: STORAGE_VERSION,
      // Completed mutations are dropped so storage does not grow indefinitely
      mutations: this.mutations.filter(m => m.status !== 'completed'),
      idMap: Array.from(this.idMap.entries(), ([k, v]): IdMapEntry => [k, v, this.idMapLastUsed.get(k)])
    };
    await this.storage.setItem(this.storageKey, JSON.stringify(state));
  }

  /** Drops temp-ID mappings unused for `idMapTtlMs`, unless a queued mutation still references them. */
  private pruneIdMap(now = Date.now()): void {
    if (!Number.isFinite(this.idMapTtlMs)) return;
    const expired: string[] = [];
    for (const [tempId, lastUsed] of this.idMapLastUsed) {
      if (now - lastUsed > this.idMapTtlMs) expired.push(tempId);
    }
    if (expired.length === 0) return;

    const referenced = new Set<string>();
    const collect = (val: unknown): void => {
      if (typeof val === 'string') referenced.add(val);
      else if (Array.isArray(val)) val.forEach(collect);
      else if (isPlainObject(val)) Object.values(val).forEach(collect);
    };
    for (const m of this.mutations) {
      if (m.status !== 'completed') collect(m.payload);
    }

    for (const tempId of expired) {
      if (referenced.has(tempId)) continue;
      this.idMap.delete(tempId);
      this.idMapLastUsed.delete(tempId);
    }
  }

  private reportStorageError(operation: 'hydrate' | 'persist', error: unknown): void {
    this.logger?.error(`[resilient-outbox:${this.name}] Failed to ${operation} queue storage:`, error);
    this.emit('storage_error', { operation, error });
  }

  // --- Enqueue & Mutation Management ---

  async enqueue<TPayload = unknown, TResult = unknown>(
    options: EnqueueOptions<TPayload>
  ): Promise<Mutation<TPayload, TResult>> {
    const raw = options as Partial<EnqueueOptions<TPayload>> | undefined;
    if (typeof raw?.type !== 'string' || raw.type.length === 0) {
      throw new TypeError('[resilient-outbox] `type` must be a non-empty string');
    }

    const now = Date.now();
    const mutation: Mutation<TPayload, TResult> = {
      id: generateUUID(),
      idempotencyKey: options.idempotencyKey ?? generateUUID(),
      type: options.type,
      payload: options.payload,
      tempId: options.tempId ?? legacyTempId(options.payload),
      dependsOn: options.dependsOn,
      createdAt: now,
      attempts: 0,
      maxAttempts: options.maxAttempts ?? this.retryConfig.maxAttempts,
      nextAttemptAt: now,
      status: 'pending'
    };
    if (mutation.tempId === undefined) delete mutation.tempId;
    if (mutation.dependsOn === undefined) delete mutation.dependsOn;

    await this.transact(() => {
      this.mutations.push(mutation);
      this.index = null;
    });
    this.emit('enqueued', { mutation });

    if (this.autoFlush?.options.flushOnEnqueue) {
      this.triggerAutoFlush();
    }
    return mutation;
  }

  private findById(id: string): Mutation | undefined {
    this.index ??= new Map(this.mutations.map(m => [m.id, m]));
    return this.index.get(id);
  }

  get(id: string): Mutation | undefined {
    return this.findById(id);
  }

  getAll(): ReadonlyArray<Mutation> {
    return [...this.mutations];
  }

  getPending(): Mutation[] {
    return this.mutations.filter(m => m.status === 'pending');
  }

  getDeadLetter(): Mutation[] {
    return this.mutations.filter(m => m.status === 'dead_letter');
  }

  /**
   * Removes a mutation. A mutation that is currently syncing cannot be removed (returns `false`).
   * Pending dependents of a removed, uncompleted mutation are moved to the Dead-Letter Queue with
   * code `PARENT_REMOVED`, because they would otherwise run against an entity that never existed.
   */
  remove(id: string): Promise<boolean> {
    return this.transact(
      () => {
        const idx = this.mutations.findIndex(m => m.id === id);
        if (idx === -1) return false;
        const mutation = this.mutations[idx]!;
        if (mutation.status === 'syncing') return false;

        this.mutations.splice(idx, 1);
        this.index = null;
        this.removed.add(mutation);
        if (mutation.status !== 'completed') {
          this.cascadeDeadLetter(
            mutation.id,
            'PARENT_REMOVED',
            `Parent mutation (${mutation.id}) was removed`
          );
        }
        return true;
      },
      { write: removed => removed }
    );
  }

  clear(): Promise<void> {
    return this.transact(() => {
      for (const m of this.mutations) this.removed.add(m);
      this.mutations = [];
      this.index = null;
      this.idMap.clear();
      this.idMapLastUsed.clear();
    });
  }

  /**
   * Moves a dead-lettered mutation back to `pending`, together with every descendant that was
   * dead-lettered only because of it (code `PARENT_FAILED`).
   */
  async retryDeadLetter(id: string): Promise<boolean> {
    const revived = await this.transact(
      () => {
        const mutation = this.findById(id);
        if (mutation?.status !== 'dead_letter') return false;
        const now = Date.now();
        this.revive(mutation, now);
        this.reviveDescendants(mutation.id, now);
        return true;
      },
      { write: r => r }
    );
    if (revived) this.triggerAutoFlush();
    return revived;
  }

  async retryAllDeadLetter(): Promise<number> {
    const count = await this.transact(
      () => {
        const now = Date.now();
        let n = 0;
        for (const m of this.mutations) {
          if (m.status === 'dead_letter') {
            this.revive(m, now);
            n++;
          }
        }
        return n;
      },
      { write: n => n > 0 }
    );
    if (count > 0) this.triggerAutoFlush();
    return count;
  }

  private revive(mutation: Mutation, now: number): void {
    mutation.status = 'pending';
    mutation.attempts = 0;
    mutation.nextAttemptAt = now;
    delete mutation.error;
  }

  private reviveDescendants(parentId: string, now: number): void {
    for (const child of this.mutations) {
      if (
        child.dependsOn === parentId &&
        child.status === 'dead_letter' &&
        child.error?.code === 'PARENT_FAILED'
      ) {
        this.revive(child, now);
        this.reviveDescendants(child.id, now);
      }
    }
  }

  private moveToDeadLetter(mutation: Mutation, error: MutationError): void {
    mutation.status = 'dead_letter';
    mutation.error = error;
    this.emit('dead_letter', { mutation, error });
    this.cascadeDeadLetter(
      mutation.id,
      'PARENT_FAILED',
      `Parent mutation (${mutation.id}) failed in Dead-Letter Queue`
    );
  }

  private cascadeDeadLetter(parentId: string, code: MutationErrorCode, message: string): void {
    for (const child of this.mutations) {
      if (child.dependsOn === parentId && child.status === 'pending') {
        this.moveToDeadLetter(child, { message, code, timestamp: Date.now() });
      }
    }
  }

  // --- Sync Context & ID Resolution ---

  private createSyncContext(signal: AbortSignal): SyncContext {
    const lookup = (val: string): unknown => {
      this.idMapLastUsed.set(val, Date.now());
      return this.idMap.get(val);
    };

    const substitute = (val: unknown): unknown => {
      if (typeof val === 'string') return this.idMap.has(val) ? lookup(val) : val;
      if (Array.isArray(val)) return val.map(substitute);
      // Only plain objects are walked; Dates, class instances, Blobs etc. are passed through
      if (isPlainObject(val)) {
        // fromEntries defines own properties, so a "__proto__" key is copied as data instead of
        // replacing the copy's prototype
        return Object.fromEntries(Object.entries(val).map(([k, v]) => [k, substitute(v)]));
      }
      return val;
    };

    return {
      idMap: this.idMap,
      signal,
      resolveId: (id: unknown): unknown => (typeof id === 'string' && this.idMap.has(id) ? lookup(id) : id),
      resolvePayload: <T = unknown>(payload: T): T => substitute(payload) as T
    };
  }

  // --- Flush & Execution Engine ---

  /**
   * Executes every runnable pending mutation, sequentially, in FIFO order. Concurrent calls share
   * one in-flight run; a call made during a run schedules one more pass so newly enqueued
   * mutations are picked up. Resolves with the number of mutations that completed.
   * No-op (resolves `0`) while `isOnline()` reports offline.
   *
   * With `crossTab`, a non-leader instance asks the leader to flush and resolves with its count.
   */
  flush(): Promise<number> {
    if (this.coordinator && !this.isLeader) return this.requestLeaderFlush();
    if (this.activeFlushPromise) {
      this.rerunRequested = true;
      return this.activeFlushPromise;
    }
    const run = this.runFlush().finally(() => {
      this.activeFlushPromise = null;
    });
    this.activeFlushPromise = run;
    return run;
  }

  private async runFlush(): Promise<number> {
    await this.ready();
    let total = 0;
    try {
      do {
        this.rerunRequested = false;
        if (!(await this.checkOnline())) break;
        total += await this.executeFlush();
      } while (this.shouldRerun());
    } finally {
      this.scheduleWake();
    }
    return total;
  }

  /** Read through a method: `flush()` calls made while a pass is awaiting set the flag. */
  private shouldRerun(): boolean {
    return this.rerunRequested && !this.destroyed;
  }

  private async checkOnline(): Promise<boolean> {
    try {
      // Only an explicit `false` means offline (tolerates probes that return undefined)
      const online: unknown = await this.isOnline();
      return online !== false;
    } catch {
      return true;
    }
  }

  private async executeFlush(): Promise<number> {
    this.isSyncing = true;
    this.notify();

    let syncedCount = 0;
    const now = Date.now();

    try {
      if (this.coordinator) {
        // Pick up mutations other tabs enqueued since our last sync
        await this.transact(() => undefined, { write: false, internal: true });
      }
      const candidates = this.mutations.filter(m => m.status === 'pending');

      for (const mutation of candidates) {
        if (this.destroyed) break;

        // Claim atomically; in cross-tab mode `syncing` is written so other tabs can see it
        const claim = await this.transact(() => this.claim(mutation, now), {
          internal: true,
          write: c => c === 'cascaded' || (c === 'claimed' && this.coordinator !== undefined)
        });
        if (claim !== 'claimed') continue;

        const outcome = await this.execute(mutation);
        // Checkpoint after every mutation so a crash mid-flush loses no progress
        await this.transact(
          () => {
            this.applyOutcome(mutation, outcome);
          },
          { internal: true, write: () => !this.handedOver() }
        );
        if (outcome.ok) syncedCount++;
      }
    } finally {
      this.isSyncing = false;
      this.notify();
    }

    return syncedCount;
  }

  private claim(mutation: Mutation, now: number): ClaimResult {
    // State may have changed since the flush started (removed, cascaded, or edited by another tab)
    if (mutation.status !== 'pending' || this.removed.has(mutation)) return 'skipped';
    if (mutation.nextAttemptAt > now) return 'skipped';

    if (mutation.dependsOn) {
      // A parent that is no longer in the queue completed in an earlier session and was purged
      const parent = this.findById(mutation.dependsOn);
      if (parent) {
        if (parent.status === 'dead_letter') {
          this.moveToDeadLetter(mutation, {
            message: `Parent mutation (${parent.id}) failed in Dead-Letter Queue`,
            code: 'PARENT_FAILED',
            timestamp: Date.now()
          });
          return 'cascaded';
        }
        if (parent.status !== 'completed') return 'skipped';
      }
    }

    mutation.status = 'syncing';
    return 'claimed';
  }

  private async execute(mutation: Mutation): Promise<ExecutionOutcome> {
    const controller = new AbortController();
    const onDestroy = (): void => {
      controller.abort();
    };
    this.destroyController.signal.addEventListener('abort', onDestroy);
    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      const execution = this.executor(mutation, this.createSyncContext(controller.signal));
      const timeoutMs = this.executorTimeoutMs;
      const result =
        timeoutMs !== undefined && timeoutMs > 0
          ? await Promise.race([
              execution,
              new Promise<never>((_, reject) => {
                timer = setTimeout(() => {
                  controller.abort();
                  reject(new OutboxTimeoutError(timeoutMs));
                }, timeoutMs);
              })
            ])
          : await execution;
      return { ok: true, result };
    } catch (error) {
      return { ok: false, error };
    } finally {
      clearTimeout(timer);
      this.destroyController.signal.removeEventListener('abort', onDestroy);
    }
  }

  /** After `destroy()` in cross-tab mode, the next leader owns whatever we had in flight. */
  private handedOver(): boolean {
    return this.destroyed && this.coordinator !== undefined;
  }

  private applyOutcome(mutation: Mutation, outcome: ExecutionOutcome): void {
    if (this.handedOver()) return;
    if (this.removed.has(mutation)) {
      // Cleared while in flight: record the result on the object only, with no side effects
      mutation.status = outcome.ok ? 'completed' : 'pending';
      return;
    }
    if (outcome.ok) this.handleSuccess(mutation, outcome.result);
    else this.handleFailure(mutation, outcome.error);
  }

  private handleSuccess(mutation: Mutation, result: unknown): void {
    mutation.status = 'completed';
    mutation.result = result;
    delete mutation.error;

    const tempId = mutation.tempId ?? legacyTempId(mutation.payload);
    if (tempId !== undefined) {
      const serverId = this.getServerId(result, mutation);
      if (serverId !== undefined && serverId !== null && serverId !== tempId) {
        this.idMap.set(tempId, serverId);
        this.idMapLastUsed.set(tempId, Date.now());
      }
    }

    this.emit('completed', { mutation, result });
  }

  private handleFailure(mutation: Mutation, err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    const statusCode = getErrorStatusCode(err);
    const errorInfo: MutationError = { message, timestamp: Date.now() };
    if (statusCode !== undefined) errorInfo.statusCode = statusCode;

    if (this.destroyed) {
      // Aborted by destroy(): not the mutation's fault, so no attempt is consumed
      mutation.status = 'pending';
      mutation.error = errorInfo;
      return;
    }

    if (!this.retryConfig.isRetryable(err, statusCode)) {
      this.moveToDeadLetter(mutation, { ...errorInfo, code: 'NON_RETRYABLE' });
      return;
    }

    mutation.attempts++;
    if (mutation.attempts >= mutation.maxAttempts) {
      this.moveToDeadLetter(mutation, {
        ...errorInfo,
        code: 'MAX_ATTEMPTS',
        message: `Exceeded max attempts (${mutation.maxAttempts}): ${message}`
      });
      return;
    }

    const delayMs =
      getErrorRetryAfterMs(err) ??
      calculateBackoffMs(
        mutation.attempts,
        this.retryConfig.initialBackoffMs,
        this.retryConfig.maxBackoffMs,
        this.retryConfig.backoffMultiplier,
        this.retryConfig.jitter
      );
    mutation.status = 'pending';
    mutation.error = errorInfo;
    mutation.nextAttemptAt = Date.now() + delayMs;
    this.emit('retry', { mutation, error: err, delayMs });
  }

  // --- Cross-tab coordination ---

  private message(type: ChannelMessage['type'], extra: Partial<ChannelMessage> = {}): ChannelMessage {
    return { source: MESSAGE_SOURCE, from: this.instanceId, type, ...extra };
  }

  private onChannelMessage(raw: unknown): void {
    const msg = raw as Partial<ChannelMessage> | null;
    if (msg?.source !== MESSAGE_SOURCE || msg.from === this.instanceId || this.destroyed) return;

    switch (msg.type) {
      case 'changed':
        void this.refresh().then(() => {
          if (this.autoFlush?.options.flushOnEnqueue) this.triggerAutoFlush();
        });
        break;
      case 'flush-request':
        if (this.isLeader) {
          const id = msg.id;
          void this.flush()
            .catch(() => 0)
            .then(count => {
              this.channel?.post(this.message('flush-result', { id, count }));
            });
        }
        break;
      case 'flush-result': {
        const resolve = msg.id === undefined ? undefined : this.flushRequests.get(msg.id);
        if (resolve) {
          this.flushRequests.delete(msg.id!);
          resolve(typeof msg.count === 'number' ? msg.count : 0);
        }
        break;
      }
      case 'hello':
      case 'hello-ack':
        // An instance without `crossTab` shares our storage key
        this.warnAboutOtherInstances();
        break;
    }
  }

  /** Re-reads storage after another instance changed it. */
  private async refresh(): Promise<void> {
    const coordinator = this.coordinator;
    if (!coordinator) return;
    try {
      await coordinator.withLock(this.mutexName, () => this.syncFromStorage());
      this.notify();
    } catch (err) {
      this.reportStorageError('hydrate', err);
    }
  }

  private async onBecomeLeader(): Promise<void> {
    if (this.destroyed) return;
    this.isLeader = true;
    try {
      // Only the leader ever marks mutations `syncing`. If we just took over, the previous
      // leader died mid-request: retry its in-flight mutations (same idempotency keys).
      await this.transact(
        () => {
          let reset = 0;
          for (const m of this.mutations) {
            if (m.status === 'syncing') {
              m.status = 'pending';
              reset++;
            }
          }
          return reset;
        },
        { write: reset => reset > 0 }
      );
    } catch (err) {
      this.logger?.error(`[resilient-outbox:${this.name}] Failed to take over leadership:`, err);
    }
    this.notify();

    if (this.flushRequests.size > 0) {
      const waiting = [...this.flushRequests.values()];
      this.flushRequests.clear();
      const count = await this.flush().catch(() => 0);
      for (const resolve of waiting) resolve(count);
    }
    this.triggerAutoFlush();
  }

  private requestLeaderFlush(): Promise<number> {
    if (this.followerFlush) return this.followerFlush;
    const run = (async (): Promise<number> => {
      await this.ready();
      const id = generateUUID();
      return new Promise<number>(resolve => {
        const timer = setTimeout(() => {
          this.flushRequests.delete(id);
          resolve(0);
        }, FOLLOWER_FLUSH_TIMEOUT_MS);
        unref(timer);
        this.flushRequests.set(id, count => {
          clearTimeout(timer);
          resolve(count);
        });
        this.channel?.post(this.message('flush-request', { id }));
      });
    })().finally(() => {
      this.followerFlush = null;
    });
    this.followerFlush = run;
    return run;
  }

  /**
   * Without `crossTab`, two instances on one storage key overwrite each other. In browsers, say
   * hello on a BroadcastChannel and warn if another instance answers.
   */
  private detectOtherInstances(storageProvided: boolean): void {
    const g = globalThis as {
      document?: unknown;
      BroadcastChannel?: new (name: string) => {
        onmessage: ((event: { data: unknown }) => void) | null;
        postMessage(message: unknown): void;
        close(): void;
        unref?: () => void;
      };
    };
    if (!storageProvided || this.storage instanceof MemoryStorageAdapter) return;
    if (g.document === undefined || typeof g.BroadcastChannel !== 'function') return;

    const channel = new g.BroadcastChannel(this.storageKey);
    channel.unref?.();
    channel.onmessage = event => {
      const msg = event.data as Partial<ChannelMessage> | null;
      if (msg?.source !== MESSAGE_SOURCE || msg.from === this.instanceId || this.destroyed) return;
      this.warnAboutOtherInstances();
      if (msg.type === 'hello') channel.postMessage(this.message('hello-ack'));
    };
    channel.postMessage(this.message('hello'));
    this.channel = {
      post: m => {
        channel.postMessage(m);
      },
      close: () => {
        channel.close();
      }
    };
  }

  private warnAboutOtherInstances(): void {
    if (this.warnedAboutInstances) return;
    this.warnedAboutInstances = true;
    this.logger?.warn(
      `[resilient-outbox:${this.name}] Another OutboxQueue is using the same storage key (another tab?). ` +
        'Enable `crossTab: true` on every instance to prevent lost updates and duplicate requests.'
    );
  }

  // --- Auto Flush ---

  /**
   * Flushes automatically: immediately, on an interval, after each enqueue, when the device
   * comes back online (where `online` events exist), and exactly when the earliest backoff
   * expires. Returns a function that stops it. Calling it again replaces the previous config.
   * With `crossTab`, only the leader flushes; call this in every tab so failover keeps flushing.
   *
   * On React Native, also call `queue.flush()` from your AppState / NetInfo listeners, and pass
   * an `isOnline` option backed by NetInfo.
   */
  startAutoFlush(options: AutoFlushOptions = {}): () => void {
    this.assertNotDestroyed();
    this.stopAutoFlushing();

    const resolved: Required<AutoFlushOptions> = {
      intervalMs: options.intervalMs ?? 15000,
      flushOnReconnect: options.flushOnReconnect ?? true,
      flushOnEnqueue: options.flushOnEnqueue ?? true
    };

    const tick = (): void => {
      this.triggerAutoFlush();
    };
    const interval = setInterval(tick, resolved.intervalMs);
    unref(interval);

    const target = globalThis as {
      addEventListener?: (type: string, fn: () => void) => void;
      removeEventListener?: (type: string, fn: () => void) => void;
    };
    const listening = resolved.flushOnReconnect && typeof target.addEventListener === 'function';
    if (listening) target.addEventListener!('online', tick);

    const state: AutoFlushState = {
      options: resolved,
      stop: () => {
        clearInterval(interval);
        clearTimeout(state.wakeTimer);
        if (listening) target.removeEventListener?.('online', tick);
        if (this.autoFlush === state) this.autoFlush = null;
      }
    };
    this.autoFlush = state;
    this.triggerAutoFlush();
    return state.stop;
  }

  stopAutoFlushing(): void {
    this.autoFlush?.stop();
  }

  private triggerAutoFlush(): void {
    if (!this.autoFlush || this.destroyed || !this.isLeader) return;
    if (this.isHydrated && !this.mutations.some(m => m.status === 'pending')) return;
    this.flush().catch((err: unknown) => {
      this.logger?.error(`[resilient-outbox:${this.name}] Auto-flush failed:`, err);
    });
  }

  /** Arms a timer for the earliest backoff expiry if it comes before the next interval tick. */
  private scheduleWake(): void {
    const state = this.autoFlush;
    if (!state || this.destroyed) return;
    clearTimeout(state.wakeTimer);
    state.wakeTimer = undefined;

    let earliest = Infinity;
    for (const m of this.mutations) {
      if (m.status === 'pending' && m.nextAttemptAt < earliest) earliest = m.nextAttemptAt;
    }
    if (earliest === Infinity) return;

    const delay = Math.max(0, earliest - Date.now());
    // Already-due mutations are blocked on a parent or on connectivity; leave them to the interval
    if (delay === 0 || delay >= state.options.intervalMs) return;
    state.wakeTimer = setTimeout(() => {
      this.triggerAutoFlush();
    }, delay);
    unref(state.wakeTimer);
  }

  // --- Lifecycle ---

  /**
   * Stops auto-flush, aborts any in-flight executor call (without consuming an attempt), and
   * releases all listeners. With `crossTab`, leadership passes to another instance immediately,
   * and that instance retries whatever was in flight. Persisted state is kept. The instance cannot be used afterwards.
   */
  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.stopAutoFlushing();
    this.destroyController.abort();

    for (const resolve of this.flushRequests.values()) resolve(0);
    this.flushRequests.clear();

    // Hand over immediately, even if an executor ignores its abort signal. The next leader
    // resets and retries anything we had in flight (same idempotency keys).
    this.releaseLeadership?.();
    this.channel?.close();

    this.listeners.clear();
    this.eventListeners.clear();
  }

  private assertNotDestroyed(): void {
    if (this.destroyed) throw new OutboxDestroyedError(this.name);
  }

  // --- Events ---

  /**
   * Subscribes to a lifecycle event. Returns an unsubscribe function. With `crossTab`, execution
   * events (`completed`, `retry`, `dead_letter`) fire in the leader's tab.
   */
  on<K extends OutboxEventName>(event: K, listener: (payload: OutboxEventMap[K]) => void): () => void {
    let set = this.eventListeners.get(event);
    if (!set) {
      set = new Set();
      this.eventListeners.set(event, set);
    }
    const fn = listener as (payload: unknown) => void;
    set.add(fn);
    return () => {
      set.delete(fn);
    };
  }

  private emit<K extends OutboxEventName>(event: K, payload: OutboxEventMap[K]): void {
    const set = this.eventListeners.get(event);
    if (!set) return;
    for (const listener of set) {
      try {
        listener(payload);
      } catch (err) {
        this.logger?.error(`[resilient-outbox:${this.name}] "${event}" listener error:`, err);
      }
    }
  }

  // --- Subscriptions & React Snapshot ---

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private updateSnapshot(): void {
    let pending = 0;
    let syncing = 0;
    let deadLetter = 0;

    for (const m of this.mutations) {
      if (m.status === 'pending') pending++;
      else if (m.status === 'syncing') syncing++;
      else if (m.status === 'dead_letter') deadLetter++;
    }

    const next: QueueStateSnapshot = {
      total: this.mutations.length,
      pending,
      syncing,
      deadLetter,
      // In cross-tab mode another tab may be the one syncing
      isSyncing: this.isSyncing || syncing > 0,
      isHydrated: this.isHydrated,
      isLeader: this.isLeader
    };
    const prev = this.currentSnapshot;
    const unchanged = (Object.keys(next) as (keyof QueueStateSnapshot)[]).every(k => prev[k] === next[k]);
    // Keep referential identity when nothing changed so React skips re-rendering
    if (!unchanged) this.currentSnapshot = next;
  }

  private notify(): void {
    this.version++;
    this.updateSnapshot();
    for (const listener of this.listeners) {
      try {
        listener();
      } catch (err) {
        this.logger?.error(`[resilient-outbox:${this.name}] Listener error:`, err);
      }
    }
  }

  /** Aggregate counts; referentially stable until a count changes. */
  getSnapshot(): QueueStateSnapshot {
    return this.currentSnapshot;
  }

  /**
   * Shallow copies of all mutations; referentially stable until the queue changes.
   * Suitable for `useSyncExternalStore`.
   */
  getMutationsSnapshot(): ReadonlyArray<Mutation> {
    if (this.mutationsSnapshot.version !== this.version) {
      this.mutationsSnapshot = {
        version: this.version,
        value: this.mutations.map(m => ({ ...m }))
      };
    }
    return this.mutationsSnapshot.value;
  }
}
