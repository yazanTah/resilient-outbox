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

  private autoFlush: AutoFlushState | null = null;
  private destroyed = false;
  private readonly destroyController = new AbortController();

  private version = 0;
  private mutationsSnapshot: { version: number; value: ReadonlyArray<Mutation> } = {
    version: -1,
    value: []
  };
  private currentSnapshot: QueueStateSnapshot = {
    total: 0,
    pending: 0,
    syncing: 0,
    deadLetter: 0,
    isSyncing: false,
    isHydrated: false
  };

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
  }

  get storageKey(): string {
    return `@resilient-outbox:${this.name}`;
  }

  get hydrated(): boolean {
    return this.isHydrated;
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
      await this.mergeFromStorage();
    } catch (err) {
      this.reportStorageError('hydrate', err);
    } finally {
      this.isHydrated = true;
      this.notify();
    }
  }

  /**
   * Reads storage and merges it into memory. Throws only if the read itself fails, in which
   * case writes are held back until a later read succeeds, so unread data is never overwritten.
   * Corrupt data is backed up to `<storageKey>:corrupt` and then ignored.
   */
  private async mergeFromStorage(): Promise<void> {
    let raw: string | null;
    try {
      raw = await this.storage.getItem(this.storageKey);
    } catch (err) {
      this.needsRehydrate = true;
      throw err;
    }
    this.needsRehydrate = false;
    if (!raw) return;

    let state: Pick<PersistedState, 'mutations' | 'idMap'>;
    try {
      state = parseState(raw);
    } catch (err) {
      await this.storage.setItem(`${this.storageKey}:corrupt`, raw).catch(() => undefined);
      this.reportStorageError('hydrate', err);
      return;
    }

    const known = new Set(this.mutations.map(m => m.id));
    const restored = state.mutations
      .filter(m => !known.has(m.id))
      .map((m): Mutation => ({
        ...m,
        attempts: typeof m.attempts === 'number' ? m.attempts : 0,
        maxAttempts: typeof m.maxAttempts === 'number' ? m.maxAttempts : this.retryConfig.maxAttempts,
        nextAttemptAt: typeof m.nextAttemptAt === 'number' ? m.nextAttemptAt : 0,
        // If the app was killed mid-request, the request may or may not have reached the
        // server. Replaying it is safe because it carries the same idempotency key.
        status: m.status === 'syncing' ? 'pending' : m.status
      }));
    this.mutations = [...restored, ...this.mutations];

    const now = Date.now();
    for (const [tempId, serverId, lastUsedAt] of state.idMap) {
      if (!this.idMap.has(tempId)) {
        this.idMap.set(tempId, serverId);
        this.idMapLastUsed.set(tempId, typeof lastUsedAt === 'number' ? lastUsedAt : now);
      }
    }
  }

  /**
   * Schedules a write of the current state. Writes are serialized, and while one write is
   * waiting its turn, further calls share it: the state is serialized when the write starts,
   * so it always contains every change made before it. This bounds storage I/O to at most one
   * in-flight and one queued write no matter how fast state changes.
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
      this.pruneIdMap();
      const state: PersistedState = {
        version: STORAGE_VERSION,
        // Completed mutations are dropped so storage does not grow indefinitely
        mutations: this.mutations.filter(m => m.status !== 'completed'),
        idMap: Array.from(this.idMap.entries(), ([k, v]): IdMapEntry => [k, v, this.idMapLastUsed.get(k)])
      };
      await this.storage.setItem(this.storageKey, JSON.stringify(state));
    } catch (err) {
      this.reportStorageError('persist', err);
    }
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
    await this.ready();

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

    this.mutations.push(mutation);
    this.notify();
    this.emit('enqueued', { mutation });
    await this.persist();

    if (this.autoFlush?.options.flushOnEnqueue) {
      this.triggerAutoFlush();
    }
    return mutation;
  }

  get(id: string): Mutation | undefined {
    return this.mutations.find(m => m.id === id);
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
  async remove(id: string): Promise<boolean> {
    await this.ready();
    const idx = this.mutations.findIndex(m => m.id === id);
    if (idx === -1) return false;
    const mutation = this.mutations[idx]!;
    if (mutation.status === 'syncing') return false;

    this.mutations.splice(idx, 1);
    this.removed.add(mutation);
    if (mutation.status !== 'completed') {
      this.cascadeDeadLetter(mutation.id, 'PARENT_REMOVED', `Parent mutation (${mutation.id}) was removed`);
    }
    this.notify();
    await this.persist();
    return true;
  }

  async clear(): Promise<void> {
    await this.ready();
    for (const m of this.mutations) this.removed.add(m);
    this.mutations = [];
    this.idMap.clear();
    this.idMapLastUsed.clear();
    this.notify();
    await this.persist();
  }

  /**
   * Moves a dead-lettered mutation back to `pending`, together with every descendant that was
   * dead-lettered only because of it (code `PARENT_FAILED`).
   */
  async retryDeadLetter(id: string): Promise<boolean> {
    await this.ready();
    const mutation = this.mutations.find(m => m.id === id && m.status === 'dead_letter');
    if (!mutation) return false;

    const now = Date.now();
    this.revive(mutation, now);
    this.reviveDescendants(mutation.id, now);

    this.notify();
    await this.persist();
    this.triggerAutoFlush();
    return true;
  }

  async retryAllDeadLetter(): Promise<number> {
    await this.ready();
    const now = Date.now();
    let count = 0;
    for (const m of this.mutations) {
      if (m.status === 'dead_letter') {
        this.revive(m, now);
        count++;
      }
    }
    if (count > 0) {
      this.notify();
      await this.persist();
      this.triggerAutoFlush();
    }
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
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(val)) out[k] = substitute(v);
        return out;
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
   */
  flush(): Promise<number> {
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
      const byId = new Map(this.mutations.map(m => [m.id, m]));
      const pending = this.mutations.filter(m => m.status === 'pending');

      for (const mutation of pending) {
        if (this.destroyed) break;
        // State may have changed during this flush (cascaded, removed, cleared)
        if (mutation.status !== 'pending' || this.removed.has(mutation)) continue;
        if (mutation.nextAttemptAt > now) continue;

        if (mutation.dependsOn) {
          const parent = byId.get(mutation.dependsOn);
          // A parent that is no longer in the queue completed in an earlier session and was purged
          if (parent && !this.removed.has(parent)) {
            if (parent.status === 'dead_letter') {
              this.moveToDeadLetter(mutation, {
                message: `Parent mutation (${parent.id}) failed in Dead-Letter Queue`,
                code: 'PARENT_FAILED',
                timestamp: Date.now()
              });
              continue;
            }
            if (parent.status !== 'completed') continue;
          }
        }

        if (await this.execute(mutation)) syncedCount++;
        this.notify();
        // Checkpoint after every mutation so a crash mid-flush loses no progress
        await this.persist();
      }
    } finally {
      this.isSyncing = false;
      this.notify();
    }

    return syncedCount;
  }

  private async execute(mutation: Mutation): Promise<boolean> {
    mutation.status = 'syncing';
    this.notify();

    const controller = new AbortController();
    const onDestroy = (): void => controller.abort();
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

      this.handleSuccess(mutation, result);
      return true;
    } catch (err) {
      this.handleFailure(mutation, err);
      return false;
    } finally {
      clearTimeout(timer);
      this.destroyController.signal.removeEventListener('abort', onDestroy);
    }
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

  // --- Auto Flush ---

  /**
   * Flushes automatically: immediately, on an interval, after each enqueue, when the device
   * comes back online (where `online` events exist), and exactly when the earliest backoff
   * expires. Returns a function that stops it. Calling it again replaces the previous config.
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

    const tick = (): void => this.triggerAutoFlush();
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
    if (!this.autoFlush || this.destroyed) return;
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
    state.wakeTimer = setTimeout(() => this.triggerAutoFlush(), delay);
    unref(state.wakeTimer);
  }

  // --- Lifecycle ---

  /**
   * Stops auto-flush, aborts any in-flight executor call (without consuming an attempt), and
   * releases all listeners. Persisted state is kept. The instance cannot be used afterwards.
   */
  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.stopAutoFlushing();
    this.destroyController.abort();
    this.listeners.clear();
    this.eventListeners.clear();
  }

  private assertNotDestroyed(): void {
    if (this.destroyed) throw new OutboxDestroyedError(this.name);
  }

  // --- Events ---

  /** Subscribes to a lifecycle event. Returns an unsubscribe function. */
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

    const prev = this.currentSnapshot;
    if (
      prev.total === this.mutations.length &&
      prev.pending === pending &&
      prev.syncing === syncing &&
      prev.deadLetter === deadLetter &&
      prev.isSyncing === this.isSyncing &&
      prev.isHydrated === this.isHydrated
    ) {
      return; // Keep referential identity so React skips re-rendering
    }

    this.currentSnapshot = {
      total: this.mutations.length,
      pending,
      syncing,
      deadLetter,
      isSyncing: this.isSyncing,
      isHydrated: this.isHydrated
    };
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
