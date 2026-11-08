import type {
  Mutation,
  MutationError,
  MutationExecutor,
  OutboxStorageAdapter,
  OutboxQueueOptions,
  EnqueueOptions,
  QueueStateSnapshot,
  RetryConfig,
  SyncContext
} from './types.js';
import { generateUUID } from '../utils/id.js';
import { calculateBackoffMs, isDefaultRetryable } from '../utils/backoff.js';
import { MemoryStorageAdapter } from '../adapters/index.js';

export class OutboxQueue {
  readonly name: string;
  private storage: OutboxStorageAdapter;
  private executor: MutationExecutor;
  private retryConfig: Required<RetryConfig>;
  private autoPersist: boolean;

  private mutations: Mutation[] = [];
  private idMap = new Map<string, unknown>();
  private listeners = new Set<() => void>();
  private isSyncing = false;
  private activeFlushPromise: Promise<number> | null = null;
  private isHydrated = false;
  private persistChain: Promise<void> = Promise.resolve();
  private stopAutoFlush: (() => void) | null = null;
  private currentSnapshot: QueueStateSnapshot = {
    total: 0,
    pending: 0,
    syncing: 0,
    deadLetter: 0,
    isSyncing: false
  };

  constructor(options: OutboxQueueOptions) {
    this.name = options.name;
    this.storage = options.storage ?? new MemoryStorageAdapter();
    this.executor = options.executor;
    this.autoPersist = options.autoPersist ?? true;

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

  async hydrate(): Promise<void> {
    try {
      const raw = await this.storage.getItem(this.storageKey);
      if (raw) {
        const parsed = JSON.parse(raw);
        // v2 format: { version, mutations, idMap }. Legacy v1 format: Mutation[]
        const list: Mutation[] | undefined = Array.isArray(parsed) ? parsed : parsed?.mutations;
        const ids: Array<[string, unknown]> | undefined = Array.isArray(parsed) ? undefined : parsed?.idMap;
        if (Array.isArray(list)) {
          this.mutations = list.map((m: Mutation) => ({
            ...m,
            // If the app was killed while syncing, reset in-flight state to pending
            status: m.status === 'syncing' ? 'pending' : m.status
          }));
        }
        if (Array.isArray(ids)) {
          for (const [k, v] of ids) this.idMap.set(k, v);
        }
      }
    } catch (err) {
      console.warn(`[resilient-outbox:${this.name}] Failed to hydrate queue from storage:`, err);
    } finally {
      this.isHydrated = true;
      this.notify();
    }
  }

  /**
   * Writes are serialized so concurrent enqueue/flush calls can never land in
   * storage out of order. The state is serialized at write time, so the latest
   * write always reflects the latest in-memory state.
   */
  private persist(): Promise<void> {
    if (!this.autoPersist) return Promise.resolve();
    const write = async (): Promise<void> => {
      try {
        // Filter out completed mutations so storage does not grow indefinitely.
        // The idMap is persisted so temp IDs still resolve after an app restart.
        const toStore = {
          version: 2,
          mutations: this.mutations.filter(m => m.status !== 'completed'),
          idMap: Array.from(this.idMap.entries())
        };
        await this.storage.setItem(this.storageKey, JSON.stringify(toStore));
      } catch (err) {
        console.error(`[resilient-outbox:${this.name}] Failed to persist queue to storage:`, err);
      }
    };
    this.persistChain = this.persistChain.then(write, write);
    return this.persistChain;
  }

  // --- Enqueue & Mutation Management ---

  async enqueue<TPayload = unknown, TResult = unknown>(
    options: EnqueueOptions<TPayload>
  ): Promise<Mutation<TPayload, TResult>> {
    const id = generateUUID();
    const mutation: Mutation<TPayload, TResult> = {
      id,
      idempotencyKey: options.idempotencyKey ?? generateUUID(),
      type: options.type,
      payload: options.payload,
      dependsOn: options.dependsOn,
      createdAt: Date.now(),
      attempts: 0,
      maxAttempts: options.maxAttempts ?? this.retryConfig.maxAttempts,
      nextAttemptAt: Date.now(),
      status: 'pending'
    };

    this.mutations.push(mutation as unknown as Mutation);
    await this.persist();
    this.notify();

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

  async remove(id: string): Promise<boolean> {
    const idx = this.mutations.findIndex(m => m.id === id);
    if (idx === -1) return false;
    this.mutations.splice(idx, 1);
    await this.persist();
    this.notify();
    return true;
  }

  async clear(): Promise<void> {
    this.mutations = [];
    this.idMap.clear();
    await this.persist();
    this.notify();
  }

  async retryDeadLetter(id: string): Promise<boolean> {
    const mutation = this.mutations.find(m => m.id === id && m.status === 'dead_letter');
    if (!mutation) return false;

    mutation.status = 'pending';
    mutation.attempts = 0;
    mutation.nextAttemptAt = Date.now();
    delete mutation.error;

    await this.persist();
    this.notify();
    return true;
  }

  async retryAllDeadLetter(): Promise<number> {
    let count = 0;
    const now = Date.now();
    for (const m of this.mutations) {
      if (m.status === 'dead_letter') {
        m.status = 'pending';
        m.attempts = 0;
        m.nextAttemptAt = now;
        delete m.error;
        count++;
      }
    }
    if (count > 0) {
      await this.persist();
      this.notify();
    }
    return count;
  }

  // --- Sync Context & ID Resolution ---

  private createSyncContext(): SyncContext {
    return {
      idMap: this.idMap,
      resolvePayload: <T = unknown>(payload: T): T => {
        if (!payload || typeof payload !== 'object') return payload;

        // Recursive string value replacement for any temporary ID in payload
        const substitute = (val: unknown): unknown => {
          if (typeof val === 'string' && this.idMap.has(val)) {
            return this.idMap.get(val);
          }
          if (Array.isArray(val)) {
            return val.map(substitute);
          }
          if (val && typeof val === 'object') {
            const out: Record<string, unknown> = {};
            for (const [k, v] of Object.entries(val)) {
              out[k] = substitute(v);
            }
            return out;
          }
          return val;
        };

        return substitute(payload) as T;
      }
    };
  }

  // --- Flush & Execution Engine ---

  async flush(): Promise<number> {
    if (this.activeFlushPromise) {
      return this.activeFlushPromise;
    }

    this.activeFlushPromise = this.executeFlush();
    try {
      return await this.activeFlushPromise;
    } finally {
      this.activeFlushPromise = null;
    }
  }

  private async executeFlush(): Promise<number> {
    this.isSyncing = true;
    this.notify();

    let syncedCount = 0;
    const now = Date.now();

    try {
      const pending = this.mutations.filter(m => m.status === 'pending');

      for (const mutation of pending) {
        // 1. Check if backoff window has passed
        if (mutation.nextAttemptAt > now) {
          continue;
        }

        // 2. Check dependency constraints
        if (mutation.dependsOn) {
          const parent = this.mutations.find(m => m.id === mutation.dependsOn);
          if (parent) {
            if (parent.status === 'dead_letter') {
              // Parent failed permanently; route dependent child to dead letter
              mutation.status = 'dead_letter';
              mutation.error = {
                message: `Parent mutation (${parent.id}) failed in Dead-Letter Queue`,
                timestamp: Date.now()
              };
              continue;
            }
            if (parent.status !== 'completed') {
              // Parent is still pending or syncing; child must wait
              continue;
            }
          }
        }

        // 3. Execute mutation
        mutation.status = 'syncing';
        this.notify();

        const context = this.createSyncContext();

        try {
          const result = await this.executor(mutation, context);
          mutation.status = 'completed';
          mutation.result = result;
          delete mutation.error;

          // If the mutation payload contained a clientTempId or id, map it to server response id
          const payload = mutation.payload as Record<string, unknown> | undefined;
          const resObj = result as Record<string, unknown> | undefined;

          if (payload?.tempId && resObj?.id) {
            this.idMap.set(String(payload.tempId), resObj.id);
          }
          if (payload?.id && resObj?.id && payload.id !== resObj.id) {
            this.idMap.set(String(payload.id), resObj.id);
          }

          syncedCount++;
        } catch (err: unknown) {
          const errorMsg = err instanceof Error ? err.message : String(err);
          const statusCode = (err as { statusCode?: number; status?: number })?.statusCode ??
                             (err as { statusCode?: number; status?: number })?.status;

          const errorInfo: MutationError = {
            message: errorMsg,
            statusCode,
            timestamp: Date.now()
          };

          const retryable = this.retryConfig.isRetryable(err, statusCode);

          if (!retryable) {
            // Permanent client error (4xx) -> Quarantine to Dead-Letter Queue
            mutation.status = 'dead_letter';
            mutation.error = errorInfo;
          } else {
            // Transient error (5xx or network drop)
            mutation.attempts++;
            if (mutation.attempts >= mutation.maxAttempts) {
              mutation.status = 'dead_letter';
              mutation.error = {
                ...errorInfo,
                message: `Exceeded max attempts (${mutation.maxAttempts}): ${errorMsg}`
              };
            } else {
              mutation.status = 'pending';
              mutation.error = errorInfo;
              const delay = calculateBackoffMs(
                mutation.attempts,
                this.retryConfig.initialBackoffMs,
                this.retryConfig.maxBackoffMs,
                this.retryConfig.backoffMultiplier,
                this.retryConfig.jitter
              );
              mutation.nextAttemptAt = Date.now() + delay;
            }
          }
        }

        // Checkpoint after every mutation so a crash mid-flush loses nothing
        await this.persist();
      }

      await this.persist();
    } finally {
      this.isSyncing = false;
      this.notify();
    }

    return syncedCount;
  }

  // --- Auto Flush ---

  /**
   * Periodically flushes the queue and flushes immediately when the device
   * comes back online (where `online` events exist, e.g. web / React Native Web).
   * Returns a function that stops auto-flushing. Calling it again replaces the
   * previous configuration. For React Native, call `queue.flush()` from an
   * AppState / NetInfo listener in addition.
   */
  startAutoFlush(options: { intervalMs?: number; flushOnReconnect?: boolean } = {}): () => void {
    this.stopAutoFlushing();
    const intervalMs = options.intervalMs ?? 15000;
    const run = () => {
      if (this.getPending().length > 0) {
        this.flush().catch(err =>
          console.error(`[resilient-outbox:${this.name}] Auto-flush failed:`, err)
        );
      }
    };

    const timer = setInterval(run, intervalMs);
    (timer as { unref?: () => void }).unref?.();

    const target = globalThis as unknown as {
      addEventListener?: (type: string, fn: () => void) => void;
      removeEventListener?: (type: string, fn: () => void) => void;
    };
    const listen = (options.flushOnReconnect ?? true) && typeof target.addEventListener === 'function';
    if (listen) target.addEventListener!('online', run);

    const stop = () => {
      clearInterval(timer);
      if (listen) target.removeEventListener?.('online', run);
      if (this.stopAutoFlush === stop) this.stopAutoFlush = null;
    };
    this.stopAutoFlush = stop;
    return stop;
  }

  stopAutoFlushing(): void {
    this.stopAutoFlush?.();
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

    this.currentSnapshot = {
      total: this.mutations.length,
      pending,
      syncing,
      deadLetter,
      isSyncing: this.isSyncing
    };
  }

  private notify(): void {
    this.updateSnapshot();
    for (const listener of this.listeners) {
      try {
        listener();
      } catch (err) {
        console.error(`[resilient-outbox:${this.name}] Listener error:`, err);
      }
    }
  }

  getSnapshot(): QueueStateSnapshot {
    return this.currentSnapshot;
  }
}
