// src/utils/id.ts
function generateUUID() {
  if (typeof globalThis !== "undefined" && globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }
  if (typeof globalThis !== "undefined" && globalThis.crypto && typeof globalThis.crypto.getRandomValues === "function") {
    const bytes = new Uint8Array(16);
    globalThis.crypto.getRandomValues(bytes);
    bytes[6] = bytes[6] & 15 | 64;
    bytes[8] = bytes[8] & 63 | 128;
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    const v = c === "x" ? r : r & 3 | 8;
    return v.toString(16);
  });
}

// src/utils/backoff.ts
function calculateBackoffMs(attempt, initialMs = 1e3, maxMs = 3e4, multiplier = 2, useJitter = true) {
  if (attempt <= 0) return 0;
  const rawBackoff = Math.min(maxMs, initialMs * Math.pow(multiplier, attempt - 1));
  if (!useJitter) {
    return Math.round(rawBackoff);
  }
  return Math.round(Math.random() * rawBackoff);
}
function isDefaultRetryable(error, statusCode) {
  if (statusCode !== void 0) {
    if (statusCode === 429) return true;
    if (statusCode >= 400 && statusCode < 500) return false;
    if (statusCode >= 500) return true;
  }
  if (error instanceof Error) {
    const msg = error.message.toLowerCase();
    if (msg.includes("network") || msg.includes("timeout") || msg.includes("failed to fetch") || msg.includes("econnreset") || msg.includes("econnrefused")) {
      return true;
    }
  }
  return true;
}

// src/adapters/index.ts
var MemoryStorageAdapter = class {
  storage = /* @__PURE__ */ new Map();
  async getItem(key) {
    return this.storage.get(key) ?? null;
  }
  async setItem(key, value) {
    this.storage.set(key, value);
  }
  async removeItem(key) {
    this.storage.delete(key);
  }
  clear() {
    this.storage.clear();
  }
};
var GenericStorageAdapter = class {
  constructor(client) {
    this.client = client;
  }
  client;
  async getItem(key) {
    const val = await this.client.getItem(key);
    return val ?? null;
  }
  async setItem(key, value) {
    await this.client.setItem(key, value);
  }
  async removeItem(key) {
    await this.client.removeItem(key);
  }
};

// src/core/queue.ts
var OutboxQueue = class {
  name;
  storage;
  executor;
  retryConfig;
  autoPersist;
  mutations = [];
  idMap = /* @__PURE__ */ new Map();
  listeners = /* @__PURE__ */ new Set();
  isSyncing = false;
  activeFlushPromise = null;
  isHydrated = false;
  persistChain = Promise.resolve();
  stopAutoFlush = null;
  currentSnapshot = {
    total: 0,
    pending: 0,
    syncing: 0,
    deadLetter: 0,
    isSyncing: false
  };
  constructor(options) {
    this.name = options.name;
    this.storage = options.storage ?? new MemoryStorageAdapter();
    this.executor = options.executor;
    this.autoPersist = options.autoPersist ?? true;
    this.retryConfig = {
      maxAttempts: options.retryConfig?.maxAttempts ?? 5,
      initialBackoffMs: options.retryConfig?.initialBackoffMs ?? 1e3,
      maxBackoffMs: options.retryConfig?.maxBackoffMs ?? 3e4,
      backoffMultiplier: options.retryConfig?.backoffMultiplier ?? 2,
      jitter: options.retryConfig?.jitter ?? true,
      isRetryable: options.retryConfig?.isRetryable ?? isDefaultRetryable
    };
  }
  get storageKey() {
    return `@resilient-outbox:${this.name}`;
  }
  get hydrated() {
    return this.isHydrated;
  }
  // --- Hydration & Persistence ---
  async hydrate() {
    try {
      const raw = await this.storage.getItem(this.storageKey);
      if (raw) {
        const parsed = JSON.parse(raw);
        const list = Array.isArray(parsed) ? parsed : parsed?.mutations;
        const ids = Array.isArray(parsed) ? void 0 : parsed?.idMap;
        if (Array.isArray(list)) {
          this.mutations = list.map((m) => ({
            ...m,
            // If the app was killed while syncing, reset in-flight state to pending
            status: m.status === "syncing" ? "pending" : m.status
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
  persist() {
    if (!this.autoPersist) return Promise.resolve();
    const write = async () => {
      try {
        const toStore = {
          version: 2,
          mutations: this.mutations.filter((m) => m.status !== "completed"),
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
  async enqueue(options) {
    const id = generateUUID();
    const mutation = {
      id,
      idempotencyKey: options.idempotencyKey ?? generateUUID(),
      type: options.type,
      payload: options.payload,
      dependsOn: options.dependsOn,
      createdAt: Date.now(),
      attempts: 0,
      maxAttempts: options.maxAttempts ?? this.retryConfig.maxAttempts,
      nextAttemptAt: Date.now(),
      status: "pending"
    };
    this.mutations.push(mutation);
    await this.persist();
    this.notify();
    return mutation;
  }
  get(id) {
    return this.mutations.find((m) => m.id === id);
  }
  getAll() {
    return [...this.mutations];
  }
  getPending() {
    return this.mutations.filter((m) => m.status === "pending");
  }
  getDeadLetter() {
    return this.mutations.filter((m) => m.status === "dead_letter");
  }
  async remove(id) {
    const idx = this.mutations.findIndex((m) => m.id === id);
    if (idx === -1) return false;
    this.mutations.splice(idx, 1);
    await this.persist();
    this.notify();
    return true;
  }
  async clear() {
    this.mutations = [];
    this.idMap.clear();
    await this.persist();
    this.notify();
  }
  async retryDeadLetter(id) {
    const mutation = this.mutations.find((m) => m.id === id && m.status === "dead_letter");
    if (!mutation) return false;
    mutation.status = "pending";
    mutation.attempts = 0;
    mutation.nextAttemptAt = Date.now();
    delete mutation.error;
    await this.persist();
    this.notify();
    return true;
  }
  async retryAllDeadLetter() {
    let count = 0;
    const now = Date.now();
    for (const m of this.mutations) {
      if (m.status === "dead_letter") {
        m.status = "pending";
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
  createSyncContext() {
    return {
      idMap: this.idMap,
      resolvePayload: (payload) => {
        if (!payload || typeof payload !== "object") return payload;
        const substitute = (val) => {
          if (typeof val === "string" && this.idMap.has(val)) {
            return this.idMap.get(val);
          }
          if (Array.isArray(val)) {
            return val.map(substitute);
          }
          if (val && typeof val === "object") {
            const out = {};
            for (const [k, v] of Object.entries(val)) {
              out[k] = substitute(v);
            }
            return out;
          }
          return val;
        };
        return substitute(payload);
      }
    };
  }
  // --- Flush & Execution Engine ---
  async flush() {
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
  async executeFlush() {
    this.isSyncing = true;
    this.notify();
    let syncedCount = 0;
    const now = Date.now();
    try {
      const pending = this.mutations.filter((m) => m.status === "pending");
      for (const mutation of pending) {
        if (mutation.nextAttemptAt > now) {
          continue;
        }
        if (mutation.dependsOn) {
          const parent = this.mutations.find((m) => m.id === mutation.dependsOn);
          if (parent) {
            if (parent.status === "dead_letter") {
              mutation.status = "dead_letter";
              mutation.error = {
                message: `Parent mutation (${parent.id}) failed in Dead-Letter Queue`,
                timestamp: Date.now()
              };
              continue;
            }
            if (parent.status !== "completed") {
              continue;
            }
          }
        }
        mutation.status = "syncing";
        this.notify();
        const context = this.createSyncContext();
        try {
          const result = await this.executor(mutation, context);
          mutation.status = "completed";
          mutation.result = result;
          delete mutation.error;
          const payload = mutation.payload;
          const resObj = result;
          if (payload?.tempId && resObj?.id) {
            this.idMap.set(String(payload.tempId), resObj.id);
          }
          if (payload?.id && resObj?.id && payload.id !== resObj.id) {
            this.idMap.set(String(payload.id), resObj.id);
          }
          syncedCount++;
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : String(err);
          const statusCode = err?.statusCode ?? err?.status;
          const errorInfo = {
            message: errorMsg,
            statusCode,
            timestamp: Date.now()
          };
          const retryable = this.retryConfig.isRetryable(err, statusCode);
          if (!retryable) {
            mutation.status = "dead_letter";
            mutation.error = errorInfo;
          } else {
            mutation.attempts++;
            if (mutation.attempts >= mutation.maxAttempts) {
              mutation.status = "dead_letter";
              mutation.error = {
                ...errorInfo,
                message: `Exceeded max attempts (${mutation.maxAttempts}): ${errorMsg}`
              };
            } else {
              mutation.status = "pending";
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
  startAutoFlush(options = {}) {
    this.stopAutoFlushing();
    const intervalMs = options.intervalMs ?? 15e3;
    const run = () => {
      if (this.getPending().length > 0) {
        this.flush().catch(
          (err) => console.error(`[resilient-outbox:${this.name}] Auto-flush failed:`, err)
        );
      }
    };
    const timer = setInterval(run, intervalMs);
    timer.unref?.();
    const target = globalThis;
    const listen = (options.flushOnReconnect ?? true) && typeof target.addEventListener === "function";
    if (listen) target.addEventListener("online", run);
    const stop = () => {
      clearInterval(timer);
      if (listen) target.removeEventListener?.("online", run);
      if (this.stopAutoFlush === stop) this.stopAutoFlush = null;
    };
    this.stopAutoFlush = stop;
    return stop;
  }
  stopAutoFlushing() {
    this.stopAutoFlush?.();
  }
  // --- Subscriptions & React Snapshot ---
  subscribe(listener) {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
  updateSnapshot() {
    let pending = 0;
    let syncing = 0;
    let deadLetter = 0;
    for (const m of this.mutations) {
      if (m.status === "pending") pending++;
      else if (m.status === "syncing") syncing++;
      else if (m.status === "dead_letter") deadLetter++;
    }
    this.currentSnapshot = {
      total: this.mutations.length,
      pending,
      syncing,
      deadLetter,
      isSyncing: this.isSyncing
    };
  }
  notify() {
    this.updateSnapshot();
    for (const listener of this.listeners) {
      try {
        listener();
      } catch (err) {
        console.error(`[resilient-outbox:${this.name}] Listener error:`, err);
      }
    }
  }
  getSnapshot() {
    return this.currentSnapshot;
  }
};
export {
  GenericStorageAdapter,
  MemoryStorageAdapter,
  OutboxQueue,
  calculateBackoffMs,
  generateUUID,
  isDefaultRetryable
};
//# sourceMappingURL=index.mjs.map