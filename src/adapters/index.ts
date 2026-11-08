import type { OutboxStorageAdapter } from '../core/types.js';

/**
 * In-memory storage adapter for testing and environments without persistent storage.
 */
export class MemoryStorageAdapter implements OutboxStorageAdapter {
  private storage = new Map<string, string>();

  async getItem(key: string): Promise<string | null> {
    return this.storage.get(key) ?? null;
  }

  async setItem(key: string, value: string): Promise<void> {
    this.storage.set(key, value);
  }

  async removeItem(key: string): Promise<void> {
    this.storage.delete(key);
  }

  clear(): void {
    this.storage.clear();
  }
}

export interface GenericKeyValueStorage {
  getItem: (key: string) => Promise<string | null> | string | null;
  setItem: (key: string, value: string) => Promise<void> | void;
  removeItem: (key: string) => Promise<void> | void;
}

/**
 * Adapter for any `getItem` / `setItem` / `removeItem` store, sync or async:
 * `@react-native-async-storage/async-storage`, `window.localStorage`, `expo-secure-store`
 * style wrappers, etc. For `react-native-mmkv`, use {@link MMKVStorageAdapter}.
 */
export class GenericStorageAdapter implements OutboxStorageAdapter {
  constructor(private client: GenericKeyValueStorage) {}

  async getItem(key: string): Promise<string | null> {
    const val = await this.client.getItem(key);
    return val ?? null;
  }

  async setItem(key: string, value: string): Promise<void> {
    await this.client.setItem(key, value);
  }

  async removeItem(key: string): Promise<void> {
    await this.client.removeItem(key);
  }
}

/** The subset of the `react-native-mmkv` instance API used by {@link MMKVStorageAdapter}. */
export interface MMKVLike {
  getString: (key: string) => string | undefined;
  set: (key: string, value: string) => void;
  /** react-native-mmkv v3 */
  delete?: (key: string) => void;
  /** react-native-mmkv v4+ */
  remove?: (key: string) => unknown;
}

/**
 * Adapter for `react-native-mmkv` (v3 and v4+).
 *
 * @example
 * import { createMMKV } from 'react-native-mmkv';
 * const storage = new MMKVStorageAdapter(createMMKV({ id: 'outbox' }));
 */
export class MMKVStorageAdapter implements OutboxStorageAdapter {
  constructor(private mmkv: MMKVLike) {}

  async getItem(key: string): Promise<string | null> {
    return this.mmkv.getString(key) ?? null;
  }

  async setItem(key: string, value: string): Promise<void> {
    this.mmkv.set(key, value);
  }

  async removeItem(key: string): Promise<void> {
    if (typeof this.mmkv.remove === 'function') this.mmkv.remove(key);
    else this.mmkv.delete?.(key);
  }
}
