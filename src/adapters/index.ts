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
 * Universal adapter compatible with @react-native-async-storage/async-storage,
 * react-native-mmkv, or window.localStorage.
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
