import { d as OutboxStorageAdapter } from '../types-BRkMBWU8.mjs';

/**
 * In-memory storage adapter for testing and environments without persistent storage.
 */
declare class MemoryStorageAdapter implements OutboxStorageAdapter {
    private storage;
    getItem(key: string): Promise<string | null>;
    setItem(key: string, value: string): Promise<void>;
    removeItem(key: string): Promise<void>;
    clear(): void;
}
interface GenericKeyValueStorage {
    getItem: (key: string) => Promise<string | null> | string | null;
    setItem: (key: string, value: string) => Promise<void> | void;
    removeItem: (key: string) => Promise<void> | void;
}
/**
 * Universal adapter compatible with @react-native-async-storage/async-storage,
 * react-native-mmkv, or window.localStorage.
 */
declare class GenericStorageAdapter implements OutboxStorageAdapter {
    private client;
    constructor(client: GenericKeyValueStorage);
    getItem(key: string): Promise<string | null>;
    setItem(key: string, value: string): Promise<void>;
    removeItem(key: string): Promise<void>;
}

export { type GenericKeyValueStorage, GenericStorageAdapter, MemoryStorageAdapter };
