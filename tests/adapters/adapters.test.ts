import { MemoryStorageAdapter, GenericStorageAdapter } from '../../src/adapters/index.js';

describe('Storage Adapters', () => {
  describe('MemoryStorageAdapter', () => {
    test('handles set, get, remove, and clear correctly', async () => {
      const adapter = new MemoryStorageAdapter();

      expect(await adapter.getItem('missing')).toBeNull();

      await adapter.setItem('key1', 'value1');
      expect(await adapter.getItem('key1')).toBe('value1');

      await adapter.removeItem('key1');
      expect(await adapter.getItem('key1')).toBeNull();

      await adapter.setItem('a', '1');
      await adapter.setItem('b', '2');
      adapter.clear();
      expect(await adapter.getItem('a')).toBeNull();
      expect(await adapter.getItem('b')).toBeNull();
    });
  });

  describe('GenericStorageAdapter (AsyncStorage / MMKV wrapper)', () => {
    test('wraps synchronous or asynchronous key-value drivers', async () => {
      const mockRawStore = new Map<string, string>();
      const mockDriver = {
        getItem: async (k: string) => mockRawStore.get(k) ?? null,
        setItem: async (k: string, v: string) => { mockRawStore.set(k, v); },
        removeItem: async (k: string) => { mockRawStore.delete(k); }
      };

      const adapter = new GenericStorageAdapter(mockDriver);

      await adapter.setItem('token', 'xyz-123');
      expect(await adapter.getItem('token')).toBe('xyz-123');
      expect(mockRawStore.get('token')).toBe('xyz-123');

      await adapter.removeItem('token');
      expect(await adapter.getItem('token')).toBeNull();
    });
  });
});
