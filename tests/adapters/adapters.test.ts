import { MemoryStorageAdapter, GenericStorageAdapter, MMKVStorageAdapter } from '../../src/adapters/index.js';

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
        setItem: async (k: string, v: string) => {
          mockRawStore.set(k, v);
        },
        removeItem: async (k: string) => {
          mockRawStore.delete(k);
        }
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

describe('MMKVStorageAdapter', () => {
  const make = () => {
    const map = new Map<string, string>();
    return {
      map,
      getString: (k: string) => map.get(k),
      set: (k: string, v: string) => void map.set(k, v)
    };
  };

  test('works with the v4 `remove` API', async () => {
    const mmkv = { ...make() };
    const remove = jest.fn((k: string) => mmkv.map.delete(k));
    const adapter = new MMKVStorageAdapter({ ...mmkv, remove });
    expect(await adapter.getItem('k')).toBeNull();
    await adapter.setItem('k', 'v');
    expect(await adapter.getItem('k')).toBe('v');
    await adapter.removeItem('k');
    expect(remove).toHaveBeenCalledWith('k');
    expect(await adapter.getItem('k')).toBeNull();
  });

  test('works with the v3 `delete` API', async () => {
    const mmkv = make();
    const del = jest.fn((k: string) => void mmkv.map.delete(k));
    const adapter = new MMKVStorageAdapter({ ...mmkv, delete: del });
    await adapter.setItem('k', 'v');
    await adapter.removeItem('k');
    expect(del).toHaveBeenCalledWith('k');
    expect(await adapter.getItem('k')).toBeNull();
  });

  test('GenericStorageAdapter wraps a synchronous localStorage-like store', async () => {
    const map = new Map<string, string>();
    const adapter = new GenericStorageAdapter({
      getItem: k => map.get(k) ?? null,
      setItem: (k, v) => void map.set(k, v),
      removeItem: k => void map.delete(k)
    });
    await adapter.setItem('a', '1');
    expect(await adapter.getItem('a')).toBe('1');
    await adapter.removeItem('a');
    expect(await adapter.getItem('a')).toBeNull();
  });
});
