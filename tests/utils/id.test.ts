import { generateUUID } from '../../src/utils/id.js';

const V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe('generateUUID', () => {
  const original = Object.getOwnPropertyDescriptor(globalThis, 'crypto');
  const withCrypto = (value: unknown, fn: () => void) => {
    Object.defineProperty(globalThis, 'crypto', { value, configurable: true });
    try {
      fn();
    } finally {
      if (original) Object.defineProperty(globalThis, 'crypto', original);
    }
  };

  test('uses crypto.randomUUID when available', () => {
    const id = generateUUID();
    expect(id).toMatch(V4);
    expect(generateUUID()).not.toBe(id);
  });

  test('falls back to crypto.getRandomValues', () => {
    const getRandomValues = jest.fn((arr: Uint8Array) => {
      for (let i = 0; i < arr.length; i++) arr[i] = (i * 37 + 11) & 0xff;
      return arr;
    });
    withCrypto({ getRandomValues }, () => {
      expect(generateUUID()).toMatch(V4);
      expect(getRandomValues).toHaveBeenCalled();
    });
  });

  test('falls back to Math.random without Web Crypto', () => {
    withCrypto(undefined, () => {
      const ids = new Set(Array.from({ length: 100 }, generateUUID));
      expect(ids.size).toBe(100);
      for (const id of ids) expect(id).toMatch(V4);
    });
  });
});
