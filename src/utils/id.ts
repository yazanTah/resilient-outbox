type CryptoLike = Partial<Pick<Crypto, 'randomUUID' | 'getRandomValues'>>;

/**
 * Generates an RFC 4122 version 4 UUID with zero dependencies.
 * Prefers `crypto.randomUUID`, then `crypto.getRandomValues`, and only falls back to
 * `Math.random` on runtimes without Web Crypto (e.g. old Hermes builds). For idempotency keys on
 * such runtimes, install a `crypto.getRandomValues` polyfill such as `react-native-get-random-values`.
 */
export function generateUUID(): string {
  const cryptoApi = (globalThis as { crypto?: CryptoLike }).crypto;

  if (typeof cryptoApi?.randomUUID === 'function') {
    return cryptoApi.randomUUID();
  }

  if (typeof cryptoApi?.getRandomValues === 'function') {
    const bytes = new Uint8Array(16);
    cryptoApi.getRandomValues(bytes);
    bytes[6] = (bytes[6]! & 0x0f) | 0x40; // Version 4
    bytes[8] = (bytes[8]! & 0x3f) | 0x80; // Variant RFC4122

    const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
