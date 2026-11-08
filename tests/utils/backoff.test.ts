import {
  calculateBackoffMs,
  getErrorRetryAfterMs,
  getErrorStatusCode,
  isDefaultRetryable
} from '../../src/utils/backoff.js';

describe('Backoff & Status Code Classification', () => {
  describe('calculateBackoffMs', () => {
    test('returns 0 for attempt 0 or negative', () => {
      expect(calculateBackoffMs(0)).toBe(0);
      expect(calculateBackoffMs(-1)).toBe(0);
    });

    test('calculates deterministic exponential backoff when jitter is false', () => {
      const initial = 1000;
      const multiplier = 2;
      const max = 30000;

      // attempt 1: 1000 * 2^0 = 1000
      expect(calculateBackoffMs(1, initial, max, multiplier, false)).toBe(1000);
      // attempt 2: 1000 * 2^1 = 2000
      expect(calculateBackoffMs(2, initial, max, multiplier, false)).toBe(2000);
      // attempt 3: 1000 * 2^2 = 4000
      expect(calculateBackoffMs(3, initial, max, multiplier, false)).toBe(4000);
      // attempt 4: 1000 * 2^3 = 8000
      expect(calculateBackoffMs(4, initial, max, multiplier, false)).toBe(8000);
    });

    test('caps backoff delay at maxMs', () => {
      const initial = 1000;
      const max = 5000;
      // attempt 10 would be 512,000 without cap
      expect(calculateBackoffMs(10, initial, max, 2, false)).toBe(5000);
    });

    test('returns value within range [0, rawBackoff] when jitter is true', () => {
      for (let attempt = 1; attempt <= 5; attempt++) {
        const val = calculateBackoffMs(attempt, 1000, 30000, 2, true);
        const maxForAttempt = Math.min(30000, 1000 * Math.pow(2, attempt - 1));
        expect(val).toBeGreaterThanOrEqual(0);
        expect(val).toBeLessThanOrEqual(maxForAttempt);
      }
    });
  });

  describe('isDefaultRetryable', () => {
    test('identifies 4xx errors as non-retryable (except 429)', () => {
      expect(isDefaultRetryable(new Error('Bad Request'), 400)).toBe(false);
      expect(isDefaultRetryable(new Error('Unauthorized'), 401)).toBe(false);
      expect(isDefaultRetryable(new Error('Forbidden'), 403)).toBe(false);
      expect(isDefaultRetryable(new Error('Not Found'), 404)).toBe(false);
      expect(isDefaultRetryable(new Error('Unprocessable'), 422)).toBe(false);
    });

    test('identifies 429 rate limit as retryable', () => {
      expect(isDefaultRetryable(new Error('Too Many Requests'), 429)).toBe(true);
    });

    test('identifies 5xx server errors as retryable', () => {
      expect(isDefaultRetryable(new Error('Internal Server Error'), 500)).toBe(true);
      expect(isDefaultRetryable(new Error('Bad Gateway'), 502)).toBe(true);
      expect(isDefaultRetryable(new Error('Service Unavailable'), 503)).toBe(true);
      expect(isDefaultRetryable(new Error('Gateway Timeout'), 504)).toBe(true);
    });

    test('identifies network failures and connection resets as retryable', () => {
      expect(isDefaultRetryable(new Error('Failed to fetch'))).toBe(true);
      expect(isDefaultRetryable(new Error('Network request failed'))).toBe(true);
      expect(isDefaultRetryable(new Error('Connection timeout'))).toBe(true);
      expect(isDefaultRetryable(new Error('read ECONNRESET'))).toBe(true);
    });
  });

  describe('isDefaultRetryable edge cases', () => {
    test('408 and 425 retry; 501 and 505 are permanent', () => {
      expect(isDefaultRetryable(null, 408)).toBe(true);
      expect(isDefaultRetryable(null, 425)).toBe(true);
      expect(isDefaultRetryable(null, 501)).toBe(false);
      expect(isDefaultRetryable(null, 505)).toBe(false);
      expect(isDefaultRetryable(null, 302)).toBe(true);
    });
  });

  describe('getErrorStatusCode', () => {
    test('reads statusCode, status and response.status', () => {
      expect(getErrorStatusCode({ statusCode: 500 })).toBe(500);
      expect(getErrorStatusCode({ status: 404 })).toBe(404);
      expect(getErrorStatusCode({ response: { status: 401 } })).toBe(401);
      expect(getErrorStatusCode({ status: '404' })).toBeUndefined();
      expect(getErrorStatusCode({ status: NaN })).toBeUndefined();
      expect(getErrorStatusCode(null)).toBeUndefined();
      expect(getErrorStatusCode('500')).toBeUndefined();
    });
  });

  describe('getErrorRetryAfterMs', () => {
    test('accepts finite non-negative numbers only', () => {
      expect(getErrorRetryAfterMs({ retryAfterMs: 5000 })).toBe(5000);
      expect(getErrorRetryAfterMs({ retryAfterMs: 0 })).toBe(0);
      expect(getErrorRetryAfterMs({ retryAfterMs: -1 })).toBeUndefined();
      expect(getErrorRetryAfterMs({ retryAfterMs: Infinity })).toBeUndefined();
      expect(getErrorRetryAfterMs({})).toBeUndefined();
      expect(getErrorRetryAfterMs(undefined)).toBeUndefined();
    });
  });
});
