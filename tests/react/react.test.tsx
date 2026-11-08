/**
 * @jest-environment jsdom
 */
import React from 'react';
import { renderHook, act } from '@testing-library/react';
import { OutboxQueue } from '../../src/core/queue.js';
import { useOutbox } from '../../src/react/index.js';
import { MemoryStorageAdapter } from '../../src/adapters/index.js';

describe('useOutbox React Hook', () => {
  test('subscribes to queue state and updates reactively', async () => {
    const queue = new OutboxQueue({
      name: 'react-test-queue',
      storage: new MemoryStorageAdapter(),
      executor: async () => ({ success: true })
    });

    const { result } = renderHook(() => useOutbox(queue));

    // Initial state: empty
    expect(result.current.pending).toBe(0);
    expect(result.current.deadLetter).toBe(0);
    expect(result.current.isSyncing).toBe(false);

    // Enqueue an item
    await act(async () => {
      await queue.enqueue({ type: 'SYNC_USER', payload: { name: 'Alice' } });
    });

    // Reactive update: pending is 1
    expect(result.current.pending).toBe(1);
    expect(result.current.total).toBe(1);

    // Flush via hook
    await act(async () => {
      await result.current.flush();
    });

    // Reactive update: pending is now 0
    expect(result.current.pending).toBe(0);
    expect(result.current.isSyncing).toBe(false);
  });
});
