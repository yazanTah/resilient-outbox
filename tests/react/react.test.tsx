/**
 * @jest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react';
import { OutboxQueue } from '../../src/core/queue.js';
import { useOutbox, useOutboxMutations } from '../../src/react/index.js';
import { MemoryStorageAdapter } from '../../src/adapters/index.js';

const httpError = (status: number) => Object.assign(new Error(`HTTP ${status}`), { statusCode: status });

describe('useOutbox', () => {
  test('subscribes to queue state and updates reactively', async () => {
    const queue = new OutboxQueue({
      name: 'react-test-queue',
      storage: new MemoryStorageAdapter(),
      executor: async () => ({ success: true })
    });

    const { result } = renderHook(() => useOutbox(queue));
    expect(result.current).toMatchObject({ pending: 0, deadLetter: 0, isSyncing: false });

    await act(async () => {
      await queue.enqueue({ type: 'SYNC_USER', payload: { name: 'Alice' } });
    });
    expect(result.current.pending).toBe(1);
    expect(result.current.total).toBe(1);
    expect(result.current.isHydrated).toBe(true);
    expect(result.current.getPending()).toHaveLength(1);

    await act(async () => {
      await result.current.flush();
    });
    expect(result.current.pending).toBe(0);
    expect(result.current.isSyncing).toBe(false);
  });

  test('exposes stable actions that drive the dead-letter queue', async () => {
    let fail = true;
    const queue = new OutboxQueue({
      name: 'react-dlq',
      executor: async () => {
        if (fail) throw httpError(400);
        return {};
      }
    });
    const { result, rerender } = renderHook(() => useOutbox(queue));
    const firstFlush = result.current.flush;
    rerender();
    expect(result.current.flush).toBe(firstFlush);

    let id = '';
    await act(async () => {
      id = (await queue.enqueue({ type: 'A', payload: {} })).id;
      await queue.enqueue({ type: 'B', payload: {} });
      await result.current.flush();
    });
    expect(result.current.deadLetter).toBe(2);
    expect(result.current.getDeadLetter()).toHaveLength(2);

    fail = false;
    await act(async () => {
      await result.current.remove(id);
      await result.current.retryDeadLetter(result.current.getDeadLetter()[0]!.id);
      await result.current.retryAllDeadLetter();
      await result.current.flush();
    });
    expect(result.current.deadLetter).toBe(0);
    expect(result.current.total).toBe(1);
  });

  test('does not re-render when an update leaves the counts unchanged', async () => {
    const queue = new OutboxQueue({ name: 'react-renders', executor: async () => ({}) });
    let renders = 0;
    renderHook(() => {
      renders++;
      return useOutbox(queue);
    });
    await act(async () => {
      await queue.hydrate();
    });
    const settled = renders;
    await act(async () => {
      await queue.retryAllDeadLetter(); // no-op
      await queue.remove('missing');
    });
    expect(renders).toBe(settled);
  });
});

describe('useOutboxMutations', () => {
  test('returns all mutations or those matching a status filter', async () => {
    const queue = new OutboxQueue({
      name: 'react-mutations',
      executor: async m => {
        if (m.type === 'BAD') throw httpError(422);
        return {};
      }
    });
    const all = renderHook(() => useOutboxMutations(queue));
    const failed = renderHook(() => useOutboxMutations(queue, 'dead_letter'));
    const several = renderHook(() => useOutboxMutations(queue, ['pending', 'dead_letter']));

    await act(async () => {
      await queue.enqueue({ type: 'GOOD', payload: {} });
      await queue.enqueue({ type: 'BAD', payload: {} });
    });
    expect(all.result.current).toHaveLength(2);
    expect(several.result.current).toHaveLength(2);
    expect(failed.result.current).toHaveLength(0);

    await act(async () => {
      await queue.flush();
    });
    expect(failed.result.current.map(m => m.type)).toEqual(['BAD']);
    expect(several.result.current.map(m => m.type)).toEqual(['BAD']);
  });
});
