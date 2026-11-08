import { OutboxQueue } from '../../src/core/queue.js';
import { MemoryStorageAdapter } from '../../src/adapters/index.js';
import type { Mutation, SyncContext } from '../../src/core/types.js';

describe('OutboxQueue Core Test Suite', () => {
  let storage: MemoryStorageAdapter;

  beforeEach(() => {
    storage = new MemoryStorageAdapter();
  });

  describe('FIFO Execution & Idempotency', () => {
    test('executes mutations in strict FIFO order and provides idempotency keys', async () => {
      const executedOrder: string[] = [];
      const idempotencyKeys: string[] = [];

      const queue = new OutboxQueue({
        name: 'test-fifo',
        storage,
        executor: async mutation => {
          executedOrder.push(mutation.type);
          idempotencyKeys.push(mutation.idempotencyKey);
          return { ok: true };
        }
      });

      await queue.enqueue({ type: 'TASK_1', payload: { step: 1 } });
      await queue.enqueue({ type: 'TASK_2', payload: { step: 2 } });
      await queue.enqueue({ type: 'TASK_3', payload: { step: 3 } });

      expect(queue.getPending().length).toBe(3);

      const processed = await queue.flush();
      expect(processed).toBe(3);
      expect(executedOrder).toEqual(['TASK_1', 'TASK_2', 'TASK_3']);
      expect(idempotencyKeys.length).toBe(3);
      // All idempotency keys must be unique non-empty UUIDs
      const uniqueKeys = new Set(idempotencyKeys);
      expect(uniqueKeys.size).toBe(3);
      expect(queue.getPending().length).toBe(0);
    });
  });

  describe('Transient Errors & Exponential Backoff', () => {
    test('retries transient 500 errors with backoff and routes to DLQ after maxAttempts', async () => {
      let callCount = 0;
      const queue = new OutboxQueue({
        name: 'test-retry',
        storage,
        retryConfig: {
          maxAttempts: 3,
          initialBackoffMs: 50,
          jitter: false
        },
        executor: async () => {
          callCount++;
          const err = new Error('Server Error');
          (err as { statusCode?: number }).statusCode = 500;
          throw err;
        }
      });

      const mut = await queue.enqueue({ type: 'FAILING_TASK', payload: {} });

      // First flush -> attempt 1 fails
      await queue.flush();
      expect(callCount).toBe(1);
      expect(mut.status).toBe('pending');
      expect(mut.attempts).toBe(1);
      expect(mut.nextAttemptAt).toBeGreaterThan(Date.now() + 40);

      // Second flush immediately -> skipped because backoff has not expired
      await queue.flush();
      expect(callCount).toBe(1);

      // Fast forward nextAttemptAt to simulate time passing
      mut.nextAttemptAt = Date.now() - 1;
      await queue.flush();
      expect(callCount).toBe(2);
      expect(mut.attempts).toBe(2);
      expect(mut.status).toBe('pending');

      // Fast forward attempt 3 (final attempt)
      mut.nextAttemptAt = Date.now() - 1;
      await queue.flush();
      expect(callCount).toBe(3);
      // Should now be quarantined to dead_letter!
      expect(mut.status).toBe('dead_letter');
      expect(mut.error?.message).toContain('Exceeded max attempts (3)');
      expect(queue.getDeadLetter().length).toBe(1);
      expect(queue.getPending().length).toBe(0);
    });
  });

  describe('Permanent Client Errors & Dead-Letter Queue (DLQ)', () => {
    test('instantly quarantines 4xx client errors without blocking subsequent queue items', async () => {
      const executedTypes: string[] = [];

      const queue = new OutboxQueue({
        name: 'test-dlq',
        storage,
        executor: async mutation => {
          if (mutation.type === 'BAD_MUTATION') {
            const err = new Error('Validation Failed: Invalid Email');
            (err as { statusCode?: number }).statusCode = 422;
            throw err;
          }
          executedTypes.push(mutation.type);
          return { success: true };
        }
      });

      const badMut = await queue.enqueue({ type: 'BAD_MUTATION', payload: { email: 'invalid' } });
      const goodMut = await queue.enqueue({ type: 'GOOD_MUTATION', payload: { ok: true } });

      await queue.flush();

      // Bad mutation must be quarantined to DLQ on attempt 1
      expect(badMut.status).toBe('dead_letter');
      expect(badMut.error?.statusCode).toBe(422);

      // Good mutation must NOT be blocked!
      expect(goodMut.status).toBe('completed');
      expect(executedTypes).toEqual(['GOOD_MUTATION']);

      // Retry dead letter
      const retried = await queue.retryDeadLetter(badMut.id);
      expect(retried).toBe(true);
      expect(badMut.status).toBe('pending');
      expect(badMut.attempts).toBe(0);
      expect(badMut.error).toBeUndefined();
    });
  });

  describe('Dependency Chaining & Temporary ID Mapping', () => {
    test('executes parent-child mutations sequentially and resolves temporary client IDs', async () => {
      const serverDatabase: Record<string, { id: string; title: string }> = {};
      const commentsDatabase: Record<string, { id: string; postId: string; text: string }> = {};

      const queue = new OutboxQueue({
        name: 'test-deps',
        storage,
        executor: async (mutation: Mutation, context: SyncContext) => {
          if (mutation.type === 'CREATE_POST') {
            const payload = mutation.payload as { tempId: string; title: string };
            const serverId = 'post_srv_9999';
            serverDatabase[serverId] = { id: serverId, title: payload.title };
            return { id: serverId };
          }

          if (mutation.type === 'CREATE_COMMENT') {
            // Resolve temporary IDs in payload
            const resolvedPayload = context.resolvePayload(
              mutation.payload as { postId: string; text: string }
            );
            const commentId = 'comment_srv_1111';
            commentsDatabase[commentId] = { id: commentId, ...resolvedPayload };
            return { id: commentId };
          }

          throw new Error(`Unknown type ${mutation.type}`);
        }
      });

      // 1. Create Post with temp ID
      const postMut = await queue.enqueue({
        type: 'CREATE_POST',
        payload: { tempId: 'temp_post_client_1', title: 'Offline First Architecture' }
      });

      // 2. Create Comment referencing the temp ID and depending on postMut.id
      const commentMut = await queue.enqueue({
        type: 'CREATE_COMMENT',
        payload: { postId: 'temp_post_client_1', text: 'Great article!' },
        dependsOn: postMut.id
      });

      // Flush queue
      const flushed = await queue.flush();
      expect(flushed).toBe(2);

      expect(postMut.status).toBe('completed');
      expect(commentMut.status).toBe('completed');

      // Verify that the comment's postId was dynamically mapped to the real server ID!
      const savedComment = Object.values(commentsDatabase)[0];
      expect(savedComment?.postId).toBe('post_srv_9999');
      expect(savedComment?.text).toBe('Great article!');
    });

    test('routes dependent child to DLQ if parent fails permanently', async () => {
      const queue = new OutboxQueue({
        name: 'test-parent-fail',
        storage,
        executor: async mutation => {
          if (mutation.type === 'PARENT') {
            const err = new Error('Unauthorized');
            (err as { statusCode?: number }).statusCode = 401;
            throw err;
          }
          return { ok: true };
        }
      });

      const parent = await queue.enqueue({ type: 'PARENT', payload: {} });
      const child = await queue.enqueue({ type: 'CHILD', payload: {}, dependsOn: parent.id });

      await queue.flush();

      expect(parent.status).toBe('dead_letter');
      expect(child.status).toBe('dead_letter');
      expect(child.error?.message).toContain('Parent mutation');
    });
  });

  describe('Concurrency & Non-Reentrant Locking', () => {
    test('coalesces concurrent flush calls so mutations are not executed twice', async () => {
      let executionCount = 0;

      const queue = new OutboxQueue({
        name: 'test-concurrency',
        storage,
        executor: async () => {
          executionCount++;
          // Simulate network latency
          await new Promise(resolve => setTimeout(resolve, 50));
          return { ok: true };
        }
      });

      await queue.enqueue({ type: 'SINGLE_CHARGE', payload: { amount: 100 } });

      // Trigger 5 concurrent flush calls simultaneously!
      const results = await Promise.all([
        queue.flush(),
        queue.flush(),
        queue.flush(),
        queue.flush(),
        queue.flush()
      ]);

      // Execution MUST happen exactly once!
      expect(executionCount).toBe(1);
      expect(results[0]).toBe(1);
      expect(queue.getSnapshot().total).toBe(1);
      expect(queue.getSnapshot().pending).toBe(0);
    });
  });

  describe('Durability & Hydration Resilience', () => {
    test('survives app restart and restores in-flight mutations from storage', async () => {
      const queue1 = new OutboxQueue({
        name: 'durable-app',
        storage,
        executor: async () => ({ ok: true })
      });

      await queue1.enqueue({ type: 'SAVE_NOTE', payload: { text: 'Remember the milk' } });

      // Simulate app kill: create a completely new Queue instance sharing the same storage
      const queue2 = new OutboxQueue({
        name: 'durable-app',
        storage,
        executor: async () => ({ ok: true })
      });

      expect(queue2.getPending().length).toBe(0); // Before hydration
      await queue2.hydrate();
      expect(queue2.getPending().length).toBe(1);
      expect((queue2.getPending()[0]?.payload as { text: string }).text).toBe('Remember the milk');

      // Now flush in the restored instance
      const synced = await queue2.flush();
      expect(synced).toBe(1);
      expect(queue2.getPending().length).toBe(0);
    });
  });
});
