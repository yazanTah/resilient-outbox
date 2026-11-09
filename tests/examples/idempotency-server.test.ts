import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';

/**
 * Tests for the reference server in examples/server/idempotency.mjs. The handler runs in worker
 * threads and child processes (see driver.mjs), so concurrency and crashes are real, not mocked.
 * Requires node:sqlite (Node >= 22.5); skipped on older runtimes.
 */
const driver = join(__dirname, 'driver.mjs');

function hasSqlite(): boolean {
  const [major = 0, minor = 0] = process.versions.node.split('.').map(Number);
  return major > 22 || (major === 22 && minor >= 5);
}

interface CreateResult {
  status: number;
  body: { id: number; item: string; amount: number };
  replayed: boolean;
}

interface Counts {
  orders: number;
  keys: number;
}

function runInProcess<T>(dbPath: string, action: string, key = ''): { code: number | null; result?: T } {
  const child = spawnSync(process.execPath, ['--no-warnings', driver, dbPath, action, key], {
    encoding: 'utf8'
  });
  const out = child.stdout.trim();
  return { code: child.status, result: out ? (JSON.parse(out) as T) : undefined };
}

(hasSqlite() ? describe : describe.skip)('Reference server idempotency (examples/server)', () => {
  let dir: string;
  let dbPath: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'outbox-server-'));
    dbPath = join(dir, 'app.db');
  });
  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  test('concurrent requests with the same key create exactly one order', async () => {
    const CONCURRENCY = 8;
    runInProcess(dbPath, 'count'); // create the schema before the race

    const startFlag = new SharedArrayBuffer(4);
    const workers = Array.from(
      { length: CONCURRENCY },
      () =>
        new Worker(driver, {
          workerData: { dbPath, action: 'create', key: 'pay-123', startFlag },
          execArgv: ['--no-warnings']
        })
    );
    const results = Promise.all(
      workers.map(
        w =>
          new Promise<CreateResult>((resolve, reject) => {
            w.once('message', resolve);
            w.once('error', reject);
          })
      )
    );
    // Release every worker at the same instant
    await new Promise(r => setTimeout(r, 200));
    Atomics.store(new Int32Array(startFlag), 0, 1);
    Atomics.notify(new Int32Array(startFlag), 0);

    const responses = await results;
    await Promise.all(workers.map(w => w.terminate()));

    expect(responses.filter(r => !r.replayed)).toHaveLength(1);
    expect(responses.filter(r => r.replayed)).toHaveLength(CONCURRENCY - 1);
    expect(new Set(responses.map(r => r.body.id)).size).toBe(1); // everyone got the same order
    expect(runInProcess<Counts>(dbPath, 'count').result).toEqual({ orders: 1, keys: 1 });
  });

  test('a crash between the write and saving the key leaves nothing behind, so the retry is safe', () => {
    const crashed = runInProcess(dbPath, 'crash', 'pay-456');
    expect(crashed.code).toBe(1); // the process really died mid-transaction
    expect(runInProcess<Counts>(dbPath, 'count').result).toEqual({ orders: 0, keys: 0 });

    const retry = runInProcess<CreateResult>(dbPath, 'create', 'pay-456').result!;
    expect(retry.replayed).toBe(false);
    const again = runInProcess<CreateResult>(dbPath, 'create', 'pay-456').result!;
    expect(again.replayed).toBe(true);
    expect(again.body).toEqual(retry.body);
    expect(runInProcess<Counts>(dbPath, 'count').result).toEqual({ orders: 1, keys: 1 });
  });

  test('different keys create different orders', () => {
    runInProcess(dbPath, 'create', 'a');
    runInProcess(dbPath, 'create', 'b');
    expect(runInProcess<Counts>(dbPath, 'count').result).toEqual({ orders: 2, keys: 2 });
  });
});
