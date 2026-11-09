/**
 * Runs the reference server handler in a separate thread or process, so the tests can exercise
 * real concurrency (worker threads with their own SQLite connections) and real crashes
 * (a child process that exits mid-transaction).
 */
import { isMainThread, parentPort, workerData } from 'node:worker_threads';
import { openDatabase, createOrder } from '../../examples/server/idempotency.mjs';

function run({ dbPath, action, key, startFlag }) {
  const db = openDatabase(dbPath);
  if (startFlag) {
    // Wait until every worker is ready, then all fire at the same moment
    Atomics.wait(new Int32Array(startFlag), 0, 0);
  }
  if (action === 'crash') {
    createOrder(db, key, { item: 'book', amount: 10 }, { afterWrite: () => process.exit(1) });
  }
  if (action === 'create') {
    return createOrder(db, key, { item: 'book', amount: 10 });
  }
  return {
    orders: db.prepare('SELECT COUNT(*) AS n FROM orders').get().n,
    keys: db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n
  };
}

if (!isMainThread) {
  parentPort.postMessage(run(workerData));
} else if (process.argv[2]) {
  const [dbPath, action, key] = process.argv.slice(2);
  console.log(JSON.stringify(run({ dbPath, action, key })));
}
