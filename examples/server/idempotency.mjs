/**
 * Reference server-side idempotency for resilient-outbox, using Node's built-in SQLite
 * (Node >= 22.5). The same pattern works in Postgres or MySQL; see the README.
 *
 * Pattern: the side effect (here, inserting an order) and the idempotency record are written in
 * ONE transaction, and the key is a PRIMARY KEY. That closes both gaps of "check, then write,
 * then save the key":
 *
 * - Concurrent duplicates: BEGIN IMMEDIATE takes the write lock up front, so a second request
 *   with the same key waits, then finds the first request's committed record and replays it.
 *   (In Postgres, the unique index on the key does the same: the second INSERT blocks until the
 *   first transaction commits, then fails, and you read the stored result.)
 * - Crash between the write and saving the key: both are in the same uncommitted transaction,
 *   so neither survives. The client's retry starts clean and creates exactly one order.
 */
import { DatabaseSync } from 'node:sqlite';

export function openDatabase(path) {
  const db = new DatabaseSync(path);
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA busy_timeout = 10000;
    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item TEXT NOT NULL,
      amount INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS idempotency_keys (
      key TEXT PRIMARY KEY,
      status INTEGER NOT NULL,
      body TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
  `);
  return db;
}

/**
 * Handles POST /orders with an Idempotency-Key. Returns `{ status, body, replayed }`.
 * `hooks.afterWrite` exists only so tests can crash the process between the two writes.
 */
export function createOrder(db, key, input, hooks = {}) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const existing = db.prepare('SELECT status, body FROM idempotency_keys WHERE key = ?').get(key);
    if (existing) {
      db.exec('COMMIT');
      return { status: existing.status, body: JSON.parse(existing.body), replayed: true };
    }

    const { lastInsertRowid } = db
      .prepare('INSERT INTO orders (item, amount) VALUES (?, ?)')
      .run(input.item, input.amount);
    const body = { id: Number(lastInsertRowid), item: input.item, amount: input.amount };

    hooks.afterWrite?.();

    db.prepare('INSERT INTO idempotency_keys (key, status, body, created_at) VALUES (?, ?, ?, ?)').run(
      key,
      201,
      JSON.stringify(body),
      Date.now()
    );
    db.exec('COMMIT');
    return { status: 201, body, replayed: false };
  } catch (err) {
    try {
      db.exec('ROLLBACK');
    } catch {
      // The transaction was already rolled back
    }
    throw err;
  }
}

/*
 * Wiring it into Express:
 *
 *   app.post('/orders', (req, res) => {
 *     const key = req.header('Idempotency-Key');
 *     if (!key) return res.status(400).json({ error: 'Idempotency-Key header required' });
 *     const { status, body } = createOrder(db, key, req.body);
 *     res.status(status).json(body);
 *   });
 */
