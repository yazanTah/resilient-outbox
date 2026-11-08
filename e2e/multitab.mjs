/**
 * End-to-end test of cross-tab coordination in real Chromium tabs sharing one localStorage.
 * Run with `npm run test:e2e` (builds first). Exits non-zero on failure.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const requests = [];
const hanging = [];

const server = createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname.startsWith('/api/')) {
    let body = '';
    for await (const chunk of req) body += chunk;
    const type = url.pathname.slice(5);
    requests.push({
      type,
      tab: req.headers['x-tab'],
      key: req.headers['idempotency-key'],
      body: JSON.parse(body)
    });
    if (type === 'slow') {
      hanging.push(res); // held open: the tab will be closed mid-request
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ id: `srv-${type}-${requests.length}` }));
    return;
  }
  const file = url.pathname === '/' ? 'e2e/app.html' : url.pathname.slice(1);
  if (!file.startsWith('dist/') && file !== 'e2e/app.html') {
    res.writeHead(404).end();
    return;
  }
  try {
    const content = await readFile(join(root, file));
    res.writeHead(200, { 'Content-Type': file.endsWith('.html') ? 'text/html' : 'text/javascript' });
    res.end(content);
  } catch {
    res.writeHead(404).end();
  }
});
await new Promise(resolve => server.listen(0, resolve));
const origin = `http://localhost:${server.address().port}`;

const browser = await chromium.launch();
let failed = false;

async function openTab(context, tab, crossTab = true, autoFlush = false) {
  const page = await context.newPage();
  const warnings = [];
  page.on('console', msg => {
    if (msg.type() === 'warning') warnings.push(msg.text());
  });
  page.on('pageerror', err => {
    throw err;
  });
  await page.goto(`${origin}/?tab=${tab}&crossTab=${crossTab}&autoFlush=${autoFlush}`);
  await page.waitForFunction(() => window.ready === true);
  return Object.assign(page, { warnings });
}

const isLeader = page => page.evaluate(() => window.outbox.getSnapshot().isLeader);
const stored = page =>
  page.evaluate(() => JSON.parse(localStorage.getItem('@resilient-outbox:e2e') ?? '{"mutations":[]}'));
async function waitFor(fn, label, timeout = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await fn()) return;
    await new Promise(r => setTimeout(r, 50));
  }
  throw new Error(`Timed out waiting for: ${label}`);
}

async function scenario(name, fn) {
  requests.length = 0;
  const context = await browser.newContext();
  try {
    await fn(context);
    console.log(`  ✓ ${name}`);
  } catch (err) {
    failed = true;
    console.error(`  ✗ ${name}\n`, err);
  } finally {
    await context.close();
  }
}

console.log('Cross-tab e2e (Chromium)');

await scenario(
  'concurrent enqueues from two tabs are all persisted and sent once, by one tab',
  async context => {
    const a = await openTab(context, 'A');
    const b = await openTab(context, 'B');
    await waitFor(async () => (await isLeader(a)) !== (await isLeader(b)), 'exactly one leader');

    await Promise.all([
      a.evaluate(() =>
        Promise.all(
          Array.from({ length: 25 }, (_, i) => window.outbox.enqueue({ type: `a${i}`, payload: {} }))
        )
      ),
      b.evaluate(() =>
        Promise.all(
          Array.from({ length: 25 }, (_, i) => window.outbox.enqueue({ type: `b${i}`, payload: {} }))
        )
      )
    ]);
    assert.equal((await stored(a)).mutations.length, 50, 'no enqueue was lost');

    const follower = (await isLeader(a)) ? b : a;
    const flushed = await follower.evaluate(() => window.outbox.flush());
    assert.equal(flushed, 50);
    assert.equal(requests.length, 50, 'each mutation sent exactly once');
    assert.equal(new Set(requests.map(r => r.key)).size, 50);
    assert.equal(new Set(requests.map(r => r.tab)).size, 1, 'a single tab did all the sending');
    assert.equal((await stored(a)).mutations.length, 0);
    await waitFor(
      async () => (await follower.evaluate(() => window.outbox.getSnapshot().pending)) === 0,
      'follower UI caught up'
    );
  }
);

await scenario(
  'closing the leader mid-request fails over; the new leader retries with the same key',
  async context => {
    // Realistic setup: every tab runs auto-flush, so whichever tab leads keeps the queue moving
    const a = await openTab(context, 'A', true, true);
    await waitFor(() => isLeader(a), 'A leads');
    const b = await openTab(context, 'B', true, true);

    // Enqueued in the follower; the leader (A) picks it up automatically
    const key = await b.evaluate(
      async () => (await window.outbox.enqueue({ type: 'slow', payload: { amount: 10 } })).idempotencyKey
    );
    await waitFor(() => requests.length === 1, 'A sent the request');
    assert.equal((await stored(b)).mutations[0].status, 'syncing');

    await a.close(); // the browser releases A's Web Lock
    await waitFor(() => isLeader(b), 'B took over');
    // The retried request must carry the same key, so the server can deduplicate it
    await waitFor(() => requests.length === 2, 'B retried the in-flight mutation', 8000);
    assert.deepEqual(
      requests.map(r => [r.tab, r.key]),
      [
        ['A', key],
        ['B', key]
      ]
    );
    for (const res of hanging.splice(0)) res.destroy();
  }
);

await scenario(
  'temp IDs resolved in one tab are substituted by the leader for another tab’s mutation',
  async context => {
    const a = await openTab(context, 'A');
    await waitFor(() => isLeader(a), 'A leads');
    const b = await openTab(context, 'B');
    await b.evaluate(async () => {
      const post = await window.outbox.enqueue({ type: 'post', payload: {}, tempId: 'tmp-1' });
      await window.outbox.enqueue({ type: 'comment', payload: { postId: 'tmp-1' }, dependsOn: post.id });
    });
    assert.equal(await b.evaluate(() => window.outbox.flush()), 2);
    assert.match(requests[1].body.postId, /^srv-post-/);
  }
);

await scenario('two tabs without crossTab are detected and warned about', async context => {
  const a = await openTab(context, 'A', false);
  const b = await openTab(context, 'B', false);
  await waitFor(() => a.warnings.length > 0 && b.warnings.length > 0, 'both tabs warned');
  assert.match(a.warnings[0], /crossTab: true/);
});

await browser.close();
server.close();
if (failed) process.exit(1);
