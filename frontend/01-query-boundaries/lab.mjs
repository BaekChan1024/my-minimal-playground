import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { QueryClient, QueryObserver } from '@tanstack/query-core';

// Server-side work is deliberately independent of connection lifetime.
// All completion order is released explicitly; timings are not benchmark results.
const pending = new Map();
const events = [];
let completed = 0, checks = 0;
const server = http.createServer((req, res) => {
  const id = new URL(req.url, 'http://localhost').searchParams.get('id');
  assert(!pending.has(id), `duplicate fixture id: ${id}`);
  events.push(`received:${id}`);
  pending.set(id, () => {
    completed++;
    events.push(`work-completed:${id}`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ id }));
  });
  res.on('close', () => events.push(`connection-closed:${id}`));
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const base = `http://127.0.0.1:${server.address().port}`;
const tick = () => new Promise(r => setTimeout(r, 5));
async function until(fn) {
  const deadline = Date.now() + 5000;
  while (!fn()) { if (Date.now() > deadline) throw new Error('condition timed out'); await tick(); }
}
async function release(id) { await until(() => pending.has(id)); pending.get(id)(); }
const read = (id, signal) => fetch(`${base}/?id=${encodeURIComponent(id)}`, { signal }).then(r => r.json());
const makeClient = () => new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity, staleTime: 30_000 } } });
function equal(actual, expected, label) { assert.deepEqual(actual, expected, label); checks++; }
function report(name, result) { console.log(JSON.stringify({ experiment: name, ...result })); }
const clients = [];
function client() { const c = makeClient(); clients.push(c); return c; }
try {
  // 1. One unguarded state cell accepts whichever response completes last.
  let view;
  const a = read('naive-A').then(v => { view = v.id; });
  const b = read('naive-B').then(v => { view = v.id; });
  await release('naive-B'); await b;
  equal(view, 'naive-B', 'B first');
  await release('naive-A'); await a;
  equal(view, 'naive-A', 'late A overwrites single state cell');
  report('1-naive', { completionOrder: ['B','A'], finalView: view });

  // 2. Do NOT destructure/read context.signal in the unused-signal condition.
  const c2 = client();
  const options2 = id => ({ queryKey: ['search', id], queryFn: () => read(id) });
  const o2 = new QueryObserver(c2, options2('key-A'));
  const off2 = o2.subscribe(() => {});
  await until(() => pending.has('key-A'));
  o2.setOptions(options2('key-B'));
  await release('key-B'); await until(() => o2.getCurrentResult().data?.id === 'key-B');
  await release('key-A'); await until(() => c2.getQueryData(['search','key-A'])?.id === 'key-A');
  equal(o2.getCurrentResult().data.id, 'key-B', 'old response cannot replace B observer data');
  equal(c2.getQueryData(['search','key-A']).id, 'key-A', 'unused A cached');
  report('2-key-isolation', { current: o2.getCurrentResult().data.id, oldCache: c2.getQueryData(['search','key-A']).id });
  off2();

  // 3. Consuming and passing signal cancels fetch when the last observer leaves.
  const c3 = client(); let aborted = false;
  const options3 = id => ({ queryKey: ['search', id], queryFn: ({ signal }) => {
    if (id === 'abort-A') signal.addEventListener('abort', () => { aborted = true; }, { once:true });
    return read(id, signal);
  } });
  const o3 = new QueryObserver(c3, options3('abort-A'));
  const off3 = o3.subscribe(() => {});
  await until(() => pending.has('abort-A'));
  o3.setOptions(options3('abort-B'));
  await until(() => aborted && events.includes('connection-closed:abort-A'));
  const before = completed;
  await release('abort-A');
  equal(completed, before+1, 'fixture work can complete after client cancellation');
  await release('abort-B'); await until(() => o3.getCurrentResult().data?.id === 'abort-B');
  equal(c3.getQueryData(['search','abort-A']), undefined, 'cancelled initial fetch supplies no cached data');
  equal(c3.getQueryState(['search','abort-A']).fetchStatus, 'idle', 'cancelled query idle');
  equal(o3.getCurrentResult().data.id, 'abort-B', 'B remains current');
  equal(aborted, true, 'abort event observed');
  report('3-cancellation', { signalAborted: aborted, oldConnectionClosed: true, serverWorkCompleted: true, oldCache: null, current: 'abort-B' });
  off3();

  // 4. A query key that omits its input represents both requests as one query.
  const c4 = client();
  const options4 = id => ({ queryKey: ['search'], queryFn: () => read(id) });
  const o4 = new QueryObserver(c4, options4('bad-A'));
  const off4 = o4.subscribe(() => {});
  await until(() => pending.has('bad-A'));
  o4.setOptions(options4('bad-B'));
  await release('bad-A'); await until(() => o4.getCurrentResult().isSuccess);
  equal(o4.getCurrentResult().data.id, 'bad-A', 'same key still represents A');
  equal(pending.has('bad-B'), false, 'changing only queryFn did not issue B');
  report('4-missing-key', { requestedInput: 'B', current: o4.getCurrentResult().data.id, BRequestReceived: false });
  off4();

  // 5. A completed fresh cache entry is immediately reusable (no second A fetch).
  const c5 = client(); let aCalls = 0;
  const options5 = id => ({ queryKey: ['search', id], queryFn: () => {
    if (id === 'cache-A') aCalls++;
    return read(id);
  } });
  const o5 = new QueryObserver(c5, options5('cache-A'));
  const off5 = o5.subscribe(() => {});
  await release('cache-A'); await until(() => o5.getCurrentResult().isSuccess);
  o5.setOptions(options5('cache-B'));
  await release('cache-B'); await until(() => o5.getCurrentResult().data?.id === 'cache-B');
  o5.setOptions(options5('cache-A'));
  equal(o5.getCurrentResult().data.id, 'cache-A', 'fresh A immediately reused');
  equal(o5.getCurrentResult().fetchStatus, 'idle', 'no background request for fresh A');
  equal(aCalls, 1, 'A fetched once');
  report('5-return-to-fresh-cache', { current: 'cache-A', ACalls: aCalls, fetchStatus: 'idle' });

  // 6. Explicit invalidation can refetch even within staleTime; old data stays.
  o5.setOptions({ queryKey: ['search','cache-A'], queryFn: () => read('cache-A-v2') });
  const refresh = c5.invalidateQueries({ queryKey: ['search','cache-A'], exact: true });
  await until(() => pending.has('cache-A-v2'));
  equal(o5.getCurrentResult().data.id, 'cache-A', 'old data retained while refetching');
  equal(o5.getCurrentResult().isFetching, true, 'background fetch in progress');
  equal(o5.getCurrentResult().isPending, false, 'cached data is not initial pending');
  await release('cache-A-v2'); await refresh;
  equal(o5.getCurrentResult().data.id, 'cache-A-v2', 'refetch replaced cache');
  report('6-invalidation', { duringFetch: { data:'cache-A', isFetching:true, isPending:false }, after:'cache-A-v2' });
  off5();
  console.log(JSON.stringify({ result:'PASS', experiments:6, assertions:checks, node:process.version, queryCore:'5.101.2' }));
} finally {
  for (const c of clients) c.clear();
  server.closeAllConnections();
  await new Promise(r => server.close(r));
}
