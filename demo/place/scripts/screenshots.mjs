// Captures desktop and mobile screenshots of a running place page with
// headless Chromium over the DevTools protocol: paints three strokes, starts
// the swarm, engages and releases the kill switch, and paints on a phone
// with touch input. Used for PR evidence; not part of CI.
//
//   npm run build && npx vite preview --port 5187 &
//   node scripts/screenshots.mjs http://127.0.0.1:5187/ ./shots
//
// Environment: CHROME_BIN (default /usr/bin/chromium); PLACE_OPERATOR_TOKEN
// (default the mock's token; against the real back end, a token from
// `aspen-place operator-token`).
import { spawn } from 'node:child_process';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const pageUrl = process.argv[2] ?? 'http://127.0.0.1:5187/';
const out = process.argv[3] ?? 'shots';
const port = 9333;
const operatorToken = process.env.PLACE_OPERATOR_TOKEN ?? 'mock-operator-token';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
mkdirSync(out, { recursive: true });

const chrome = spawn(
  process.env.CHROME_BIN ?? '/usr/bin/chromium',
  [
    '--headless=new',
    '--no-sandbox',
    '--disable-gpu',
    '--hide-scrollbars',
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${mkdtempSync(join(tmpdir(), 'place-shots-'))}`,
    'about:blank',
  ],
  { stdio: 'ignore' },
);

let ws;
for (let i = 0; i < 50 && !ws; i++) {
  try {
    const list = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
    const page = list.find((t) => t.type === 'page');
    if (page) ws = new WebSocket(page.webSocketDebuggerUrl);
  } catch {
    await sleep(200);
  }
}
if (!ws) throw new Error('Chromium did not start');
await new Promise((r) => ws.addEventListener('open', r, { once: true }));

let nextId = 0;
const pending = new Map();
const problems = [];
ws.addEventListener('message', (m) => {
  const msg = JSON.parse(m.data);
  if (msg.id && pending.has(msg.id)) {
    pending.get(msg.id)(msg);
    pending.delete(msg.id);
  }
  if (msg.method === 'Runtime.exceptionThrown') problems.push(msg.params.exceptionDetails.text);
  if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') {
    problems.push(msg.params.args.map((a) => a.value ?? a.description).join(' '));
  }
});
const cdp = (method, params = {}) =>
  new Promise((resolve, reject) => {
    const id = ++nextId;
    pending.set(id, (m) => (m.error ? reject(new Error(`${method}: ${m.error.message}`)) : resolve(m.result)));
    ws.send(JSON.stringify({ id, method, params }));
  });
const evaluate = async (expression) =>
  (await cdp('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true })).result.value;
const text = (id) => evaluate(`document.getElementById(${JSON.stringify(id)}).textContent`);
const shot = async (name) => {
  const { data } = await cdp('Page.captureScreenshot', { format: 'png' });
  writeFileSync(join(out, `${name}.png`), Buffer.from(data, 'base64'));
  console.log(`wrote ${name}.png`);
};
const canvasRect = () =>
  evaluate(
    `(() => { const r = document.getElementById('canvas').getBoundingClientRect(); return { x: r.x, y: r.y, w: r.width, h: r.height }; })()`,
  );
const operator = (body) =>
  evaluate(
    `fetch('/api/v1/admin/kill-switch', { method: 'POST', headers: { Authorization: ${JSON.stringify(`Bearer ${operatorToken}`)}, 'Content-Type': 'application/json' }, body: ${JSON.stringify(JSON.stringify(body))} }).then((r) => r.status)`,
  );

async function mouseStroke(points) {
  const [first, ...rest] = points;
  await cdp('Input.dispatchMouseEvent', { type: 'mouseMoved', x: first[0], y: first[1] });
  await cdp('Input.dispatchMouseEvent', {
    type: 'mousePressed',
    x: first[0],
    y: first[1],
    button: 'left',
    clickCount: 1,
  });
  for (const [x, y] of rest) {
    await cdp('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y, button: 'left', buttons: 1 });
    await sleep(8);
  }
  const last = points.at(-1);
  await cdp('Input.dispatchMouseEvent', {
    type: 'mouseReleased',
    x: last[0],
    y: last[1],
    button: 'left',
    clickCount: 1,
  });
}

function polyline(rect, cols, cells) {
  const cell = rect.w / cols;
  const pts = [];
  for (let i = 0; i < cells.length - 1; i++) {
    const [x0, y0] = cells[i];
    const [x1, y1] = cells[i + 1];
    for (let t = 0; t <= 10; t++) {
      pts.push([rect.x + (x0 + ((x1 - x0) * t) / 10 + 0.5) * cell, rect.y + (y0 + ((y1 - y0) * t) / 10 + 0.5) * cell]);
    }
  }
  return pts;
}

await cdp('Page.enable');
await cdp('Runtime.enable');

// Desktop -------------------------------------------------------------
await cdp('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
await cdp('Page.navigate', { url: pageUrl });
await sleep(3500);
const cols = await evaluate(`Number(getComputedStyle(document.documentElement).getPropertyValue('--cols'))`);
await evaluate(`document.querySelector('input[name=color][value="#2f3ab4"]').click()`);
let rect = await canvasRect();
// An "A" in three strokes, so three pushes.
for (const stroke of [
  [
    [40, 22],
    [35, 34],
  ],
  [
    [40, 22],
    [45, 34],
  ],
  [
    [37, 29],
    [43, 29],
  ],
]) {
  await mouseStroke(polyline(rect, cols, stroke));
  await sleep(200);
}
const cell = rect.w / cols;
await cdp('Input.dispatchMouseEvent', { type: 'mouseMoved', x: rect.x + 41.5 * cell, y: rect.y + 29.5 * cell });
console.log('paint status:', await text('paint-status'));
console.log('quota:', await text('quota-text'));
await evaluate(`document.getElementById('swarm-btn').click()`);
await sleep(4000);
await shot('desktop-swarm');
console.log('swarm:', await text('swarm-state'));

await operator({ engaged: true, reason: 'Paused while we look at a latency spike' });
await evaluate(`document.getElementById('swarm-panel').scrollIntoView({ block: 'center' })`);
await sleep(1200);
await shot('desktop-kill-switch');
console.log('kill switch:', await text('swarm-state'));
await operator({ engaged: false });
await evaluate('window.scrollTo(0, 0)');

const fullHeight = await evaluate('document.documentElement.scrollHeight');
await cdp('Emulation.setDeviceMetricsOverride', {
  width: 1440,
  height: Math.min(fullHeight, 2400),
  deviceScaleFactor: 1,
  mobile: false,
});
await sleep(800);
await shot('desktop-full');
console.log('stream:', await text('conn-text'));

// Mobile --------------------------------------------------------------
await cdp('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 3, mobile: true });
await cdp('Emulation.setTouchEmulationEnabled', { enabled: true, maxTouchPoints: 5 });
await cdp('Page.navigate', { url: pageUrl });
await sleep(3500);
rect = await canvasRect();
const touch = polyline(rect, cols, [
  [8, 50],
  [20, 44],
  [30, 50],
]).map(([x, y]) => ({ x, y }));
await cdp('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [touch[0]] });
for (const p of touch.slice(1)) {
  await cdp('Input.dispatchTouchEvent', { type: 'touchMove', touchPoints: [p] });
  await sleep(8);
}
await cdp('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
await sleep(600);
await shot('mobile');
console.log('mobile paint status:', await text('paint-status'));
console.log('mobile horizontal overflow:', await evaluate('document.documentElement.scrollWidth > window.innerWidth'));
const mobileHeight = await evaluate('document.documentElement.scrollHeight');
await cdp('Emulation.setDeviceMetricsOverride', {
  width: 390,
  height: Math.min(mobileHeight, 3600),
  deviceScaleFactor: 2,
  mobile: true,
});
await sleep(800);
await shot('mobile-full');

console.log('page errors:', problems.length === 0 ? 'none' : JSON.stringify(problems));
ws.close();
chrome.kill('SIGKILL');
process.exit(problems.length === 0 ? 0 : 1);
