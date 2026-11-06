// Tests for the live canvas CDN's CloudFront Function
// (place-live-viewer-request.js).
//
//   node deploy/terraform/aws/public-demo/environment/tests/place-live-viewer-request.test.mjs
//
// The function is plain ES5 for CloudFront's runtime, with no exports, so it
// is loaded the way CloudFront loads it: as a script whose `handler` is then
// called with a viewer-request event.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../place-live-viewer-request.js', import.meta.url), 'utf8');

// Runs the function at a fixed time (Unix milliseconds).
function run(uri, nowMs, querystring = {}) {
  const context = vm.createContext({ Date: { now: () => nowMs }, Math, String });
  vm.runInContext(source, context);
  const event = { request: { method: 'GET', uri, querystring, headers: {} } };
  return context.handler(event);
}

const NOW = 1_800_000_001_234;

test('the canvas image is keyed by the current 2-second bucket, whatever t the viewer sent', () => {
  for (const qs of [{}, { t: { value: '1' } }, { t: { value: 'x' }, other: { value: '2' } }]) {
    const r = run('/api/v1/canvas.svg', NOW, qs);
    assert.equal(r.statusCode, undefined);
    assert.equal(r.uri, '/api/v1/canvas.svg');
    assert.deepEqual(Object.keys(r.querystring), ['t']);
    assert.equal(r.querystring.t.value, '900000000');
  }
});

test('viewers in the same 2 seconds share a key; the next 2 seconds get a new one', () => {
  const key = (ms) => run('/api/v1/canvas.svg', ms).querystring.t.value;
  assert.equal(key(1_800_000_000_000), key(1_800_000_001_999));
  assert.notEqual(key(1_800_000_001_999), key(1_800_000_002_000));
});

test('everything else on place is not served', () => {
  for (const uri of ['/', '/api/v1/config', '/api/v1/admin/kill-switch', '/canvas.git/info/refs', '/api/v1/canvas.svg/', '/api/v1/canvas.png']) {
    const r = run(uri, NOW);
    assert.equal(r.statusCode, 404, uri);
    assert.equal(r.headers['cache-control'].value, 'no-store');
  }
});
