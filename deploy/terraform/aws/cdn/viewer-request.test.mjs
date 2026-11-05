// Tests for the pack CDN's CloudFront Function (viewer-request.js).
//
//   node deploy/terraform/aws/cdn/viewer-request.test.mjs
//
// The function is plain ES5 for CloudFront's runtime, with no exports, so it
// is loaded the way CloudFront loads it: as a script whose `handler` is then
// called with a viewer-request event.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('./viewer-request.js', import.meta.url), 'utf8');

// Runs the function at a fixed time (Unix seconds).
function run(uri, nowSeconds, querystring = {}) {
  const context = vm.createContext({
    Date: { now: () => nowSeconds * 1000 },
    Math,
    parseInt,
  });
  vm.runInContext(source, context);
  const event = { request: { method: 'GET', uri, querystring, headers: {} } };
  return context.handler(event);
}

const NOW = 1_800_000_000;
const pack = 'repos/7/packs/0123456789abcdef0123456789abcdef01234567.pack';
const url = (expires) => `/-/express/k1/${expires}/c2lnbmF0dXJl/${pack}`;

test('a URL that has not expired goes to the cache and origin, without its query', () => {
  const r = run(url(NOW + 1), NOW, { x: { value: '1' } });
  assert.equal(r.uri, url(NOW + 1));
  assert.equal(Object.keys(r.querystring).length, 0);
  assert.equal(r.statusCode, undefined);
});

test('a URL is refused from its expiry second on, as the origin refuses it', () => {
  for (const expires of [NOW, NOW - 1, 1]) {
    const r = run(url(expires), NOW);
    assert.equal(r.statusCode, 403, `expires=${expires}`);
    assert.equal(r.headers['cache-control'].value, 'no-store');
  }
});

test('the health probe passes through', () => {
  const r = run('/-/express/-/health', NOW);
  assert.equal(r.uri, '/-/express/-/health');
});

test('paths outside /-/express are not served by the CDN', () => {
  for (const uri of ['/', '/acme/app.git/info/refs', '/-/express', '/-/blobs/x', '/healthz']) {
    assert.equal(run(uri, NOW).statusCode, 404, uri);
  }
});

test('malformed express paths are refused', () => {
  for (const uri of [
    '/-/express/',
    '/-/express/k1',
    `/-/express/k1/${NOW + 60}`,
    `/-/express/k1/${NOW + 60}/sig`,
    `/-/express//${NOW + 60}/sig/${pack}`,
    `/-/express/k1/${NOW + 60}//${pack}`,
    `/-/express/k1/0${NOW + 60}/sig/${pack}`,
    `/-/express/k1/+${NOW + 60}/sig/${pack}`,
    `/-/express/k1/${NOW + 60}.5/sig/${pack}`,
    `/-/express/k1/never/sig/${pack}`,
    `/-/express/k1/99999999999999999999/sig/${pack}`,
  ]) {
    assert.equal(run(uri, NOW).statusCode, 403, uri);
  }
});

test('paths that something downstream could normalize are refused', () => {
  const later = NOW + 3600;
  const earlier = NOW - 3600;
  for (const uri of [
    // A live expiry here, an expired one after dot segments are resolved.
    `/-/express/k1/${later}/x/../../${earlier}/sig/${pack}`,
    `/-/express/k1/${later}/./sig/${pack}`,
    `/-/express/k1/${later}/sig/repos/7/../8/packs/x.pack`,
    `/-/express/k1/${later}/sig/repos/7/./packs/x.pack`,
    `/-/express/k1/${later}/sig/repos/7//packs/x.pack`,
    `/-/express/k1/${later}/sig/${pack}/`,
    // Percent-encoding anywhere: encoded slashes, dots, or anything else.
    `/-/express/k1/${later}/sig/repos%2F7/packs/x.pack`,
    `/-/express/k1/${later}/x/%2e%2e/%2E%2E/${earlier}/sig/${pack}`,
    `/-/express/k1/${later}/sig/repos/7/packs/x%20y.pack`,
    `/-/express/k1/${later}/sig%3D/${pack}`,
  ]) {
    assert.equal(run(uri, NOW).statusCode, 403, uri);
  }
});

test('every URL Aspen signs still passes: key id [a-z0-9_], hex signature, pack key', () => {
  const uri = `/-/express/k_2/${NOW + 60}/0123abcd/repos/7/packs/0123456789abcdef0123456789abcdef01234567.pack`;
  assert.equal(run(uri, NOW).uri, uri);
});
