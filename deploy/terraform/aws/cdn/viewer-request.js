// CloudFront Function (runtime cloudfront-js-2.0) on every viewer request to
// the pack CDN, before CloudFront looks in its cache.
//
// The CDN's origin is Aspen's /-/express, which verifies every URL's
// signature and expiry (docs/EXPRESS.md). CloudFront does not, and it serves
// an expired cached object when the origin answers 5xx or times out
// ("How CloudFront processes HTTP 4xx and 5xx status codes from your
// origin", CloudFront Developer Guide). So this function refuses, at the
// edge, every URL whose expiry has passed, and everything that is not an
// express URL at all. It does not check signatures: it has no keys, and the
// origin checks them on every cache miss.
//
// Express URL path: /-/express/<key id>/<expires>/<signature>/<pack key>,
// where <expires> is the Unix second from which the URL is refused. The
// health probe path /-/express/-/health is passed through.
//
// Tests: node deploy/terraform/aws/cdn/viewer-request.test.mjs

var PREFIX = '/-/express/';

function refuse(code, description) {
  return {
    statusCode: code,
    statusDescription: description,
    headers: { 'cache-control': { value: 'no-store' } },
  };
}

function handler(event) {
  var request = event.request;
  var uri = request.uri;

  if (uri.indexOf(PREFIX) !== 0) {
    return refuse(404, 'Not Found');
  }
  var rest = uri.substring(PREFIX.length);
  if (rest === '-/health') {
    return request;
  }

  // Only plain paths. The expiry below is read from the raw path, so a path
  // that something further on could normalize (a `.` or `..` segment, an
  // empty one, or any percent-encoding) could check one expiry here and
  // serve another. Aspen never signs such paths: key ids are [a-z0-9_],
  // expiries decimal, signatures hex, and pack keys
  // repos/<id>/packs/<checksum>.pack (crates/aspen-express/src/url.rs).
  if (uri.indexOf('%') !== -1) {
    return refuse(403, 'Forbidden');
  }
  var parts = rest.split('/');
  for (var i = 0; i < parts.length; i++) {
    if (parts[i] === '' || parts[i] === '.' || parts[i] === '..') {
      return refuse(403, 'Forbidden');
    }
  }
  // key id, expires, signature, and a pack key of at least one segment.
  if (parts.length < 4) {
    return refuse(403, 'Forbidden');
  }
  var expires = parts[1];
  // Canonical decimal, as Aspen signs it: no sign, no leading zero.
  if (!/^[1-9][0-9]{0,15}$/.test(expires)) {
    return refuse(403, 'Forbidden');
  }
  var now = Math.floor(Date.now() / 1000);
  if (now >= parseInt(expires, 10)) {
    return refuse(403, 'Forbidden');
  }

  // The cache key is the path alone; drop the query string so it cannot
  // reach the origin either.
  request.querystring = {};
  return request;
}
