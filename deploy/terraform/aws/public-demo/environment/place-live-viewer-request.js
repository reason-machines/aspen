// CloudFront Function (runtime cloudfront-js-2.0) on every viewer request to
// the live canvas CDN (place-live.tf), before CloudFront looks in its cache.
//
// It serves one path, /api/v1/canvas.svg, and answers 404 for everything
// else, so the rest of place stays off this hostname. It sets the query
// string to t=<current 2-second bucket> (floor of Unix milliseconds / 2000),
// replacing whatever the viewer sent: the cache key is the path and t, so all
// viewers in the same 2 seconds share one cached object and no viewer can
// make the origin render more often by inventing t values.
//
// Tests: node deploy/terraform/aws/public-demo/environment/tests/place-live-viewer-request.test.mjs

var PATH = '/api/v1/canvas.svg';
var BUCKET_MS = 2000;

function handler(event) {
  var request = event.request;

  if (request.uri !== PATH) {
    return {
      statusCode: 404,
      statusDescription: 'Not Found',
      headers: { 'cache-control': { value: 'no-store' } },
    };
  }

  request.querystring = { t: { value: String(Math.floor(Date.now() / BUCKET_MS)) } };
  return request;
}
