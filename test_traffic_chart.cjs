// Verify that the browser decoder understands the shared binary format fixture.
const assert = require('node:assert/strict');
const fixture = require('./test-fixtures/traffic-metrics.json');

async function main() {
  const {decodeTraffic} = await import('./static/traffic-metrics.mjs');
  const bytes = Buffer.from(fixture.hex, 'hex');
  const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  const expected = fixture.samples.map(sample => ({...sample, wall_time: new Date(sample.wall_time * 1000).toISOString()}));
  assert.deepEqual(decodeTraffic(buffer), expected);
  assert.equal(decodeTraffic(buffer.slice(0, 8)).length, 0);
  assert.throws(() => decodeTraffic(buffer.slice(0, 7)), /Invalid traffic/);
  assert.throws(() => decodeTraffic(buffer.slice(0, -1)), /Invalid traffic/);
  const unsupported = buffer.slice(0);
  new DataView(unsupported).setUint32(4, 2, true);
  assert.throws(() => decodeTraffic(unsupported), /Unsupported traffic/);
  console.log('Binary format fixture decoding checks passed.');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
