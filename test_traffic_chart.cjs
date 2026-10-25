// Verify that the actual browser decoder understands Python's binary samples.
const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');

async function main() {
  const {decodeTraffic} = await import('./static/traffic-metrics.mjs');
  const fixture = JSON.parse(execFileSync('python3', ['-c', `
import base64, json
from traffic_metrics import HEADER, pack_sample
samples = [
    {'wall_time': 1792857600.123456, 'expected': 123, 'attempted': 120, 'acked': 119, 'errors': 1},
    {'wall_time': 1792857601.5, 'expected': 4294967295, 'attempted': 2147483648, 'acked': 0, 'errors': 4},
]
data = HEADER + b''.join(pack_sample(sample, sample['wall_time']) for sample in samples)
print(json.dumps({'encoded': base64.b64encode(data).decode(), 'samples': samples}))
`], {cwd: __dirname, env: {...process.env, PYTHONDONTWRITEBYTECODE: '1'}, encoding: 'utf8'}));

  const bytes = Buffer.from(fixture.encoded, 'base64');
  const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  const expected = fixture.samples.map(sample => ({...sample, wall_time: new Date(sample.wall_time * 1000).toISOString()}));
  assert.deepEqual(decodeTraffic(buffer), expected);
  assert.equal(decodeTraffic(buffer.slice(0, 8)).length, 0);
  assert.throws(() => decodeTraffic(buffer.slice(0, 7)), /Invalid traffic/);
  assert.throws(() => decodeTraffic(buffer.slice(0, -1)), /Invalid traffic/);
  const unsupported = buffer.slice(0);
  new DataView(unsupported).setUint32(4, 2, true);
  assert.throws(() => decodeTraffic(unsupported), /Unsupported traffic/);
  console.log('Python-to-JavaScript binary decoding checks passed.');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
