// Verify that the actual browser decoder understands Python's binary samples.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {execFileSync} = require('node:child_process');

const html = fs.readFileSync(path.join(__dirname, 'control-panel.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new vm.Script(script);
const decoder = script.slice(script.indexOf('function decodeTraffic('), script.indexOf('async function getTraffic('));
const decodeTraffic = vm.runInNewContext(`(${decoder.trim()})`);
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
const decoded = JSON.parse(JSON.stringify(decodeTraffic(buffer)));
const expected = fixture.samples.map(sample => ({...sample, wall_time: new Date(sample.wall_time * 1000).toISOString()}));
assert.deepEqual(decoded, expected);
assert.equal(decodeTraffic(buffer.slice(0, 8)).length, 0);
assert.throws(() => decodeTraffic(buffer.slice(0, 7)), /Invalid traffic/);
assert.throws(() => decodeTraffic(buffer.slice(0, -1)), /Invalid traffic/);
const unsupported = buffer.slice(0);
new DataView(unsupported).setUint32(4, 2, true);
assert.throws(() => decodeTraffic(unsupported), /Unsupported traffic/);
console.log('Browser syntax and Python-to-JavaScript binary decoding checks passed.');
