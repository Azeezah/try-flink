export function decodeTraffic(buffer) {
  const headerSize = 8, recordSize = 24;
  if (buffer.byteLength < headerSize || (buffer.byteLength - headerSize) % recordSize !== 0) throw Error('Invalid traffic sample data');
  const view = new DataView(buffer);
  if (String.fromCharCode(...new Uint8Array(buffer, 0, 4)) !== 'TRFM' || view.getUint32(4, true) !== 1) throw Error('Unsupported traffic sample format');
  const samples = [];
  for (let offset = headerSize; offset < buffer.byteLength; offset += recordSize) {
    samples.push({
      wall_time: new Date(view.getFloat64(offset, true) * 1000).toISOString(),
      expected: view.getUint32(offset + 8, true),
      attempted: view.getUint32(offset + 12, true),
      acked: view.getUint32(offset + 16, true),
      errors: view.getUint32(offset + 20, true),
    });
  }
  return samples;
}
