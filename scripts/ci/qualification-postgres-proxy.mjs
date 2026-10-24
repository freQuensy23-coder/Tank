import { createServer as createTcpServer, connect } from 'node:net';
import { createServer as createHttpServer } from 'node:http';
import { createHash } from 'node:crypto';

const token = process.env.QUALIFICATION_PROVIDER_KEY;
if (!token) throw Error('Fresh owned control credential required');
let armed = null;
let barrier = null;
const observations = [];
const pending = [];
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
function publication(frame) {
  if (frame[0] !== 66) return null;
  let offset = 5;
  for (let index = 0; index < 2; index++) offset = frame.indexOf(0, offset) + 1;
  const formats = frame.readInt16BE(offset); offset += 2 + 2 * formats;
  const count = frame.readInt16BE(offset); offset += 2;
  for (let index = 0; index < count; index++) {
    const length = frame.readInt32BE(offset); offset += 4;
    if (length < 0) continue;
    const value = frame.subarray(offset, offset + length); offset += length;
    try {
      const decoded = JSON.parse(value.toString());
      const metadata = decoded?.workspaceCandidate?.metadata;
      if (metadata?.schema !== 2 || typeof metadata.manifest !== 'string') continue;
      const manifest = JSON.parse(metadata.manifest);
      const scope = JSON.parse(metadata.scope);
      if (manifest.state === 'retained' && manifest.upgradedFrom) return { sessionId: scope.sessionId, runtimeId: manifest.handle.runtimeId, digest: digest(value) };
    } catch {}
  }
  return null;
}
createTcpServer(client => {
  const upstream = connect(5432, 'database');
  let initial = true;
  let buffered = Buffer.alloc(0);
  let holdBefore = false;
  let holdAck = false;
  const heldClient = [];
  const heldServer = [];
  const release = () => {
    holdBefore = false; holdAck = false;
    for (const frame of heldClient.splice(0)) upstream.write(frame);
    for (const chunk of heldServer.splice(0)) client.write(chunk);
  };
  pending.push(release);
  client.on('data', bytes => {
    buffered = Buffer.concat([buffered, bytes]);
    while (buffered.length >= (initial ? 4 : 5)) {
      const length = initial ? buffered.readInt32BE(0) : buffered.readInt32BE(1) + 1;
      if (length < 4 || length > 32 * 1024 ** 2) { client.destroy(); upstream.destroy(); return; }
      if (buffered.length < length) return;
      const frame = buffered.subarray(0, length); buffered = buffered.subarray(length);
      if (initial && length === 8 && frame.readInt32BE(4) === 80877103) { client.write('N'); continue; }
      const found = initial ? null : publication(frame);
      initial = false;
      if (found && armed && found.sessionId === armed.sessionId && !barrier) {
        barrier = { ...found, mode: armed.mode, phase: 'held', at: new Date().toISOString() };
        observations.push({ ...barrier });
        holdBefore = armed.mode === 'publication-before';
        holdAck = armed.mode === 'publication-ack';
        armed = null;
      }
      if (holdBefore) heldClient.push(frame); else upstream.write(frame);
    }
  });
  upstream.on('data', bytes => { if (holdAck) heldServer.push(bytes); else client.write(bytes); });
  client.on('error', () => upstream.destroy());
  upstream.on('error', () => client.destroy());
  client.on('close', () => { upstream.destroy(); const index = pending.indexOf(release); if (index >= 0) pending.splice(index, 1); });
  upstream.on('close', () => client.destroy());
}).listen(5432, '0.0.0.0');
createHttpServer(async (request, response) => {
  if (request.headers['x-api-key'] !== token) { response.writeHead(401).end(); return; }
  response.setHeader('content-type', 'application/json');
  try {
    if (request.url === '/arm' && request.method === 'POST') {
      let bytes = ''; for await (const chunk of request) { bytes += chunk; if (bytes.length > 4096) throw Error('control too large'); }
      const value = JSON.parse(bytes);
      if (!['publication-before', 'publication-ack'].includes(value.mode) || typeof value.sessionId !== 'string' || armed || barrier) throw Error('invalid barrier');
      armed = value;
    } else if (request.url === '/release' && request.method === 'POST') {
      for (const release of pending) release();
      if (barrier) observations.push({ ...barrier, phase: 'released' });
      barrier = null; armed = null;
    } else if (request.url !== '/state') { response.writeHead(404).end(); return; }
    response.end(JSON.stringify({ armed, barrier, observations }));
  } catch (error) { response.writeHead(400).end(JSON.stringify({ error: error.message })); }
}).listen(9001, '0.0.0.0');
