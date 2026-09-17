import { WebSocket } from 'ws';
import { spawn } from 'node:child_process';

const port = 18787;
const server = spawn(process.execPath, ['server.js'], {
  cwd: new URL('.', import.meta.url),
  env: { ...process.env, PORT: String(port) },
  stdio: ['ignore', 'pipe', 'inherit']
});

const waitOpen = ws => new Promise((resolve, reject) => {
  ws.once('open', resolve); ws.once('error', reject);
});
const waitMessage = ws => new Promise((resolve, reject) => {
  ws.once('message', (d, b) => resolve({ d, b })); ws.once('error', reject);
});
const url = `ws://127.0.0.1:${port}`;

await new Promise((resolve, reject) => {
  server.stdout.on('data', d => d.toString().includes('listening') && resolve());
  server.once('error', reject);
});
const gateway = new WebSocket(url);
await waitOpen(gateway);
gateway.send(JSON.stringify({ type: 'register', role: 'gateway', stationId: '0123456789abcdef0123456789abcdef' }));
await waitMessage(gateway);

const controller = new WebSocket(url);
await waitOpen(controller);
controller.send(JSON.stringify({ type: 'register', role: 'controller', stationId: '0123456789abcdef0123456789abcdef', controllerId: 'ctrl-test' }));
const reg = JSON.parse((await waitMessage(controller)).d.toString());
if (!reg.sessionId || !reg.gatewayOnline) throw new Error('controller registration failed');

await waitMessage(gateway);
const payload = Buffer.from('MOS_WAN_E2E_TEST');
controller.send(payload);
const inbound = await waitMessage(gateway);
const sid = inbound.d.subarray(0, 16).toString('ascii');
if (sid !== reg.sessionId) throw new Error('session prefix mismatch');
if (!inbound.d.subarray(16).equals(payload)) throw new Error('controller->gateway payload mismatch');
gateway.send(Buffer.concat([Buffer.from(reg.sessionId, 'ascii'), Buffer.from('MOS_WAN_RETURN')]));
const returned = await waitMessage(controller);
if (returned.d.toString() !== 'MOS_WAN_RETURN') throw new Error('gateway->controller payload mismatch');

console.log('MOS WAN RELAY E2E: PASS');
controller.close();
gateway.close();
server.kill();
