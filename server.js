import http from 'node:http';
import crypto from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';

const port = Number(process.env.PORT || 8787);
const MAX_PAYLOAD = 8 * 1024 * 1024;
const stations = new Map();

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, stations: stations.size }));
  }
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('MOS WAN Relay');
});

const wss = new WebSocketServer({ server: httpServer, maxPayload: MAX_PAYLOAD });

function station(id) {
  let s = stations.get(id);
  if (!s) { s = { gateway: null, controllers: new Map() }; stations.set(id, s); }
  return s;
}
function sendJson(ws, obj) {
  if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}
function closeIfEmpty(id, s) {
  if (!s.gateway && s.controllers.size === 0) stations.delete(id);
}
function validStationId(v) {
  return typeof v === 'string' && /^[a-f0-9]{32,128}$/i.test(v);
}

wss.on('connection', (ws) => {
  const meta = { role: '', stationId: '', sessionId: '', controllerId: '' };
  let alive = true;
  ws.on('pong', () => { alive = true; });
  const registerTimer = setTimeout(() => {
    if (!meta.role) ws.close(1008, 'register timeout');
  }, 8000);

  ws.on('message', (data, isBinary) => {
    if (!meta.role) {
      if (isBinary) return ws.close(1008, 'register first');
      let msg;
      try { msg = JSON.parse(data.toString('utf8')); }
      catch { return ws.close(1008, 'bad register'); }
      if (msg?.type !== 'register' || !validStationId(msg.stationId)) return ws.close(1008, 'bad register');
      clearTimeout(registerTimer);
      meta.role = msg.role;
      meta.stationId = String(msg.stationId);
      const s = station(meta.stationId);

      if (meta.role === 'gateway') {
        if (s.gateway && s.gateway !== ws) s.gateway.close(1012, 'gateway replaced');
        s.gateway = ws;
        sendJson(ws, { type: 'registered', role: 'gateway', stationId: meta.stationId });
        for (const sid of s.controllers.keys()) sendJson(ws, { type: 'controller_up', sessionId: sid });
        return;
      }
      if (meta.role === 'controller') {
        meta.controllerId = String(msg.controllerId || 'controller').slice(0, 128);
        meta.sessionId = crypto.randomBytes(8).toString('hex');
        s.controllers.set(meta.sessionId, ws);
        sendJson(ws, { type: 'registered', role: 'controller', stationId: meta.stationId,
          sessionId: meta.sessionId, gatewayOnline: !!s.gateway });
        sendJson(s.gateway, { type: 'controller_up', sessionId: meta.sessionId,
          controllerId: meta.controllerId });
        return;
      }
      ws.close(1008, 'bad role');
      return;
    }

    const s = stations.get(meta.stationId);
    if (!s || !isBinary) return;
    if (meta.role === 'controller') {
      if (s.gateway?.readyState !== WebSocket.OPEN) return;
      s.gateway.send(Buffer.concat([Buffer.from(meta.sessionId, 'ascii'), Buffer.from(data)]));
      return;
    }
    if (meta.role === 'gateway') {
      const buf = Buffer.from(data);
      if (buf.length < 16) return;
      const sid = buf.subarray(0, 16).toString('ascii');
      const client = s.controllers.get(sid);
      if (client?.readyState === WebSocket.OPEN) client.send(buf.subarray(16));
    }
  });

  ws.on('close', () => {
    clearTimeout(registerTimer);
    if (!meta.stationId) return;
    const s = stations.get(meta.stationId);
    if (!s) return;
    if (meta.role === 'gateway' && s.gateway === ws) s.gateway = null;
    if (meta.role === 'controller' && meta.sessionId) {
      s.controllers.delete(meta.sessionId);
      sendJson(s.gateway, { type: 'controller_down', sessionId: meta.sessionId });
    }
    closeIfEmpty(meta.stationId, s);
  });

  ws._mosAlive = () => alive;
  ws._mosMarkPing = () => { alive = false; };
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws._mosAlive && !ws._mosAlive()) { ws.terminate(); continue; }
    if (ws._mosMarkPing) ws._mosMarkPing();
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }
}, 30000).unref();

httpServer.listen(port, () => console.log(`MOS WAN relay listening on ${port}`));
