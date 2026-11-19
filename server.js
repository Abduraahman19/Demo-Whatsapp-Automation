import express from 'express';
import makeWASocket, { DisconnectReason, fetchLatestBaileysVersion, useMultiFileAuthState } from '@whiskeysockets/baileys';
import QRCode from 'qrcode';
import pino from 'pino';
import { rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

// Resolve both public assets and session files relative to this app.
process.chdir(fileURLToPath(new URL('.', import.meta.url)));
const app = express();
const logger = pino({ level: 'silent' });
const logs = [];
const queue = [];
const seen = new Set();
let sock;
let status = 'DISCONNECTED';
let qrImage = null;
let jid = null;
let generation = 0;
let session = 0;
let reconnectTimer;
let starting;
let resetting = false;
let stopping = false;
let processing = false;
let nextSendAt = 0;
let credentialWrites = Promise.resolve();

const templates = {
  CONFIRM_ORDER: 'Salam! Test Store se aap ka Rs. 2,500 ka COD order pack ho raha hai.\n\nConfirm karne ke liye *YES* reply karein.\nAgar aap ne yeh order nahi kiya to *NOT ME* likhein.',
  FEEDBACK: 'Salam! Aap ko Test Store se parcel mil gaya tha. Apna feedback dein:\nReply *1* - Bilkul Theek\nReply *2* - Ghalat ya Kharab Item\nReply *3* - Parcel Nahi Mila',
};
const replies = {
  YES: ['ORDER_CONFIRMED', 'Shukriya! Aap ka order confirm ho gaya hai.'],
  'NOT ME': ['PRANK_VICTIM_CANCELLED (No Blacklist)', 'Maazrat! Hum ne yeh fake order cancel kar diya hai.'],
  NO: ['PRANK_VICTIM_CANCELLED (No Blacklist)', 'Maazrat! Hum ne yeh fake order cancel kar diya hai.'],
  1: ['FEEDBACK_PERFECT', 'Shukriya! Aap ka positive feedback record ho gaya.'],
  2: ['FEEDBACK_WRONG_ITEM', 'Afsos! Aap ki complaint record kar li gayi hai.'],
  3: ['FEEDBACK_NOT_RECEIVED', 'Shukriya! Hum is parcel ko check kar rahe hain.'],
};

function log(event, details = {}) {
  logs.push({ id: randomUUID(), time: new Date().toISOString(), event, ...details });
  if (logs.length > 200) logs.shift();
  console.log(`[${event}]`, details);
}

function normalizePhone(value) {
  if (typeof value !== 'string' || !/^[+\d\s().-]+$/.test(value)) {
    throw Object.assign(new Error('Enter a Pakistani mobile number, e.g. 03001234567.'), { status: 400 });
  }
  let phone = value.replace(/\D/g, '');
  if (phone.startsWith('00')) phone = phone.slice(2);
  if (/^03\d{9}$/.test(phone)) phone = `92${phone.slice(1)}`;
  if (!/^923\d{9}$/.test(phone)) {
    throw Object.assign(new Error('Use 03XXXXXXXXX or +923XXXXXXXXX.'), { status: 400 });
  }
  return phone;
}

function connectedSocket() {
  if (resetting || status !== 'CONNECTED' || !sock) {
    throw Object.assign(new Error('Connect WhatsApp by scanning the QR code first.'), { status: 409 });
  }
  return sock;
}

function enqueue(to, text, kind) {
  if (queue.length >= 100) throw Object.assign(new Error('Queue full. Wait for messages to send.'), { status: 429 });
  const item = { id: randomUUID(), to, text, kind, session };
  queue.push(item);
  log('QUEUED', { to, text, kind, messageId: item.id });
  void processQueue();
  return item.id;
}

async function processQueue() {
  if (processing) return;
  processing = true;
  try {
    while (queue.length && status === 'CONNECTED' && !resetting && !stopping) {
      const current = sock;
      const item = queue[0];
      await sleep(Math.max(0, nextSendAt - Date.now()));
      if (resetting || stopping || current !== sock || status !== 'CONNECTED') break;
      if (queue[0] !== item || item.session !== session) continue;
      queue.shift();
      try {
        await current.sendMessage(item.to, { text: item.text });
        log('SENT', { to: item.to, text: item.text, kind: item.kind, messageId: item.id });
      } catch (error) {
        // Do not retry an uncertain send: it could duplicate a delivered message.
        log('SEND_FAILED', { to: item.to, messageId: item.id, error: error.message });
      } finally {
        nextSendAt = Date.now() + randomInt(3000, 5001);
      }
    }
  } finally {
    processing = false;
    if (queue.length && status === 'CONNECTED' && !resetting && !stopping) void processQueue();
  }
}

function scheduleReconnect() {
  if (reconnectTimer || resetting || stopping) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    void connect();
  }, 3000);
}

async function connect() {
  if (starting || resetting || stopping) return;
  starting = (async () => {
    const token = ++generation;
    try {
      const { state, saveCreds } = await useMultiFileAuthState('./auth_info');
      const { version } = await fetchLatestBaileysVersion();
      if (token !== generation || resetting || stopping) return;
      const current = makeWASocket({ auth: state, version, logger, markOnlineOnConnect: false, syncFullHistory: false });
      sock = current;
      current.ev.on('creds.update', () => {
        if (token !== generation) return;
        credentialWrites = credentialWrites.then(saveCreds).catch(error => log('AUTH_ERROR', { error: error.message }));
      });
      current.ev.on('connection.update', update => {
        if (token !== generation) return;
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
          status = 'QR_READY';
          qrImage = null;
          void QRCode.toDataURL(qr, { width: 300, margin: 2 }).then(data => {
            if (token === generation && status === 'QR_READY') qrImage = data;
          }).catch(error => log('QR_ERROR', { error: error.message }));
          log('QR_READY');
        }
        if (connection === 'open') {
          status = 'CONNECTED';
          qrImage = null;
          jid = current.user?.id ?? null;
          log('CONNECTED', { jid });
          void processQueue();
        }
        if (connection === 'close') {
          ++generation; // Ignore any remaining events from this socket.
          current.ev.removeAllListeners();
          status = 'DISCONNECTED';
          qrImage = null;
          jid = null;
          sock = null;
          const code = lastDisconnect?.error?.output?.statusCode;
          log('DISCONNECTED', { code, error: lastDisconnect?.error?.message });
          if (code !== DisconnectReason.loggedOut) scheduleReconnect();
          else {
            queue.length = 0;
            log('LOGGED_OUT', { message: 'Use Disconnect / Reset SIM to generate a fresh QR.' });
          }
        }
      });
      current.ev.on('messages.upsert', ({ messages, type }) => {
        if (token !== generation || resetting || type !== 'notify') return;
        for (const msg of messages) {
          const to = msg.key.remoteJid;
          if (msg.key.fromMe || !to || (!to.endsWith('@s.whatsapp.net') && !to.endsWith('@lid'))) continue;
          const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text;
          if (!text) continue;
          const key = `${to}:${msg.key.id}`;
          if (seen.has(key)) continue;
          seen.add(key);
          if (seen.size > 500) seen.delete(seen.values().next().value);
          log('INCOMING', { from: to, text });
          const reply = replies[text.trim().replace(/\s+/g, ' ').toUpperCase()];
          if (!reply) continue;
          const [action, acknowledgement] = reply;
          log(action, { from: to, text });
          try { enqueue(to, acknowledgement, action); }
          catch (error) { log('ACK_FAILED', { to, error: error.message }); }
        }
      });
    } catch (error) {
      log('CONNECTION_ERROR', { error: error.message });
      scheduleReconnect();
    }
  })();
  try { await starting; } finally { starting = null; }
}

app.use(express.json({ limit: '8kb' }));
app.use('/api', (_req, res, next) => { res.set('Cache-Control', 'no-store'); next(); });
app.use(express.static(fileURLToPath(new URL('./public', import.meta.url))));

app.get('/api/status', (_req, res) => {
  res.json({ status, jid, phone: jid?.split('@')[0].split(':')[0] ?? null, qr: status === 'QR_READY' ? qrImage : null, queueLength: queue.length, resetting, logs });
});

app.post('/api/check-number', async (req, res) => {
  const phone = normalizePhone(req.body?.phone);
  const current = connectedSocket();
  const result = await current.onWhatsApp(phone);
  if (current !== sock || resetting) throw Object.assign(new Error('Connection changed. Try again.'), { status: 409 });
  const account = result?.find(entry => entry.exists);
  log('PRESENCE_CHECK', { phone, exists: Boolean(account) });
  res.json({ phone, exists: Boolean(account), jid: account?.jid ?? null });
});

app.post('/api/send-test', (req, res) => {
  const phone = normalizePhone(req.body?.phone);
  const type = req.body?.type;
  if (!Object.hasOwn(templates, type)) return res.status(400).json({ error: 'Type must be CONFIRM_ORDER or FEEDBACK.' });
  connectedSocket();
  const messageId = enqueue(`${phone}@s.whatsapp.net`, templates[type], type);
  res.status(202).json({ queued: true, phone, type, messageId });
});

app.post('/api/logout', async (_req, res) => {
  if (resetting) return res.status(409).json({ error: 'Reset already in progress.' });
  resetting = true;
  ++generation;
  ++session;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  status = 'DISCONNECTED';
  jid = qrImage = null;
  const cancelled = queue.length;
  queue.length = 0;
  seen.clear();
  try {
    await starting;
    const previous = sock;
    sock = null;
    if (previous) {
      previous.ev.removeAllListeners();
      try { await previous.logout(); }
      catch (error) { log('LOGOUT_WARNING', { error: error.message }); }
      finally { previous.end(new Error('Session reset')); }
    }
    await credentialWrites;
    await rm('./auth_info', { recursive: true, force: true });
    log('SESSION_RESET', { cancelled });
  } finally {
    resetting = false;
  }
  void connect();
  res.json({ ok: true, message: 'Session cleared. Waiting for a fresh QR code.' });
});

app.use((error, _req, res, _next) => {
  const code = error.status || 500;
  log('API_ERROR', { error: error.message });
  res.status(code).json({ error: error.message });
});

const server = app.listen(4000, '127.0.0.1', () => {
  console.log('WhatsApp PoC: http://localhost:4000');
  void connect();
});
server.on('error', error => { console.error(error.message); process.exitCode = 1; });
function shutdown() {
  stopping = true;
  ++generation;
  clearTimeout(reconnectTimer);
  sock?.ev.removeAllListeners();
  sock?.end(new Error('Server stopped'));
  server.close();
  setTimeout(() => process.exit(0), 1000).unref();
}
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
