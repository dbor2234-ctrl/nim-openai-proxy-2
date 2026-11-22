const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 8080;
const PROXY_KEY = process.env.PROXY_KEY || ''; // optional: your own private password
const NIM_BASE = 'https://integrate.api.nvidia.com/v1';

const DEFAULT_MODEL = 'moonshotai/kimi-k3.1-pro';
const MODEL_MAP = {
  'gpt-3.5-turbo': 'deepseek-ai/deepseek-v4.1-flash',
  'gpt-4':         'minimaxai/minimax-m3.1-flash',
  'gpt-4-turbo':  'z-ai/glm-5.3-flash',
  'gpt-4o':        'moonshotai/kimi-k3.1-pro'
};

/* ============================================================
 * NEW: MULTI-KEY POOL + ROTATION + AUTOMATIC FAILOVER
 * ------------------------------------------------------------
 * On Railway set ONE variable (comma-separated, spaces ignored):
 *   NIM_API_KEYS = nvapi-xxxxxxxx,nvapi-yyyyyyyy,nvapi-zzzzzzzz
 * A single old NIM_API_KEY still works as a 1-key pool.
 * ============================================================ */
const KEYS = (process.env.NIM_API_KEYS || process.env.NIM_API_KEY || '')
  .split(',')
  .map(k => k.trim())
  .filter(Boolean);

// 'round-robin' spreads load evenly; 'random' shuffles randomly
const KEY_STRATEGY = (process.env.KEY_STRATEGY || 'round-robin').toLowerCase();

const COOLDOWN_MS = Number(process.env.KEY_COOLDOWN_MS || 15000);   // park a key 15s after 429 / network error
const DEAD_KEY_MS = 10 * 60 * 1000;                                // park a dead key (401/402/403) 10 min
const RETRY_DELAY_MS = Number(process.env.RETRY_DELAY_MS || 300);  // pause between key attempts
const HEADER_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS || 300000); // wait max 5 min for upstream answer
const MAX_ATTEMPTS_RAW = parseInt(process.env.MAX_KEY_ATTEMPTS, 10) || KEYS.length;

// statuses meaning "this key can't serve right now -> try another one"
const KEY_SPECIFIC = new Set([401, 402, 403]);            // invalid / out of credits / suspended
const TRANSIENT = new Set([408, 429, 500, 502, 503, 504]); // rate limit / upstream hiccup

const pool = KEYS.map((key, i) => ({
  id: i,
  key,
  tail: key.slice(-4), // masked id for logs - never log full keys
  cooldownUntil: 0,
  served: 0,
  failed: 0,
  lastError: ''
}));

let rr = 0; // round-robin cursor

function pickKey() {
  const now = Date.now();
  const ready = [];
  pool.forEach((k, i) => { if (k.cooldownUntil <= now) ready.push(i); });

  if (ready.length) {
    if (KEY_STRATEGY === 'random') {
      return ready[Math.floor(Math.random() * ready.length)];
    }
    for (let i = 0; i < pool.length; i++) {
      const idx = (rr + i) % pool.length;
      if (pool[idx].cooldownUntil <= now) {
        rr = (idx + 1) % pool.length;
        return idx;
      }
    }
  }
  // every key is cooling down -> use the one that frees up soonest
  let best = 0;
  for (let i = 1; i < pool.length; i++) {
    if (pool[i].cooldownUntil < pool[best].cooldownUntil) best = i;
  }
  return best;
}
/* ============================================================ */

function checkAuth(req, res) {
  if (!PROXY_KEY) return true;
  const auth = req.headers.authorization || '';
  if (auth === `Bearer ${PROXY_KEY}`) return true;
  res.status(401).json({ error: { message: 'Invalid proxy key' } });
  return false;
}

app.get('/health', (req, res) => res.json({
  ok: true,
  keys: pool.length,
  keysReady: pool.filter(k => k.cooldownUntil <= Date.now()).length
}));

// NEW: live status of every key (masked) so you can see who is rate-limited
app.get('/keys', (req, res) => {
  if (!checkAuth(req, res)) return;
  const now = Date.now();
  res.json(pool.map(k => ({
    key: `...${k.tail}`,
    status: now < k.cooldownUntil ? 'cooldown' : 'ready',
    cooldownForMs: Math.max(0, k.cooldownUntil - now),
    served: k.served,
    failed: k.failed,
    lastError: k.lastError || null
  })));
});

app.get('/v1/models', (req, res) => {
  res.json({
    object: 'list',
    data: Object.keys(MODEL_MAP).map(id => ({ id, object: 'model', owned_by: 'nim-proxy' }))
  });
});

app.post('/v1/chat/completions', async (req, res) => {
  const startedAt = Date.now();
  if (!checkAuth(req, res)) return;
  if (!pool.length) {
    return res.status(500).json({ error: { message: 'No API keys configured - set NIM_API_KEYS (comma-separated) in Railway Variables' } });
  }

  const body = req.body || {};
  const nimModel = MODEL_MAP[body.model] || DEFAULT_MODEL;
  const isStream = !!body.stream;
  console.log(`[request] JanitorAI asked for "${body.model}" -> using NIM model "${nimModel}"${isStream ? ' (stream)' : ''}`);

  const payload = {
    model: nimModel,
    messages: body.messages,
    temperature: body.temperature ?? 1,
    top_p: body.top_p ?? 0.95,
    max_tokens: body.max_tokens ?? 208000,
    stream: isStream
  };
  const payloadStr = JSON.stringify(payload); // built once, reused on every retry

  const attempts = Math.max(1, Math.min(pool.length, MAX_ATTEMPTS_RAW));
  let lastErr = '';

  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (attempt > 1) await new Promise(r => setTimeout(r, RETRY_DELAY_MS));

    const idx = pickKey();
    const entry = pool[idx];
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), HEADER_TIMEOUT_MS);

    try {
      const nimRes = await fetch(`${NIM_BASE}/chat/completions`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${entry.key}`,
          'Content-Type': 'application/json'
        },
        body: payloadStr,
        signal: controller.signal
      });
      clearTimeout(timer);

      // NEW: key is rate-limited / dead -> park it and retry on the next key
      if (KEY_SPECIFIC.has(nimRes.status) || TRANSIENT.has(nimRes.status)) {
        const reason = KEY_SPECIFIC.has(nimRes.status)
          ? `key problem (HTTP ${nimRes.status})`
          : nimRes.status === 429 ? 'rate limited (429)' : `upstream error ${nimRes.status}`;
        const errText = await nimRes.text().catch(() => '');
        entry.cooldownUntil = Date.now() + (KEY_SPECIFIC.has(nimRes.status) ? DEAD_KEY_MS : COOLDOWN_MS);
        entry.failed += 1;
        entry.lastError = reason;
        lastErr = reason;
        console.log(`[keys] key #${idx} (...${entry.tail}) parked: ${reason} ${errText.slice(0, 120)} -> trying next key`);
        continue;
      }

      // payload-level error (400/413/422...): same for every key, forward it as JSON
      // instead of piping garbage into the event-stream (this caused JanitorAI "network error")
      if (!nimRes.ok) {
        const errText = await nimRes.text().catch(() => '');
        console.log(`[upstream] HTTP ${nimRes.status}: ${errText.slice(0, 200)}`);
        res.status(nimRes.status).type('application/json')
          .send(errText || JSON.stringify({ error: { message: `NIM returned HTTP ${nimRes.status}` } }));
        return;
      }

      entry.served += 1;
      entry.lastError = '';
      console.log(`[keys] key #${idx} (...${entry.tail}) serving this request [HTTP ${nimRes.status}] after ${Date.now() - startedAt}ms`);

      if (isStream) {
        // headers are verified OK (not 429/5xx), so the stream is safe to pipe
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        nimRes.body.pipe(res);
        return;
      }

      const data = await nimRes.json();
      console.log(`[timing] NIM responded in ${Date.now() - startedAt}ms`);
      res.status(nimRes.status).json(data);
      return;

    } catch (err) {
      clearTimeout(timer);
      // network error / timeout -> this key gets parked, the next one takes over
      const reason = err.name === 'AbortError' ? `no response within ${HEADER_TIMEOUT_MS}ms` : err.message;
      entry.cooldownUntil = Date.now() + COOLDOWN_MS;
      entry.failed += 1;
      entry.lastError = reason;
      lastErr = reason;
      console.log(`[keys] key #${idx} (...${entry.tail}) failed: ${reason} -> trying next key`);
      continue;
    }
  }

  // every key failed - tell JanitorAI clearly instead of a broken stream
  res.status(503).json({
    error: { message: `All ${attempts} API key attempts failed. Last problem: ${lastErr || 'unknown'}` }
  });
});

app.use((req, res) => res.status(404).json({ error: { message: `${req.path} not found` } }));

app.listen(PORT, () => console.log(`Proxy running on port ${PORT} with ${pool.length} API key(s) [strategy: ${KEY_STRATEGY}]`));
