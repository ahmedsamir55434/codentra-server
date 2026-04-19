const fs = require('fs/promises');
const path = require('path');
const http = require('http');

require('dotenv').config();

const fetchFn =
  typeof fetch === 'function' ? fetch.bind(globalThis) : require('node-fetch');

const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT ? Number(process.env.PORT) : 4000;
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const DB_PATH = path.join(__dirname, 'data', 'db.json');
const DATASET_NAME = path.basename(DB_PATH);
const PUBLIC_DIR = path.join(__dirname, 'public');
const IS_VERCEL = Boolean(process.env.VERCEL);
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const GEMINI_MODEL = process.env.GEMINI_MODEL || '';
const TURN_URL = String(process.env.TURN_URL || '').trim();
const TURN_USERNAME = String(process.env.TURN_USERNAME || '').trim();
const TURN_CREDENTIAL = String(process.env.TURN_CREDENTIAL || '').trim();
const TRUST_PROXY = /^(1|true|yes|on)$/i.test(String(process.env.TRUST_PROXY || ''));
const ADMIN_EMAILS = String(process.env.ADMIN_EMAILS || process.env.ADMIN_EMAIL || '')
  .split(',')
  .map((s) => String(s || '').trim().toLowerCase())
  .filter(Boolean);

function createInitialDb() {
  return {
    users: [],
    posts: [],
    chats: [],
    messages: [],
    sessions: [],
    notifications: [],
    sounds: [],
    stories: [],
    meetings: [],
  };
}

async function ensureLocalDbFile() {
  try {
    await fs.access(DB_PATH);
  } catch {
    await fs.mkdir(path.dirname(DB_PATH), { recursive: true });
    await atomicWriteJson(DB_PATH, createInitialDb());
  }
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function atomicWriteJson(filePath, data) {
  const tmpPath = `${filePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(data, null, 2), 'utf8');
  await fs.rename(tmpPath, filePath);
}

async function loadLocalSeedDb() {
  try {
    await ensureLocalDbFile();
    const db = await readJson(DB_PATH);
    normalizeDb(db);
    return db;
  } catch {
    const initial = createInitialDb();
    normalizeDb(initial);
    return initial;
  }
}

let pool = null;
let storageReadyPromise = null;

function usePostgresStorage() {
  return Boolean(DATABASE_URL);
}

function getDbPool() {
  if (!usePostgresStorage()) return null;
  if (!pool) {
    pool = new Pool({
      connectionString: DATABASE_URL,
      max: process.env.VERCEL ? 3 : 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    });
  }
  return pool;
}

async function ensurePostgresStore() {
  const dbPool = getDbPool();
  if (!dbPool) return;

  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS data_store (
      name TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function ensureStorageReady() {
  if (!storageReadyPromise) {
    storageReadyPromise = (usePostgresStorage() ? ensurePostgresStore() : ensureLocalDbFile()).catch((err) => {
      storageReadyPromise = null;
      throw err;
    });
  }
  return storageReadyPromise;
}

async function loadPostgresDbForUpdate(client) {
  let result = await client.query('SELECT data FROM data_store WHERE name = $1 FOR UPDATE', [DATASET_NAME]);
  if (result.rowCount) {
    const db = result.rows[0].data;
    normalizeDb(db);
    return db;
  }

  const initial = await loadLocalSeedDb();
  normalizeDb(initial);

  await client.query(
    `
      INSERT INTO data_store (name, data)
      VALUES ($1, $2::jsonb)
      ON CONFLICT (name) DO NOTHING
    `,
    [DATASET_NAME, JSON.stringify(initial)]
  );

  result = await client.query('SELECT data FROM data_store WHERE name = $1 FOR UPDATE', [DATASET_NAME]);
  if (!result.rowCount) {
    throw new Error(`failed_to_initialize_dataset:${DATASET_NAME}`);
  }

  const db = result.rows[0].data;
  normalizeDb(db);
  return db;
}

function normalizeDb(db) {
  if (!db || typeof db !== 'object') return;
  if (!Array.isArray(db.users)) db.users = [];
  if (!Array.isArray(db.posts)) db.posts = [];
  if (!Array.isArray(db.chats)) db.chats = [];
  if (!Array.isArray(db.messages)) db.messages = [];
  if (!Array.isArray(db.sessions)) db.sessions = [];
  if (!Array.isArray(db.notifications)) db.notifications = [];
  if (!Array.isArray(db.sounds)) db.sounds = [];
  if (!Array.isArray(db.stories)) db.stories = [];
  if (!Array.isArray(db.meetings)) db.meetings = [];

  db.users.forEach((u) => {
    if (!u || typeof u !== 'object') return;
    if (typeof u.verified !== 'boolean') u.verified = Boolean(u.verified);
    if (!Array.isArray(u.pinnedPosts)) u.pinnedPosts = [];
    u.pinnedPosts = u.pinnedPosts.map(String).filter(Boolean).slice(0, 3);
    if (!Array.isArray(u.highlights)) u.highlights = [];
    u.highlights.forEach((h) => {
      if (!h || typeof h !== 'object') return;
      if (!h.id) h.id = uuidv4();
      if (!h.title) h.title = '';
      if (!Array.isArray(h.items)) h.items = [];
      if (!h.createdAt) h.createdAt = new Date().toISOString();
      if (!h.updatedAt) h.updatedAt = h.createdAt;
      h.items.forEach((it) => {
        if (!it || typeof it !== 'object') return;
        if (!it.id) it.id = uuidv4();
        if (!it.media || typeof it.media !== 'object') it.media = { type: '', url: '' };
        if (!it.text) it.text = '';
        if (typeof it.soundId !== 'string') it.soundId = String(it.soundId || '');
        if (!it.style || typeof it.style !== 'object') it.style = {};
        if (typeof it.style.bg !== 'string') it.style.bg = '#0b0d10';
        if (typeof it.style.color !== 'string') it.style.color = '#ffffff';
        if (!Number.isFinite(it.style.fontSize)) it.style.fontSize = Number(it.style.fontSize || 32);
        if (!Number.isFinite(it.style.x)) it.style.x = Number(it.style.x || 0.5);
        if (!Number.isFinite(it.style.y)) it.style.y = Number(it.style.y || 0.5);
        if (!Number.isFinite(it.style.mediaScale)) it.style.mediaScale = Number(it.style.mediaScale || 1);
        if (!it.createdAt) it.createdAt = new Date().toISOString();
        if (!it.sourceStoryId) it.sourceStoryId = '';
      });
    });
  });

  db.posts.forEach((p) => {
    if (!p || typeof p !== 'object') return;
    if (!p.kind) p.kind = 'post';
    if (!Array.isArray(p.media)) p.media = [];
    if (!Array.isArray(p.likes)) p.likes = [];
    if (!Array.isArray(p.saves)) p.saves = [];
    if (!Array.isArray(p.comments)) p.comments = [];
    if (!Number.isFinite(p.sharesCount)) p.sharesCount = Number(p.sharesCount || 0);
  });

  db.stories.forEach((s) => {
    if (!s || typeof s !== 'object') return;
    if (!s.id) s.id = uuidv4();
    if (!s.userId) s.userId = '';
    if (!s.media || typeof s.media !== 'object') s.media = { type: '', url: '' };
    if (typeof s.text !== 'string') s.text = String(s.text || '');
    if (typeof s.soundId !== 'string') s.soundId = String(s.soundId || '');
    if (s.interactive && typeof s.interactive === 'object') {
      const kind = String(s.interactive.kind || s.interactive.type || '').trim().toLowerCase();
      const question = String(s.interactive.question || '').trim();

      if (!kind) {
        delete s.interactive;
      } else if (kind === 'poll') {
        if (!question) {
          delete s.interactive;
        } else {
        const opts = Array.isArray(s.interactive.options) ? s.interactive.options : [];
        s.interactive.kind = 'poll';
        s.interactive.question = question;
        s.interactive.options = opts
          .map((o) => {
            const id = String(o?.id || uuidv4());
            const text = String(o?.text || '').trim();
            return { id, text };
          })
          .filter((o) => o.text)
          .slice(0, 4);
        if (!s.interactive.options.length) delete s.interactive;
        if (s.interactive && (!s.interactive.votesByUser || typeof s.interactive.votesByUser !== 'object')) s.interactive.votesByUser = {};
        }
      } else if (kind === 'question') {
        if (!question) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'question';
          s.interactive.question = question;
          if (!Array.isArray(s.interactive.answers)) s.interactive.answers = [];
          if (!s.interactive.answersByUser || typeof s.interactive.answersByUser !== 'object') s.interactive.answersByUser = {};
        }
      } else if (kind === 'link') {
        const url = String(s.interactive.url || s.interactive.href || '').trim();
        const title = String(s.interactive.title || '').trim();
        if (!url || !/^https?:\/\//i.test(url)) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'link';
          s.interactive.url = url.length > 600 ? url.slice(0, 600) : url;
          s.interactive.title = title.length > 80 ? title.slice(0, 80) : title;
        }
      } else if (kind === 'mention') {
        const raw = String(s.interactive.username || s.interactive.handle || s.interactive.mention || '').trim();
        const username = raw.startsWith('@') ? raw.slice(1).trim() : raw;
        if (!username || username.length > 40) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'mention';
          s.interactive.username = username;
        }
      } else if (kind === 'location') {
        const name = String(s.interactive.name || s.interactive.location || '').trim();
        if (!name || name.length > 80) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'location';
          s.interactive.name = name;
        }
      } else if (kind === 'countdown') {
        const title = String(s.interactive.title || '').trim();
        const endAt = String(s.interactive.endAt || s.interactive.endsAt || '').trim();
        const dt = endAt ? new Date(endAt) : null;
        if (!title || title.length > 80 || !dt || !Number.isFinite(dt.getTime())) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'countdown';
          s.interactive.title = title;
          s.interactive.endAt = dt.toISOString();
        }
      } else if (kind === 'slider') {
        const emoji = String(s.interactive.emoji || '❤️').trim() || '❤️';
        if (!question || question.length > 160 || emoji.length > 8) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'slider';
          s.interactive.question = question;
          s.interactive.emoji = emoji;
          if (!Array.isArray(s.interactive.responses)) s.interactive.responses = [];
          if (!s.interactive.responsesByUser || typeof s.interactive.responsesByUser !== 'object') s.interactive.responsesByUser = {};
        }
      } else if (kind === 'quiz') {
        const opts = Array.isArray(s.interactive.options) ? s.interactive.options : [];
        if (!question || question.length > 160) {
          delete s.interactive;
        } else {
          s.interactive.kind = 'quiz';
          s.interactive.question = question;
          s.interactive.options = opts
            .map((o) => {
              const id = String(o?.id || uuidv4());
              const text = String(o?.text || '').trim();
              return { id, text };
            })
            .filter((o) => o.text)
            .slice(0, 4);
          if (!s.interactive.options.length) {
            delete s.interactive;
          } else {
            const correct = String(s.interactive.correctOptionId || '').trim();
            if (correct && !s.interactive.options.some((o) => String(o.id) === correct)) delete s.interactive.correctOptionId;
            if (!Array.isArray(s.interactive.answers)) s.interactive.answers = [];
            if (!s.interactive.answersByUser || typeof s.interactive.answersByUser !== 'object') s.interactive.answersByUser = {};
          }
        }
      } else {
        delete s.interactive;
      }
    } else if (s.interactive != null) {
      delete s.interactive;
    }
    if (!s.style || typeof s.style !== 'object') s.style = {};
    if (typeof s.style.bg !== 'string') s.style.bg = '#0b0d10';
    if (typeof s.style.color !== 'string') s.style.color = '#ffffff';
    if (!Number.isFinite(s.style.fontSize)) s.style.fontSize = Number(s.style.fontSize || 32);
    if (!Number.isFinite(s.style.x)) s.style.x = Number(s.style.x || 0.5);
    if (!Number.isFinite(s.style.y)) s.style.y = Number(s.style.y || 0.5);
    if (!Number.isFinite(s.style.mediaScale)) s.style.mediaScale = Number(s.style.mediaScale || 1);
    if (!Array.isArray(s.views)) s.views = [];
    if (!Array.isArray(s.comments)) s.comments = [];
  });

  db.meetings.forEach((m) => {
    if (!m || typeof m !== 'object') return;
    if (!m.id) m.id = uuidv4();
    if (!m.title) m.title = '';
    if (!m.hostId) m.hostId = '';
    if (!Array.isArray(m.participantIds)) m.participantIds = [];
    m.participantIds = m.participantIds.map(String).filter(Boolean).slice(0, 300);
    if (!m.createdAt) m.createdAt = new Date().toISOString();
    if (typeof m.endedAt !== 'string') m.endedAt = String(m.endedAt || '');
  });
}

const meetingsRuntime = new Map();

function getMeetingRuntime(meetingId) {
  const id = String(meetingId || '').trim();
  if (!id) return null;
  if (!meetingsRuntime.has(id)) {
    meetingsRuntime.set(id, {
      clientsByUserId: new Map(),
      pendingByRequestId: new Map(),
      chatHistory: [],
    });
  }
  return meetingsRuntime.get(id);
}

function safeUserForMeeting(u) {
  if (!u || typeof u !== 'object') return null;
  return {
    id: u.id,
    username: u.username,
    avatarUrl: u.avatarUrl,
    verified: Boolean(u.verified),
  };
}

const STORY_TTL_MS = 24 * 60 * 60 * 1000;

function isStoryExpired(story, nowMs) {
  if (!story || typeof story !== 'object') return true;
  const expiresAt = story.expiresAt ? Date.parse(String(story.expiresAt)) : NaN;
  if (Number.isFinite(expiresAt)) return expiresAt <= nowMs;
  const createdAt = story.createdAt ? Date.parse(String(story.createdAt)) : NaN;
  if (!Number.isFinite(createdAt)) return true;
  return createdAt + STORY_TTL_MS <= nowMs;
}

function purgeExpiredStories(db) {
  if (!db || typeof db !== 'object') return;
  if (!Array.isArray(db.stories)) db.stories = [];
  const nowMs = Date.now();
  db.stories = db.stories.filter((s) => !isStoryExpired(s, nowMs));
}

function normalizeTag(tag) {
  return String(tag || '')
    .trim()
    .replace(/^#/, '')
    .toLowerCase();
}

function extractHashtags(text) {
  const t = String(text || '');
  const out = new Set();
  const re = /(^|\s)#([\p{L}\p{N}_]{2,50})/gu;
  let m;
  while ((m = re.exec(t))) {
    out.add(normalizeTag(m[2]));
  }
  return Array.from(out);
}

let dbQueue = Promise.resolve();
function withDb(fn) {
  if (!usePostgresStorage()) {
    dbQueue = dbQueue.then(async () => {
      await ensureStorageReady();
      const db = await readJson(DB_PATH);
      normalizeDb(db);
      const result = await fn(db);
      await atomicWriteJson(DB_PATH, db);
      return result;
    });
    return dbQueue;
  }

  return (async () => {
    await ensureStorageReady();
    const client = await getDbPool().connect();
    try {
      await client.query('BEGIN');
      const db = await loadPostgresDbForUpdate(client);
      const result = await fn(db);
      normalizeDb(db);
      await client.query(
        `
          INSERT INTO data_store (name, data, updated_at)
          VALUES ($1, $2::jsonb, NOW())
          ON CONFLICT (name)
          DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
        `,
        [DATASET_NAME, JSON.stringify(db)]
      );
      await client.query('COMMIT');
      return result;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ignore rollback failures
      }
      throw err;
    } finally {
      client.release();
    }
  })();
}

function pickPublicUser(user) {
  const { passwordHash, ...rest } = user;
  return rest;
}

function decoratePostForResponse(usersById, post) {
  const u = usersById.get(post.userId);
  const author = u ? { id: u.id, username: u.username, avatarUrl: u.avatarUrl, verified: Boolean(u.verified) } : null;
  const comments = (Array.isArray(post.comments) ? post.comments : []).map((c) => {
    const cu = usersById.get(c.userId);
    const user = cu ? { id: cu.id, username: cu.username, avatarUrl: cu.avatarUrl, verified: Boolean(cu.verified) } : null;
    return { ...c, user };
  });

  const soundId = post && typeof post === 'object' ? String(post.soundId || '') : '';
  const sound = soundId && usersById && typeof usersById.get === 'function' && usersById.soundsById
    ? usersById.soundsById.get(soundId) || null
    : null;

  return { ...post, author, comments, sound };
}

function requireAuth(req, res, next) {
  const header = req.headers.authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'missing_token' });

  withDb(async (db) => {
    const session = db.sessions.find((s) => s.token === token);
    if (!session) {
      res.status(401).json({ error: 'invalid_token' });
      return;
    }
    const user = db.users.find((u) => u.id === session.userId);
    if (!user) {
      res.status(401).json({ error: 'invalid_session_user' });
      return;
    }
    req.user = user;
    req.token = token;
    next();
  }).catch((err) => {
    res.status(500).json({ error: 'db_error', details: String(err?.message || err) });
  });
}

function isAdminUser(user) {
  if (!ADMIN_EMAILS.length) return false;
  const email = String(user?.email || '').toLowerCase().trim();
  return Boolean(email && ADMIN_EMAILS.includes(email));
}

function requireAdmin(req, res, next) {
  if (!ADMIN_EMAILS.length) return res.status(403).json({ error: 'admin_not_configured' });
  if (!isAdminUser(req.user)) return res.status(403).json({ error: 'admin_forbidden' });
  next();
}

const app = express();
if (TRUST_PROXY) app.set('trust proxy', 1);
app.use(cors());
app.use(express.json({ limit: '80mb' }));
app.use(express.static(PUBLIC_DIR));

app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') {
    res.status(413).json({ error: 'payload_too_large' });
    return;
  }
  if (err && err instanceof SyntaxError && 'body' in err) {
    res.status(400).json({ error: 'invalid_json' });
    return;
  }
  next(err);
});

app.get('/', (req, res) => {
  const accept = String(req.headers.accept || '');
  if (accept.includes('text/html')) {
    res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
    return;
  }
  res.json({ ok: true, name: 'NeuraConnect JSON Server' });
});

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    name: 'NeuraConnect JSON Server',
    gemini: Boolean(GEMINI_API_KEY),
    storage: usePostgresStorage() ? 'postgres' : 'json-file',
    dataset: DATASET_NAME,
    realtimeMeetings: !IS_VERCEL,
  });
});

app.get('/admin/status', requireAuth, (req, res) => {
  res.json({
    isAdmin: isAdminUser(req.user),
    adminEmailConfigured: ADMIN_EMAILS.length > 0,
  });
});

app.get('/sounds', requireAuth, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const sounds = (db.sounds || [])
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .map((s) => ({
          id: s.id,
          title: s.title || '',
          url: s.url || '',
          createdAt: s.createdAt,
        }));
      return { status: 200, body: { sounds } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/rtc/config', requireAuth, (req, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }];
  if (TURN_URL && TURN_USERNAME && TURN_CREDENTIAL) {
    iceServers.push({ urls: TURN_URL, username: TURN_USERNAME, credential: TURN_CREDENTIAL });
  }
  res.json({ iceServers });
});

app.post('/meetings', requireAuth, async (req, res) => {
  const { title } = req.body || {};
  const t = String(title || '').trim().slice(0, 80);
  try {
    const result = await withDb(async (db) => {
      normalizeDb(db);
      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const now = new Date().toISOString();
      const meeting = {
        id: uuidv4(),
        title: t,
        hostId: String(me.id),
        participantIds: [String(me.id)],
        createdAt: now,
        endedAt: '',
      };
      db.meetings.push(meeting);
      return { status: 201, body: { meeting } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/meetings/:id', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      normalizeDb(db);
      const meeting = (db.meetings || []).find((m) => m.id === String(id));
      if (!meeting) return { status: 404, body: { error: 'not_found' } };
      if (meeting.endedAt) return { status: 410, body: { error: 'ended' } };
      const host = db.users.find((u) => u.id === String(meeting.hostId));
      return { status: 200, body: { meeting: { ...meeting, host: safeUserForMeeting(host) } } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories/:id/slider/respond', requireAuth, async (req, res) => {
  const { id } = req.params;
  const rawValue = req.body?.value;
  const valueNum = Number(rawValue);
  if (!Number.isFinite(valueNum)) return res.status(400).json({ error: 'invalid_value' });
  const value = Math.max(0, Math.min(100, Math.round(valueNum)));

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'slider') return { status: 400, body: { error: 'not_a_slider' } };
      if (!Array.isArray(inter.responses)) inter.responses = [];
      if (!inter.responsesByUser || typeof inter.responsesByUser !== 'object') inter.responsesByUser = {};

      const uid = String(me.id);
      if (inter.responsesByUser[uid] != null) return { status: 409, body: { error: 'already_responded' } };

      const resp = {
        id: uuidv4(),
        storyId: String(story.id),
        userId: uid,
        value,
        createdAt: new Date().toISOString(),
      };
      inter.responses.push(resp);
      inter.responsesByUser[uid] = value;

      const ownerId = String(story.userId || '');
      if (ownerId && ownerId !== uid) {
        addNotification(db, {
          userId: ownerId,
          type: 'story_slider_response',
          actorId: me.id,
          storyId: story.id,
          message: `${me.username} تفاعل مع Emoji Slider في الستوري`,
        });
      }

      const view = buildStoryInteractiveView(inter, uid, String(story.userId) === uid);
      return { status: 201, body: { ok: true, interactive: view } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/stories/:id/slider/responses', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      if (String(story.userId) !== String(me.id)) return { status: 403, body: { error: 'forbidden' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'slider') return { status: 400, body: { error: 'not_a_slider' } };
      if (!Array.isArray(inter.responses)) inter.responses = [];

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const responses = inter.responses
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .slice(0, 300)
        .map((r) => {
          const u = usersById.get(String(r.userId));
          const user = u ? { id: u.id, username: u.username, avatarUrl: u.avatarUrl, verified: Boolean(u.verified) } : null;
          return { ...r, user };
        });

      return { status: 200, body: { question: String(inter.question || ''), emoji: String(inter.emoji || '❤️'), responses } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories/:id/quiz/answer', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { optionId } = req.body || {};
  const oid = String(optionId || '').trim();
  if (!oid) return res.status(400).json({ error: 'missing_optionId' });

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'quiz') return { status: 400, body: { error: 'not_a_quiz' } };
      if (!Array.isArray(inter.options)) inter.options = [];
      if (!Array.isArray(inter.answers)) inter.answers = [];
      if (!inter.answersByUser || typeof inter.answersByUser !== 'object') inter.answersByUser = {};

      const exists = inter.options.some((o) => String(o?.id || '') === oid);
      if (!exists) return { status: 400, body: { error: 'invalid_option' } };

      const uid = String(me.id);
      if (inter.answersByUser[uid]) return { status: 409, body: { error: 'already_answered' } };

      const answer = {
        id: uuidv4(),
        storyId: String(story.id),
        userId: uid,
        optionId: oid,
        createdAt: new Date().toISOString(),
      };

      inter.answers.push(answer);
      inter.answersByUser[uid] = oid;

      const ownerId = String(story.userId || '');
      if (ownerId && ownerId !== uid) {
        addNotification(db, {
          userId: ownerId,
          type: 'story_quiz_answer',
          actorId: me.id,
          storyId: story.id,
          message: `${me.username} أجاب على Quiz في الستوري`,
        });
      }

      const view = buildStoryInteractiveView(inter, uid, String(story.userId) === uid);
      return { status: 201, body: { ok: true, interactive: view } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/stories/:id/quiz/answers', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      if (String(story.userId) !== String(me.id)) return { status: 403, body: { error: 'forbidden' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'quiz') return { status: 400, body: { error: 'not_a_quiz' } };
      if (!Array.isArray(inter.answers)) inter.answers = [];
      if (!Array.isArray(inter.options)) inter.options = [];

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const answers = inter.answers
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .slice(0, 400)
        .map((a) => {
          const u = usersById.get(String(a.userId));
          const user = u ? { id: u.id, username: u.username, avatarUrl: u.avatarUrl, verified: Boolean(u.verified) } : null;
          return { ...a, user };
        });

      return {
        status: 200,
        body: {
          question: String(inter.question || ''),
          correctOptionId: String(inter.correctOptionId || ''),
          options: inter.options.map((o) => ({ id: String(o?.id || ''), text: String(o?.text || '') })),
          answers,
        },
      };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories/:id/poll/vote', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { optionId } = req.body || {};
  const oid = String(optionId || '').trim();
  if (!oid) return res.status(400).json({ error: 'missing_optionId' });

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'poll') return { status: 400, body: { error: 'not_a_poll' } };
      if (!Array.isArray(inter.options)) inter.options = [];
      if (!inter.votesByUser || typeof inter.votesByUser !== 'object') inter.votesByUser = {};

      const exists = inter.options.some((o) => String(o?.id || '') === oid);
      if (!exists) return { status: 400, body: { error: 'invalid_option' } };

      const uid = String(me.id);
      if (inter.votesByUser[uid]) return { status: 409, body: { error: 'already_voted' } };

      inter.votesByUser[uid] = oid;
      const view = buildStoryInteractiveView(inter, uid, String(story.userId) === uid);
      return { status: 200, body: { interactive: view } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories/:id/question/answer', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { text } = req.body || {};
  const answerText = String(text || '').trim();
  if (!answerText) return res.status(400).json({ error: 'missing_text' });
  if (answerText.length > 500) return res.status(400).json({ error: 'text_too_long' });

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'question') return { status: 400, body: { error: 'not_a_question' } };
      if (!Array.isArray(inter.answers)) inter.answers = [];
      if (!inter.answersByUser || typeof inter.answersByUser !== 'object') inter.answersByUser = {};

      const uid = String(me.id);
      if (inter.answersByUser[uid]) return { status: 409, body: { error: 'already_answered' } };

      const answer = {
        id: uuidv4(),
        storyId: String(story.id),
        userId: uid,
        text: answerText,
        createdAt: new Date().toISOString(),
      };

      inter.answers.push(answer);
      inter.answersByUser[uid] = answer.id;

      const ownerId = String(story.userId || '');
      if (ownerId && ownerId !== uid) {
        addNotification(db, {
          userId: ownerId,
          type: 'story_question_answer',
          actorId: me.id,
          storyId: story.id,
          message: `${me.username} رد على سؤالك في الستوري`,
        });
      }

      const view = buildStoryInteractiveView(inter, uid, String(story.userId) === uid);
      return { status: 201, body: { ok: true, interactive: view } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/stories/:id/question/answers', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      if (String(story.userId) !== String(me.id)) return { status: 403, body: { error: 'forbidden' } };

      const inter = story.interactive && typeof story.interactive === 'object' ? story.interactive : null;
      if (!inter || String(inter.kind || '') !== 'question') return { status: 400, body: { error: 'not_a_question' } };
      if (!Array.isArray(inter.answers)) inter.answers = [];

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const answers = inter.answers
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .slice(0, 200)
        .map((a) => {
          const u = usersById.get(String(a.userId));
          const user = u ? { id: u.id, username: u.username, avatarUrl: u.avatarUrl, verified: Boolean(u.verified) } : null;
          return { ...a, user };
        });

      return { status: 200, body: { question: String(inter.question || ''), answers } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/users/:id/highlights', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      const user = db.users.find((u) => u.id === String(id));
      if (!user) return { status: 404, body: { error: 'not_found' } };
      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      const highlights = (Array.isArray(user.highlights) ? user.highlights : [])
        .slice()
        .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1))
        .map((h) => {
          const items = (Array.isArray(h.items) ? h.items : [])
            .slice()
            .sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
          const itemsWithSound = items.map((it) => {
            const sid = it && typeof it === 'object' ? String(it.soundId || '') : '';
            const sound = sid ? soundsById.get(sid) || null : null;
            return { ...it, sound };
          });
          const coverUrl = String(h.coverUrl || items?.[0]?.media?.url || '');
          return {
            id: h.id,
            title: h.title || '',
            coverUrl,
            items: itemsWithSound,
            createdAt: h.createdAt,
            updatedAt: h.updatedAt,
          };
        });
      return { status: 200, body: { highlights } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/highlights', requireAuth, async (req, res) => {
  const { title, coverUrl } = req.body || {};
  const t = String(title || '').trim();
  if (!t) return res.status(400).json({ error: 'missing_title' });

  try {
    const result = await withDb(async (db) => {
      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      if (!Array.isArray(me.highlights)) me.highlights = [];
      const now = new Date().toISOString();
      const h = {
        id: uuidv4(),
        userId: me.id,
        title: t,
        coverUrl: coverUrl ? String(coverUrl) : '',
        items: [],
        createdAt: now,
        updatedAt: now,
      };
      me.highlights.push(h);
      return { status: 201, body: { highlight: h } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/highlights/:id/items', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { storyId } = req.body || {};
  const sid = String(storyId || '').trim();
  if (!sid) return res.status(400).json({ error: 'missing_storyId' });

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      const h = (me.highlights || []).find((x) => String(x.id) === String(id));
      if (!h) return { status: 404, body: { error: 'highlight_not_found' } };
      if (!Array.isArray(h.items)) h.items = [];

      const story = (db.stories || []).find((s) => String(s.id) === sid);
      if (!story) return { status: 404, body: { error: 'story_not_found' } };
      if (String(story.userId) !== String(me.id)) return { status: 403, body: { error: 'forbidden' } };

      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      const storySoundId = story && typeof story === 'object' ? String(story.soundId || '') : '';
      const storySound = storySoundId ? soundsById.get(storySoundId) || null : null;

      const storyInteractive = story && typeof story === 'object' ? story.interactive : null;
      const safeInteractive = buildStoryInteractiveView(storyInteractive, String(me.id), true);

      const item = {
        id: uuidv4(),
        sourceStoryId: String(story.id),
        media: story.media && typeof story.media === 'object' ? story.media : { type: '', url: '' },
        text: story.text ? String(story.text) : '',
        style: story.style && typeof story.style === 'object' ? story.style : undefined,
        soundId: storySoundId,
        sound: storySound,
        interactive: safeInteractive,
        createdAt: new Date().toISOString(),
      };
      h.items.push(item);
      h.updatedAt = new Date().toISOString();

      return { status: 201, body: { item } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/stories/:id/comments', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };
      if (!Array.isArray(story.comments)) story.comments = [];

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const comments = story.comments
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .slice(0, 50)
        .map((c) => {
          const cu = usersById.get(String(c.userId));
          const user = cu ? { id: cu.id, username: cu.username, avatarUrl: cu.avatarUrl, verified: Boolean(cu.verified) } : null;
          return { ...c, user };
        });

      return { status: 200, body: { comments } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories/:id/comments', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { text } = req.body || {};
  if (!text || !String(text).trim()) return res.status(400).json({ error: 'missing_text' });

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };
      if (!Array.isArray(story.comments)) story.comments = [];

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const comment = {
        id: uuidv4(),
        storyId: String(story.id),
        userId: String(me.id),
        text: String(text).trim(),
        createdAt: new Date().toISOString(),
      };
      story.comments.push(comment);

      const ownerId = String(story.userId || '');
      if (ownerId && ownerId !== String(me.id)) {
        addNotification(db, {
          userId: ownerId,
          type: 'story_comment',
          actorId: me.id,
          storyId: story.id,
          message: `${me.username} علّق على الستوري: ${summarizeText(comment.text, 80)}`,
        });
      }

      return { status: 201, body: { comment } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/stories', requireAuth, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);

      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };

      const allowedUserIds = new Set([String(me.id), ...((me.following || []).map(String))]);

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      const stories = (db.stories || [])
        .filter((s) => allowedUserIds.has(String(s.userId)))
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .map((s) => {
          const owner = usersById.get(String(s.userId));
          const author = owner
            ? { id: owner.id, username: owner.username, avatarUrl: owner.avatarUrl, verified: Boolean(owner.verified) }
            : null;
          const sid = s && typeof s === 'object' ? String(s.soundId || '') : '';
          const sound = sid ? soundsById.get(sid) || null : null;
          const viewedBy = Array.isArray(s.views) ? s.views.map(String) : [];
          const seenByMe = viewedBy.includes(String(me.id));
          const isOwner = String(s.userId) === String(me.id);
          const interactive = buildStoryInteractiveView(s?.interactive, String(me.id), isOwner);
          return {
            ...s,
            author,
            sound,
            interactive,
            viewsCount: viewedBy.length,
            seenByMe,
          };
        });

      return { status: 200, body: { stories } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories', requireAuth, async (req, res) => {
  const { media, text, style, soundId, interactive } = req.body || {};
  const storyText = typeof text === 'string' ? text : String(text || '');
  const m = media && typeof media === 'object' ? media : null;
  const type = String(m?.type || '');
  const url = String(m?.url || '');

  const hasMedia = Boolean(type && url);
  if (!hasMedia && !storyText.trim()) return res.status(400).json({ error: 'empty_story' });
  if (hasMedia && !type.startsWith('image') && !type.startsWith('video')) return res.status(400).json({ error: 'invalid_media_type' });

  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      if (!Array.isArray(db.stories)) db.stories = [];

      const now = new Date();

      const nextSoundId = soundId ? String(soundId) : '';
      if (nextSoundId) {
        const exists = (db.sounds || []).some((s) => s.id === nextSoundId);
        if (!exists) return { status: 400, body: { error: 'invalid_sound' } };
      }

      const normalizedInteractive = normalizeStoryInteractive(interactive);
      if (interactive && !normalizedInteractive) {
        return { status: 400, body: { error: 'invalid_interactive' } };
      }

      const st = style && typeof style === 'object' ? style : {};
      const normalizedStyle = {
        bg: typeof st.bg === 'string' ? st.bg : '#0b0d10',
        color: typeof st.color === 'string' ? st.color : '#ffffff',
        fontSize: Number.isFinite(Number(st.fontSize)) ? Number(st.fontSize) : 32,
        x: Number.isFinite(Number(st.x)) ? Number(st.x) : 0.5,
        y: Number.isFinite(Number(st.y)) ? Number(st.y) : 0.5,
        mediaScale: Number.isFinite(Number(st.mediaScale)) ? Number(st.mediaScale) : 1,
      };
      const story = {
        id: uuidv4(),
        userId: String(req.user.id),
        text: storyText,
        media: hasMedia ? { type, url } : { type: '', url: '' },
        style: normalizedStyle,
        soundId: nextSoundId || undefined,
        interactive: normalizedInteractive || undefined,
        views: [],
        createdAt: now.toISOString(),
        expiresAt: new Date(now.getTime() + STORY_TTL_MS).toISOString(),
      };

      if (normalizedInteractive && normalizedInteractive.kind === 'mention') {
        const uname = String(normalizedInteractive.username || '').trim().toLowerCase();
        if (uname) {
          const target = db.users.find((u) => String(u.username || '').trim().toLowerCase() === uname);
          if (target && String(target.id) !== String(req.user.id)) {
            addNotification(db, {
              userId: target.id,
              type: 'story_mention',
              actorId: req.user.id,
              storyId: story.id,
              message: `${req.user.username} عملك Mention في الستوري`,
            });
          }
        }
      }

      db.stories.push(story);
      return { status: 201, body: { story } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/stories/:id/view', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const story = (db.stories || []).find((s) => s.id === String(id));
      if (!story) return { status: 404, body: { error: 'not_found' } };

      if (!Array.isArray(story.views)) story.views = [];
      const uid = String(req.user.id);
      if (!story.views.map(String).includes(uid)) story.views.push(uid);
      return { status: 200, body: { ok: true } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.delete('/stories/:id', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      purgeExpiredStories(db);
      const idx = (db.stories || []).findIndex((s) => s.id === String(id));
      if (idx === -1) return { status: 404, body: { error: 'not_found' } };
      const story = db.stories[idx];
      const isOwner = String(story.userId) === String(req.user.id);
      const isAdmin = isAdminUser(req.user);
      if (!isOwner && !isAdmin) return { status: 403, body: { error: 'forbidden' } };
      db.stories.splice(idx, 1);
      return { status: 200, body: { ok: true } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/sounds', requireAuth, async (req, res) => {
  const { title, url } = req.body || {};
  if (!title || !url) return res.status(400).json({ error: 'missing_fields' });
  if (!req.user?.verified) return res.status(403).json({ error: 'sound_add_requires_verified' });

  try {
    const result = await withDb(async (db) => {
      const now = new Date().toISOString();
      if (!Array.isArray(db.sounds)) db.sounds = [];

      const sound = {
        id: uuidv4(),
        title: String(title),
        url: String(url),
        createdBy: String(req.user.id),
        createdAt: now,
      };

      db.sounds.push(sound);
      return { status: 201, body: { sound } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/sounds/upload', requireAuth, async (req, res) => {
  const { filename, base64 } = req.body || {};
  if (!req.user?.verified) return res.status(403).json({ error: 'sound_upload_requires_verified' });
  if (!filename || !base64) return res.status(400).json({ error: 'missing_fields' });
  if (IS_VERCEL) return res.status(501).json({ error: 'vercel_upload_storage_not_configured' });

  try {
    const name = String(filename || 'audio.mp3');
    const ext = path.extname(name).toLowerCase();
    if (ext !== '.mp3') return res.status(400).json({ error: 'only_mp3_allowed' });

    const buf = Buffer.from(String(base64), 'base64');
    const maxBytes = 20 * 1024 * 1024;
    if (!buf.length) return res.status(400).json({ error: 'invalid_file' });
    if (buf.length > maxBytes) return res.status(413).json({ error: 'file_too_large' });

    const uploadsDir = path.join(PUBLIC_DIR, 'uploads');
    await fs.mkdir(uploadsDir, { recursive: true });
    const id = uuidv4();
    const outName = `${id}.mp3`;
    const outPath = path.join(uploadsDir, outName);
    await fs.writeFile(outPath, buf);

    res.status(201).json({ url: `/uploads/${outName}` });
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

async function listGeminiModels() {
  if (!GEMINI_API_KEY) throw new Error('missing_gemini_api_key');

  const versionsToTry = ['v1', 'v1beta'];
  let lastErr = null;

  for (const version of versionsToTry) {
    const url = `https://generativelanguage.googleapis.com/${version}/models?key=${encodeURIComponent(
      GEMINI_API_KEY
    )}`;

    try {
      const resp = await fetchFn(url);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        const message = String(data?.error?.message || 'gemini_list_models_failed');
        lastErr = new Error(message);
        continue;
      }

      const models = Array.isArray(data?.models) ? data.models : [];
      return models.map((m) => {
        const rawName = m?.name;
        const id = String(rawName || '').startsWith('models/') ? String(rawName).slice('models/'.length) : rawName;
        return {
          name: rawName,
          id,
          displayName: m?.displayName,
          supportedGenerationMethods: m?.supportedGenerationMethods,
        };
      });
    } catch (e) {
      lastErr = e;
    }
  }

  throw new Error(String(lastErr?.message || lastErr || 'gemini_list_models_failed'));
}

const geminiModelsCache = {
  v1: { expiresAt: 0, models: [] },
  v1beta: { expiresAt: 0, models: [] },
};

async function listGeminiModelsForVersion(version) {
  if (!GEMINI_API_KEY) throw new Error('missing_gemini_api_key');
  const url = `https://generativelanguage.googleapis.com/${version}/models?key=${encodeURIComponent(
    GEMINI_API_KEY
  )}`;

  const resp = await fetchFn(url);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const message = String(data?.error?.message || 'gemini_list_models_failed');
    throw new Error(message);
  }

  const models = Array.isArray(data?.models) ? data.models : [];
  return models.map((m) => {
    const rawName = m?.name;
    const id = String(rawName || '').startsWith('models/') ? String(rawName).slice('models/'.length) : rawName;
    return {
      name: rawName,
      id,
      displayName: m?.displayName,
      supportedGenerationMethods: m?.supportedGenerationMethods,
    };
  });
}

async function getGeminiGenerateContentModelIds(version) {
  const now = Date.now();
  const cached = geminiModelsCache[version];
  if (cached && cached.expiresAt > now && Array.isArray(cached.models) && cached.models.length) {
    return cached.models;
  }

  const models = await listGeminiModelsForVersion(version);
  const ids = models
    .filter((m) => Array.isArray(m?.supportedGenerationMethods) && m.supportedGenerationMethods.includes('generateContent'))
    .map((m) => normalizeGeminiModelId(m?.id || m?.name))
    .filter(Boolean);

  if (cached) {
    cached.models = ids;
    cached.expiresAt = now + 5 * 60 * 1000;
  }
  return ids;
}

app.get('/api/gemini/models', requireAuth, async (req, res) => {
  try {
    const models = await listGeminiModels();
    res.json({ models });
  } catch (err) {
    res.status(502).json({ error: 'gemini_list_models_failed', details: String(err?.message || err) });
  }
});

app.get('/admin/overview', requireAuth, requireAdmin, async (req, res) => {
  const days = req.query.days ? Number(req.query.days) : 30;
  const windowDays = Number.isFinite(days) && days > 0 ? Math.min(365, Math.max(1, Math.floor(days))) : 30;

  try {
    const result = await withDb(async (db) => {
      const nowMs = Date.now();
      const sinceMs = nowMs - windowDays * 24 * 60 * 60 * 1000;

      const posts = db.posts.slice();
      const reels = posts.filter((p) => String(p.kind || 'post') === 'reel');

      const postsLast = posts.filter((p) => {
        const t = Date.parse(p.createdAt || '') || 0;
        return t >= sinceMs;
      });
      const reelsLast = postsLast.filter((p) => String(p.kind || 'post') === 'reel');

      const recent = postsLast
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .slice(0, 40)
        .map((p) => ({
          id: p.id,
          userId: p.userId,
          kind: p.kind || 'post',
          text: p.text || '',
          createdAt: p.createdAt,
          likesCount: (p.likes || []).length,
          savesCount: (p.saves || []).length,
          commentsCount: (p.comments || []).length,
        }));

      return {
        status: 200,
        body: {
          totals: {
            users: db.users.length,
            posts: posts.length,
            reels: reels.length,
            chats: db.chats.length,
            messages: db.messages.length,
          },
          lastDays: windowDays,
          lastWindow: {
            posts: postsLast.length,
            reels: reelsLast.length,
            recent,
          },
        },
      };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/admin/users', requireAuth, requireAdmin, async (req, res) => {
  const q = (req.query.q ? String(req.query.q) : '').trim().toLowerCase();

  try {
    const result = await withDb(async (db) => {
      let users = db.users;
      if (q) {
        users = users.filter((u) => {
          const username = String(u.username || '').toLowerCase();
          const email = String(u.email || '').toLowerCase();
          return username.includes(q) || email.includes(q);
        });
      }

      users = users
        .slice()
        .sort((a, b) => (a.createdAt > b.createdAt ? 1 : -1))
        .map(pickPublicUser);

      return { status: 200, body: { users } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/admin/users/stats', requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const posts = db.posts.slice();
      const byUser = new Map();
      db.users.forEach((u) => {
        byUser.set(u.id, {
          id: u.id,
          username: u.username,
          email: u.email,
          avatarUrl: u.avatarUrl || '',
          verified: Boolean(u.verified),
          followersCount: (u.followers || []).length,
          followingCount: (u.following || []).length,
          postsCount: 0,
          reelsCount: 0,
          likesReceived: 0,
          savesReceived: 0,
          commentsReceived: 0,
          createdAt: u.createdAt,
        });
      });

      posts.forEach((p) => {
        const s = byUser.get(p.userId);
        if (!s) return;
        if (String(p.kind || 'post') === 'reel') s.reelsCount += 1;
        else s.postsCount += 1;
        s.likesReceived += (p.likes || []).length;
        s.savesReceived += (p.saves || []).length;
        s.commentsReceived += (p.comments || []).length;
      });

      const stats = Array.from(byUser.values()).sort((a, b) => {
        const scoreA = a.likesReceived + a.commentsReceived * 2 + a.savesReceived;
        const scoreB = b.likesReceived + b.commentsReceived * 2 + b.savesReceived;
        return scoreB - scoreA;
      });

      return { status: 200, body: { stats } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/admin/users/:id/verify', requireAuth, requireAdmin, async (req, res) => {
  const { id } = req.params;
  const { verified } = req.body || {};
  const nextValue = typeof verified === 'boolean' ? verified : Boolean(verified);

  try {
    const result = await withDb(async (db) => {
      const user = db.users.find((u) => u.id === String(id));
      if (!user) return { status: 404, body: { error: 'not_found' } };

      user.verified = nextValue;

      addNotification(db, {
        userId: user.id,
        type: 'admin',
        actorId: req.user.id,
        level: 'info',
        message: nextValue ? 'تم توثيق حسابك ✅' : 'تم إزالة توثيق حسابك',
      });

      return { status: 200, body: { ok: true, verified: Boolean(user.verified) } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/admin/sounds', requireAuth, requireAdmin, async (req, res) => {
  const { title, url } = req.body || {};
  if (!title || !url) return res.status(400).json({ error: 'missing_fields' });

  try {
    const result = await withDb(async (db) => {
      const now = new Date().toISOString();
      const sound = {
        id: uuidv4(),
        title: String(title),
        url: String(url),
        createdAt: now,
        createdBy: req.user.id,
      };
      db.sounds.push(sound);
      return {
        status: 201,
        body: { sound: { id: sound.id, title: sound.title, url: sound.url, createdAt: sound.createdAt } },
      };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.delete('/admin/sounds/:id', requireAuth, requireAdmin, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const exists = (db.sounds || []).find((s) => s.id === String(id));
      if (!exists) return { status: 404, body: { error: 'not_found' } };

      db.sounds = (db.sounds || []).filter((s) => s.id !== String(id));
      db.posts = (db.posts || []).map((p) => {
        if (!p || typeof p !== 'object') return p;
        if (String(p.soundId || '') === String(id)) delete p.soundId;
        return p;
      });

      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/admin/posts', requireAuth, requireAdmin, async (req, res) => {
  const kind = req.query.kind ? String(req.query.kind) : '';
  const userId = req.query.userId ? String(req.query.userId) : '';
  const days = req.query.days ? Number(req.query.days) : 0;
  const q = (req.query.q ? String(req.query.q) : '').trim().toLowerCase();

  const windowDays = Number.isFinite(days) && days > 0 ? Math.min(365, Math.max(1, Math.floor(days))) : 0;
  const sinceMs = windowDays ? Date.now() - windowDays * 24 * 60 * 60 * 1000 : 0;

  try {
    const result = await withDb(async (db) => {
      const usersById = new Map(db.users.map((u) => [u.id, u]));
      let posts = db.posts.slice();

      if (kind) posts = posts.filter((p) => String(p.kind || 'post') === kind);
      if (userId) posts = posts.filter((p) => String(p.userId) === userId);
      if (windowDays) {
        posts = posts.filter((p) => {
          const t = Date.parse(p.createdAt || '') || 0;
          return t >= sinceMs;
        });
      }
      if (q) {
        posts = posts.filter((p) => {
          const text = String(p.text || '').toLowerCase();
          return text.includes(q) || String(p.id || '').toLowerCase().includes(q);
        });
      }

      posts = posts
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .map((p) => {
          const author = usersById.get(p.userId);
          return {
            id: p.id,
            userId: p.userId,
            kind: p.kind || 'post',
            text: p.text || '',
            createdAt: p.createdAt,
            likesCount: (p.likes || []).length,
            savesCount: (p.saves || []).length,
            commentsCount: (p.comments || []).length,
            author: author ? { id: author.id, username: author.username, avatarUrl: author.avatarUrl } : null,
          };
        });

      return { status: 200, body: { posts } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.delete('/admin/posts/:id', requireAuth, requireAdmin, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      db.posts = db.posts.filter((p) => p.id !== id);
      db.notifications = db.notifications.filter((n) => String(n.postId || '') !== id);
      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.delete('/admin/users/:id', requireAuth, requireAdmin, async (req, res) => {
  const { id } = req.params;
  if (id === req.user.id) return res.status(400).json({ error: 'cannot_delete_self' });

  try {
    const result = await withDb(async (db) => {
      const user = db.users.find((u) => u.id === id);
      if (!user) return { status: 404, body: { error: 'not_found' } };

      db.users = db.users.filter((u) => u.id !== id);
      db.sessions = db.sessions.filter((s) => s.userId !== id);
      db.notifications = db.notifications.filter((n) => n.userId !== id && n.actorId !== id);

      db.users.forEach((u) => {
        u.followers = (u.followers || []).filter((x) => x !== id);
        u.following = (u.following || []).filter((x) => x !== id);
      });

      db.posts = (db.posts || [])
        .filter((p) => p.userId !== id)
        .map((p) => {
          p.likes = (p.likes || []).filter((x) => x !== id);
          p.saves = (p.saves || []).filter((x) => x !== id);
          p.comments = (p.comments || []).filter((c) => c && c.userId !== id);
          return p;
        });

      const removedChatIds = new Set(
        (db.chats || []).filter((c) => (c.memberIds || []).includes(id)).map((c) => c.id),
      );

      db.chats = (db.chats || []).filter((c) => !(c.memberIds || []).includes(id));
      db.messages = (db.messages || []).filter((m) => m.senderId !== id && !removedChatIds.has(m.chatId));
      db.notifications = (db.notifications || []).filter((n) => !removedChatIds.has(n.chatId));

      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/admin/notify', requireAuth, requireAdmin, async (req, res) => {
  const { userId, title, message, level } = req.body || {};
  if (!userId) return res.status(400).json({ error: 'missing_userId' });
  if (!message) return res.status(400).json({ error: 'missing_message' });

  try {
    const result = await withDb(async (db) => {
      const target = db.users.find((u) => u.id === String(userId));
      if (!target) return { status: 404, body: { error: 'user_not_found' } };

      const text = title ? `${String(title)}: ${String(message)}` : String(message);
      addNotification(db, {
        userId: target.id,
        type: 'admin',
        actorId: req.user.id,
        level: level ? String(level) : 'info',
        message: text,
      });

      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

function normalizeGeminiModelId(model) {
  const m = String(model || '').trim();
  if (!m) return '';
  return m.startsWith('models/') ? m.slice('models/'.length) : m;
}

async function callGemini(prompt) {
  if (!GEMINI_API_KEY) throw new Error('missing_gemini_api_key');

  const versionsToTry = ['v1', 'v1beta'];
  const preferredModels = [
    GEMINI_MODEL,
    'gemini-2.0-flash',
    'gemini-1.5-flash',
    'gemini-1.5-flash-latest',
    'gemini-1.5-pro',
    'gemini-1.5-pro-latest',
    'gemini-pro',
  ]
    .map(normalizeGeminiModelId)
    .filter(Boolean);

  let lastErr = null;

  for (const version of versionsToTry) {
    let modelsToTry = preferredModels;
    try {
      const available = await getGeminiGenerateContentModelIds(version);
      if (Array.isArray(available) && available.length) {
        const availableSet = new Set(available);
        const ordered = preferredModels.filter((m) => availableSet.has(m));
        const rest = available.filter((m) => !ordered.includes(m));
        modelsToTry = [...ordered, ...rest];
      }
    } catch {
      // ignore; fall back to preferredModels
    }

    for (const model of modelsToTry) {
      const url = `https://generativelanguage.googleapis.com/${version}/models/${encodeURIComponent(
        model
      )}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;

      try {
        const resp = await fetchFn(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ role: 'user', parts: [{ text: String(prompt) }] }],
            generationConfig: { temperature: 0.4, maxOutputTokens: 512 },
          }),
        });

        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
          const message = String(data?.error?.message || 'gemini_request_failed');
          const retriable =
            message.toLowerCase().includes('not found') ||
            message.toLowerCase().includes('not supported') ||
            message.toLowerCase().includes('method') ||
            message.toLowerCase().includes('generatecontent');

          lastErr = new Error(message);
          if (retriable) continue;
          throw lastErr;
        }

        const text =
          data?.candidates?.[0]?.content?.parts
            ?.map((p) => p?.text)
            .filter(Boolean)
            .join('') || '';

        if (text) return text;
        lastErr = new Error('empty_gemini_response');
      } catch (err) {
        lastErr = err;
      }
    }
  }

  throw new Error(String(lastErr?.message || lastErr || 'gemini_request_failed'));
}

function addNotification(db, notification) {
  const now = new Date().toISOString();
  db.notifications.push({
    id: uuidv4(),
    read: false,
    createdAt: now,
    ...notification,
  });
}

function normalizeStoryInteractive(input) {
  const i = input && typeof input === 'object' ? input : null;
  if (!i) return null;

  const kind = String(i.kind || i.type || '').trim().toLowerCase();
  if (!kind) return null;

  const question = String(i.question || '').trim();
  const title = String(i.title || '').trim();

  if (kind === 'poll') {
    if (!question) return null;
    if (question.length > 160) return null;
    const rawOptions = Array.isArray(i.options) ? i.options : Array.isArray(i.choices) ? i.choices : [];
    const texts = rawOptions
      .map((o) => (typeof o === 'string' ? o : String(o?.text || '')))
      .map((t) => String(t || '').trim())
      .filter(Boolean);

    const unique = [];
    texts.forEach((t) => {
      const trimmed = t.length > 60 ? t.slice(0, 60) : t;
      if (!unique.some((x) => x.toLowerCase() === trimmed.toLowerCase())) unique.push(trimmed);
    });

    if (unique.length < 2 || unique.length > 4) return null;

    return {
      kind: 'poll',
      question,
      options: unique.map((text) => ({ id: uuidv4(), text })),
      votesByUser: {},
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'question') {
    if (!question) return null;
    if (question.length > 160) return null;
    return {
      kind: 'question',
      question,
      answers: [],
      answersByUser: {},
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'link') {
    const url = String(i.url || i.href || '').trim();
    if (!url) return null;
    if (!/^https?:\/\//i.test(url)) return null;
    if (url.length > 600) return null;
    const nextTitle = title.length > 80 ? title.slice(0, 80) : title;
    return {
      kind: 'link',
      title: nextTitle,
      url,
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'mention') {
    const raw = String(i.username || i.handle || i.mention || '').trim();
    if (!raw) return null;
    const username = raw.startsWith('@') ? raw.slice(1).trim() : raw;
    if (!username) return null;
    if (username.length > 40) return null;
    return {
      kind: 'mention',
      username,
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'location') {
    const name = String(i.name || i.location || '').trim();
    if (!name) return null;
    if (name.length > 80) return null;
    return {
      kind: 'location',
      name,
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'countdown') {
    const endAt = String(i.endAt || i.endsAt || '').trim();
    if (!title || !endAt) return null;
    if (title.length > 80) return null;
    const dt = new Date(endAt);
    if (!Number.isFinite(dt.getTime())) return null;
    const now = Date.now();
    if (dt.getTime() < now - 5 * 60 * 1000) return null;
    return {
      kind: 'countdown',
      title,
      endAt: dt.toISOString(),
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'slider') {
    if (!question) return null;
    if (question.length > 160) return null;
    const emoji = String(i.emoji || '❤️').trim() || '❤️';
    if (emoji.length > 8) return null;
    return {
      kind: 'slider',
      question,
      emoji,
      responses: [],
      responsesByUser: {},
      createdAt: new Date().toISOString(),
    };
  }

  if (kind === 'quiz') {
    if (!question) return null;
    if (question.length > 160) return null;
    const rawOptions = Array.isArray(i.options) ? i.options : Array.isArray(i.choices) ? i.choices : [];
    const texts = rawOptions
      .map((o) => (typeof o === 'string' ? o : String(o?.text || '')))
      .map((t) => String(t || '').trim())
      .filter(Boolean);

    const unique = [];
    texts.forEach((t) => {
      const trimmed = t.length > 60 ? t.slice(0, 60) : t;
      if (!unique.some((x) => x.toLowerCase() === trimmed.toLowerCase())) unique.push(trimmed);
    });
    if (unique.length < 2 || unique.length > 4) return null;

    const options = unique.map((text) => ({ id: uuidv4(), text }));
    const correctRaw = String(i.correctOptionId || '').trim();
    const correctIndex = Number.isFinite(Number(i.correctIndex)) ? Number(i.correctIndex) : null;
    let correctOptionId = '';
    if (correctRaw && options.some((o) => String(o.id) === correctRaw)) correctOptionId = correctRaw;
    if (!correctOptionId && correctIndex != null && correctIndex >= 0 && correctIndex < options.length) {
      correctOptionId = String(options[correctIndex].id);
    }

    return {
      kind: 'quiz',
      question,
      options,
      correctOptionId: correctOptionId || undefined,
      answers: [],
      answersByUser: {},
      createdAt: new Date().toISOString(),
    };
  }

  return null;
}

function buildStoryInteractiveView(interactive, meId, isOwner) {
  const i = interactive && typeof interactive === 'object' ? interactive : null;
  if (!i) return null;
  const kind = String(i.kind || '').trim().toLowerCase();

  if (kind === 'poll') {
    const options = Array.isArray(i.options) ? i.options : [];
    const votesByUser = i.votesByUser && typeof i.votesByUser === 'object' ? i.votesByUser : {};
    const totalVotes = Object.keys(votesByUser).length;
    const countsByOption = {};
    Object.values(votesByUser).forEach((oid) => {
      const k = String(oid || '');
      if (!k) return;
      countsByOption[k] = (countsByOption[k] || 0) + 1;
    });

    const myVoteOptionId = meId ? String(votesByUser[String(meId)] || '') : '';
    return {
      kind: 'poll',
      question: String(i.question || ''),
      totalVotes,
      myVoteOptionId: myVoteOptionId || null,
      options: options.map((o) => {
        const id = String(o?.id || '');
        const count = countsByOption[id] || 0;
        const pct = totalVotes ? Math.round((count / totalVotes) * 100) : 0;
        return { id, text: String(o?.text || ''), count, pct };
      }),
      canVote: !myVoteOptionId,
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'question') {
    const answers = Array.isArray(i.answers) ? i.answers : [];
    const answersByUser = i.answersByUser && typeof i.answersByUser === 'object' ? i.answersByUser : {};
    const answeredByMe = meId ? Boolean(answersByUser[String(meId)]) : false;
    return {
      kind: 'question',
      question: String(i.question || ''),
      answersCount: answers.length,
      answeredByMe,
      canAnswer: !answeredByMe,
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'link') {
    return {
      kind: 'link',
      title: String(i.title || ''),
      url: String(i.url || ''),
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'mention') {
    return {
      kind: 'mention',
      username: String(i.username || ''),
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'location') {
    return {
      kind: 'location',
      name: String(i.name || ''),
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'countdown') {
    const endAt = String(i.endAt || '');
    const dt = new Date(endAt);
    const ms = Number.isFinite(dt.getTime()) ? dt.getTime() : 0;
    const now = Date.now();
    const remainingMs = ms ? Math.max(0, ms - now) : 0;
    return {
      kind: 'countdown',
      title: String(i.title || ''),
      endAt,
      remainingMs,
      ended: Boolean(ms && now >= ms),
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'slider') {
    const responsesByUser = i.responsesByUser && typeof i.responsesByUser === 'object' ? i.responsesByUser : {};
    const values = Object.values(responsesByUser)
      .map((v) => Number(v))
      .filter((v) => Number.isFinite(v));
    const responsesCount = values.length;
    const avg = responsesCount ? Math.round(values.reduce((a, b) => a + b, 0) / responsesCount) : 0;
    const myValue = meId ? responsesByUser[String(meId)] : undefined;
    const myNum = Number(myValue);
    const my = Number.isFinite(myNum) ? Math.max(0, Math.min(100, Math.round(myNum))) : null;
    return {
      kind: 'slider',
      question: String(i.question || ''),
      emoji: String(i.emoji || '❤️'),
      responsesCount,
      average: avg,
      myValue: my,
      canRespond: my == null,
      isOwner: Boolean(isOwner),
    };
  }

  if (kind === 'quiz') {
    const options = Array.isArray(i.options) ? i.options : [];
    const answersByUser = i.answersByUser && typeof i.answersByUser === 'object' ? i.answersByUser : {};
    const totalAnswers = Object.keys(answersByUser).length;
    const countsByOption = {};
    Object.values(answersByUser).forEach((oid) => {
      const k = String(oid || '');
      if (!k) return;
      countsByOption[k] = (countsByOption[k] || 0) + 1;
    });
    const myAnswerOptionId = meId ? String(answersByUser[String(meId)] || '') : '';
    const correctOptionId = String(i.correctOptionId || '');
    const answered = Boolean(myAnswerOptionId);
    const showResults = Boolean(isOwner) || answered;
    const isCorrect = answered && correctOptionId ? myAnswerOptionId === correctOptionId : null;

    return {
      kind: 'quiz',
      question: String(i.question || ''),
      correctOptionId: Boolean(isOwner) ? (correctOptionId || null) : null,
      totalAnswers,
      myAnswerOptionId: myAnswerOptionId || null,
      isCorrect,
      canAnswer: !answered,
      showResults,
      isOwner: Boolean(isOwner),
      options: options.map((o) => {
        const id = String(o?.id || '');
        const count = countsByOption[id] || 0;
        const pct = totalAnswers ? Math.round((count / totalAnswers) * 100) : 0;
        return { id, text: String(o?.text || ''), count, pct };
      }),
    };
  }

  return null;
}

app.post('/auth/register', async (req, res) => {
  const { username, email, password } = req.body || {};
  if (!username || !email || !password) return res.status(400).json({ error: 'missing_fields' });

  try {
    const result = await withDb(async (db) => {
      const exists = db.users.some((u) => u.email.toLowerCase() === String(email).toLowerCase());
      if (exists) return { status: 409, body: { error: 'email_exists' } };

      const now = new Date().toISOString();
      const user = {
        id: uuidv4(),
        username: String(username),
        email: String(email),
        passwordHash: await bcrypt.hash(String(password), 10),
        bio: '',
        avatarUrl: '',
        followers: [],
        following: [],
        pinnedPosts: [],
        createdAt: now,
        updatedAt: now,
      };
      db.users.push(user);

      const token = uuidv4();
      db.sessions.push({ token, userId: user.id, createdAt: now });

      return { status: 201, body: { token, user: pickPublicUser(user) } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/notifications', requireAuth, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const notifications = db.notifications
        .filter((n) => n.userId === req.user.id)
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));

      const unreadCount = notifications.filter((n) => !n.read).length;
      return { status: 200, body: { unreadCount, notifications } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/notifications/:id/read', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const n = db.notifications.find((x) => x.id === id && x.userId === req.user.id);
      if (!n) return { status: 404, body: { error: 'not_found' } };
      n.read = true;
      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/users', requireAuth, async (req, res) => {
  const q = (req.query.q ? String(req.query.q) : '').trim().toLowerCase();

  try {
    const result = await withDb(async (db) => {
      let users = db.users;
      if (q) {
        users = users.filter((u) => {
          const username = String(u.username || '').toLowerCase();
          const email = String(u.email || '').toLowerCase();
          return username.includes(q) || email.includes(q);
        });
      }

      users = users
        .slice()
        .sort((a, b) => (a.createdAt > b.createdAt ? 1 : -1))
        .map(pickPublicUser);

      return { status: 200, body: { users } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.patch('/me', requireAuth, async (req, res) => {
  const { username, bio, avatarUrl } = req.body || {};

  try {
    const result = await withDb(async (db) => {
      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 404, body: { error: 'not_found' } };

      if (typeof username === 'string' && username.trim()) me.username = username.trim();
      if (typeof bio === 'string') me.bio = bio;
      if (typeof avatarUrl === 'string') me.avatarUrl = avatarUrl;
      me.updatedAt = new Date().toISOString();

      return { status: 200, body: { user: pickPublicUser(me) } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/auth/login', async (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: 'missing_fields' });

  try {
    const result = await withDb(async (db) => {
      const user = db.users.find((u) => u.email.toLowerCase() === String(email).toLowerCase());
      if (!user) return { status: 401, body: { error: 'invalid_credentials' } };

      const ok = await bcrypt.compare(String(password), user.passwordHash);
      if (!ok) return { status: 401, body: { error: 'invalid_credentials' } };

      const token = uuidv4();
      db.sessions.push({ token, userId: user.id, createdAt: new Date().toISOString() });

      return { status: 200, body: { token, user: pickPublicUser(user) } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/auth/logout', requireAuth, async (req, res) => {
  try {
    await withDb(async (db) => {
      db.sessions = db.sessions.filter((s) => s.token !== req.token);
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/me', requireAuth, (req, res) => {
  res.json({ user: pickPublicUser(req.user) });
});

app.get('/users/:id', async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const user = db.users.find((u) => u.id === id);
      if (!user) return { status: 404, body: { error: 'not_found' } };
      return { status: 200, body: { user: pickPublicUser(user) } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/users/:id/follow', requireAuth, async (req, res) => {
  const targetId = req.params.id;
  if (targetId === req.user.id) return res.status(400).json({ error: 'cannot_follow_self' });

  try {
    const result = await withDb(async (db) => {
      const me = db.users.find((u) => u.id === req.user.id);
      const target = db.users.find((u) => u.id === targetId);
      if (!me || !target) return { status: 404, body: { error: 'not_found' } };

      const alreadyFollowing = me.following.includes(targetId);
      if (!alreadyFollowing) me.following.push(targetId);
      if (!target.followers.includes(me.id)) target.followers.push(me.id);
      me.updatedAt = new Date().toISOString();
      target.updatedAt = new Date().toISOString();

      if (!alreadyFollowing) {
        addNotification(db, {
          userId: target.id,
          type: 'follow',
          actorId: me.id,
          message: `${me.username} بدأ متابعتك`,
        });
      }

      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/users/:id/unfollow', requireAuth, async (req, res) => {
  const targetId = req.params.id;

  try {
    const result = await withDb(async (db) => {
      const me = db.users.find((u) => u.id === req.user.id);
      const target = db.users.find((u) => u.id === targetId);
      if (!me || !target) return { status: 404, body: { error: 'not_found' } };

      me.following = me.following.filter((x) => x !== targetId);
      target.followers = target.followers.filter((x) => x !== me.id);
      me.updatedAt = new Date().toISOString();
      target.updatedAt = new Date().toISOString();

      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/posts', async (req, res) => {
  const { userId, kind, tag } = req.query;

  try {
    const result = await withDb(async (db) => {
      let posts = db.posts;
      if (userId) posts = posts.filter((p) => p.userId === String(userId));
      if (kind) posts = posts.filter((p) => String(p.kind || 'post') === String(kind));
      if (tag) {
        const t = normalizeTag(tag);
        if (t) {
          posts = posts.filter((p) => {
            const hashtags = extractHashtags(p?.text || '');
            return hashtags.includes(t);
          });
        }
      }
      let pinnedOrder = [];
      if (userId) {
        const owner = db.users.find((u) => String(u.id) === String(userId));
        pinnedOrder = (Array.isArray(owner?.pinnedPosts) ? owner.pinnedPosts : []).map(String).filter(Boolean).slice(0, 3);
      }

      posts = posts.slice();
      if (pinnedOrder.length) {
        const rank = new Map(pinnedOrder.map((id, idx) => [String(id), idx]));
        posts.sort((a, b) => {
          const ar = rank.has(String(a.id)) ? rank.get(String(a.id)) : null;
          const br = rank.has(String(b.id)) ? rank.get(String(b.id)) : null;
          if (ar !== null && br !== null) return ar - br;
          if (ar !== null) return -1;
          if (br !== null) return 1;
          return a.createdAt < b.createdAt ? 1 : -1;
        });
      } else {
        posts.sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
      }

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      usersById.soundsById = soundsById;
      const pinnedSet = new Set(pinnedOrder.map(String));
      const postsWithAuthor = posts.map((p) => {
        const base = decoratePostForResponse(usersById, p);
        const slot = pinnedSet.has(String(p.id)) ? pinnedOrder.indexOf(String(p.id)) + 1 : 0;
        return slot ? { ...base, pinnedSlot: slot } : { ...base, pinnedSlot: 0 };
      });

      return { status: 200, body: { posts: postsWithAuthor } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/pin', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { slot } = req.body || {};
  const slotNum = Number(slot);

  try {
    const result = await withDb(async (db) => {
      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      if (!Array.isArray(me.pinnedPosts)) me.pinnedPosts = [];
      me.pinnedPosts = me.pinnedPosts.map(String).filter(Boolean);

      const post = db.posts.find((p) => String(p.id) === String(id));
      if (!post) return { status: 404, body: { error: 'not_found' } };
      if (String(post.userId) !== String(me.id)) return { status: 403, body: { error: 'forbidden' } };

      me.pinnedPosts = me.pinnedPosts.filter((pid) => String(pid) !== String(id));
      if (Number.isFinite(slotNum) && slotNum >= 1 && slotNum <= 3) {
        const idx = slotNum - 1;
        me.pinnedPosts.splice(Math.min(idx, me.pinnedPosts.length), 0, String(id));
      } else {
        me.pinnedPosts.push(String(id));
      }
      me.pinnedPosts = me.pinnedPosts.slice(0, 3);
      me.updatedAt = new Date().toISOString();
      return { status: 200, body: { pinnedPosts: me.pinnedPosts } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/unpin', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const result = await withDb(async (db) => {
      const me = db.users.find((u) => u.id === req.user.id);
      if (!me) return { status: 401, body: { error: 'invalid_session_user' } };
      if (!Array.isArray(me.pinnedPosts)) me.pinnedPosts = [];
      const before = me.pinnedPosts.length;
      me.pinnedPosts = me.pinnedPosts.map(String).filter(Boolean).filter((pid) => String(pid) !== String(id));
      me.pinnedPosts = me.pinnedPosts.slice(0, 3);
      if (me.pinnedPosts.length !== before) me.updatedAt = new Date().toISOString();
      return { status: 200, body: { pinnedPosts: me.pinnedPosts } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/reels', async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const posts = db.posts
        .filter((p) => String(p.kind || 'post') === 'reel')
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));

      // De-dupe quick duplicates (e.g. double submit) within a short window.
      const dedupeWindowMs = 5000;
      const lastSeen = new Map();
      const unique = [];
      for (const p of posts) {
        const url = String(p?.media?.[0]?.url || '');
        const sig = `${String(p.userId)}|reel|${String(p.text || '')}|${url}`;
        const t = Date.parse(p.createdAt || '') || 0;
        const prev = lastSeen.get(sig);
        if (typeof prev === 'number' && Math.abs(t - prev) <= dedupeWindowMs) continue;
        lastSeen.set(sig, t);
        unique.push(p);
      }

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      usersById.soundsById = soundsById;
      const reels = unique.map((p) => decoratePostForResponse(usersById, p));
      return { status: 200, body: { reels } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

function summarizeText(text, maxLength) {
  const clean = String(text || '').trim().replace(/\s+/g, ' ');
  if (!clean) return '';
  const len = Number.isFinite(maxLength) ? maxLength : 180;
  if (clean.length <= len) return clean;
  return `${clean.slice(0, Math.max(0, len - 1))}…`;
}

function rewriteText(text, style) {
  const clean = String(text || '').trim();
  if (!clean) return '';
  const s = String(style || 'neutral');
  if (s === 'formal') return `صياغة رسمية: ${clean}`;
  if (s === 'casual') return `صياغة ودّية: ${clean}`;
  if (s === 'short') return summarizeText(clean, 120);
  return clean;
}

function improveText(text) {
  const clean = String(text || '').trim().replace(/\s+/g, ' ');
  if (!clean) return '';
  const first = clean.charAt(0).toUpperCase() + clean.slice(1);
  return first;
}

app.post('/ai/post/summarize', requireAuth, async (req, res) => {
  const { text, maxLength } = req.body || {};
  if (!text) return res.status(400).json({ error: 'missing_text' });

  if (!GEMINI_API_KEY) return res.status(503).json({ error: 'gemini_not_configured' });
  try {
    const prompt = `لخّص النص التالي في حدود ${Number(maxLength) || 180} حرف وبشكل واضح:\n\n${String(text)}`;
    const summary = await callGemini(prompt);
    res.json({ summary });
  } catch (e) {
    res.status(502).json({ error: 'gemini_failed', details: String(e?.message || e) });
  }
});

app.post('/ai/post/rewrite', requireAuth, async (req, res) => {
  const { text, style } = req.body || {};
  if (!text) return res.status(400).json({ error: 'missing_text' });

  if (!GEMINI_API_KEY) return res.status(503).json({ error: 'gemini_not_configured' });
  try {
    const s = style ? String(style) : 'neutral';
    const prompt = `أعد صياغة النص التالي بأسلوب ${s} بدون تغيير المعنى:\n\n${String(text)}`;
    const rewritten = await callGemini(prompt);
    res.json({ rewritten });
  } catch (e) {
    res.status(502).json({ error: 'gemini_failed', details: String(e?.message || e) });
  }
});

app.post('/ai/post/improve', requireAuth, async (req, res) => {
  const { text } = req.body || {};
  if (!text) return res.status(400).json({ error: 'missing_text' });

  if (!GEMINI_API_KEY) return res.status(503).json({ error: 'gemini_not_configured' });
  try {
    const prompt = `حسّن اللغة والأسلوب للنص التالي وخليه طبيعي وواضح:\n\n${String(text)}`;
    const improved = await callGemini(prompt);
    res.json({ improved });
  } catch (e) {
    res.status(502).json({ error: 'gemini_failed', details: String(e?.message || e) });
  }
});

app.post('/ai/post/translate', requireAuth, async (req, res) => {
  const { text, targetLang } = req.body || {};
  if (!text) return res.status(400).json({ error: 'missing_text' });
  const lang = targetLang ? String(targetLang) : 'en';

  if (!GEMINI_API_KEY) return res.status(503).json({ error: 'gemini_not_configured' });
  try {
    const prompt = `ترجم النص التالي للغة ${lang} فقط بدون شرح:\n\n${String(text)}`;
    const translated = await callGemini(prompt);
    res.json({ translated });
  } catch (e) {
    res.status(502).json({ error: 'gemini_failed', details: String(e?.message || e) });
  }
});

app.get('/posts/:id', async (req, res, next) => {
  const { id } = req.params;
  if (id === 'saved') return next();

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      usersById.soundsById = soundsById;
      return { status: 200, body: { post: decoratePostForResponse(usersById, post) } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts', requireAuth, async (req, res) => {
  const { text, media, kind, soundId } = req.body || {};
  const postKind = kind ? String(kind) : 'post';
  if (postKind !== 'post' && postKind !== 'reel') {
    return res.status(400).json({ error: 'invalid_kind' });
  }

  const mediaArr = Array.isArray(media) ? media : [];

  if (postKind === 'reel') {
    if (mediaArr.length !== 1) return res.status(400).json({ error: 'reel_requires_single_video' });
    const type = String(mediaArr?.[0]?.type || '');
    const url = String(mediaArr?.[0]?.url || '');
    if (!type.startsWith('video') || !url) return res.status(400).json({ error: 'reel_requires_video' });
  }

  if (!text && (!Array.isArray(media) || media.length === 0)) {
    return res.status(400).json({ error: 'empty_post' });
  }

  try {
    const result = await withDb(async (db) => {
      const now = new Date().toISOString();

      const nextSoundId = soundId ? String(soundId) : '';
      if (nextSoundId) {
        const exists = (db.sounds || []).some((s) => s.id === nextSoundId);
        if (!exists) return { status: 400, body: { error: 'invalid_sound' } };
      }

      // Server-side dedupe for accidental double-submit (common on mobile).
      // Only within a short time window so legitimate reposts still work.
      const dedupeWindowMs = 5000;
      const nowMs = Date.now();
      const textSig = text ? String(text) : '';
      const mediaUrlSig = String(mediaArr?.[0]?.url || '');
      const dup = db.posts
        .slice()
        .reverse()
        .find((p) => {
          if (!p || typeof p !== 'object') return false;
          if (String(p.userId) !== String(req.user.id)) return false;
          if (String(p.kind || 'post') !== postKind) return false;
          if (String(p.text || '') !== textSig) return false;
          if (postKind === 'reel' && String(p?.media?.[0]?.url || '') !== mediaUrlSig) return false;
          const t = Date.parse(p.createdAt || '') || 0;
          return Math.abs(nowMs - t) <= dedupeWindowMs;
        });

      if (dup) return { status: 200, body: { post: dup, deduped: true } };

      const post = {
        id: uuidv4(),
        userId: req.user.id,
        kind: postKind,
        text: text ? String(text) : '',
        media: mediaArr,
        soundId: nextSoundId || undefined,
        likes: [],
        saves: [],
        sharesCount: 0,
        comments: [],
        createdAt: now,
        updatedAt: now,
      };
      db.posts.push(post);
      return { status: 201, body: { post } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.patch('/posts/:id', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { text, media } = req.body || {};

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      if (post.userId !== req.user.id) return { status: 403, body: { error: 'forbidden' } };

      if (typeof text === 'string') post.text = text;
      if (Array.isArray(media)) post.media = media;
      post.updatedAt = new Date().toISOString();

      return { status: 200, body: { post } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.delete('/posts/:id', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      if (post.userId !== req.user.id) return { status: 403, body: { error: 'forbidden' } };

      db.posts = db.posts.filter((p) => p.id !== id);
      return { status: 200, body: { ok: true } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/like', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };

      const alreadyLiked = post.likes.includes(req.user.id);
      if (!alreadyLiked) post.likes.push(req.user.id);
      post.updatedAt = new Date().toISOString();

      if (!alreadyLiked && post.userId !== req.user.id) {
        addNotification(db, {
          userId: post.userId,
          type: 'like',
          actorId: req.user.id,
          postId: post.id,
          message: `${req.user.username} عمل لايك على بوستك`,
        });
      }

      return { status: 200, body: { ok: true, likesCount: post.likes.length } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/posts/saved', requireAuth, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const posts = db.posts
        .filter((p) => Array.isArray(p.saves) && p.saves.includes(req.user.id))
        .slice()
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const soundsById = new Map((db.sounds || []).map((s) => [s.id, { id: s.id, title: s.title || '', url: s.url || '' }]));
      usersById.soundsById = soundsById;
      const postsWithAuthor = posts.map((p) => decoratePostForResponse(usersById, p));

      return { status: 200, body: { posts: postsWithAuthor } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/unlike', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      post.likes = post.likes.filter((x) => x !== req.user.id);
      post.updatedAt = new Date().toISOString();
      return { status: 200, body: { ok: true, likesCount: post.likes.length } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/save', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      if (!post.saves.includes(req.user.id)) post.saves.push(req.user.id);
      post.updatedAt = new Date().toISOString();
      return { status: 200, body: { ok: true, savesCount: post.saves.length } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/unsave', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      post.saves = post.saves.filter((x) => x !== req.user.id);
      post.updatedAt = new Date().toISOString();
      return { status: 200, body: { ok: true, savesCount: post.saves.length } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/share', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };
      post.sharesCount = Number(post.sharesCount || 0) + 1;
      post.updatedAt = new Date().toISOString();
      return { status: 200, body: { ok: true, sharesCount: post.sharesCount } };
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/posts/:id/comments', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { text } = req.body || {};
  if (!text) return res.status(400).json({ error: 'missing_text' });

  try {
    const result = await withDb(async (db) => {
      const post = db.posts.find((p) => p.id === id);
      if (!post) return { status: 404, body: { error: 'not_found' } };

      if (!Array.isArray(post.comments)) post.comments = [];

      const now = new Date().toISOString();
      const comment = { id: uuidv4(), userId: req.user.id, text: String(text), createdAt: now };
      post.comments.push(comment);
      post.updatedAt = now;

      const usersById = new Map(db.users.map((u) => [u.id, u]));
      const cu = usersById.get(comment.userId);
      const user = cu ? { id: cu.id, username: cu.username, avatarUrl: cu.avatarUrl } : null;

      return { status: 201, body: { comment: { ...comment, user } } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

function chatBelongsToUser(chat, userId) {
  return Array.isArray(chat.memberIds) && chat.memberIds.includes(userId);
}

function decorateChatForUser(db, chat, userId) {
  if (!chat || typeof chat !== 'object') return chat;

  if (chat.type === 'direct') {
    const otherId = (chat.memberIds || []).find((id) => id !== userId) || '';
    const other = db.users.find((u) => u.id === otherId);
    return {
      ...chat,
      otherUser: other ? { id: other.id, username: other.username, avatarUrl: other.avatarUrl } : null,
      displayTitle: other ? other.username : 'Direct',
    };
  }

  if (chat.type === 'ai') {
    return { ...chat, displayTitle: '🤖 AI' };
  }

  if (chat.type === 'group') {
    return { ...chat, displayTitle: chat.title || 'Group' };
  }

  return chat;
}

async function getOrCreateAiChat(db, userId) {
  const existing = db.chats.find((c) => c.type === 'ai' && chatBelongsToUser(c, userId));
  if (existing) return existing;

  const now = new Date().toISOString();
  const chat = {
    id: uuidv4(),
    type: 'ai',
    title: 'AI',
    memberIds: [userId],
    createdAt: now,
    lastMessageAt: now,
  };
  db.chats.push(chat);
  return chat;
}

app.get('/chats', requireAuth, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const chats = db.chats
        .filter((c) => chatBelongsToUser(c, req.user.id))
        .slice()
        .sort((a, b) => (a.lastMessageAt < b.lastMessageAt ? 1 : -1));

      const decorated = chats.map((c) => decorateChatForUser(db, c, req.user.id));
      return { status: 200, body: { chats: decorated } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/chats/direct', requireAuth, async (req, res) => {
  const { otherUserId } = req.body || {};
  if (!otherUserId) return res.status(400).json({ error: 'missing_otherUserId' });
  if (otherUserId === req.user.id) return res.status(400).json({ error: 'invalid_otherUserId' });

  try {
    const result = await withDb(async (db) => {
      const other = db.users.find((u) => u.id === String(otherUserId));
      if (!other) return { status: 404, body: { error: 'user_not_found' } };

      const membersKey = [req.user.id, other.id].sort().join(':');
      const existing = db.chats.find((c) => {
        if (c.type !== 'direct') return false;
        const key = (c.memberIds || []).slice().sort().join(':');
        return key === membersKey;
      });

      if (existing) return { status: 200, body: { chat: existing } };

      const now = new Date().toISOString();
      const chat = {
        id: uuidv4(),
        type: 'direct',
        memberIds: [req.user.id, other.id],
        createdAt: now,
        lastMessageAt: now,
      };
      db.chats.push(chat);
      return { status: 201, body: { chat } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/chats/group', requireAuth, async (req, res) => {
  const { title, memberIds } = req.body || {};
  if (!Array.isArray(memberIds) || memberIds.length < 2) {
    return res.status(400).json({ error: 'memberIds_min_2' });
  }

  try {
    const result = await withDb(async (db) => {
      const set = new Set([req.user.id, ...memberIds.map(String)]);
      const members = Array.from(set);

      const allExist = members.every((id) => db.users.some((u) => u.id === id));
      if (!allExist) return { status: 400, body: { error: 'invalid_memberIds' } };

      const now = new Date().toISOString();
      const chat = {
        id: uuidv4(),
        type: 'group',
        title: title ? String(title) : 'Group',
        memberIds: members,
        createdAt: now,
        lastMessageAt: now,
      };
      db.chats.push(chat);
      return { status: 201, body: { chat } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/chats/ai', requireAuth, async (req, res) => {
  try {
    const result = await withDb(async (db) => {
      const chat = await getOrCreateAiChat(db, req.user.id);
      return { status: 200, body: { chat } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.get('/chats/:id/messages', requireAuth, async (req, res) => {
  const { id } = req.params;

  try {
    const result = await withDb(async (db) => {
      const chat = db.chats.find((c) => c.id === id);
      if (!chat) return { status: 404, body: { error: 'chat_not_found' } };
      if (!chatBelongsToUser(chat, req.user.id)) return { status: 403, body: { error: 'forbidden' } };

      const messages = db.messages
        .filter((m) => m.chatId === id)
        .slice()
        .sort((a, b) => (a.createdAt > b.createdAt ? 1 : -1));

      const decoratedChat = decorateChatForUser(db, chat, req.user.id);
      return { status: 200, body: { chat: decoratedChat, messages } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/chats/:id/messages', requireAuth, async (req, res) => {
  const { id } = req.params;
  const { type, text, mediaUrl } = req.body || {};
  const msgType = type ? String(type) : 'text';
  if (msgType === 'text' && !text) return res.status(400).json({ error: 'missing_text' });
  if (msgType !== 'text' && !mediaUrl) return res.status(400).json({ error: 'missing_mediaUrl' });

  try {
    const result = await withDb(async (db) => {
      const chat = db.chats.find((c) => c.id === id);
      if (!chat) return { status: 404, body: { error: 'chat_not_found' } };
      if (!chatBelongsToUser(chat, req.user.id)) return { status: 403, body: { error: 'forbidden' } };

      const now = new Date().toISOString();
      const message = {
        id: uuidv4(),
        chatId: id,
        senderId: req.user.id,
        type: msgType,
        text: text ? String(text) : '',
        mediaUrl: mediaUrl ? String(mediaUrl) : '',
        createdAt: now,
      };

      db.messages.push(message);
      chat.lastMessageAt = now;

      const receivers = (chat.memberIds || []).filter((uid) => uid !== req.user.id);
      receivers.forEach((uid) => {
        addNotification(db, {
          userId: uid,
          type: 'message',
          actorId: req.user.id,
          chatId: chat.id,
          message: `${req.user.username} بعتلك رسالة`,
        });
      });

      return { status: 201, body: { message } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.post('/chats/ai/message', requireAuth, async (req, res) => {
  const { text } = req.body || {};
  if (!text) return res.status(400).json({ error: 'missing_text' });

  try {
    const result = await withDb(async (db) => {
      const chat = await getOrCreateAiChat(db, req.user.id);
      const now = new Date().toISOString();

      const userMessage = {
        id: uuidv4(),
        chatId: chat.id,
        senderId: req.user.id,
        type: 'text',
        text: String(text),
        mediaUrl: '',
        createdAt: now,
      };
      db.messages.push(userMessage);

      let aiText = '';
      try {
        const contextMessages = db.messages
          .filter((m) => m.chatId === chat.id)
          .slice(-12)
          .map((m) => {
            const role = m.senderId === 'ai' ? 'AI' : 'User';
            return `${role}: ${m.text || ''}`;
          })
          .join('\n');

        const prompt = `أنت مساعد داخل تطبيق تواصل اجتماعي اسمه NeuraConnect. ساعد المستخدم بشكل مختصر وواضح.\n\n${contextMessages}\n\nUser: ${String(
          text
        )}`;
        aiText = await callGemini(prompt);
      } catch (e) {
        const reason = String(e?.message || e || 'unknown_error');
        aiText = `Gemini فشل: ${reason}`;
      }

      const aiMessage = {
        id: uuidv4(),
        chatId: chat.id,
        senderId: 'ai',
        type: 'text',
        text: aiText,
        mediaUrl: '',
        createdAt: new Date().toISOString(),
      };
      db.messages.push(aiMessage);

      chat.lastMessageAt = aiMessage.createdAt;

      return { status: 201, body: { chatId: chat.id, userMessage, aiMessage } };
    });

    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({ error: 'server_error', details: String(err?.message || err) });
  }
});

app.use((req, res) => {
  if (req.method === 'GET') {
    const isApi =
      req.path === '/api/health' ||
      /^\/(auth|users|posts|chats|me|ai|notifications|admin|meetings|rtc)\b/.test(req.path);
    if (!isApi) {
      res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
      return;
    }
  }
  res.status(404).json({ error: 'not_found' });
});

function attachRealtimeServer(server) {
  const wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws, req) => {
    (async () => {
      try {
        const url = new URL(req.url || '', `http://${req.headers.host || 'localhost'}`);
        const token = String(url.searchParams.get('token') || '').trim();
        const meetingId = String(url.searchParams.get('meetingId') || '').trim();
        if (!token || !meetingId) {
          try {
            ws.close(1008, 'missing_params');
          } catch {
            // ignore
          }
          return;
        }

        const auth = await withDb(async (db) => {
          normalizeDb(db);
          const session = db.sessions.find((s) => s.token === token);
          if (!session) return { ok: false, error: 'invalid_token' };
          const user = db.users.find((u) => u.id === session.userId);
          if (!user) return { ok: false, error: 'invalid_session_user' };
          const meeting = (db.meetings || []).find((m) => m.id === String(meetingId));
          if (!meeting) return { ok: false, error: 'meeting_not_found' };
          if (meeting.endedAt) return { ok: false, error: 'meeting_ended' };
          return { ok: true, user: safeUserForMeeting(user), meeting };
        });

        if (!auth.ok) {
          try {
            ws.close(1008, String(auth.error || 'unauthorized'));
          } catch {
            // ignore
          }
          return;
        }

        const me = auth.user;
        const meeting = auth.meeting;
        const runtime = getMeetingRuntime(meetingId);
        if (!runtime) {
          try {
            ws.close(1011, 'runtime_error');
          } catch {
            // ignore
          }
          return;
        }

        const isHost = String(meeting.hostId) === String(me.id);
        const wasParticipant = Array.isArray(meeting.participantIds) && meeting.participantIds.includes(String(me.id));
        const approved = Boolean(isHost || wasParticipant);
        const client = { ws, user: me, userId: String(me.id), meetingId: String(meetingId), approved, isHost };
        runtime.clientsByUserId.set(client.userId, client);

        const send = (targetWs, payload) => {
          try {
            if (targetWs && targetWs.readyState === 1) targetWs.send(JSON.stringify(payload));
          } catch {
            // ignore
          }
        };

        const broadcastApproved = (payload, exceptUserId) => {
          runtime.clientsByUserId.forEach((c) => {
            if (!c.approved) return;
            if (exceptUserId && c.userId === exceptUserId) return;
            send(c.ws, payload);
          });
        };

        const approvedPeers = Array.from(runtime.clientsByUserId.values())
          .filter((c) => c.approved && c.userId !== client.userId)
          .map((c) => c.user);

        if (approved) {
          send(ws, {
            type: 'state',
            approved: true,
            isHost,
            me,
            meeting: { id: meeting.id, title: meeting.title, hostId: meeting.hostId },
            peers: approvedPeers,
            chatHistory: Array.isArray(runtime.chatHistory) ? runtime.chatHistory : [],
          });
          broadcastApproved({ type: 'peer_joined', peer: me }, client.userId);
        } else {
          send(ws, { type: 'state', approved: false, isHost: false, me, meeting: { id: meeting.id, title: meeting.title, hostId: meeting.hostId }, peers: [] });

          const hostClient = runtime.clientsByUserId.get(String(meeting.hostId));
          if (!hostClient || hostClient.ws.readyState !== 1) {
            send(ws, { type: 'error', error: 'host_offline' });
            try {
              ws.close(1013, 'host_offline');
            } catch {
              // ignore
            }
            return;
          }

          const requestId = uuidv4();
          runtime.pendingByRequestId.set(requestId, { requestId, user: me, userId: client.userId });
          send(hostClient.ws, { type: 'join_request', requestId, user: me });
        }

        ws.on('message', async (raw) => {
          let msg;
          try {
            msg = JSON.parse(String(raw || '{}'));
          } catch {
            return;
          }
          if (!msg || typeof msg !== 'object') return;

          const type = String(msg.type || '').trim();
          if (!type) return;

          if (type === 'chat_message') {
            if (!client.approved) return;
            const text = String(msg.text || '').trim();
            if (!text) return;
            if (text.length > 800) return;

            const clientId = String(msg.clientId || '').trim();
            if (clientId && clientId.length > 80) return;

            const message = {
              id: uuidv4(),
              clientId: clientId || undefined,
              user: me,
              userId: client.userId,
              text,
              createdAt: new Date().toISOString(),
            };

            if (!Array.isArray(runtime.chatHistory)) runtime.chatHistory = [];
            runtime.chatHistory.push(message);
            if (runtime.chatHistory.length > 60) runtime.chatHistory.splice(0, runtime.chatHistory.length - 60);

            broadcastApproved({ type: 'chat_message', message });
            return;
          }

          if (type === 'reaction') {
            if (!client.approved) return;
            const reaction = String(msg.reaction || '').trim();
            if (!reaction) return;
            if (reaction.length > 16) return;
            broadcastApproved({ type: 'reaction', from: client.userId, user: me, reaction, createdAt: new Date().toISOString() });
            return;
          }

          if (type === 'approve' || type === 'reject') {
            if (!client.isHost) return;
            const requestId = String(msg.requestId || '').trim();
            if (!requestId) return;
            const pending = runtime.pendingByRequestId.get(requestId);
            if (!pending) return;
            runtime.pendingByRequestId.delete(requestId);

            const target = runtime.clientsByUserId.get(String(pending.userId));
            if (!target) return;

            if (type === 'reject') {
              send(target.ws, { type: 'join_rejected' });
              try {
                target.ws.close(1008, 'rejected');
              } catch {
                // ignore
              }
              return;
            }

            target.approved = true;
            send(target.ws, {
              type: 'join_approved',
              peers: Array.from(runtime.clientsByUserId.values())
                .filter((c) => c.approved && c.userId !== target.userId)
                .map((c) => c.user),
              chatHistory: Array.isArray(runtime.chatHistory) ? runtime.chatHistory : [],
            });

            broadcastApproved({ type: 'peer_joined', peer: target.user }, target.userId);

            await withDb(async (db) => {
              normalizeDb(db);
              const meetingDb = (db.meetings || []).find((m) => m.id === String(meetingId));
              if (!meetingDb || meetingDb.endedAt) return;
              if (!Array.isArray(meetingDb.participantIds)) meetingDb.participantIds = [];
              if (!meetingDb.participantIds.includes(String(target.userId))) {
                meetingDb.participantIds.push(String(target.userId));
              }
            });
            return;
          }

          if (type === 'signal') {
            if (!client.approved) return;
            const to = String(msg.to || '').trim();
            if (!to) return;
            const target = runtime.clientsByUserId.get(to);
            if (!target || !target.approved) return;
            send(target.ws, { type: 'signal', from: client.userId, data: msg.data || {} });
          }
        });

        ws.on('close', async () => {
          runtime.clientsByUserId.delete(client.userId);

          if (client.approved) {
            broadcastApproved({ type: 'peer_left', peerId: client.userId }, client.userId);
          }

          if (client.isHost) {
            await withDb(async (db) => {
              normalizeDb(db);
              const meetingDb = (db.meetings || []).find((m) => m.id === String(meetingId));
              if (meetingDb && !meetingDb.endedAt) meetingDb.endedAt = new Date().toISOString();
            });
            runtime.clientsByUserId.forEach((c) => {
              try {
                send(c.ws, { type: 'meeting_ended' });
              } catch {
                // ignore
              }
              try {
                c.ws.close(1012, 'meeting_ended');
              } catch {
                // ignore
              }
            });
            runtime.clientsByUserId.clear();
            runtime.pendingByRequestId.clear();
            meetingsRuntime.delete(String(meetingId));
          }
        });
      } catch {
        try {
          ws.close(1011, 'server_error');
        } catch {
          // ignore
        }
      }
    })();
  });
}

async function startLocalServer() {
  await ensureStorageReady();
  const server = http.createServer(app);
  attachRealtimeServer(server);
  server.listen(PORT, () => {
    process.stdout.write(`NeuraConnect JSON Server listening on http://localhost:${PORT}\n`);
  });
}

if (require.main === module) {
  startLocalServer().catch((err) => {
    process.stderr.write(`Failed to start server: ${String(err?.message || err)}\n`);
    process.exit(1);
  });
}

module.exports = app;
