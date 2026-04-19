const fs = require('fs/promises');
const path = require('path');

require('dotenv').config();

const { Pool } = require('pg');

const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const DB_PATH = path.join(__dirname, '..', 'data', 'db.json');
const DATASET_NAME = path.basename(DB_PATH);

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

async function loadSeedData() {
  try {
    const raw = await fs.readFile(DB_PATH, 'utf8');
    const data = JSON.parse(raw);
    if (!data || typeof data !== 'object') return createInitialDb();
    return data;
  } catch {
    return createInitialDb();
  }
}

async function main() {
  if (!DATABASE_URL) {
    throw new Error('DATABASE_URL is required');
  }

  const seedData = await loadSeedData();
  const pool = new Pool({
    connectionString: DATABASE_URL,
    max: 1,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
  });

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS data_store (
        name TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await pool.query(
      `
        INSERT INTO data_store (name, data, updated_at)
        VALUES ($1, $2::jsonb, NOW())
        ON CONFLICT (name)
        DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
      `,
      [DATASET_NAME, JSON.stringify(seedData)]
    );

    process.stdout.write(`Neon initialized successfully for dataset "${DATASET_NAME}"\n`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  process.stderr.write(`Failed to initialize Neon: ${String(err?.message || err)}\n`);
  process.exit(1);
});
