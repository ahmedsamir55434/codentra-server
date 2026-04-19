# NeuraConnect

Social platform built with Express and a static frontend. The app now supports bootstrapping its old JSON storage into Neon Postgres using a single `JSONB` row in `data_store`.

## What Changed

- Storage can run on `Neon Postgres` through `pg`
- The legacy local file `data/db.json` is used only as a first-run seed when the dataset row does not exist yet
- The dataset is stored in one table: `data_store(name TEXT PRIMARY KEY, data JSONB)`
- Vercel deployment is configured through `api/index.js` and `vercel.json`

## Local Run

1. Install dependencies
   ```bash
   npm install
   ```
2. Create your env file
   ```bash
   cp .env.example .env
   ```
3. Add at least `DATABASE_URL` if you want Neon locally, plus any optional Gemini/admin values
4. Start the app
   ```bash
   npm start
   ```
5. Open [http://localhost:4000](http://localhost:4000)

If `DATABASE_URL` is not set, the app falls back to a local JSON file at `data/db.json`.

## Neon Setup

1. Create a project in Neon
2. Copy the connection string that includes `sslmode=require`
3. Put that value in `DATABASE_URL`

On first request, the app checks row `db.json` inside `data_store`. If it does not exist yet, it seeds that row from the local JSON file when available, otherwise from an empty default dataset.

If you want to create and seed Neon before deployment, run:

```bash
DATABASE_URL="your-neon-url-with-sslmode=require" npm run init:neon
```

## Vercel Deploy From Scratch

1. Push this code to your GitHub repo:
   ```bash
   git init
   git remote add origin https://github.com/ahmedsamir55434/neuraconnect.git
   git add .
   git commit -m "Prepare NeuraConnect for Neon and Vercel"
   git branch -M main
   git push -u origin main
   ```
2. In Vercel, choose `Add New Project`
3. Import `ahmedsamir55434/neuraconnect`
4. Keep the root directory as the repo root
5. Add these environment variables in Project Settings:
   - `DATABASE_URL` = your Neon connection string
   - `TRUST_PROXY` = `true`
   - `GEMINI_API_KEY` = optional
   - `GEMINI_MODEL` = optional
   - `ADMIN_EMAILS` = optional
   - `SESSION_SECRET` = optional compatibility value
   - `JWT_SECRET` = optional compatibility value
   - `COOKIE_SECURE` = optional compatibility value, usually `true`
6. Remove `COOKIE_DOMAIN` if you had it in older deployments
7. Deploy

After deployment, verify:

- `GET /api/health` returns `storage: "postgres"`
- your first app request creates row `db.json` in `data_store`

## Important Vercel Limits In This Repo

- Meetings realtime uses a native WebSocket server on `/ws`. Standard Vercel Functions are not a fit for hosting that server, so the meetings realtime feature will not work there without a separate realtime service.
- `POST /sounds/upload` writes MP3 files to local disk. That is not durable on Vercel, so this endpoint now returns an explicit error there until you move uploads to Blob, S3, or similar storage.

## Environment Variables

- `DATABASE_URL`: Neon/Postgres connection string
- `TRUST_PROXY`: set to `true` on Vercel
- `GEMINI_API_KEY`: optional Gemini integration
- `GEMINI_MODEL`: optional, defaults to `gemini-2.0-flash`
- `PORT`: local only, defaults to `4000`
- `ADMIN_EMAILS`: optional comma-separated admin emails

## Project Structure

```text
├── api/index.js          # Vercel function entrypoint
├── public/               # Frontend assets
├── server.js             # Express app + local websocket server
├── vercel.json           # Rewrite all routes to the Express handler
├── .env.example          # Environment template
└── package.json
```
