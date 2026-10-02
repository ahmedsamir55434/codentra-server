# Codentra Server Architecture

This directory is the migration target for the current Express monolith.

The project should stay a modular monolith: one deployable app, split by
responsibility instead of feature code living directly in `server.js`.

Current first step:

- `config/env.js`: centralizes environment parsing and defaults.
- `config/paths.js`: centralizes runtime/data/upload paths for local and Vercel.
- `infrastructure/storage/fileStorage.js`: centralizes stored-path resolution and runtime directory setup.
- `modules/auth/apiAuth.js`: centralizes JWT generation/verification and API bearer-token middleware.
- `api/userRoutes.js`: starts moving `/api/*` route handlers out of `server.js`.
- `api/projectRoutes.js`: moves read-only project API routes out of `server.js`.
- `api/cartRoutes.js`: moves read-only cart API routes out of `server.js`.
- `api/accountRoutes.js`: moves account read APIs for purchases, notifications, loyalty, and live summary.
- `api/deviceRoutes.js`: moves small push-device register/unregister API mutations.
- `api/messageRoutes.js`: moves general and purchase message list/create APIs.
- `api/purchaseRoutes.js`: moves purchase download URL and token-download APIs.

Next extraction order:

1. `modules/auth`: session cookie auth and OAuth helpers.
2. `infrastructure/storage`: JSON/Postgres state store.
3. `modules/purchases`, `modules/wallet`, `modules/subscriptions`: money-sensitive domain logic.
4. More `api/*` route modules: separate mobile/API routes from browser pages.
5. `jobs`: background tasks such as abandoned cart and file-health reports.
