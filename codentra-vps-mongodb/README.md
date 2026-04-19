# Codentra MongoDB VPS Package

نسخة كاملة من التطبيق مجهزة للعمل على MongoDB مع Docker وNginx، ومناسبة للنشر على VPS.

## المحتوى

- `server.js`: سيرفر التطبيق بعد تحويل التخزين إلى MongoDB
- `db/mongo-state.js`: طبقة Mongo-backed cache مع استيراد أولي من `data/`
- `docker-compose.yml`: تشغيل `app + mongo + nginx`
- `Dockerfile`: بناء التطبيق للإنتاج
- `nginx.conf`: Reverse proxy مع WebSocket ودعم رفع ملفات كبير
- `deploy.sh`: تجهيز VPS وتثبيت Docker
- `setup.sh`: نسخ المشروع إلى `/opt/codentra-mongodb` وتشغيله
- `README-VPS.md`: خطوات النشر والإدارة بالتفصيل

## ملاحظات مهمة

- عند أول تشغيل، لو MongoDB فارغ، سيتم استيراد البيانات الموجودة في `data/` تلقائيًا.
- مجلد `uploads/` ولقطات `data/` محفوظة داخل Docker volumes.
- يوجد مستخدم أدمن افتراضي يتم إنشاؤه فقط لو لم يكن موجودًا:
  `admin@codentra.com / admin123`

## تشغيل سريع محلي

```bash
cp .env.example .env
docker compose up -d --build
```

بعد التشغيل:

- التطبيق: `http://localhost` أو `http://localhost:${APP_HTTP_PORT}`
- Health check: `http://localhost/health`

للنشر على VPS راجع `README-VPS.md`.
