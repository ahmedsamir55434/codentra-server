# Codentra MongoDB VPS Deployment

الدليل ده خاص بالنسخة الموجودة في هذا المجلد، وهي نسخة Docker + MongoDB + Nginx.

## 1. رفع المشروع إلى السيرفر

```bash
scp -r codentra-vps-mongodb root@YOUR_VPS_IP:/root/
```

## 2. تثبيت المتطلبات على الـ VPS

```bash
ssh root@YOUR_VPS_IP
cd /root/codentra-vps-mongodb
chmod +x deploy.sh setup.sh
sudo ./deploy.sh
./setup.sh
```

النسخة النهائية سيتم وضعها في:

```bash
/opt/codentra-mongodb
```

## 3. تعديل البيئة قبل الإنتاج

حرر الملف:

```bash
nano /opt/codentra-mongodb/.env
```

راجع على الأقل:

- `APP_HTTP_PORT`
- `MONGO_ROOT_PASSWORD`
- `SESSION_SECRET`
- `JWT_SECRET`
- `COOKIE_SECURE=true` إذا كنت تنهي SSL أمام Nginx أو عبر Cloudflare/Proxy
- `CORS_ORIGIN` إذا كان عندك دومين أو تطبيق موبايل محدد

## 4. أوامر الإدارة

```bash
cd /opt/codentra-mongodb
docker compose ps
docker compose logs -f app
docker compose logs -f mongo
docker compose restart
docker compose down
docker compose up -d --build
```

## 5. النسخ الاحتياطي

MongoDB:

```bash
cd /opt/codentra-mongodb
docker compose exec mongo mongodump --username "$MONGO_ROOT_USERNAME" --password "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin --db "$MONGO_DATABASE" --out /data/db/dump
```

الملفات:

- `uploads/` محفوظة في volume باسم `uploads_data`
- snapshots من `data/` محفوظة في volume باسم `data_snapshots`

## 6. الصحة والمسارات

- التطبيق داخليًا على `app:3000`
- MongoDB داخليًا على `mongo:27017`
- Nginx يخرج على المنفذ المحدد في `APP_HTTP_PORT`
- Health check: `/health`

## 7. ملاحظات التشغيل

- أول تشغيل فقط: لو MongoDB فاضي سيتم استيراد البيانات الحالية من مجلد `data/`.
- جلسات المستخدمين محفوظة داخل MongoDB باستخدام `connect-mongo`.
- التطبيق مازال يحافظ على snapshots JSON داخل `data/` كنسخة احتياطية متزامنة.
