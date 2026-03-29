require('dotenv').config();

const express = require('express');
const session = require('express-session');
const cookieSession = require('cookie-session');
const multer = require('multer');
const bcrypt = require('bcryptjs');
const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const { Server } = require('socket.io');
const PDFDocument = require('pdfkit');
const https = require('https');
const nodemailer = require('nodemailer');
const jwt = require('jsonwebtoken');
const WatermarkProcessor = require('./utils/watermark');

const app = express();
const PORT = process.env.PORT || 3000;
const IS_VERCEL = Boolean(process.env.VERCEL || process.env.NOW_REGION);
const SOCKETS_ENABLED = !IS_VERCEL;
const SESSION_SECRET = process.env.SESSION_SECRET || 'codentra-secret-key-2024';
const JWT_SECRET = process.env.JWT_SECRET || 'codentra-jwt-secret-2024';
const TRUST_PROXY = ['1', 'true', 'yes'].includes(String(process.env.TRUST_PROXY || '').toLowerCase());
const COOKIE_SECURE = ['1', 'true', 'yes'].includes(String(process.env.COOKIE_SECURE || '').toLowerCase());
const COOKIE_DOMAIN = process.env.COOKIE_DOMAIN || undefined;
const SESSION_MAX_AGE_MS = 1000 * 60 * 60 * 24 * 7;
const USE_DATABASE = Boolean(process.env.DATABASE_URL);

// CORS middleware
app.use(async (req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  if (req.method === 'OPTIONS') {
    res.sendStatus(200);
  } else {
    next();
  }
});

app.use((req, res, next) => {
  res.locals.socketsEnabled = SOCKETS_ENABLED;
  next();
});

// Initialize watermark processor
const watermarkProcessor = new WatermarkProcessor();

const normalizeStoredPath = (storedPath) => {
  if (!storedPath) return null;
  if (typeof storedPath !== 'string') return null;
  return storedPath.startsWith('/') ? storedPath.slice(1) : storedPath;
};

const BUNDLED_DATA_DIR = path.join(__dirname, 'data');
const BUNDLED_UPLOADS_DIR = path.join(__dirname, 'uploads');
const RUNTIME_ROOT_DIR = IS_VERCEL ? path.join('/tmp', 'codentra-runtime') : __dirname;

const toAbsolutePath = (storedPath) => {
  const normalized = normalizeStoredPath(storedPath);
  if (!normalized) return null;

  if (normalized.startsWith('uploads/')) {
    const relativeUploadPath = normalized.slice('uploads/'.length);
    const runtimeCandidate = path.join(UPLOADS_DIR, relativeUploadPath);
    const bundledCandidate = path.join(BUNDLED_UPLOADS_DIR, relativeUploadPath);
    if (fs.existsSync(runtimeCandidate)) return runtimeCandidate;
    if (fs.existsSync(bundledCandidate)) return bundledCandidate;
    return runtimeCandidate;
  }

  return path.join(__dirname, normalized);
};

const formatMoney = (v) => {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return '0';
  return (Math.round(n * 100) / 100).toFixed(2);
};

const buildInvoiceNumber = () => {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const stamp = `${yyyy}${mm}${dd}`;
  return `INV-${stamp}-${String(Date.now()).slice(-6)}`;
};

const HIGH_VALUE_PAYMENT_THRESHOLD = 5000;
const PAYMENT_VERIFICATION_TTL_MS = 10 * 60 * 1000;
const PAYMENT_CODE_LENGTH = 6;
const SMTP_HOST = process.env.SMTP_HOST || '';
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_USER = process.env.SMTP_USER || '';
const SMTP_PASS = process.env.SMTP_PASS || '';
const SMTP_FROM = process.env.SMTP_FROM || SMTP_USER || '';
let smtpTransporter = null;

const normalizeWalletCardNumber = (value) => String(value || '').replace(/\D/g, '');

const formatWalletCardNumber = (value) => {
  const digits = normalizeWalletCardNumber(value);
  return digits.replace(/(\d{4})(?=\d)/g, '$1 ').trim();
};

const maskWalletCardNumber = (value) => {
  const digits = normalizeWalletCardNumber(value);
  if (digits.length < 4) return formatWalletCardNumber(digits);
  return `**** **** **** ${digits.slice(-4)}`;
};

const maskEmail = (email) => {
  if (!email || typeof email !== 'string' || !email.includes('@')) return 'البريد المسجل';
  const [localPart, domainPart] = email.split('@');
  if (!localPart || !domainPart) return 'البريد المسجل';
  const visibleLocal = localPart.length <= 2
    ? `${localPart[0] || '*'}*`
    : `${localPart.slice(0, 2)}${'*'.repeat(Math.max(1, localPart.length - 2))}`;
  const domainPieces = domainPart.split('.');
  const domainRoot = domainPieces.shift() || '';
  const domainTld = domainPieces.join('.');
  const visibleDomain = domainRoot.length <= 2
    ? `${domainRoot[0] || '*'}*`
    : `${domainRoot.slice(0, 2)}${'*'.repeat(Math.max(1, domainRoot.length - 2))}`;
  return `${visibleLocal}@${visibleDomain}${domainTld ? `.${domainTld}` : ''}`;
};

const createWalletCardNumber = (users, currentUserId) => {
  const used = new Set(
    (Array.isArray(users) ? users : [])
      .filter(u => u && u.id !== currentUserId)
      .map(u => normalizeWalletCardNumber(u.walletCardNumber))
      .filter(Boolean)
  );

  let cardNumber = '';
  do {
    cardNumber = `5274${String(Math.floor(Math.random() * 1e12)).padStart(12, '0')}`;
  } while (used.has(cardNumber));

  return cardNumber;
};

const generatePaymentVerificationCode = () => {
  const max = 10 ** PAYMENT_CODE_LENGTH;
  return String(Math.floor(Math.random() * max)).padStart(PAYMENT_CODE_LENGTH, '0');
};

const hashPaymentVerificationCode = (code) => {
  return crypto.createHash('sha256').update(String(code || '')).digest('hex');
};

const isPaymentAttemptExpired = (attempt) => {
  if (!attempt || !attempt.expiresAt) return true;
  const expiresAt = new Date(attempt.expiresAt);
  if (Number.isNaN(expiresAt.getTime())) return true;
  return expiresAt.getTime() <= Date.now();
};

const getMailTransporter = () => {
  if (!SMTP_HOST || !SMTP_FROM) return null;
  if (!smtpTransporter) {
    smtpTransporter = nodemailer.createTransport({
      host: SMTP_HOST,
      port: SMTP_PORT,
      secure: SMTP_PORT === 465,
      auth: SMTP_USER ? { user: SMTP_USER, pass: SMTP_PASS } : undefined
    });
  }
  return smtpTransporter;
};

const sendPaymentVerificationCode = async ({ user, buyerUser, amount, code, maskedCardNumber }) => {
  const transporter = getMailTransporter();
  if (!transporter || !user || !user.email) {
    return { delivered: false, reason: 'تعذر الوصول إلى بريد صاحب البطاقة أو إعدادات الإرسال غير مكتملة' };
  }

  try {
    await transporter.sendMail({
      from: SMTP_FROM,
      to: user.email,
      subject: 'Codentra payment verification code',
      text: [
        `Hello ${user.name || 'User'},`,
        '',
        `A Codentra payment request was created using your wallet card ${maskedCardNumber || ''}.`,
        `Buyer account: ${(buyerUser && (buyerUser.name || buyerUser.email || buyerUser.id)) || 'Unknown user'}`,
        `Your payment verification code is: ${code}`,
        `Amount: ${formatMoney(amount)} EGP`,
        `This code expires in ${Math.round(PAYMENT_VERIFICATION_TTL_MS / 60000)} minutes.`,
        '',
        'If you did not request this payment, ignore this message.'
      ].join('\n')
    });
    return { delivered: true };
  } catch (error) {
    return { delivered: false, error, reason: 'تعذر إرسال كود التحقق إلى بريد صاحب البطاقة' };
  }
};

const ensureUserPaymentProfile = ({ users, user }) => {
  if (!user || user.role !== 'user') return false;

  let changed = false;
  if (!user.referralCode) {
    user.referralCode = ensureUniqueReferralCode(users);
    changed = true;
  }
  if (user.walletBalance == null) {
    user.walletBalance = 0;
    changed = true;
  }
  if (user.referredBy === undefined) {
    user.referredBy = null;
    changed = true;
  }
  if (!normalizeWalletCardNumber(user.walletCardNumber)) {
    user.walletCardNumber = createWalletCardNumber(users, user.id);
    changed = true;
  }
  if (user.walletPaymentPasswordHash === undefined) {
    user.walletPaymentPasswordHash = null;
    changed = true;
  }
  return changed;
};

const buildSessionUser = ({ user, activeSubscription }) => {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    isSuperAdmin: Boolean(user.isSuperAdmin),
    adminPermissions: user.role === 'admin' ? (user.adminPermissions || null) : null,
    referralCode: user.referralCode || null,
    walletBalance: Number(user.walletBalance || 0),
    walletCardNumberMasked: user.role === 'user' ? maskWalletCardNumber(user.walletCardNumber) : null,
    hasWalletPaymentPassword: user.role === 'user' ? Boolean(user.walletPaymentPasswordHash) : false,
    loyaltyPoints: user.role === 'user' ? normalizeLoyaltyPoints(user.loyaltyPoints) : 0,
    subscription: activeSubscription ? {
      planId: activeSubscription.planId,
      status: activeSubscription.status,
      currentPeriodEnd: activeSubscription.currentPeriodEnd
    } : null
  };
};

const BUNDLED_ARABIC_FONT_PATH = path.join(__dirname, 'assets', 'fonts', 'NotoNaskhArabic-Regular.ttf');
const ARABIC_FONT_URL = process.env.ARABIC_FONT_URL || 'https://raw.githubusercontent.com/google/fonts/main/ofl/notonaskharabic/NotoNaskhArabic-Regular.ttf';
const ARABIC_FONT_PATH = path.join(RUNTIME_ROOT_DIR, 'fonts', 'NotoNaskhArabic-Regular.ttf');
const ARABIC_FONT_NAME = 'NotoNaskhArabic';
const ARABIC_CHAR_REGEX = /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]/;
const LATIN_CHAR_REGEX = /[A-Za-z]/;
let arabicFontPromise = null;

const downloadFile = (url, destination, redirects = 0) => new Promise((resolve, reject) => {
  https.get(url, (res) => {
    if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirects < 5) {
      res.resume();
      return resolve(downloadFile(res.headers.location, destination, redirects + 1));
    }
    if (res.statusCode !== 200) {
      res.resume();
      return reject(new Error(`Failed to download font (HTTP ${res.statusCode})`));
    }
    const file = fs.createWriteStream(destination);
    res.pipe(file);
    file.on('finish', () => file.close(() => resolve(destination)));
    file.on('error', reject);
  }).on('error', reject);
});

const ensureArabicFont = async () => {
  try {
    if (fs.existsSync(BUNDLED_ARABIC_FONT_PATH)) return BUNDLED_ARABIC_FONT_PATH;
    if (fs.existsSync(ARABIC_FONT_PATH)) return ARABIC_FONT_PATH;
    if (arabicFontPromise) return arabicFontPromise;
    arabicFontPromise = (async () => {
      fs.mkdirSync(path.dirname(ARABIC_FONT_PATH), { recursive: true });
      await downloadFile(ARABIC_FONT_URL, ARABIC_FONT_PATH);
      return ARABIC_FONT_PATH;
    })();
    return arabicFontPromise;
  } catch (e) {
    return null;
  }
};

const renderInvoicePdf = async ({ res, inv, items = [], includeEmail = false, includeCoupon = false }) => {
  const doc = new PDFDocument({ size: 'A4', margin: 50 });
  doc.pipe(res);

  const arabicFontPath = await ensureArabicFont();
  const hasArabicFont = Boolean(arabicFontPath);
  if (hasArabicFont) {
    try {
      doc.registerFont(ARABIC_FONT_NAME, arabicFontPath);
    } catch (e) {
      // ignore font registration issues and continue with default font
    }
  }

  const writeLine = (text, options = {}) => {
    const content = text === null || text === undefined ? '-' : String(text);
    const isArabic = hasArabicFont && ARABIC_CHAR_REGEX.test(content);
    const { size, ...rest } = options;
    if (size) doc.fontSize(size);
    doc.font(isArabic ? ARABIC_FONT_NAME : 'Helvetica');
    const align = rest.align || (isArabic ? 'right' : 'left');
    doc.text(content, { ...rest, align });
  };

  const writeValueLine = (value, options = {}) => {
    const content = value === null || value === undefined || value === '' ? '-' : String(value);
    const isArabicValue = hasArabicFont && ARABIC_CHAR_REGEX.test(content);
    const size = options.size || 11;
    doc.fontSize(size);
    doc.font(isArabicValue ? ARABIC_FONT_NAME : 'Helvetica');
    doc.text(content, { align: options.align || 'right' });
  };

  const writeMixedLeftLine = (value, options = {}) => {
    const content = value === null || value === undefined || value === '' ? '-' : String(value);
    const size = options.size || 11;
    const segments = content.match(/[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+|[^\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+/g) || [content];

    doc.fontSize(size);
    segments.forEach((segment, index) => {
      const isArabicSegment = hasArabicFont && ARABIC_CHAR_REGEX.test(segment);
      doc.font(isArabicSegment ? ARABIC_FONT_NAME : 'Helvetica');
      doc.text(segment, {
        align: 'left',
        continued: index < segments.length - 1
      });
    });
  };

  const writeBilingualField = (arabicLabel, englishLabel, value, options = {}) => {
    const content = value === null || value === undefined || value === '' ? '-' : String(value);
    const arabicValue = options.arabicValue === null || options.arabicValue === undefined || options.arabicValue === ''
      ? content
      : String(options.arabicValue);
    const englishValue = options.englishValue === null || options.englishValue === undefined || options.englishValue === ''
      ? content
      : String(options.englishValue);
    const size = options.size || 11;
    const gap = options.gap == null ? 0.2 : options.gap;

    writeLine(arabicLabel, { size, align: 'right' });
    writeValueLine(arabicValue, { size, align: 'right' });
    writeLine(englishLabel, { size, align: 'left' });
    writeMixedLeftLine(englishValue, { size });
    doc.moveDown(gap);
  };

  const invoiceItems = Array.isArray(items) && items.length
    ? items
    : (await db.purchases())
        .filter(p => p && (p.orderId === inv.orderId || p.id === inv.orderId))
        .map(p => ({
          projectTitle: p.projectTitle || p.projectId || 'Project',
          projectId: p.projectId || null,
          priceBefore: p.priceBefore != null ? p.priceBefore : p.price,
          discountAmount: p.discountAmount != null ? p.discountAmount : 0,
          priceAfter: p.priceAfter != null ? p.priceAfter : p.price,
          price: p.price
        }));

  writeLine('Codentra', { size: 20, align: 'center' });
  writeLine('فاتورة', { size: 16, align: 'center' });
  writeLine('Invoice', { size: 16, align: 'center' });
  doc.moveDown(1);

  writeBilingualField('رقم الفاتورة', 'Invoice No', inv.invoiceNumber || '-');
  writeBilingualField('التاريخ', 'Date', inv.createdAt ? new Date(inv.createdAt).toLocaleString('en-GB') : '-');
  writeBilingualField('اسم العميل', 'Customer', inv.userName || '-');
  if (includeEmail && inv.userEmail) {
    writeBilingualField('البريد الإلكتروني', 'Email', inv.userEmail);
  }
  writeBilingualField('رقم الطلب', 'Order ID', inv.orderId || '-');
  if (includeCoupon && inv.couponCode) {
    writeBilingualField('كود الخصم', 'Coupon', inv.couponCode);
  }

  doc.moveDown(0.5);
  writeLine('العناصر', { size: 14, underline: true, align: 'center' });
  writeLine('Items', { size: 14, underline: true, align: 'center' });
  doc.moveDown(0.5);

  if (!invoiceItems.length) {
    writeLine('لا توجد عناصر في هذه الفاتورة', { size: 11, align: 'right' });
    writeLine('No items available for this invoice', { size: 11, align: 'left' });
  } else {
    invoiceItems.forEach((it, idx) => {
      const title = it.projectTitle || it.projectId || 'Project';
      const isMixedTitle = ARABIC_CHAR_REGEX.test(title) && LATIN_CHAR_REGEX.test(title);
      writeLine(`العنصر رقم ${idx + 1}`, { size: 11, align: 'right' });
      if (!isMixedTitle) {
        writeValueLine(title, { size: 11, align: 'right' });
      }
      writeLine(`Item ${idx + 1}`, { size: 11, align: 'left' });
      writeMixedLeftLine(title, { size: 11 });

      if (typeof it.priceBefore !== 'undefined') {
        writeLine(
          `قبل الخصم: ${formatMoney(it.priceBefore)} جنيه | الخصم: ${formatMoney(it.discountAmount)} جنيه | بعد الخصم: ${formatMoney(it.priceAfter)} جنيه`,
          { size: 10, align: 'right' }
        );
        writeLine(
          `Before: ${formatMoney(it.priceBefore)} EGP | Discount: ${formatMoney(it.discountAmount)} EGP | After: ${formatMoney(it.priceAfter)} EGP`,
          { size: 10, align: 'left' }
        );
        doc.moveDown(0.3);
        return;
      }

      if (typeof it.price !== 'undefined') {
        writeLine(`السعر: ${formatMoney(it.price)} جنيه`, { size: 10, align: 'right' });
        writeLine(`Price: ${formatMoney(it.price)} EGP`, { size: 10, align: 'left' });
        doc.moveDown(0.3);
      }
    });
  }

  doc.moveDown(0.8);
  writeLine('الملخص', { size: 14, underline: true, align: 'center' });
  writeLine('Summary', { size: 14, underline: true, align: 'center' });
  doc.moveDown(0.5);
  writeBilingualField('الإجمالي قبل الخصم', 'Total Before', `${formatMoney(inv.totalBefore || 0)} جنيه`, {
    englishValue: `${formatMoney(inv.totalBefore || 0)} EGP`
  });
  writeBilingualField('إجمالي الخصم', 'Total Discount', `${formatMoney(inv.totalDiscount || 0)} جنيه`, {
    englishValue: `${formatMoney(inv.totalDiscount || 0)} EGP`
  });
  writeBilingualField('الإجمالي بعد الخصم', 'Total After', `${formatMoney(inv.totalAfter || 0)} جنيه`, {
    englishValue: `${formatMoney(inv.totalAfter || 0)} EGP`,
    size: 13
  });

  doc.end();
};

// JWT Helpers
const JWT_EXPIRES_IN = '7d';

const generateToken = (user) => {
  const payload = {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role
  };
  return jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
};

const verifyToken = (token) => {
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (e) {
    return null;
  }
};

const requireApiUserAuth = async (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid token' });
  }
  const token = authHeader.split(' ')[1];
  const decoded = verifyToken(token);
  if (!decoded) {
    return res.status(401).json({ error: 'Invalid token' });
  }
  if (decoded.role !== 'user') {
    return res.status(403).json({ error: 'API for users only' });
  }
  req.apiUser = decoded;
  next();
};

const calculateRefundForRejectedItem = ({ rejectedPurchase, allPurchases }) => {
  // For single-item purchases, refund its own walletDebitAmount (if set)
  if (!rejectedPurchase.orderId) {
    return Number(rejectedPurchase.walletDebitAmount || 0);
  }

  // For multi-item orders (cart), distribute total debit proportionally
  const orderPurchases = allPurchases.filter(p => p && p.orderId === rejectedPurchase.orderId);
  if (orderPurchases.length <= 1) {
    return Number(rejectedPurchase.walletDebitAmount || 0);
  }

  const totalOrderDebit = orderPurchases.reduce((sum, p) => sum + Number(p.walletDebitAmount || 0), 0);
  if (totalOrderDebit <= 0) return 0;

  const rejectedPrice = Number(rejectedPurchase.price || 0);
  const totalOrderPrice = orderPurchases.reduce((sum, p) => sum + Number(p.price || 0), 0);
  if (totalOrderPrice <= 0) return 0;

  const proportion = rejectedPrice / totalOrderPrice;
  return Math.round((totalOrderDebit * proportion) * 100) / 100;
};

// Data storage paths
const DATA_DIR = path.join(RUNTIME_ROOT_DIR, 'data');
const UPLOADS_DIR = path.join(RUNTIME_ROOT_DIR, 'uploads');
const MEETING_RECORDINGS_DIR = path.join(UPLOADS_DIR, 'meeting-recordings');
const ADMIN_TEAM_UPLOADS_DIR = path.join(UPLOADS_DIR, 'admin-team');

// Ensure directories exist
[DATA_DIR, UPLOADS_DIR, MEETING_RECORDINGS_DIR, ADMIN_TEAM_UPLOADS_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const seedRuntimeData = () => {
  if (!IS_VERCEL) return;

  const seedFiles = [
    'users.json',
    'projects.json',
    'purchases.json',
    'modifications.json',
    'messages.json',
    'admin-team-messages.json',
    'carts.json',
    'invoices.json',
    'reviews.json',
    'coupons.json',
    'referrals.json',
    'wallet-codes.json',
    'appointments.json',
    'meeting-recordings.json',
    'wallet-payment-attempts.json',
    'subscription-plans.json',
    'subscriptions.json',
    'subscription-payments.json',
    'subscription-coupons.json',
    'loyalty-settings.json'
  ];

  seedFiles.forEach((file) => {
    const runtimePath = path.join(DATA_DIR, file);
    if (fs.existsSync(runtimePath)) return;
    const bundledPath = path.join(BUNDLED_DATA_DIR, file);
    if (fs.existsSync(bundledPath)) {
      fs.copyFileSync(bundledPath, runtimePath);
    }
  });
};

seedRuntimeData();

// JSON storage helpers
const JSON_CACHE = new Map();
let dbPool;
let dbInitPromise = null;

const getPool = () => {
  if (!dbPool) {
    const sslRequired = String(process.env.DATABASE_URL || '').includes('sslmode=require');
    dbPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: sslRequired ? { rejectUnauthorized: false } : undefined,
      max: 2,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 5000
    });
  }
  return dbPool;
};

const cloneJson = (value) => {
  if (value === null || value === undefined) return value;
  return JSON.parse(JSON.stringify(value));
};

const readJsonFile = (fileName, fallback) => {
  const filePath = path.join(DATA_DIR, fileName);
  if (JSON_CACHE.has(filePath)) {
    return cloneJson(JSON_CACHE.get(filePath));
  }

  let data;
  if (!fs.existsSync(filePath)) {
    data = typeof fallback === 'function' ? fallback() : fallback;
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  } else {
    const raw = fs.readFileSync(filePath, 'utf8');
    data = raw ? JSON.parse(raw) : (typeof fallback === 'function' ? fallback() : fallback);
  }

  JSON_CACHE.set(filePath, data);
  return cloneJson(data);
};

const writeJsonFile = (fileName, data) => {
  const filePath = path.join(DATA_DIR, fileName);
  JSON_CACHE.set(filePath, data);
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
};

const DATASET_DEFS = [
  { name: 'users', file: 'users.json', fallback: [] },
  { name: 'projects', file: 'projects.json', fallback: [] },
  { name: 'purchases', file: 'purchases.json', fallback: [] },
  { name: 'modifications', file: 'modifications.json', fallback: [] },
  { name: 'coupons', file: 'coupons.json', fallback: [] },
  { name: 'referrals', file: 'referrals.json', fallback: [] },
  { name: 'walletCodes', file: 'wallet-codes.json', fallback: [] },
  { name: 'appointments', file: 'appointments.json', fallback: () => ({ timeSlots: [], bookings: [] }) },
  { name: 'reviews', file: 'reviews.json', fallback: [] },
  { name: 'messages', file: 'messages.json', fallback: [] },
  { name: 'notifications', file: 'notifications.json', fallback: [] },
  { name: 'adminTeamMessages', file: 'admin-team-messages.json', fallback: [] },
  { name: 'carts', file: 'carts.json', fallback: [] },
  { name: 'invoices', file: 'invoices.json', fallback: [] },
  { name: 'meetingRecordings', file: 'meeting-recordings.json', fallback: [] },
  { name: 'walletPaymentAttempts', file: 'wallet-payment-attempts.json', fallback: [] },
  { name: 'subscriptionPlans', file: 'subscription-plans.json', fallback: [] },
  { name: 'subscriptions', file: 'subscriptions.json', fallback: [] },
  { name: 'subscriptionPayments', file: 'subscription-payments.json', fallback: [] },
  { name: 'subscriptionCoupons', file: 'subscription-coupons.json', fallback: [] },
  {
    name: 'loyaltySettings',
    file: 'loyalty-settings.json',
    fallback: () => ({ enabled: true, pointsPerEGP: 0.1, redeem: { enabled: true, pointsToEGP: 0.1, minPoints: 100 } })
  }
];

const DATASET_MAP = new Map(DATASET_DEFS.map((def) => [def.name, def]));

const ensureDatabase = async () => {
  if (!USE_DATABASE) return;
  if (dbInitPromise) return dbInitPromise;
  dbInitPromise = (async () => {
    const pool = getPool();
    await pool.query(
      'CREATE TABLE IF NOT EXISTS data_store (name TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())'
    );
    await pool.query(
      'CREATE TABLE IF NOT EXISTS meeting_signals (id BIGSERIAL PRIMARY KEY, room_id TEXT NOT NULL, sender_id TEXT, payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())'
    );
    await pool.query(
      'CREATE INDEX IF NOT EXISTS meeting_signals_room_id_idx ON meeting_signals (room_id, id)'
    );

    // Table exists; seeding happens lazily per dataset in readData/writeData.
  })();
  return dbInitPromise;
};

const readData = async (name) => {
  const def = DATASET_MAP.get(name);
  if (!def) throw new Error(`Unknown dataset: ${name}`);
  if (!USE_DATABASE) {
    return readJsonFile(def.file, def.fallback);
  }

  await ensureDatabase();
  const pool = getPool();
  const res = await pool.query('SELECT data FROM data_store WHERE name = $1', [name]);
  if (res.rowCount === 0) {
    let seedData = readJsonFile(def.file, def.fallback);
    if (name === 'users' && Array.isArray(seedData) && !seedData.find(u => u && u.email === 'admin@codentra.com')) {
      seedData = [...seedData];
      seedData.push({
        id: uuidv4(),
        name: 'Admin',
        email: 'admin@codentra.com',
        password: bcrypt.hashSync('admin123', 10),
        role: 'admin',
        isSuperAdmin: true,
        createdAt: new Date().toISOString()
      });
    }
    await pool.query('INSERT INTO data_store (name, data) VALUES ($1, $2::jsonb)', [name, JSON.stringify(seedData)]);
    return cloneJson(seedData);
  }

  return cloneJson(res.rows[0].data);
};

const writeData = async (name, data) => {
  const def = DATASET_MAP.get(name);
  if (!def) throw new Error(`Unknown dataset: ${name}`);
  if (!USE_DATABASE) {
    writeJsonFile(def.file, data);
    return;
  }

  await ensureDatabase();
  const pool = getPool();
  await pool.query(
    'INSERT INTO data_store (name, data, updated_at) VALUES ($1, $2::jsonb, NOW()) ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()',
    [name, JSON.stringify(data)]
  );
};

const db = {
  users: () => readData('users'),
  projects: () => readData('projects'),
  purchases: () => readData('purchases'),
  modifications: () => readData('modifications'),
  coupons: () => readData('coupons'),
  referrals: () => readData('referrals'),
  walletCodes: () => readData('walletCodes'),
  appointments: () => readData('appointments'),
  saveUsers: (data) => writeData('users', data),
  saveProjects: (data) => writeData('projects', data),
  savePurchases: (data) => writeData('purchases', data),
  saveModifications: (data) => writeData('modifications', data),
  saveCoupons: (data) => writeData('coupons', data),
  saveReferrals: (data) => writeData('referrals', data),
  saveWalletCodes: (data) => writeData('walletCodes', data),
  reviews: () => readData('reviews'),
  saveReviews: (data) => writeData('reviews', data),
  messages: () => readData('messages'),
  saveMessages: (data) => writeData('messages', data),
  notifications: () => readData('notifications'),
  saveNotifications: (data) => writeData('notifications', data),
  adminTeamMessages: () => readData('adminTeamMessages'),
  saveAdminTeamMessages: (data) => writeData('adminTeamMessages', data),
  carts: () => readData('carts'),
  saveCarts: (data) => writeData('carts', data),
  invoices: () => readData('invoices'),
  saveInvoices: (data) => writeData('invoices', data),
  saveAppointments: (data) => writeData('appointments', data),
  meetingRecordings: () => readData('meetingRecordings'),
  saveMeetingRecordings: (data) => writeData('meetingRecordings', data),
  walletPaymentAttempts: () => readData('walletPaymentAttempts'),
  saveWalletPaymentAttempts: (data) => writeData('walletPaymentAttempts', data),
  subscriptionPlans: () => readData('subscriptionPlans'),
  subscriptions: () => readData('subscriptions'),
  subscriptionPayments: () => readData('subscriptionPayments'),
  saveSubscriptionPlans: (data) => writeData('subscriptionPlans', data),
  saveSubscriptions: (data) => writeData('subscriptions', data),
  saveSubscriptionPayments: (data) => writeData('subscriptionPayments', data),
  subscriptionCoupons: () => readData('subscriptionCoupons'),
  saveSubscriptionCoupons: (data) => writeData('subscriptionCoupons', data),
  loyaltySettings: () => readData('loyaltySettings'),
  saveLoyaltySettings: (data) => writeData('loyaltySettings', data)
};

const getUserNotifications = async ({ userId, limit = null }) => {
  if (!userId) return [];
  const notificationsRaw = await db.notifications();
  const notifications = Array.isArray(notificationsRaw) ? notificationsRaw : [];
  const sorted = notifications
    .filter(notification => notification && notification.userId === userId)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  if (limit && Number.isFinite(Number(limit)) && Number(limit) > 0) {
    return sorted.slice(0, Number(limit));
  }
  return sorted;
};

const getUnreadNotificationCount = async ({ userId }) => {
  const notifications = await getUserNotifications({ userId });
  return notifications.filter(notification => !notification.readAt).length;
};

const createNotification = async ({ userId, title, message, link = null, kind = 'info', meta = null }) => {
  if (!userId || !title || !message) return null;
  const notificationsRaw = await db.notifications();
  const notifications = Array.isArray(notificationsRaw) ? notificationsRaw : [];

  notifications.push({
    id: uuidv4(),
    userId,
    title: String(title).trim(),
    message: String(message).trim(),
    link: link || null,
    kind: kind || 'info',
    meta: meta || null,
    readAt: null,
    createdAt: new Date().toISOString()
  });

  const trimmedNotifications = notifications
    .sort((a, b) => new Date(a.createdAt || 0).getTime() - new Date(b.createdAt || 0).getTime())
    .slice(-1000);

  await db.saveNotifications(trimmedNotifications);
  return trimmedNotifications[trimmedNotifications.length - 1] || null;
};

const serializeNotification = (notification) => {
  if (!notification) return null;
  return {
    id: notification.id,
    title: notification.title || '',
    message: notification.message || '',
    link: notification.link || null,
    kind: notification.kind || 'info',
    readAt: notification.readAt || null,
    createdAt: notification.createdAt || null
  };
};

const markNotificationRead = async ({ userId, notificationId }) => {
  if (!userId || !notificationId) return false;
  const notificationsRaw = await db.notifications();
  const notifications = Array.isArray(notificationsRaw) ? notificationsRaw : [];
  const index = notifications.findIndex(notification => notification && notification.id === notificationId && notification.userId === userId);
  if (index === -1) return false;
  if (!notifications[index].readAt) {
    notifications[index].readAt = new Date().toISOString();
    await db.saveNotifications(notifications);
  }
  return true;
};

const markAllNotificationsRead = async ({ userId }) => {
  if (!userId) return 0;
  const notificationsRaw = await db.notifications();
  const notifications = Array.isArray(notificationsRaw) ? notificationsRaw : [];
  let updated = 0;
  notifications.forEach(notification => {
    if (notification && notification.userId === userId && !notification.readAt) {
      notification.readAt = new Date().toISOString();
      updated += 1;
    }
  });
  if (updated > 0) {
    await db.saveNotifications(notifications);
  }
  return updated;
};

const normalizeCouponCode = (code) => {
  if (!code || typeof code !== 'string') return '';
  return code.trim().toUpperCase();
};

const getSubscriptionCouponEligibility = (coupon) => {
  if (!coupon) return { eligible: false, reason: 'كوبون غير صحيح' };
  if (!coupon.active) return { eligible: false, reason: 'الكوبون غير فعّال' };

  if (coupon.expiresAt) {
    const expires = new Date(coupon.expiresAt);
    if (!Number.isNaN(expires.getTime()) && new Date() > expires) {
      return { eligible: false, reason: 'الكوبون منتهي' };
    }
  }

  const used = Number(coupon.usedCount || 0);
  const limit = coupon.usageLimit != null ? Number(coupon.usageLimit) : null;
  if (Number.isFinite(limit) && limit > 0 && used >= limit) {
    return { eligible: false, reason: 'تم الوصول للحد الأقصى لاستخدام الكوبون' };
  }

  return { eligible: true, reason: null };
};

const calculateSubscriptionCouponDiscount = ({ priceBefore, coupon }) => {
  const base = Number(priceBefore || 0);
  if (!coupon) return { discountAmount: 0, priceAfter: base };

  const type = coupon.type;
  const value = Number(coupon.value || 0);

  let discount = 0;
  if (type === 'percent') {
    discount = Math.round((base * (value / 100)) * 100) / 100;
  } else if (type === 'fixed') {
    discount = value;
  }

  if (!Number.isFinite(discount) || discount < 0) discount = 0;
  if (discount > base) discount = base;

  const after = Math.round((base - discount) * 100) / 100;
  return { discountAmount: discount, priceAfter: after };
};

const normalizeLoyaltyPoints = (value) => {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
};

const getLoyaltyEarnedPointsForPurchase = async ({ amountEGP }) => {
  const settings = await db.loyaltySettings();
  if (!settings || !settings.enabled) return 0;
  const rate = Number(settings.pointsPerEGP || 0);
  if (!Number.isFinite(rate) || rate <= 0) return 0;
  const amt = Number(amountEGP || 0);
  if (!Number.isFinite(amt) || amt <= 0) return 0;
  return Math.floor(amt * rate);
};

const parseOptionalIsoDate = (value) => {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
};

const getCouponEligibility = (coupon) => {
  if (!coupon) return { eligible: false, reason: 'كوبون غير صحيح' };
  if (!coupon.active) return { eligible: false, reason: 'الكوبون غير فعّال' };

  if (coupon.expiresAt) {
    const expires = new Date(coupon.expiresAt);
    if (!Number.isNaN(expires.getTime()) && new Date() > expires) {
      return { eligible: false, reason: 'الكوبون منتهي' };
    }
  }

  const used = Number(coupon.usedCount || 0);
  const limit = coupon.usageLimit != null ? Number(coupon.usageLimit) : null;
  if (Number.isFinite(limit) && limit > 0 && used >= limit) {
    return { eligible: false, reason: 'تم الوصول للحد الأقصى لاستخدام الكوبون' };
  }

  return { eligible: true, reason: null };
};

const getActiveSubscriptionForUser = async ({ userId }) => {
  if (!userId) return null;
  const subs = await db.subscriptions();
  const now = new Date();
  const active = subs
    .filter(s => s && s.userId === userId && s.status === 'active' && s.currentPeriodEnd)
    .filter(s => {
      const end = new Date(s.currentPeriodEnd);
      return !Number.isNaN(end.getTime()) && end > now;
    })
    .sort((a, b) => new Date(b.currentPeriodEnd).getTime() - new Date(a.currentPeriodEnd).getTime());
  return active[0] || null;
};

const getSubscriptionPlanById = async ({ planId }) => {
  if (!planId) return null;
  const plans = await db.subscriptionPlans();
  return plans.find(p => p && p.id === planId && p.active) || null;
};

const getUserSubscriptionTier = ({ sessionUser }) => {
  if (!sessionUser || sessionUser.role !== 'user') return 'none';
  const sub = sessionUser.subscription;
  if (!sub || sub.status !== 'active') return 'none';
  if (sub.planId === 'premium') return 'premium';
  if (sub.planId === 'basic') return 'basic';
  return 'none';
};

const isProjectVisibleToUser = ({ project, sessionUser }) => {
  if (!project) return false;
  const visibility = project.visibility || 'public';
  if (visibility === 'public') return true;

  const tier = getUserSubscriptionTier({ sessionUser });
  if (visibility === 'basic') return tier === 'basic' || tier === 'premium';
  if (visibility === 'premium') return tier === 'premium';
  return true;
};

const getSubscriberDiscountPercent = ({ sessionUser }) => {
  const tier = getUserSubscriptionTier({ sessionUser });
  if (tier === 'premium') return 20;
  if (tier === 'basic') return 10;
  return 0;
};

const getOrCreateCartForUser = async ({ userId }) => {
  const carts = await db.carts();
  const idx = carts.findIndex(c => c && c.userId === userId);
  if (idx !== -1) {
    const cart = carts[idx];
    if (!cart.items || !Array.isArray(cart.items)) cart.items = [];
    return { carts, cart, cartIndex: idx };
  }
  const cart = { userId, items: [], updatedAt: new Date().toISOString() };
  carts.push(cart);
  return { carts, cart, cartIndex: carts.length - 1 };
};

const summarizeCart = async ({ cart, couponCode, sessionUser }) => {
  const items = (cart && Array.isArray(cart.items)) ? cart.items : [];
  const totalBefore = Math.round(items.reduce((sum, it) => sum + Number(it.price || 0), 0) * 100) / 100;

  let appliedCoupon = null;
  let couponDiscount = 0;
  if (couponCode) {
    const normalized = normalizeCouponCode(couponCode);
    const coupons = await db.coupons();
    const coupon = coupons.find(c => normalizeCouponCode(c.code) === normalized) || null;
    const eligibility = getCouponEligibility(coupon);
    if (eligibility.eligible) {
      appliedCoupon = coupon;
      const calc = calculateDiscount({ priceBefore: totalBefore, coupon });
      couponDiscount = Math.round(Number(calc.discountAmount || 0) * 100) / 100;
    }
  }

  const afterCoupon = Math.round((totalBefore - couponDiscount) * 100) / 100;
  const subPercent = getSubscriberDiscountPercent({ sessionUser });
  const subscriberDiscount = subPercent > 0
    ? Math.round((afterCoupon * (subPercent / 100)) * 100) / 100
    : 0;

  const totalAfter = Math.round((afterCoupon - subscriberDiscount) * 100) / 100;
  return {
    totalBefore,
    couponDiscount,
    subscriberDiscount,
    totalAfter,
    appliedCoupon
  };
};

const finalizeSingleProjectPurchase = async ({ req, buyerUserId, payerUserId, projectId, couponCode, referralCode }) => {
  const projects = await db.projects();
  const project = projects.find(p => p && p.id === projectId);
  if (!project) {
    return { ok: false, redirect: '/', error: 'Project not found' };
  }

  const users = await db.users();
  const buyerIndex = users.findIndex(u => u && u.id === buyerUserId && u.role === 'user');
  const payerIndex = users.findIndex(u => u && u.id === payerUserId && u.role === 'user');
  const buyerUser = buyerIndex !== -1 ? users[buyerIndex] : null;
  const payerUser = payerIndex !== -1 ? users[payerIndex] : null;
  if (!buyerUser) {
    return { ok: false, redirect: '/login', error: 'User not found' };
  }
  if (!payerUser) {
    return { ok: false, redirect: '/my-purchases?walletCardError=' + encodeURIComponent('صاحب البطاقة غير موجود') };
  }

  ensureUserPaymentProfile({ users, user: buyerUser });
  ensureUserPaymentProfile({ users, user: payerUser });

  const activeSubscription = await getActiveSubscriptionForUser({ userId: buyerUserId });
  const pricingUser = buildSessionUser({ user: buyerUser, activeSubscription });

  if (!isProjectVisibleToUser({ project, sessionUser: pricingUser })) {
    return { ok: false, redirect: '/', error: 'Not allowed' };
  }

  const purchases = await db.purchases();
  const alreadyPurchased = purchases.find(p => p && p.userId === buyerUserId && p.projectId === project.id);
  if (alreadyPurchased) {
    return { ok: true, redirect: '/my-purchases' };
  }

  const normalizedReferral = normalizeReferralCode(referralCode);
  const hasAnyPurchase = purchases.some(p => p && p.userId === buyerUserId);
  const canApplyReferralNow = !buyerUser.referredBy && !hasAnyPurchase;

  if (normalizedReferral && !canApplyReferralNow) {
    return {
      ok: false,
      redirect: `/project/${project.id}?referralError=${encodeURIComponent('لا يمكنك استخدام كود إحالة الآن')}`
    };
  }

  if (normalizedReferral && canApplyReferralNow) {
    const referralCheck = validateReferralCodeForUser({ users, code: normalizedReferral, targetUserId: buyerUserId });
    if (!referralCheck.valid) {
      return {
        ok: false,
        redirect: `/project/${project.id}?referralError=${encodeURIComponent(referralCheck.reason || 'كود الإحالة غير صحيح')}`
      };
    }

    buyerUser.referredBy = {
      referrerUserId: referralCheck.referrerUserId,
      code: referralCheck.normalized,
      createdAt: new Date().toISOString(),
      rewardedAt: null
    };

    const referrals = await db.referrals();
    const alreadyRecorded = referrals.some(r => r && r.referredUserId === buyerUser.id);
    if (!alreadyRecorded) {
      referrals.push({
        id: uuidv4(),
        code: referralCheck.normalized,
        referrerUserId: referralCheck.referrerUserId,
        referredUserId: buyerUser.id,
        status: 'pending',
        rewardAmount: 100,
        createdAt: new Date().toISOString(),
        rewardedAt: null,
        rewardPurchaseId: null
      });
      await db.saveReferrals(referrals);
    }
  }

  const normalizedCoupon = normalizeCouponCode(couponCode);
  let appliedCoupon = null;
  let discountAmount = 0;
  let priceAfterDiscount = Number(project.price || 0);
  let coupons = null;
  let couponIndex = -1;

  if (normalizedCoupon) {
    coupons = await db.coupons();
    couponIndex = coupons.findIndex(c => normalizeCouponCode(c.code) === normalizedCoupon);
    const coupon = couponIndex !== -1 ? coupons[couponIndex] : null;
    const eligibility = getCouponEligibility(coupon);

    if (!eligibility.eligible) {
      return {
        ok: false,
        redirect: `/project/${project.id}?couponError=${encodeURIComponent(eligibility.reason || 'كوبون غير صالح')}`
      };
    }

    const calc = calculateDiscount({ priceBefore: project.price, coupon });
    discountAmount = calc.discountAmount;
    priceAfterDiscount = calc.priceAfter;
    appliedCoupon = coupon;
  }

  const subPercent = getSubscriberDiscountPercent({ sessionUser: pricingUser });
  if (subPercent > 0) {
    const base = Number(priceAfterDiscount || 0);
    const subDiscount = Math.round((base * (subPercent / 100)) * 100) / 100;
    discountAmount = Math.round((Number(discountAmount || 0) + subDiscount) * 100) / 100;
    priceAfterDiscount = Math.round((base - subDiscount) * 100) / 100;
  }

  const payerWalletBalance = Number(payerUser.walletBalance || 0);
  if (payerWalletBalance < Number(priceAfterDiscount || 0)) {
    return {
      ok: false,
      redirect: `/project/${project.id}?couponError=${encodeURIComponent('رصيد البطاقة غير كافٍ')}`
    };
  }

  payerUser.walletBalance = Math.round((payerWalletBalance - Number(priceAfterDiscount || 0)) * 100) / 100;

  const earnedPoints = await getLoyaltyEarnedPointsForPurchase({ amountEGP: Number(priceAfterDiscount || 0) });
  if (earnedPoints > 0) {
    buyerUser.loyaltyPoints = normalizeLoyaltyPoints(buyerUser.loyaltyPoints) + earnedPoints;
  }

  users[buyerIndex] = buyerUser;
  users[payerIndex] = payerUser;
  await db.saveUsers(users);

  if (appliedCoupon && coupons && couponIndex !== -1) {
    coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
    await db.saveCoupons(coupons);
  }

  purchases.push({
    id: uuidv4(),
    userId: buyerUserId,
    payerUserId: payerUserId,
    payerCardLast4: normalizeWalletCardNumber(payerUser.walletCardNumber).slice(-4),
    projectId: project.id,
    projectTitle: project.title,
    price: priceAfterDiscount,
    priceBefore: Number(project.price || 0),
    discountAmount,
    couponCode: appliedCoupon ? normalizeCouponCode(appliedCoupon.code) : null,
    walletDebitAmount: Number(priceAfterDiscount || 0),
    walletRefundedAt: null,
    loyaltyPointsEarned: earnedPoints,
    status: 'pending',
    purchasedAt: new Date().toISOString(),
    filePath: project.filePath,
    originalFileName: project.originalFileName || null
  });

  await db.savePurchases(purchases);
  await createNotification({
    userId: buyerUserId,
    title: 'تم تسجيل طلب شراء جديد',
    message: `تم تسجيل طلب شراء المشروع "${project.title}" وبانتظار مراجعة الأدمن.`,
    link: '/my-purchases',
    kind: 'info',
    meta: {
      type: 'purchase_pending',
      projectId: project.id
    }
  });
  req.session.user = buildSessionUser({ user: buyerUser, activeSubscription });
  return { ok: true, redirect: '/my-purchases?paymentSuccess=' + encodeURIComponent('تم تأكيد الدفع وإرسال الطلب بنجاح') };
};

const finalizeCartPurchase = async ({ req, buyerUserId, payerUserId, itemsSnapshot, couponCode }) => {
  const items = Array.isArray(itemsSnapshot) ? itemsSnapshot.filter(Boolean) : [];
  if (items.length === 0) {
    return { ok: false, redirect: '/cart?error=' + encodeURIComponent('السلة فارغة') };
  }

  const users = await db.users();
  const buyerIndex = users.findIndex(u => u && u.id === buyerUserId && u.role === 'user');
  const payerIndex = users.findIndex(u => u && u.id === payerUserId && u.role === 'user');
  const buyerUser = buyerIndex !== -1 ? users[buyerIndex] : null;
  const payerUser = payerIndex !== -1 ? users[payerIndex] : null;
  if (!buyerUser) {
    return { ok: false, redirect: '/login', error: 'User not found' };
  }
  if (!payerUser) {
    return { ok: false, redirect: '/my-purchases?walletCardError=' + encodeURIComponent('صاحب البطاقة غير موجود') };
  }

  ensureUserPaymentProfile({ users, user: buyerUser });
  ensureUserPaymentProfile({ users, user: payerUser });

  const activeSubscription = await getActiveSubscriptionForUser({ userId: buyerUserId });
  const pricingUser = buildSessionUser({ user: buyerUser, activeSubscription });
  const projects = await db.projects();
  const purchases = await db.purchases();
  const summary = await summarizeCart({ cart: { items }, couponCode, sessionUser: pricingUser });
  const totalAfter = Number(summary.totalAfter || 0);

  if (!(totalAfter > 0)) {
    return { ok: false, redirect: '/cart?error=' + encodeURIComponent('إجمالي غير صحيح') };
  }

  for (const it of items) {
    const project = projects.find(projectItem => projectItem && projectItem.id === it.projectId);
    if (!project) {
      return { ok: false, redirect: '/cart?error=' + encodeURIComponent('يوجد مشروع غير موجود بالسلة') };
    }
    if (!isProjectVisibleToUser({ project, sessionUser: pricingUser })) {
      return { ok: false, redirect: '/cart?error=' + encodeURIComponent('يوجد مشروع غير مسموح لك بشرائه') };
    }
    const already = purchases.some(existing => existing && existing.userId === buyerUserId && existing.projectId === it.projectId);
    if (already) {
      return { ok: false, redirect: '/cart?error=' + encodeURIComponent('يوجد مشروع تم شراؤه مسبقاً') };
    }
  }

  const payerWalletBalance = Number(payerUser.walletBalance || 0);
  if (payerWalletBalance < totalAfter) {
    return { ok: false, redirect: '/cart?error=' + encodeURIComponent('رصيد البطاقة غير كافٍ') };
  }

  payerUser.walletBalance = Math.round((payerWalletBalance - totalAfter) * 100) / 100;

  const earnedPoints = await getLoyaltyEarnedPointsForPurchase({ amountEGP: totalAfter });
  if (earnedPoints > 0) {
    buyerUser.loyaltyPoints = normalizeLoyaltyPoints(buyerUser.loyaltyPoints) + earnedPoints;
  }

  if (summary.appliedCoupon) {
    const coupons = await db.coupons();
    const idx = coupons.findIndex(c => c && normalizeCouponCode(c.code) === normalizeCouponCode(summary.appliedCoupon.code));
    if (idx !== -1) {
      coupons[idx].usedCount = Number(coupons[idx].usedCount || 0) + 1;
      await db.saveCoupons(coupons);
    }
  }

  users[buyerIndex] = buyerUser;
  users[payerIndex] = payerUser;
  await db.saveUsers(users);

  const orderId = uuidv4();
  const totalDiscount = Math.round((Number(summary.couponDiscount || 0) + Number(summary.subscriberDiscount || 0)) * 100) / 100;
  const perItemDiscount = items.length ? Math.round((totalDiscount / items.length) * 100) / 100 : 0;
  const perItemDebit = items.length ? Math.round((totalAfter / items.length) * 100) / 100 : 0;

  items.forEach((it, itemIndex) => {
    const project = projects.find(projectItem => projectItem && projectItem.id === it.projectId);
    if (!project) return;
    const itemDiscount = itemIndex === items.length - 1
      ? Math.round((totalDiscount - (perItemDiscount * (items.length - 1))) * 100) / 100
      : perItemDiscount;
    const itemDebit = itemIndex === items.length - 1
      ? Math.round((totalAfter - (perItemDebit * (items.length - 1))) * 100) / 100
      : perItemDebit;

    purchases.push({
      id: uuidv4(),
      orderId,
      userId: buyerUserId,
      payerUserId: payerUserId,
      payerCardLast4: normalizeWalletCardNumber(payerUser.walletCardNumber).slice(-4),
      projectId: project.id,
      projectTitle: project.title,
      price: itemDebit,
      priceBefore: Number(it.price || project.price || 0),
      discountAmount: itemDiscount,
      couponCode: summary.appliedCoupon ? normalizeCouponCode(summary.appliedCoupon.code) : null,
      walletDebitAmount: itemDebit,
      walletRefundedAt: null,
      loyaltyPointsEarned: itemIndex === 0 ? earnedPoints : 0,
      status: 'pending',
      purchasedAt: new Date().toISOString(),
      filePath: project.filePath,
      originalFileName: project.originalFileName || null
    });
  });

  await db.savePurchases(purchases);
  await createNotification({
    userId: buyerUserId,
    title: 'تم تسجيل طلب السلة',
    message: `تم تسجيل ${items.length} مشروع/مشاريع من السلة وبانتظار مراجعة الأدمن.`,
    link: '/my-purchases',
    kind: 'info',
    meta: {
      type: 'cart_purchase_pending',
      orderSize: items.length
    }
  });

  const { carts, cartIndex } = await getOrCreateCartForUser({ userId: buyerUserId });
  const projectIds = new Set(items.map(it => it.projectId));
  const existingCart = carts[cartIndex] || { userId: buyerUserId, items: [] };
  carts[cartIndex] = {
    ...existingCart,
    items: (Array.isArray(existingCart.items) ? existingCart.items : []).filter(it => it && !projectIds.has(it.projectId)),
    updatedAt: new Date().toISOString()
  };
  await db.saveCarts(carts);

  req.session.user = buildSessionUser({ user: buyerUser, activeSubscription });
  return { ok: true, redirect: '/my-purchases?paymentSuccess=' + encodeURIComponent('تم تأكيد الدفع وإرسال الطلب بنجاح') };
};

const calculateDiscount = ({ priceBefore, coupon }) => {
  const base = Number(priceBefore || 0);
  if (!coupon) return { discountAmount: 0, priceAfter: base };

  const type = coupon.type;
  const value = Number(coupon.value || 0);

  let discount = 0;
  if (type === 'percent') {
    discount = Math.round((base * (value / 100)) * 100) / 100;
  } else if (type === 'fixed') {
    discount = value;
  }

  if (!Number.isFinite(discount) || discount < 0) discount = 0;
  if (discount > base) discount = base;

  const after = Math.round((base - discount) * 100) / 100;
  return { discountAmount: discount, priceAfter: after };
};

const normalizeReferralCode = (code) => {
  if (!code || typeof code !== 'string') return '';
  return code.trim().toUpperCase();
};

const generateReferralCode = () => {
  return uuidv4().replace(/-/g, '').slice(0, 8).toUpperCase();
};

const ensureUniqueReferralCode = (users) => {
  const used = new Set(users.map(u => normalizeReferralCode(u.referralCode)).filter(Boolean));
  let code = generateReferralCode();
  while (used.has(code)) code = generateReferralCode();
  return code;
};

const validateReferralCodeForUser = ({ users, code, targetUserId }) => {
  const normalized = normalizeReferralCode(code);
  if (!normalized) return { valid: true, normalized: '' };
  const referrer = users.find(u => normalizeReferralCode(u.referralCode) === normalized);
  if (!referrer) return { valid: false, normalized, reason: 'كود الإحالة غير صحيح' };
  if (targetUserId && referrer.id === targetUserId) {
    return { valid: false, normalized, reason: 'لا يمكنك استخدام كود الإحالة الخاص بك' };
  }
  return { valid: true, normalized, referrerUserId: referrer.id };
};

const normalizeWalletCode = (code) => {
  if (!code || typeof code !== 'string') return '';
  return code.trim().toUpperCase();
};

const findUserByWalletCardNumber = ({ users, walletCardNumber }) => {
  const normalized = normalizeWalletCardNumber(walletCardNumber);
  if (!normalized) return null;
  return (Array.isArray(users) ? users : []).find(user => user && user.role === 'user' && normalizeWalletCardNumber(user.walletCardNumber) === normalized) || null;
};

const getWalletCodeEligibility = (walletCode) => {
  if (!walletCode) return { eligible: false, reason: 'كود غير صحيح' };
  if (!walletCode.active) return { eligible: false, reason: 'الكود غير فعّال' };

  if (walletCode.expiresAt) {
    const expires = new Date(walletCode.expiresAt);
    if (!Number.isNaN(expires.getTime()) && new Date() > expires) {
      return { eligible: false, reason: 'الكود منتهي' };
    }
  }

  const used = Number(walletCode.usedCount || 0);
  const limit = walletCode.usageLimit != null ? Number(walletCode.usageLimit) : null;
  if (Number.isFinite(limit) && limit > 0 && used >= limit) {
    return { eligible: false, reason: 'تم الوصول للحد الأقصى لاستخدام الكود' };
  }

  return { eligible: true, reason: null };
};

const getWalletPaymentAttemptsState = async () => {
  const attemptsRaw = await db.walletPaymentAttempts();
  const attempts = Array.isArray(attemptsRaw) ? attemptsRaw : [];
  const activeAttempts = attempts.filter(attempt => attempt && !attempt.usedAt && !isPaymentAttemptExpired(attempt));
  if (activeAttempts.length !== attempts.length) {
    await db.saveWalletPaymentAttempts(activeAttempts);
  }
  return activeAttempts;
};

const createWalletPaymentAttempt = async ({ req, buyerUser, payerUser, amount, kind, payload }) => {
  const attempts = await getWalletPaymentAttemptsState();
  const code = generatePaymentVerificationCode();
  const attempt = {
    id: uuidv4(),
    userId: buyerUser.id,
    payerUserId: payerUser.id,
    kind,
    payload,
    amount: Math.round(Number(amount || 0) * 100) / 100,
    requiresPassword: Number(amount || 0) > HIGH_VALUE_PAYMENT_THRESHOLD,
    payerCardLast4: normalizeWalletCardNumber(payerUser.walletCardNumber).slice(-4),
    ownerVisibleCode: code,
    codeHash: hashPaymentVerificationCode(code),
    expiresAt: new Date(Date.now() + PAYMENT_VERIFICATION_TTL_MS).toISOString(),
    createdAt: new Date().toISOString(),
    usedAt: null
  };

  attempts.push(attempt);
  await db.saveWalletPaymentAttempts(attempts);

  await createNotification({
    userId: payerUser.id,
    title: 'طلب دفع جديد ببطاقتك',
    message: [
      `المشتري: ${buyerUser.name || buyerUser.email || buyerUser.id}`,
      `المبلغ: ${formatMoney(attempt.amount)} جنيه`,
      `كود التحقق: ${code}`,
      attempt.requiresPassword ? 'هذه العملية تحتاج أيضًا كلمة مرور البطاقة.' : null
    ].filter(Boolean).join(' | '),
    link: '/notifications',
    kind: 'warning',
    meta: {
      type: 'wallet_payment_code',
      attemptId: attempt.id
    }
  });

  return { attempt };
};

const getWalletPaymentAttemptById = async ({ attemptId, userId }) => {
  const attempts = await getWalletPaymentAttemptsState();
  return attempts.find(attempt => attempt && attempt.id === attemptId && attempt.userId === userId) || null;
};

const markWalletPaymentAttemptUsed = async ({ attemptId }) => {
  const attempts = await getWalletPaymentAttemptsState();
  const idx = attempts.findIndex(attempt => attempt && attempt.id === attemptId);
  if (idx === -1) return;
  attempts[idx].usedAt = new Date().toISOString();
  await db.saveWalletPaymentAttempts(attempts);
};

const getDownloadFileName = ({ originalFileName, projectTitle, filePath }) => {
  if (originalFileName && typeof originalFileName === 'string') return originalFileName;
  if (filePath && typeof filePath === 'string') {
    const normalized = normalizeStoredPath(filePath);
    const base = normalized ? path.basename(normalized) : null;
    if (base) {
      const idx = base.indexOf('-');
      if (idx !== -1 && idx < base.length - 1) {
        return base.slice(idx + 1);
      }
    }
  }
  return `${projectTitle || 'project'}.zip`;
};

const ensureLocalJsonFiles = () => {
  ['users.json', 'projects.json', 'purchases.json', 'modifications.json', 'messages.json', 'admin-team-messages.json', 'carts.json', 'invoices.json', 'reviews.json', 'coupons.json', 'referrals.json', 'wallet-codes.json', 'wallet-payment-attempts.json'].forEach(file => {
    const filePath = path.join(DATA_DIR, file);
    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, '[]');
    }
  });
};

const ensureLocalAdminUser = () => {
  const users = readJsonFile('users.json', []);
  if (!users.find(u => u && u.email === 'admin@codentra.com')) {
    users.push({
      id: uuidv4(),
      name: 'Admin',
      email: 'admin@codentra.com',
      password: bcrypt.hashSync('admin123', 10),
      role: 'admin',
      isSuperAdmin: true,
      createdAt: new Date().toISOString()
    });
    writeJsonFile('users.json', users);
  }
};

ensureLocalJsonFiles();
ensureLocalAdminUser();

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(UPLOADS_DIR));
if (IS_VERCEL) {
  app.use('/uploads', express.static(BUNDLED_UPLOADS_DIR));
}

if (TRUST_PROXY) {
  app.set('trust proxy', 1);
}

if (IS_VERCEL) {
  app.use(cookieSession({
    name: 'codentra',
    keys: [SESSION_SECRET],
    maxAge: SESSION_MAX_AGE_MS,
    httpOnly: true,
    sameSite: 'lax',
    secure: COOKIE_SECURE,
    domain: COOKIE_DOMAIN
  }));
} else {
  app.use(session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      maxAge: SESSION_MAX_AGE_MS
    }
  }));
}

const isBlockedExpired = (u) => {
  if (!u) return false;
  if (!u.isBlocked) return false;
  if (!u.blockedUntil) return false;
  const until = new Date(u.blockedUntil);
  if (Number.isNaN(until.getTime())) return false;
  return until.getTime() <= Date.now();
};

const unblockUserInPlace = (u) => {
  if (!u) return;
  u.isBlocked = false;
  u.blockedReason = null;
  u.blockedBy = null;
  u.blockedAt = null;
  u.blockedUntil = null;
};

const getBlockedMessage = (u) => {
  if (!u || !u.isBlocked) return null;
  const reason = u.blockedReason ? `سبب الحظر: ${u.blockedReason}` : 'تم حظر الحساب';
  if (u.blockedUntil) {
    return `${reason} (حتى ${new Date(u.blockedUntil).toLocaleString('ar-EG')})`;
  }
  return `${reason} (حظر دائم)`;
};

app.use(async (req, res, next) => {
  try {
    if (!req.session || !req.session.user || !req.session.user.id) return next();

    const allowWhileBlocked = (
      req.path === '/blocked' ||
      req.path === '/logout' ||
      req.path === '/login' ||
      req.path.startsWith('/css/') ||
      req.path.startsWith('/js/') ||
      req.path.startsWith('/images/') ||
      req.path.startsWith('/uploads/')
    );

    const users = await db.users();
    const idx = users.findIndex(u => u && u.id === req.session.user.id);
    if (idx === -1) return next();

    const fullUser = users[idx];
    const profileChanged = ensureUserPaymentProfile({ users, user: fullUser });

    if (isBlockedExpired(fullUser)) {
      unblockUserInPlace(fullUser);
      users[idx] = fullUser;
      await db.saveUsers(users);
    } else if (profileChanged) {
      users[idx] = fullUser;
      await db.saveUsers(users);
    }

    if (fullUser.isBlocked) {
      if (allowWhileBlocked) return next();
      return res.redirect('/blocked');
    }

    req.session.user = {
      ...req.session.user,
      name: fullUser.name,
      email: fullUser.email,
      role: fullUser.role,
      isSuperAdmin: fullUser.isSuperAdmin,
      adminPermissions: fullUser.adminPermissions,
      walletBalance: Number(fullUser.walletBalance || 0),
      referralCode: fullUser.referralCode || null,
      walletCardNumberMasked: fullUser.role === 'user' ? maskWalletCardNumber(fullUser.walletCardNumber) : null,
      hasWalletPaymentPassword: fullUser.role === 'user' ? Boolean(fullUser.walletPaymentPasswordHash) : false,
      loyaltyPoints: fullUser.role === 'user' ? normalizeLoyaltyPoints(fullUser.loyaltyPoints) : 0
    };
  } catch (e) {
    // ignore
  }
  next();
});

app.use(async (req, res, next) => {
  res.locals.user = req.session && req.session.user ? req.session.user : null;
  res.locals.notificationUnreadCount = 0;

  try {
    if (req.session && req.session.user && req.session.user.id && req.session.user.role === 'user') {
      res.locals.notificationUnreadCount = await getUnreadNotificationCount({ userId: req.session.user.id });
    }
  } catch (error) {
    res.locals.notificationUnreadCount = 0;
  }

  next();
});

// View engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Multer config for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOADS_DIR),
  filename: (req, file, cb) => cb(null, `${uuidv4()}-${file.originalname}`)
});
const upload = multer({ storage, limits: { fileSize: 100 * 1024 * 1024 } }); // 100MB max

const meetingRecordingStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, MEETING_RECORDINGS_DIR),
  filename: (req, file, cb) => cb(null, `${uuidv4()}.webm`)
});
const meetingRecordingUpload = multer({ storage: meetingRecordingStorage, limits: { fileSize: 500 * 1024 * 1024 } }); // 500MB max

const adminTeamUploadStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, ADMIN_TEAM_UPLOADS_DIR),
  filename: (req, file, cb) => {
    const safeOriginal = (file.originalname || 'file').replace(/[^a-zA-Z0-9._-]+/g, '_');
    cb(null, `${uuidv4()}-${safeOriginal}`);
  }
});
const adminTeamUpload = multer({ storage: adminTeamUploadStorage, limits: { fileSize: 200 * 1024 * 1024 } }); // 200MB max

// Project images storage with watermark
const PROJECT_IMAGES_DIR = path.join(UPLOADS_DIR, 'project-images');
if (!fs.existsSync(PROJECT_IMAGES_DIR)) fs.mkdirSync(PROJECT_IMAGES_DIR, { recursive: true });

const projectImagesStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, PROJECT_IMAGES_DIR),
  filename: (req, file, cb) => {
    const safeOriginal = (file.originalname || 'image').replace(/[^a-zA-Z0-9._-]+/g, '_');
    const ext = path.extname(safeOriginal) || '.jpg';
    cb(null, `${uuidv4()}-image${ext}`);
  }
});

const projectImagesUpload = multer({ 
  storage: projectImagesStorage, 
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB per image (increased from 5MB)
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) {
      cb(null, true);
    } else {
      cb(new Error('Only image files are allowed'), false);
    }
  }
});

// Custom file processor for watermark
const processImageWithWatermark = async (file) => {
  try {
    const imageBuffer = fs.readFileSync(file.path);
    const watermarkedBuffer = await watermarkProcessor.addWatermarkToBuffer(imageBuffer, {
      position: 'bottom-right',
      opacity: 0.7,
      scale: 0.15,
      margin: 20
    });
    
    // Save the watermarked image
    fs.writeFileSync(file.path, watermarkedBuffer);
    return file;
  } catch (error) {
    console.error('Error processing image with watermark:', error);
    // Return original file if watermark fails
    return file;
  }
};

// Combined upload for project form (single file + multiple images)
const projectUpload = multer({ 
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      if (file.fieldname === 'projectFile') {
        cb(null, UPLOADS_DIR);
      } else if (file.fieldname === 'projectImages') {
        cb(null, PROJECT_IMAGES_DIR);
      }
    },
    filename: (req, file, cb) => {
      if (file.fieldname === 'projectFile') {
        cb(null, `${uuidv4()}-${file.originalname}`);
      } else if (file.fieldname === 'projectImages') {
        const safeOriginal = (file.originalname || 'image').replace(/[^a-zA-Z0-9._-]+/g, '_');
        const ext = path.extname(safeOriginal) || '.jpg';
        cb(null, `${uuidv4()}-image${ext}`);
      }
    }
  }),
  limits: { 
    fileSize: 500 * 1024 * 1024, // 500MB for project files and images
    files: 6 // 1 project file + 5 images
  },
  fileFilter: (req, file, cb) => {
    if (file.fieldname === 'projectFile') {
      cb(null, true); // Allow any file for project files
    } else if (file.fieldname === 'projectImages') {
      if (file.mimetype.startsWith('image/')) {
        cb(null, true);
      } else {
        cb(new Error('Only image files are allowed'), false);
      }
    }
  }
}).fields([
  { name: 'projectFile', maxCount: 1 },
  { name: 'projectImages', maxCount: 5 }
]);

// Auth middleware
const requireAuth = async (req, res, next) => {
  if (!req.session.user) return res.redirect('/login');
  next();
};

const requireAdmin = async (req, res, next) => {
  if (!req.session.user || req.session.user.role !== 'admin') return res.redirect('/');
  next();
};

const requireSuperAdmin = async (req, res, next) => {
  if (!req.session.user || req.session.user.role !== 'admin' || !req.session.user.isSuperAdmin) return res.redirect('/');
  next();
};

const ADMIN_PERMISSIONS = {
  projects: 'projects',
  purchases: 'purchases',
  modifications: 'modifications',
  messages: 'messages',
  reviews: 'reviews',
  coupons: 'coupons',
  referrals: 'referrals',
  walletCodes: 'walletCodes',
  walletBalances: 'walletBalances',
  users: 'users',
  appointments: 'appointments',
  subscriptionCoupons: 'subscriptionCoupons',
  subscriptionPlans: 'subscriptionPlans',
  subscriptionReports: 'subscriptionReports',
  meetingRecordings: 'meetingRecordings',
  admins: 'admins'
};

const hasAdminPermission = ({ sessionUser, permission }) => {
  if (!sessionUser || sessionUser.role !== 'admin') return false;
  if (sessionUser.isSuperAdmin) return true;
  const perms = sessionUser.adminPermissions;
  if (!perms || typeof perms !== 'object') return false;
  return perms[permission] === true;
};

const requireAdminPermission = (permission) => {
  return async (req, res, next) => {
    if (!req.session.user || req.session.user.role !== 'admin') return res.redirect('/');
    if (!hasAdminPermission({ sessionUser: req.session.user, permission })) {
      return res.redirect('/admin?error=' + encodeURIComponent('ليس لديك صلاحية لهذه الصفحة'));
    }
    next();
  };
};

// Routes

// Home - Projects listing
app.get('/', async (req, res) => {
  const projects = (await db.projects()).filter(p => isProjectVisibleToUser({ project: p, sessionUser: req.session.user }));
  res.render('index', { projects, user: req.session.user });
});

// Auth routes
app.get('/login', async (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('login', { error: req.query.error || null, user: null });
});

app.get('/blocked', async (req, res) => {
  try {
    if (!req.session || !req.session.user || !req.session.user.id) {
      return res.redirect('/login');
    }
    const users = await db.users();
    const u = users.find(x => x && x.id === req.session.user.id);
    if (!u) return res.redirect('/login');

    if (isBlockedExpired(u)) {
      const idx = users.findIndex(x => x && x.id === u.id);
      unblockUserInPlace(u);
      if (idx !== -1) users[idx] = u;
      await db.saveUsers(users);
      return res.redirect('/');
    }

    if (!u.isBlocked) return res.redirect('/');

    return res.render('blocked', {
      user: req.session.user,
      blockedReason: u.blockedReason || null,
      blockedUntil: u.blockedUntil || null
    });
  } catch (e) {
    return res.redirect('/login');
  }
});

app.post('/login', async (req, res) => {
  const { email, password } = req.body;
  const users = await db.users();
  const user = users.find(u => u.email === email);
  
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.render('login', { error: 'Invalid email or password', user: null });
  }

  if (isBlockedExpired(user)) {
    unblockUserInPlace(user);
    const idx = users.findIndex(u => u && u.id === user.id);
    if (idx !== -1) users[idx] = user;
    await db.saveUsers(users);
  }

  if (user.isBlocked) {
    req.session.user = {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role,
      isSuperAdmin: user.isSuperAdmin,
      adminPermissions: user.adminPermissions,
      walletBalance: Number(user.walletBalance || 0)
    };
    return res.redirect('/blocked');
  }

  let shouldSaveUsers = false;
  if (user.role === 'user') {
    if (ensureUserPaymentProfile({ users, user })) {
      shouldSaveUsers = true;
    }
  } else if (user.role === 'admin') {
    if (user.isSuperAdmin == null) {
      user.isSuperAdmin = false;
      shouldSaveUsers = true;
    }
    if (user.email === 'admin@codentra.com' && user.isSuperAdmin !== true) {
      user.isSuperAdmin = true;
      shouldSaveUsers = true;
    }
  }

  if (shouldSaveUsers) {
    const idx = users.findIndex(u => u.id === user.id);
    if (idx !== -1) users[idx] = user;
    await db.saveUsers(users);
  }

  const activeSubscription = user.role === 'user'
    ? await getActiveSubscriptionForUser({ userId: user.id })
    : null;

  req.session.user = buildSessionUser({ user, activeSubscription });
  res.redirect(user.role === 'admin' ? '/admin' : '/');
});

app.get('/register', async (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('register', { error: null, user: null, referralPrefill: req.query.ref || '' });
});

app.post('/register', async (req, res) => {
  const { name, email, password } = req.body;
  const walletPassword = String(req.body.walletPassword || '');
  const referralInput = normalizeReferralCode(req.body.referralCode);
  const users = await db.users();
  
  if (users.find(u => u.email === email)) {
    return res.render('register', { error: 'Email already registered', user: null, referralPrefill: referralInput });
  }

  if (walletPassword.length < 4) {
    return res.render('register', { error: 'كلمة مرور البطاقة يجب أن تكون 4 أحرف أو أرقام على الأقل', user: null, referralPrefill: referralInput });
  }

  const referralCheck = validateReferralCodeForUser({ users, code: referralInput, targetUserId: null });
  if (!referralCheck.valid) {
    return res.render('register', { error: referralCheck.reason, user: null, referralPrefill: referralInput });
  }
  
  const newUser = {
    id: uuidv4(),
    name,
    email,
    password: bcrypt.hashSync(password, 10),
    role: 'user',
    referralCode: ensureUniqueReferralCode(users),
    walletCardNumber: createWalletCardNumber(users, null),
    walletPaymentPasswordHash: bcrypt.hashSync(walletPassword, 10),
    walletBalance: 0,
    referredBy: referralCheck.referrerUserId
      ? {
          referrerUserId: referralCheck.referrerUserId,
          code: referralCheck.normalized,
          createdAt: new Date().toISOString(),
          rewardedAt: null
        }
      : null,
    createdAt: new Date().toISOString()
  };

  if (newUser.referredBy) {
    const referrals = await db.referrals();
    referrals.push({
      id: uuidv4(),
      code: newUser.referredBy.code,
      referrerUserId: newUser.referredBy.referrerUserId,
      referredUserId: newUser.id,
      status: 'pending',
      rewardAmount: 100,
      createdAt: new Date().toISOString(),
      rewardedAt: null,
      rewardPurchaseId: null
    });
    await db.saveReferrals(referrals);
  }
  
  users.push(newUser);
  await db.saveUsers(users);
  
  req.session.user = buildSessionUser({ user: newUser, activeSubscription: null });
  res.redirect('/');
});

app.get('/logout', async (req, res) => {
  const clearOptions = {
    path: '/',
    sameSite: 'lax',
    secure: COOKIE_SECURE,
    domain: COOKIE_DOMAIN
  };

  const finish = () => {
    res.clearCookie('codentra', clearOptions);
    res.clearCookie('connect.sid', clearOptions);
    res.redirect('/');
  };

  if (req.session) {
    if (typeof req.session.destroy === 'function') {
      return req.session.destroy(() => finish());
    }
    req.session = null;
  }
  finish();
});

// API Routes for Mobile App
app.post('/api/auth/login', async (req, res) => {
  const { email, password } = req.body;
  const users = await db.users();
  const user = users.find(u => u.email === email);
  
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }

  const token = generateToken(user);
  res.json({ token, user: { id: user.id, name: user.name, email: user.email, role: user.role } });
});

app.get('/api/me', requireApiUserAuth, async (req, res) => {
  const users = await db.users();
  const user = users.find(u => u.id === req.apiUser.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  res.json({ user: { id: user.id, name: user.name, email: user.email, role: user.role, walletBalance: Number(user.walletBalance || 0) } });
});

app.get('/api/projects', requireApiUserAuth, async (req, res) => {
  const projects = (await db.projects()).filter(p => isProjectVisibleToUser({ project: p, sessionUser: { role: 'user', id: req.apiUser.id } }));
  res.json({ projects });
});

app.get('/api/projects/:id', requireApiUserAuth, async (req, res) => {
  const projects = await db.projects();
  const project = projects.find(p => p.id === req.params.id);
  if (!project) return res.status(404).json({ error: 'Project not found' });
  if (!isProjectVisibleToUser({ project, sessionUser: { role: 'user', id: req.apiUser.id } })) {
    return res.status(403).json({ error: 'Not allowed' });
  }
  res.json({ project });
});

app.get('/api/cart', requireApiUserAuth, async (req, res) => {
  const { cart } = await getOrCreateCartForUser({ userId: req.apiUser.id });
  res.json({ cart });
});

app.get('/api/cart/summary', requireApiUserAuth, async (req, res) => {
  const { cart } = await getOrCreateCartForUser({ userId: req.apiUser.id });
  const couponCode = normalizeCouponCode(req.query.couponCode);
  const summary = await summarizeCart({ cart, couponCode, sessionUser: { role: 'user', id: req.apiUser.id } });
  res.json({ summary });
});

app.post('/api/cart/add', requireApiUserAuth, async (req, res) => {
  const { projectId } = req.body;
  if (!projectId) return res.status(400).json({ error: 'Missing projectId' });
  
  const projects = await db.projects();
  const project = projects.find(p => p && p.id === projectId);
  if (!project) return res.status(404).json({ error: 'Project not found' });
  if (!isProjectVisibleToUser({ project, sessionUser: { role: 'user', id: req.apiUser.id } })) {
    return res.status(403).json({ error: 'Not allowed' });
  }

  const purchases = await db.purchases();
  const alreadyPurchased = purchases.some(p => p.userId === req.apiUser.id && p.projectId === projectId);
  if (alreadyPurchased) return res.status(400).json({ error: 'Already purchased' });

  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.apiUser.id });
  const items = Array.isArray(cart.items) ? cart.items : [];
  const exists = items.some(it => it && it.projectId === projectId);
  if (!exists) {
    items.push({
      projectId: project.id,
      projectTitle: project.title,
      price: Number(project.price || 0),
      createdAt: new Date().toISOString()
    });
  }
  carts[cartIndex] = { ...cart, items, updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  res.json({ cart: carts[cartIndex] });
});

app.post('/api/cart/remove', requireApiUserAuth, async (req, res) => {
  const { projectId } = req.body;
  if (!projectId) return res.status(400).json({ error: 'Missing projectId' });
  
  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.apiUser.id });
  const nextItems = (Array.isArray(cart.items) ? cart.items : []).filter(it => it && it.projectId !== projectId);
  carts[cartIndex] = { ...cart, items: nextItems, updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  res.json({ cart: carts[cartIndex] });
});

app.post('/api/cart/checkout', requireApiUserAuth, async (req, res) => {
  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.apiUser.id });
  const items = (cart && Array.isArray(cart.items)) ? cart.items : [];
  if (items.length === 0) return res.status(400).json({ error: 'Cart empty' });

  const users = await db.users();
  const currentUserIndex = users.findIndex(u => u.id === req.apiUser.id);
  const currentUser = currentUserIndex !== -1 ? users[currentUserIndex] : null;
  if (!currentUser) return res.status(404).json({ error: 'User not found' });

  const projects = await db.projects();
  const purchases = await db.purchases();

  const couponCode = normalizeCouponCode(req.body.couponCode);
  const summary = await summarizeCart({ cart, couponCode, sessionUser: { role: 'user', id: req.apiUser.id } });
  const totalAfter = Number(summary.totalAfter || 0);
  if (!(totalAfter > 0)) return res.status(400).json({ error: 'Invalid total' });

  const walletBalance = Number(currentUser.walletBalance || 0);
  if (walletBalance < totalAfter) return res.status(400).json({ error: 'Insufficient balance' });

  // validate all items still purchasable
  for (const it of items) {
    const p = projects.find(pp => pp && pp.id === it.projectId);
    if (!p) return res.status(400).json({ error: 'Item not found' });
    if (!isProjectVisibleToUser({ project: p, sessionUser: { role: 'user', id: req.apiUser.id } })) {
      return res.status(403).json({ error: 'Not allowed' });
    }
    const already = purchases.some(x => x.userId === req.apiUser.id && x.projectId === it.projectId);
    if (already) return res.status(400).json({ error: 'Already purchased' });
  }

  // debit wallet once
  currentUser.walletBalance = Math.round((walletBalance - totalAfter) * 100) / 100;

  const earnedPoints = await getLoyaltyEarnedPointsForPurchase({ amountEGP: totalAfter });
  if (earnedPoints > 0) {
    currentUser.loyaltyPoints = normalizeLoyaltyPoints(currentUser.loyaltyPoints) + earnedPoints;
  }

  // increment coupon usage (if valid)
  if (summary.appliedCoupon) {
    const coupons = await db.coupons();
    const idx = coupons.findIndex(c => c && normalizeCouponCode(c.code) === normalizeCouponCode(summary.appliedCoupon.code));
    if (idx !== -1) {
      coupons[idx].usedCount = Number(coupons[idx].usedCount || 0) + 1;
      await db.saveCoupons(coupons);
    }
  }

  users[currentUserIndex] = currentUser;
  await db.saveUsers(users);

  const orderId = uuidv4();
  const totalBefore = Number(summary.totalBefore || 0);
  const totalDiscount = Math.round((Number(summary.couponDiscount || 0) + Number(summary.subscriberDiscount || 0)) * 100) / 100;
  const perItemDiscount = items.length ? Math.round((totalDiscount / items.length) * 100) / 100 : 0;
  const perItemDebit = items.length ? Math.round((totalAfter / items.length) * 100) / 100 : 0;

  for (const it of items) {
    const project = projects.find(pp => pp && pp.id === it.projectId);
    purchases.push({
      id: uuidv4(),
      orderId,
      userId: req.apiUser.id,
      projectId: project.id,
      projectTitle: project.title,
      price: Math.max(0, Math.round((Number(project.price || 0) - perItemDiscount) * 100) / 100),
      priceBefore: Number(project.price || 0),
      discountAmount: perItemDiscount,
      couponCode: summary.appliedCoupon ? normalizeCouponCode(summary.appliedCoupon.code) : null,
      walletDebitAmount: perItemDebit,
      walletRefundedAt: null,
      loyaltyPointsEarned: null,
      status: 'pending',
      purchasedAt: new Date().toISOString(),
      filePath: project.filePath,
      originalFileName: project.originalFileName || null
    });
  }

  await db.savePurchases(purchases);
  carts[cartIndex] = { ...cart, items: [], updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  res.json({ orderId, message: 'Order created' });
});

app.get('/api/purchases', requireApiUserAuth, async (req, res) => {
  const purchases = (await db.purchases()).filter(p => p.userId === req.apiUser.id);
  const invoices = await db.invoices();
  res.json({ purchases, invoices });
});

// Mobile API - Invoice PDF
app.get('/api/invoice/:orderId.pdf', async (req, res) => {
  // Get token from header or query param
  const authHeader = req.headers.authorization;
  const tokenFromQuery = req.query.token;
  
  const token = authHeader?.startsWith('Bearer ') 
    ? authHeader.split(' ')[1] 
    : tokenFromQuery;
  
  if (!token) {
    return res.status(401).json({ error: 'Missing or invalid token' });
  }
  
  const decoded = verifyToken(token);
  if (!decoded || decoded.role !== 'user') {
    return res.status(401).json({ error: 'Invalid token' });
  }
  
  const orderId = req.params.orderId;
  const invoices = await db.invoices();
  const inv = invoices.find(i => i && i.orderId === orderId && i.userId === decoded.id) || null;
  if (!inv) return res.status(404).send('Invoice not found');

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${inv.invoiceNumber || 'invoice'}.pdf"`);
  await renderInvoicePdf({ res, inv });
});

// Mobile API - Subscriptions
app.get('/api/subscriptions/plans', requireApiUserAuth, async (req, res) => {
  const plans = (await db.subscriptionPlans()).filter(p => p && p.active);
  const coupons = (await db.subscriptionCoupons()).filter(c => c && c.active);
  const activeSubscription = await getActiveSubscriptionForUser({ userId: req.apiUser.id });
  res.json({ plans, coupons, activeSubscription });
});

app.post('/api/subscriptions/subscribe', requireApiUserAuth, async (req, res) => {
  const { planId, couponCode } = req.body;
  
  const plans = (await db.subscriptionPlans()).filter(p => p && p.active);
  const plan = plans.find(p => p.id === planId);
  if (!plan) return res.status(400).json({ error: 'الخطة غير صحيحة' });

  const existing = await getActiveSubscriptionForUser({ userId: req.apiUser.id });
  if (existing) return res.status(400).json({ error: 'لديك اشتراك نشط بالفعل' });

  const users = await db.users();
  const idx = users.findIndex(u => u.id === req.apiUser.id);
  if (idx === -1) return res.status(404).json({ error: 'المستخدم غير موجود' });

  const basePrice = Number(plan.price || 0);
  if (!Number.isFinite(basePrice) || basePrice <= 0) {
    return res.status(400).json({ error: 'سعر الخطة غير صحيح' });
  }

  const normalizedCoupon = normalizeCouponCode(couponCode);
  let appliedCoupon = null;
  let couponDiscountAmount = 0;
  let priceAfterDiscount = basePrice;

  let coupons = null;
  let couponIndex = -1;
  if (normalizedCoupon) {
    coupons = await db.subscriptionCoupons();
    couponIndex = coupons.findIndex(c => normalizeCouponCode(c.code) === normalizedCoupon);
    const coupon = couponIndex !== -1 ? coupons[couponIndex] : null;
    const eligibility = getSubscriptionCouponEligibility(coupon);

    if (!eligibility.eligible) {
      return res.status(400).json({ error: eligibility.reason || 'كوبون غير صالح' });
    }

    const calc = calculateSubscriptionCouponDiscount({ priceBefore: basePrice, coupon });
    couponDiscountAmount = calc.discountAmount;
    priceAfterDiscount = calc.priceAfter;
    appliedCoupon = coupon;
  }

  const bal = Number(users[idx].walletBalance || 0);
  if (bal < Number(priceAfterDiscount || 0)) {
    return res.status(400).json({ error: 'الرصيد غير كافي' });
  }

  users[idx].walletBalance = Math.round((bal - Number(priceAfterDiscount || 0)) * 100) / 100;
  await db.saveUsers(users);

  if (appliedCoupon && coupons && couponIndex !== -1) {
    coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
    coupons[couponIndex].lastUsedAt = new Date().toISOString();
    await db.saveSubscriptionCoupons(coupons);
  }

  const now = new Date();
  const end = new Date(now.getTime() + (Number(plan.durationDays || 30) * 24 * 60 * 60 * 1000));

  const subs = await db.subscriptions();
  const sub = {
    id: uuidv4(),
    userId: req.apiUser.id,
    planId: plan.id,
    status: 'active',
    currentPeriodStart: now.toISOString(),
    currentPeriodEnd: end.toISOString(),
    canceledAt: null,
    createdAt: now.toISOString()
  };
  subs.push(sub);
  await db.saveSubscriptions(subs);

  const payments = await db.subscriptionPayments();
  payments.push({
    id: uuidv4(),
    subscriptionId: sub.id,
    userId: sub.userId,
    planId: sub.planId,
    amount: Number(priceAfterDiscount || 0),
    currency: plan.currency || 'EGP',
    method: 'wallet',
    priceBefore: basePrice,
    discountAmount: couponDiscountAmount,
    couponCode: appliedCoupon ? normalizeCouponCode(appliedCoupon.code) : null,
    createdAt: now.toISOString()
  });
  await db.saveSubscriptionPayments(payments);

  res.json({ success: true, subscription: sub, newBalance: users[idx].walletBalance });
});

app.post('/api/subscriptions/cancel', requireApiUserAuth, async (req, res) => {
  const subs = await db.subscriptions();
  const active = await getActiveSubscriptionForUser({ userId: req.apiUser.id });
  if (!active) return res.status(400).json({ error: 'لا يوجد اشتراك نشط' });

  const idx = subs.findIndex(s => s.id === active.id);
  if (idx === -1) return res.status(400).json({ error: 'لا يوجد اشتراك نشط' });

  subs[idx].status = 'canceled';
  subs[idx].canceledAt = new Date().toISOString();
  await db.saveSubscriptions(subs);

  res.json({ success: true });
});

// Mobile API - Wallet Code Redeem
app.post('/api/wallet/redeem', requireApiUserAuth, async (req, res) => {
  const code = normalizeWalletCode(req.body.code);
  if (!code) return res.status(400).json({ error: 'الكود مطلوب' });

  const walletCodes = await db.walletCodes();
  const idx = walletCodes.findIndex(c => normalizeWalletCode(c.code) === code);
  if (idx === -1) {
    return res.status(400).json({ error: 'الكود غير صحيح' });
  }

  const eligibility = getWalletCodeEligibility(walletCodes[idx]);
  if (!eligibility.eligible) {
    return res.status(400).json({ error: eligibility.reason || 'الكود غير صالح' });
  }

  const amount = Number(walletCodes[idx].amount || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return res.status(400).json({ error: 'قيمة الكود غير صحيحة' });
  }

  const users = await db.users();
  const userIndex = users.findIndex(u => u.id === req.apiUser.id);
  if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

  users[userIndex].walletBalance = Number(users[userIndex].walletBalance || 0) + amount;
  await db.saveUsers(users);

  walletCodes[idx].usedCount = Number(walletCodes[idx].usedCount || 0) + 1;
  walletCodes[idx].lastUsedAt = new Date().toISOString();
  await db.saveWalletCodes(walletCodes);

  res.json({ 
    success: true, 
    amount, 
    newBalance: users[userIndex].walletBalance,
    message: 'تم إضافة الرصيد بنجاح'
  });
});

// Project detail
app.get('/project/:id', async (req, res) => {
  const projects = await db.projects();
  const project = projects.find(p => p.id === req.params.id);
  if (!project) return res.status(404).send('Project not found');

  if (!isProjectVisibleToUser({ project, sessionUser: req.session.user })) {
    return res.status(403).send('Not allowed');
  }
  
  let hasPurchased = false;
  let canReview = false;
  let existingReview = null;
  if (req.session.user) {
    const purchases = await db.purchases();
    hasPurchased = purchases.some(p => p.userId === req.session.user.id && p.projectId === project.id);

    const hasApprovedPurchase = purchases.some(p => p.userId === req.session.user.id && p.projectId === project.id && p.status === 'approved');
    const reviews = await db.reviews();
    existingReview = reviews.find(r => r.userId === req.session.user.id && r.projectId === project.id) || null;
    canReview = hasApprovedPurchase && !existingReview;
  }

  const projectReviews = (await db.reviews()).filter(r => r.projectId === project.id);
  const avgRating = projectReviews.length
    ? (projectReviews.reduce((sum, r) => sum + Number(r.rating || 0), 0) / projectReviews.length)
    : 0;

  let canApplyReferral = false;
  let referralPrefill = '';
  if (req.session.user) {
    const users = await db.users();
    const currentUser = users.find(u => u.id === req.session.user.id);
    const purchases = await db.purchases();
    const hasAnyPurchase = purchases.some(p => p.userId === req.session.user.id);
    canApplyReferral = !!currentUser && !currentUser.referredBy && !hasAnyPurchase;
    referralPrefill = req.query.ref || '';
  }
  
  res.render('project', {
    project,
    user: req.session.user,
    hasPurchased,
    reviews: projectReviews,
    avgRating,
    canReview,
    existingReview,
    couponError: req.query.couponError || null,
    referralError: req.query.referralError || null,
    canApplyReferral,
    referralPrefill
  });
});

app.post('/project/:id/reviews', requireAuth, async (req, res) => {
  const projects = await db.projects();
  const project = projects.find(p => p.id === req.params.id);
  if (!project) return res.status(404).send('Project not found');

  const rating = Number(req.body.rating);
  const comment = (req.body.comment || '').trim();

  if (!Number.isFinite(rating) || rating < 1 || rating > 5) {
    return res.status(400).send('Invalid rating');
  }

  if (!comment) {
    return res.status(400).send('Comment is required');
  }

  const purchases = await db.purchases();
  const hasApprovedPurchase = purchases.some(
    p => p.userId === req.session.user.id && p.projectId === project.id && p.status === 'approved'
  );

  if (!hasApprovedPurchase) {
    return res.status(403).send('You can review only after an approved purchase');
  }

  const reviews = await db.reviews();
  const alreadyReviewed = reviews.some(r => r.userId === req.session.user.id && r.projectId === project.id);
  if (alreadyReviewed) {
    return res.status(400).send('You already reviewed this project');
  }

  reviews.push({
    id: uuidv4(),
    projectId: project.id,
    projectTitle: project.title,
    userId: req.session.user.id,
    userName: req.session.user.name,
    rating,
    comment,
    createdAt: new Date().toISOString()
  });

  await db.saveReviews(reviews);
  res.redirect(`/project/${project.id}`);
});

// Purchase - creates pending purchase request
app.post('/purchase/:id', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const projects = await db.projects();
  const project = projects.find(p => p && p.id === req.params.id);
  if (!project) return res.status(404).send('Project not found');

  if (!isProjectVisibleToUser({ project, sessionUser: req.session.user })) {
    return res.status(403).send('Not allowed');
  }

  const users = await db.users();
  const currentUserIndex = users.findIndex(u => u.id === req.session.user.id);
  const currentUser = currentUserIndex !== -1 ? users[currentUserIndex] : null;
  if (!currentUser) return res.status(404).send('User not found');

  const changed = ensureUserPaymentProfile({ users, user: currentUser });
  if (changed) {
    users[currentUserIndex] = currentUser;
    await db.saveUsers(users);
  }

  const purchases = await db.purchases();
  const alreadyPurchased = purchases.find(p => p && p.userId === req.session.user.id && p.projectId === project.id);
  if (alreadyPurchased) {
    return res.redirect('/my-purchases');
  }

  const couponCode = normalizeCouponCode(req.body.couponCode);
  let priceAfterDiscount = Number(project.price || 0);
  if (couponCode) {
    const coupons = await db.coupons();
    const coupon = coupons.find(c => normalizeCouponCode(c.code) === couponCode) || null;
    const eligibility = getCouponEligibility(coupon);
    if (!eligibility.eligible) {
      return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent(eligibility.reason || 'كوبون غير صالح')}`);
    }
    const calc = calculateDiscount({ priceBefore: project.price, coupon });
    priceAfterDiscount = calc.priceAfter;
  }

  const activeSubscription = await getActiveSubscriptionForUser({ userId: currentUser.id });
  const pricingUser = buildSessionUser({ user: currentUser, activeSubscription });
  const subPercent = getSubscriberDiscountPercent({ sessionUser: pricingUser });
  if (subPercent > 0) {
    const base = Number(priceAfterDiscount || 0);
    const subDiscount = Math.round((base * (subPercent / 100)) * 100) / 100;
    priceAfterDiscount = Math.round((base - subDiscount) * 100) / 100;
  }

  const enteredCardNumber = normalizeWalletCardNumber(req.body.walletCardNumber) || normalizeWalletCardNumber(currentUser.walletCardNumber);
  const payerUser = findUserByWalletCardNumber({ users, walletCardNumber: enteredCardNumber });
  if (!payerUser) {
    return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('رقم بطاقة المحفظة غير صحيح')}`);
  }

  const payerIndex = users.findIndex(u => u && u.id === payerUser.id);
  if (ensureUserPaymentProfile({ users, user: payerUser }) && payerIndex !== -1) {
    users[payerIndex] = payerUser;
    await db.saveUsers(users);
  }

  const walletBalance = Number(payerUser.walletBalance || 0);
  if (walletBalance < Number(priceAfterDiscount || 0)) {
    return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('رصيد المحفظة غير كافٍ')}`);
  }

  if (Number(priceAfterDiscount || 0) > HIGH_VALUE_PAYMENT_THRESHOLD && !payerUser.walletPaymentPasswordHash) {
    return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('صاحب البطاقة لم يضبط كلمة مرور البطاقة بعد')}`);
  }

  let attempt = null;
  try {
    ({ attempt } = await createWalletPaymentAttempt({
      req,
      buyerUser: currentUser,
      payerUser,
      amount: priceAfterDiscount,
      kind: 'single',
      payload: {
        projectId: project.id,
        couponCode,
        referralCode: normalizeReferralCode(req.body.referralCode)
      }
    }));
  } catch (error) {
    return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent(error.message || 'تعذر إرسال كود التحقق إلى صاحب البطاقة')}`);
  }

  req.session.user = {
    ...req.session.user,
    ...pricingUser
  };

  res.redirect(`/payment/verify/${attempt.id}`);
});

// Cart
app.get('/cart', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = await db.users();
  const currentUser = users.find(u => u.id === req.session.user.id);
  if (currentUser) {
    const changed = ensureUserPaymentProfile({ users, user: currentUser });
    if (changed) {
      const currentUserIndex = users.findIndex(u => u && u.id === currentUser.id);
      if (currentUserIndex !== -1) users[currentUserIndex] = currentUser;
      await db.saveUsers(users);
    }
    const activeSubscription = await getActiveSubscriptionForUser({ userId: currentUser.id });
    req.session.user = buildSessionUser({ user: currentUser, activeSubscription });
  }

  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.session.user.id });
  carts[cartIndex] = { ...cart, updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);

  const couponCode = (req.query.couponCode || '').toString();
  const summary = await summarizeCart({ cart, couponCode, sessionUser: req.session.user });

  res.render('cart', {
    user: req.session.user,
    cart,
    summary,
    couponCode,
    walletCardNumber: currentUser ? formatWalletCardNumber(currentUser.walletCardNumber) : '',
    error: req.query.error || null
  });
});

app.post('/cart/add', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const projectId = (req.body.projectId || '').toString();
  if (!projectId) return res.redirect('/?error=' + encodeURIComponent('مشروع غير صحيح'));

  const projects = await db.projects();
  const project = projects.find(p => p && p.id === projectId);
  if (!project) return res.status(404).send('Project not found');
  if (!isProjectVisibleToUser({ project, sessionUser: req.session.user })) {
    return res.status(403).send('Not allowed');
  }

  const purchases = await db.purchases();
  const alreadyPurchased = purchases.some(p => p.userId === req.session.user.id && p.projectId === projectId);
  if (alreadyPurchased) return res.redirect(`/project/${projectId}`);

  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.session.user.id });
  const items = Array.isArray(cart.items) ? cart.items : [];
  const exists = items.some(it => it && it.projectId === projectId);
  if (!exists) {
    items.push({
      projectId: project.id,
      projectTitle: project.title,
      price: Number(project.price || 0),
      createdAt: new Date().toISOString()
    });
  }

  carts[cartIndex] = { ...cart, items, updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  res.redirect('/cart');
});

app.post('/cart/remove', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const projectId = (req.body.projectId || '').toString();
  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.session.user.id });
  const nextItems = (Array.isArray(cart.items) ? cart.items : []).filter(it => it && it.projectId !== projectId);
  carts[cartIndex] = { ...cart, items: nextItems, updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  res.redirect('/cart');
});

app.post('/cart/clear', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.session.user.id });
  carts[cartIndex] = { ...cart, items: [], updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  res.redirect('/cart');
});

app.post('/cart/checkout', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const { carts, cart, cartIndex } = await getOrCreateCartForUser({ userId: req.session.user.id });
  const items = (cart && Array.isArray(cart.items)) ? cart.items : [];
  if (items.length === 0) return res.redirect('/cart?error=' + encodeURIComponent('السلة فارغة'));

  const users = await db.users();
  const currentUserIndex = users.findIndex(u => u.id === req.session.user.id);
  const currentUser = currentUserIndex !== -1 ? users[currentUserIndex] : null;
  if (!currentUser) return res.status(404).send('User not found');

  const changed = ensureUserPaymentProfile({ users, user: currentUser });
  if (changed) {
    users[currentUserIndex] = currentUser;
    await db.saveUsers(users);
  }

  const couponCode = normalizeCouponCode(req.body.couponCode);
  const activeSubscription = await getActiveSubscriptionForUser({ userId: currentUser.id });
  const pricingUser = buildSessionUser({ user: currentUser, activeSubscription });
  const summary = await summarizeCart({ cart, couponCode, sessionUser: pricingUser });
  const totalAfter = Number(summary.totalAfter || 0);
  if (!(totalAfter > 0)) return res.redirect('/cart?error=' + encodeURIComponent('إجمالي غير صحيح'));

  const enteredCardNumber = normalizeWalletCardNumber(req.body.walletCardNumber) || normalizeWalletCardNumber(currentUser.walletCardNumber);
  const payerUser = findUserByWalletCardNumber({ users, walletCardNumber: enteredCardNumber });
  if (!payerUser) {
    return res.redirect('/cart?error=' + encodeURIComponent('رقم بطاقة المحفظة غير صحيح'));
  }
  const payerIndex = users.findIndex(u => u && u.id === payerUser.id);
  if (ensureUserPaymentProfile({ users, user: payerUser }) && payerIndex !== -1) {
    users[payerIndex] = payerUser;
    await db.saveUsers(users);
  }

  const walletBalance = Number(payerUser.walletBalance || 0);
  if (walletBalance < totalAfter) {
    return res.redirect('/cart?error=' + encodeURIComponent('رصيد المحفظة غير كافٍ'));
  }

  if (totalAfter > HIGH_VALUE_PAYMENT_THRESHOLD && !payerUser.walletPaymentPasswordHash) {
    return res.redirect('/cart?error=' + encodeURIComponent('صاحب البطاقة لم يضبط كلمة مرور البطاقة بعد'));
  }

  let attempt = null;
  try {
    ({ attempt } = await createWalletPaymentAttempt({
      req,
      buyerUser: currentUser,
      payerUser,
      amount: totalAfter,
      kind: 'cart',
      payload: {
        couponCode,
        itemsSnapshot: items
      }
    }));
  } catch (error) {
    return res.redirect('/cart?error=' + encodeURIComponent(error.message || 'تعذر إرسال كود التحقق إلى صاحب البطاقة'));
  }

  carts[cartIndex] = { ...cart, updatedAt: new Date().toISOString() };
  await db.saveCarts(carts);
  req.session.user = { ...req.session.user, ...pricingUser };
  res.redirect(`/payment/verify/${attempt.id}`);
});

app.get('/payment/verify/:attemptId', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const attempt = await getWalletPaymentAttemptById({
    attemptId: req.params.attemptId,
    userId: req.session.user.id
  });
  if (!attempt) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('طلب الدفع غير موجود أو انتهت صلاحيته'));
  }

  const users = await db.users();
  const currentUser = users.find(u => u && u.id === req.session.user.id);
  if (!currentUser) {
    return res.redirect('/login');
  }
  const payerUser = users.find(u => u && u.id === attempt.payerUserId);
  if (!payerUser) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('صاحب البطاقة غير موجود'));
  }

  ensureUserPaymentProfile({ users, user: currentUser });
  ensureUserPaymentProfile({ users, user: payerUser });

  res.render('payment-verify', {
    user: req.session.user,
    attempt,
    walletCardNumber: formatWalletCardNumber(payerUser.walletCardNumber),
    payerName: payerUser.name || payerUser.email || 'صاحب البطاقة',
    ownerVisibleCode: attempt.payerUserId === req.session.user.id ? attempt.ownerVisibleCode : null,
    error: req.query.error || null,
    success: req.query.success || null,
    highValueThreshold: HIGH_VALUE_PAYMENT_THRESHOLD
  });
});

app.post('/payment/verify/:attemptId', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const attempt = await getWalletPaymentAttemptById({
    attemptId: req.params.attemptId,
    userId: req.session.user.id
  });
  if (!attempt) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('طلب الدفع غير موجود أو انتهت صلاحيته'));
  }

  const verificationCode = String(req.body.verificationCode || '').trim();
  if (!verificationCode || hashPaymentVerificationCode(verificationCode) !== attempt.codeHash) {
    return res.redirect(`/payment/verify/${attempt.id}?error=${encodeURIComponent('كود التحقق غير صحيح')}`);
  }

  const users = await db.users();
  const buyerUser = users.find(u => u && u.id === req.session.user.id);
  if (!buyerUser) {
    return res.redirect('/login');
  }
  const payerUser = users.find(u => u && u.id === attempt.payerUserId);
  if (!payerUser) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('صاحب البطاقة غير موجود'));
  }

  ensureUserPaymentProfile({ users, user: buyerUser });
  ensureUserPaymentProfile({ users, user: payerUser });

  if (attempt.requiresPassword) {
    const walletPassword = String(req.body.walletPassword || '');
    if (!payerUser.walletPaymentPasswordHash || !bcrypt.compareSync(walletPassword, payerUser.walletPaymentPasswordHash)) {
      return res.redirect(`/payment/verify/${attempt.id}?error=${encodeURIComponent('كلمة مرور البطاقة غير صحيحة')}`);
    }
  }

  let result = null;
  if (attempt.kind === 'single') {
    result = await finalizeSingleProjectPurchase({
      req,
      buyerUserId: req.session.user.id,
      payerUserId: attempt.payerUserId,
      projectId: attempt.payload ? attempt.payload.projectId : null,
      couponCode: attempt.payload ? attempt.payload.couponCode : '',
      referralCode: attempt.payload ? attempt.payload.referralCode : ''
    });
  } else if (attempt.kind === 'cart') {
    result = await finalizeCartPurchase({
      req,
      buyerUserId: req.session.user.id,
      payerUserId: attempt.payerUserId,
      itemsSnapshot: attempt.payload ? attempt.payload.itemsSnapshot : [],
      couponCode: attempt.payload ? attempt.payload.couponCode : ''
    });
  }

  if (!result || !result.ok) {
    return res.redirect((result && result.redirect) ? result.redirect : '/cart?error=' + encodeURIComponent('تعذر إتمام الدفع'));
  }

  await markWalletPaymentAttemptUsed({ attemptId: attempt.id });
  return res.redirect(result.redirect || '/my-purchases');
});

// My purchases
app.get('/my-purchases', requireAuth, async (req, res) => {
  const users = await db.users();
  const currentUser = users.find(u => u.id === req.session.user.id);
  if (currentUser) {
    const changed = ensureUserPaymentProfile({ users, user: currentUser });
    if (changed) {
      const currentUserIndex = users.findIndex(u => u && u.id === currentUser.id);
      if (currentUserIndex !== -1) users[currentUserIndex] = currentUser;
      await db.saveUsers(users);
    }
    req.session.user = {
      ...req.session.user,
      referralCode: currentUser.referralCode || null,
      walletBalance: Number(currentUser.walletBalance || 0),
      loyaltyPoints: normalizeLoyaltyPoints(currentUser.loyaltyPoints),
      walletCardNumberMasked: maskWalletCardNumber(currentUser.walletCardNumber),
      hasWalletPaymentPassword: Boolean(currentUser.walletPaymentPasswordHash)
    };
  }
  const purchases = (await db.purchases()).filter(p => p.userId === req.session.user.id);
  const invoices = (await db.invoices()).filter(i => i && i.userId === req.session.user.id);
  const subscriptionPlans = await db.subscriptionPlans();
  const subscriptions = (await db.subscriptions()).filter(s => s && s.userId === req.session.user.id);
  const subscriptionPayments = (await db.subscriptionPayments()).filter(p => p && p.userId === req.session.user.id);

  res.render('my-purchases', {
    purchases,
    invoices,
    user: req.session.user,
    walletCardNumber: currentUser ? formatWalletCardNumber(currentUser.walletCardNumber) : null,
    hasWalletPaymentPassword: currentUser ? Boolean(currentUser.walletPaymentPasswordHash) : false,
    walletCardError: req.query.walletCardError || null,
    walletCardSuccess: req.query.walletCardSuccess || null,
    paymentError: req.query.paymentError || null,
    paymentSuccess: req.query.paymentSuccess || null,
    redeemError: req.query.redeemError || null,
    redeemSuccess: req.query.redeemSuccess || null,
    subscriptionPlans,
    subscriptions,
    subscriptionPayments
  });
});

app.get('/notifications', requireAuth, async (req, res) => {
  const notifications = await getUserNotifications({ userId: req.session.user.id });
  res.render('notifications', {
    user: req.session.user,
    notifications,
    success: req.query.success || null
  });
});

app.get('/api/notifications/summary', requireAuth, async (req, res) => {
  const allNotifications = await getUserNotifications({ userId: req.session.user.id });
  const notifications = allNotifications.slice(0, 8);
  const unreadCount = allNotifications.filter(notification => !notification.readAt).length;

  return res.json({
    unreadCount,
    notifications: notifications.map(serializeNotification)
  });
});

app.post('/api/notifications/:id/read', requireAuth, async (req, res) => {
  const ok = await markNotificationRead({ userId: req.session.user.id, notificationId: req.params.id });
  return res.json({ ok });
});

app.post('/api/notifications/read-all', requireAuth, async (req, res) => {
  const updated = await markAllNotificationsRead({ userId: req.session.user.id });
  return res.json({ ok: true, updated });
});

app.post('/notifications/:id/read', requireAuth, async (req, res) => {
  await markNotificationRead({ userId: req.session.user.id, notificationId: req.params.id });
  const nextUrl = (req.body.next || '').trim();
  return res.redirect(nextUrl || '/notifications');
});

app.post('/notifications/read-all', requireAuth, async (req, res) => {
  await markAllNotificationsRead({ userId: req.session.user.id });
  return res.redirect('/notifications?success=' + encodeURIComponent('تم تعليم كل الإشعارات كمقروءة'));
});

app.get('/loyalty', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = await db.users();
  const currentUser = users.find(u => u.id === req.session.user.id);
  if (!currentUser) return res.redirect('/?error=' + encodeURIComponent('المستخدم غير موجود'));

  const settings = await db.loyaltySettings();
  const redeem = (settings && settings.redeem) ? settings.redeem : {};
  const minPoints = normalizeLoyaltyPoints(redeem.minPoints);
  const pointsToEGP = Number(redeem.pointsToEGP || 0);

  req.session.user = {
    ...req.session.user,
    walletBalance: Number(currentUser.walletBalance || 0),
    loyaltyPoints: normalizeLoyaltyPoints(currentUser.loyaltyPoints),
    walletCardNumberMasked: maskWalletCardNumber(currentUser.walletCardNumber),
    hasWalletPaymentPassword: Boolean(currentUser.walletPaymentPasswordHash)
  };

  res.render('loyalty', {
    user: req.session.user,
    points: normalizeLoyaltyPoints(currentUser.loyaltyPoints),
    minPoints,
    pointsToEGP: Number.isFinite(pointsToEGP) ? pointsToEGP : 0,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/wallet-card/password', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const password = String(req.body.password || '');
  const confirmPassword = String(req.body.confirmPassword || '');

  if (password.length < 4) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('كلمة مرور البطاقة يجب أن تكون 4 أحرف أو أرقام على الأقل'));
  }
  if (password !== confirmPassword) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('تأكيد كلمة المرور غير مطابق'));
  }

  const users = await db.users();
  const idx = users.findIndex(u => u && u.id === req.session.user.id);
  if (idx === -1) {
    return res.redirect('/my-purchases?walletCardError=' + encodeURIComponent('المستخدم غير موجود'));
  }

  ensureUserPaymentProfile({ users, user: users[idx] });
  users[idx].walletPaymentPasswordHash = bcrypt.hashSync(password, 10);
  await db.saveUsers(users);

  req.session.user = {
    ...req.session.user,
    hasWalletPaymentPassword: true,
    walletCardNumberMasked: maskWalletCardNumber(users[idx].walletCardNumber)
  };

  return res.redirect('/my-purchases?walletCardSuccess=' + encodeURIComponent('تم تحديث كلمة مرور البطاقة'));
});

app.post('/loyalty/redeem', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = await db.users();
  const idx = users.findIndex(u => u.id === req.session.user.id);
  if (idx === -1) return res.redirect('/loyalty?error=' + encodeURIComponent('المستخدم غير موجود'));

  const settings = await db.loyaltySettings();
  if (!settings || !settings.enabled) return res.redirect('/loyalty?error=' + encodeURIComponent('النظام غير متاح حالياً'));
  const redeem = settings.redeem || {};
  if (!redeem.enabled) return res.redirect('/loyalty?error=' + encodeURIComponent('الاستبدال غير متاح حالياً'));

  const pointsRequested = normalizeLoyaltyPoints(req.body.points);
  const minPoints = normalizeLoyaltyPoints(redeem.minPoints);
  const rate = Number(redeem.pointsToEGP || 0);
  if (!Number.isFinite(rate) || rate <= 0) return res.redirect('/loyalty?error=' + encodeURIComponent('إعدادات الاستبدال غير صحيحة'));

  if (pointsRequested <= 0) return res.redirect('/loyalty?error=' + encodeURIComponent('عدد النقاط غير صحيح'));
  if (pointsRequested < minPoints) return res.redirect('/loyalty?error=' + encodeURIComponent('أقل عدد للاستبدال غير محقق'));

  const currentPoints = normalizeLoyaltyPoints(users[idx].loyaltyPoints);
  if (currentPoints < pointsRequested) return res.redirect('/loyalty?error=' + encodeURIComponent('نقاط غير كافية'));

  const credit = Math.round((pointsRequested * rate) * 100) / 100;
  if (!Number.isFinite(credit) || credit <= 0) return res.redirect('/loyalty?error=' + encodeURIComponent('قيمة الاستبدال غير صحيحة'));

  users[idx].loyaltyPoints = currentPoints - pointsRequested;
  users[idx].walletBalance = Math.round((Number(users[idx].walletBalance || 0) + credit) * 100) / 100;
  await db.saveUsers(users);

  req.session.user = {
    ...req.session.user,
    walletBalance: Number(users[idx].walletBalance || 0),
    loyaltyPoints: normalizeLoyaltyPoints(users[idx].loyaltyPoints)
  };

  await createNotification({
    userId: users[idx].id,
    title: 'تم تحويل النقاط إلى رصيد',
    message: `تم استبدال ${pointsRequested} نقطة وإضافة ${credit} جنيه إلى محفظتك.`,
    link: '/loyalty',
    kind: 'success',
    meta: {
      type: 'loyalty_redeem'
    }
  });

  res.redirect('/loyalty?success=' + encodeURIComponent('تم الاستبدال بنجاح'));
});

// Wallet - redeem top-up code
app.post('/wallet/redeem', requireAuth, async (req, res) => {
  const code = normalizeWalletCode(req.body.code);
  if (!code) return res.redirect(`/my-purchases?redeemError=${encodeURIComponent('الكود مطلوب')}`);

  const walletCodes = await db.walletCodes();
  const idx = walletCodes.findIndex(c => normalizeWalletCode(c.code) === code);
  if (idx === -1) {
    return res.redirect(`/my-purchases?redeemError=${encodeURIComponent('الكود غير صحيح')}`);
  }

  const eligibility = getWalletCodeEligibility(walletCodes[idx]);
  if (!eligibility.eligible) {
    return res.redirect(`/my-purchases?redeemError=${encodeURIComponent(eligibility.reason || 'الكود غير صالح')}`);
  }

  const amount = Number(walletCodes[idx].amount || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return res.redirect(`/my-purchases?redeemError=${encodeURIComponent('قيمة الكود غير صحيحة')}`);
  }

  const users = await db.users();
  const userIndex = users.findIndex(u => u.id === req.session.user.id);
  if (userIndex === -1) return res.status(404).send('User not found');

  users[userIndex].walletBalance = Number(users[userIndex].walletBalance || 0) + amount;
  await db.saveUsers(users);

  walletCodes[idx].usedCount = Number(walletCodes[idx].usedCount || 0) + 1;
  walletCodes[idx].lastUsedAt = new Date().toISOString();
  await db.saveWalletCodes(walletCodes);

  req.session.user = {
    ...req.session.user,
    walletBalance: Number(users[userIndex].walletBalance || 0)
  };

  await createNotification({
    userId: users[userIndex].id,
    title: 'تم شحن المحفظة',
    message: `تم إضافة ${amount} جنيه إلى محفظتك باستخدام كود الشحن ${code}.`,
    link: '/my-purchases',
    kind: 'success',
    meta: {
      type: 'wallet_topup'
    }
  });

  return res.redirect(`/my-purchases?redeemSuccess=${encodeURIComponent('تم إضافة الرصيد بنجاح')}`);
});

// Protected download - only approved purchases can download
app.get('/download/:purchaseId', requireAuth, async (req, res) => {
  const purchases = await db.purchases();
  const purchase = purchases.find(p => p.id === req.params.purchaseId && p.userId === req.session.user.id);
  
  if (!purchase || !purchase.filePath) {
    return res.status(403).send('Access denied or file not available');
  }
  
  if (purchase.status !== 'approved') {
    return res.status(403).send('Purchase not approved yet. Please wait for admin approval.');
  }
  
  const absoluteFilePath = toAbsolutePath(purchase.filePath);
  if (!absoluteFilePath || !fs.existsSync(absoluteFilePath)) {
    return res.status(404).send('File not found');
  }
  
  const downloadFileName = getDownloadFileName(purchase);
  res.download(absoluteFilePath, downloadFileName);
});

// Admin Dashboard
app.get('/admin', requireAdmin, async (req, res) => {
  const users = await db.users();
  const projects = await db.projects();
  const purchases = await db.purchases();

  const now = new Date();
  const startOfDay = new Date(now);
  startOfDay.setHours(0, 0, 0, 0);

  const startOfWeek = new Date(startOfDay);
  startOfWeek.setDate(startOfWeek.getDate() - 6);

  const startOfMonth = new Date(startOfDay);
  startOfMonth.setDate(1);

  const getPurchaseDate = (p) => {
    const value = p && (p.approvedAt || p.createdAt);
    const d = value ? new Date(value) : null;
    if (!d || Number.isNaN(d.getTime())) return null;
    return d;
  };

  const approvedPurchases = purchases.filter(p => p.status === 'approved');
  const approvedToday = approvedPurchases.filter(p => {
    const d = getPurchaseDate(p);
    return d && d >= startOfDay;
  });
  const approvedWeek = approvedPurchases.filter(p => {
    const d = getPurchaseDate(p);
    return d && d >= startOfWeek;
  });
  const approvedMonth = approvedPurchases.filter(p => {
    const d = getPurchaseDate(p);
    return d && d >= startOfMonth;
  });

  const sum = (arr, selector) => arr.reduce((acc, item) => acc + Number(selector(item) || 0), 0);

  const reports = {
    salesToday: Math.round(sum(approvedToday, p => p.price) * 100) / 100,
    salesWeek: Math.round(sum(approvedWeek, p => p.price) * 100) / 100,
    salesMonth: Math.round(sum(approvedMonth, p => p.price) * 100) / 100,
    totalDiscounts: Math.round(sum(approvedPurchases, p => p.discountAmount) * 100) / 100,
    totalPurchases: purchases.length,
    approvedCount: purchases.filter(p => p.status === 'approved').length,
    rejectedCount: purchases.filter(p => p.status === 'rejected').length
  };

  const denom = reports.totalPurchases || 0;
  reports.approvalRate = denom ? Math.round((reports.approvedCount / denom) * 1000) / 10 : 0;
  reports.rejectionRate = denom ? Math.round((reports.rejectedCount / denom) * 1000) / 10 : 0;

  const projectTitleById = new Map(projects.map(p => [p.id, p.title]));
  const topMap = new Map();
  for (const p of approvedPurchases) {
    const key = p.projectId || p.projectTitle || 'unknown';
    if (!topMap.has(key)) {
      topMap.set(key, { key, projectId: p.projectId || null, title: projectTitleById.get(p.projectId) || p.projectTitle || 'غير معروف', count: 0, revenue: 0 });
    }
    const row = topMap.get(key);
    row.count += 1;
    row.revenue += Number(p.price || 0);
  }
  const topProjects = Array.from(topMap.values())
    .map(r => ({ ...r, revenue: Math.round(r.revenue * 100) / 100 }))
    .sort((a, b) => (b.count - a.count) || (b.revenue - a.revenue))
    .slice(0, 10);

  res.render('admin/dashboard', { users, projects, purchases, topProjects, reports, user: req.session.user });
});

// Admin Team - Group chat for admins
app.get('/admin/team', requireAdmin, async (req, res) => {
  const messages = await db.adminTeamMessages()
    .slice()
    .sort((a, b) => new Date(a.createdAt || 0).getTime() - new Date(b.createdAt || 0).getTime());
  res.render('admin/team', { user: req.session.user, messages, error: req.query.error || null });
});

app.post('/admin/team/message', requireAdmin, async (req, res) => {
  const content = (req.body.content || '').toString().trim();
  if (!content) return res.redirect('/admin/team?error=' + encodeURIComponent('اكتب رسالة'));

  const messages = await db.adminTeamMessages();
  const msg = {
    id: uuidv4(),
    type: 'text',
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    content,
    createdAt: new Date().toISOString()
  };
  messages.push(msg);
  await db.saveAdminTeamMessages(messages);

  io.to('admin-team').emit('admin-team-message', msg);
  res.redirect('/admin/team');
});

app.post('/admin/team/upload', requireAdmin, adminTeamUpload.single('file'), async (req, res) => {
  if (!req.file) return res.redirect('/admin/team?error=' + encodeURIComponent('لم يتم رفع الملف'));

  const storedPath = `uploads/admin-team/${req.file.filename}`;
  const messages = await db.adminTeamMessages();

  const msg = {
    id: uuidv4(),
    type: 'file',
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    filePath: storedPath,
    originalFileName: req.file.originalname || null,
    mimeType: req.file.mimetype || null,
    size: req.file.size || null,
    createdAt: new Date().toISOString()
  };
  messages.push(msg);
  await db.saveAdminTeamMessages(messages);

  io.to('admin-team').emit('admin-team-message', msg);
  res.redirect('/admin/team');
});

app.post('/admin/team/meeting', requireAdmin, async (req, res) => {
  const roomId = `${uuidv4()}`;
  const meetingLink = `/meet/${roomId}`;

  const messages = await db.adminTeamMessages();
  const msg = {
    id: uuidv4(),
    type: 'meeting',
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    roomId,
    meetingLink,
    createdAt: new Date().toISOString()
  };
  messages.push(msg);
  await db.saveAdminTeamMessages(messages);

  io.to('admin-team').emit('admin-team-message', msg);
  res.redirect('/admin/team');
});

const isValidFutureSlot = (slot) => {
  if (!slot) return false;
  const startAt = slot.startAt ? new Date(slot.startAt) : null;
  if (!startAt || Number.isNaN(startAt.getTime())) return false;
  return startAt.getTime() > Date.now();
};

const getAdminUsers = async () => {
  return (await db.users()).filter(u => u.role === 'admin');
};

const formatSlotLabel = (slot) => {
  const startAt = slot && slot.startAt ? new Date(slot.startAt) : null;
  if (!startAt || Number.isNaN(startAt.getTime())) return '';
  const yyyy = startAt.getFullYear();
  const mm = String(startAt.getMonth() + 1).padStart(2, '0');
  const dd = String(startAt.getDate()).padStart(2, '0');
  const hh = String(startAt.getHours()).padStart(2, '0');
  const mi = String(startAt.getMinutes()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
};

const buildMeetingRoomId = ({ adminId, userId, slotId }) => {
  const safe = (v) => String(v || '').toLowerCase().replace(/[^a-z0-9\-]/g, '-');
  return `codentra-${safe(adminId)}-${safe(userId)}-${safe(slotId)}`;
};

const canAccessMeetingRoom = async ({ roomId, sessionUser }) => {
  if (!roomId || !sessionUser || !sessionUser.id) return false;
  if (typeof roomId === 'string' && roomId.startsWith('team-')) {
    return sessionUser.role === 'admin';
  }
  const data = await migrateAppointmentsBookingsMeetingLinks();
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];
  const booking = bookings.find(b => b && b.roomId === roomId) || null;
  if (!booking) return false;
  return booking.userId === sessionUser.id || booking.adminId === sessionUser.id;
};

const migrateAppointmentsBookingsMeetingLinks = async () => {
  const data = await db.appointments();
  const timeSlots = Array.isArray(data.timeSlots) ? data.timeSlots : [];
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];

  let changed = false;
  const nextBookings = bookings.map(b => {
    if (!b) return b;

    const hasInternalLink = typeof b.meetingLink === 'string' && b.meetingLink.startsWith('/meet/');
    const hasRoomId = typeof b.roomId === 'string' && b.roomId.trim();

    if (hasInternalLink && hasRoomId) return b;

    const fallbackSlotId = b.slotId || b.id;
    const roomId = hasRoomId
      ? b.roomId
      : buildMeetingRoomId({ adminId: b.adminId, userId: b.userId, slotId: fallbackSlotId });

    const meetingLink = `/meet/${roomId}`;
    const needsUpdate = (b.roomId !== roomId) || (b.meetingLink !== meetingLink);
    if (!needsUpdate) return b;

    changed = true;
    return { ...b, roomId, meetingLink };
  });

  if (changed) {
    await db.saveAppointments({ timeSlots, bookings: nextBookings });
    return { timeSlots, bookings: nextBookings };
  }
  return { timeSlots, bookings };
};

// ========== APPOINTMENTS SYSTEM ==========

// User - Browse available admin slots
app.get('/appointments', requireAuth, async (req, res) => {
  const admins = await getAdminUsers();
  const data = await migrateAppointmentsBookingsMeetingLinks();

  const availableSlots = (data.timeSlots || [])
    .filter(s => s && s.status === 'available')
    .filter(isValidFutureSlot)
    .sort((a, b) => new Date(a.startAt).getTime() - new Date(b.startAt).getTime());

  const adminsById = new Map(admins.map(a => [a.id, a]));
  const slotsByAdmin = {};
  for (const slot of availableSlots) {
    if (!slot || !slot.adminId) continue;
    if (!adminsById.has(slot.adminId)) continue;
    if (!slotsByAdmin[slot.adminId]) slotsByAdmin[slot.adminId] = [];
    slotsByAdmin[slot.adminId].push(slot);
  }

  const adminsWithSlots = admins
    .map(a => ({ ...a, slots: (slotsByAdmin[a.id] || []).slice() }))
    .filter(a => a.slots.length > 0);

  res.render('appointments', {
    user: req.session.user,
    admins: adminsWithSlots,
    success: req.query.success || null,
    error: req.query.error || null
  });
});

// User - Book a slot
app.post('/appointments/book', requireAuth, async (req, res) => {
  const { slotId, notes } = req.body;
  if (!slotId) return res.redirect('/appointments?error=اختر ميعاد للحجز');

  const admins = await getAdminUsers();
  const adminsById = new Map(admins.map(a => [a.id, a]));

  const data = await db.appointments();
  const timeSlots = Array.isArray(data.timeSlots) ? data.timeSlots : [];
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];

  const slotIndex = timeSlots.findIndex(s => s && s.id === slotId);
  if (slotIndex === -1) return res.redirect('/appointments?error=الموعد غير موجود');

  const slot = timeSlots[slotIndex];
  if (!adminsById.has(slot.adminId)) return res.redirect('/appointments?error=الأدمن غير موجود');
  if (slot.status !== 'available') return res.redirect('/appointments?error=الموعد غير متاح');
  if (!isValidFutureSlot(slot)) return res.redirect('/appointments?error=الموعد انتهى');

  timeSlots[slotIndex] = { ...slot, status: 'booked' };
  const roomId = buildMeetingRoomId({ adminId: slot.adminId, userId: req.session.user.id, slotId: slot.id });
  const booking = {
    id: uuidv4(),
    userId: req.session.user.id,
    userName: req.session.user.name,
    adminId: slot.adminId,
    adminName: adminsById.get(slot.adminId).name,
    slotId: slot.id,
    startAt: slot.startAt,
    durationMinutes: Number(slot.durationMinutes || 30),
    roomId,
    meetingLink: `/meet/${roomId}`,
    notes: (notes && String(notes).trim()) ? String(notes).trim() : null,
    status: 'confirmed',
    createdAt: new Date().toISOString()
  };
  bookings.push(booking);

  await db.saveAppointments({ timeSlots, bookings });
  res.redirect('/my-appointments?success=تم الحجز بنجاح');
});

// User - View my appointments
app.get('/my-appointments', requireAuth, async (req, res) => {
  const data = await migrateAppointmentsBookingsMeetingLinks();
  const myBookings = (data.bookings || [])
    .filter(b => b && b.userId === req.session.user.id)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());

  res.render('my-appointments', {
    user: req.session.user,
    bookings: myBookings,
    success: req.query.success || null,
    error: req.query.error || null
  });
});

// Admin - Manage availability and see bookings
app.get('/admin/appointments', requireAdminPermission(ADMIN_PERMISSIONS.appointments), async (req, res) => {
  const data = await migrateAppointmentsBookingsMeetingLinks();
  const adminId = req.session.user.id;

  const mySlots = (data.timeSlots || [])
    .filter(s => s && s.adminId === adminId)
    .sort((a, b) => new Date(a.startAt || 0).getTime() - new Date(b.startAt || 0).getTime())
    .map(s => ({ ...s, label: formatSlotLabel(s) }));

  const myBookings = (data.bookings || [])
    .filter(b => b && b.adminId === adminId)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());

  res.render('admin/appointments', {
    user: req.session.user,
    slots: mySlots,
    bookings: myBookings,
    success: req.query.success || null,
    error: req.query.error || null
  });
});

// Admin - Create available slot
app.post('/admin/appointments/slots', requireAdminPermission(ADMIN_PERMISSIONS.appointments), async (req, res) => {
  const { date, time, durationMinutes } = req.body;
  if (!date || !time) return res.redirect('/admin/appointments?error=أدخل التاريخ والوقت');

  const duration = Number(durationMinutes || 30);
  if (!Number.isFinite(duration) || duration < 15 || duration > 240) {
    return res.redirect('/admin/appointments?error=مدة الجلسة غير صحيحة');
  }

  const startAt = new Date(`${date}T${time}:00`);
  if (Number.isNaN(startAt.getTime())) return res.redirect('/admin/appointments?error=التاريخ أو الوقت غير صحيح');
  if (startAt.getTime() <= Date.now() + 60 * 1000) return res.redirect('/admin/appointments?error=اختر وقت في المستقبل');

  const data = await db.appointments();
  const timeSlots = Array.isArray(data.timeSlots) ? data.timeSlots : [];
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];

  const adminId = req.session.user.id;
  const exists = timeSlots.some(s => s && s.adminId === adminId && s.startAt === startAt.toISOString() && s.status === 'available');
  if (exists) return res.redirect('/admin/appointments?error=هذا الموعد موجود بالفعل');

  timeSlots.push({
    id: uuidv4(),
    adminId,
    startAt: startAt.toISOString(),
    durationMinutes: duration,
    status: 'available',
    createdAt: new Date().toISOString()
  });

  await db.saveAppointments({ timeSlots, bookings });
  res.redirect('/admin/appointments?success=تم إضافة الموعد');
});

// Admin - Delete available slot
app.post('/admin/appointments/slots/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.appointments), async (req, res) => {
  const slotId = req.params.id;
  const data = await db.appointments();
  const timeSlots = Array.isArray(data.timeSlots) ? data.timeSlots : [];
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];

  const adminId = req.session.user.id;
  const slotIndex = timeSlots.findIndex(s => s && s.id === slotId && s.adminId === adminId);
  if (slotIndex === -1) return res.redirect('/admin/appointments?error=الموعد غير موجود');

  const slot = timeSlots[slotIndex];
  if (slot.status !== 'available') return res.redirect('/admin/appointments?error=لا يمكن حذف موعد محجوز');

  timeSlots.splice(slotIndex, 1);
  await db.saveAppointments({ timeSlots, bookings });
  res.redirect('/admin/appointments?success=تم حذف الموعد');
});

app.get('/meet/:roomId', requireAuth, async (req, res) => {
  const roomId = req.params.roomId;
  if (typeof roomId === 'string' && roomId.startsWith('team-')) {
    if (!req.session.user || req.session.user.role !== 'admin') {
      return res.status(403).send('Not allowed');
    }
    return res.render('meet', { user: req.session.user, roomId });
  }
  const data = await migrateAppointmentsBookingsMeetingLinks();
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];
  const booking = bookings.find(b => b && b.roomId === roomId) || null;
  if (!booking) return res.status(404).send('Meeting not found');

  const uid = req.session.user && req.session.user.id;
  const isAllowed = uid && (booking.userId === uid || booking.adminId === uid);
  if (!isAllowed) return res.status(403).send('Not allowed');

  res.render('meet', { user: req.session.user, roomId });
});

app.post('/meet/:roomId/signal', requireAuth, async (req, res) => {
  if (!USE_DATABASE) return res.status(400).json({ ok: false, error: 'Database required' });
  const roomId = req.params.roomId;
  const isAllowed = await canAccessMeetingRoom({ roomId, sessionUser: req.session.user });
  if (!isAllowed) return res.status(403).json({ ok: false, error: 'Not allowed' });

  const payload = req.body && req.body.payload ? req.body.payload : req.body;
  if (!payload || typeof payload !== 'object' || !payload.type) {
    return res.status(400).json({ ok: false, error: 'Invalid payload' });
  }

  await ensureDatabase();
  const pool = getPool();
  const senderId = req.session.user ? req.session.user.id : null;
  const result = await pool.query(
    'INSERT INTO meeting_signals (room_id, sender_id, payload) VALUES ($1, $2, $3::jsonb) RETURNING id',
    [roomId, senderId, JSON.stringify(payload)]
  );
  res.json({ ok: true, id: result.rows[0] ? result.rows[0].id : null });
});

app.get('/meet/:roomId/signal', requireAuth, async (req, res) => {
  if (!USE_DATABASE) return res.json({ ok: true, items: [], lastId: Number(req.query.since) || 0 });
  const roomId = req.params.roomId;
  const isAllowed = await canAccessMeetingRoom({ roomId, sessionUser: req.session.user });
  if (!isAllowed) return res.status(403).json({ ok: false, error: 'Not allowed' });

  const since = Math.max(parseInt(req.query.since || '0', 10) || 0, 0);
  const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10) || 50, 1), 200);

  await ensureDatabase();
  const pool = getPool();
  const result = await pool.query(
    'SELECT id, sender_id, payload FROM meeting_signals WHERE room_id = $1 AND id > $2 ORDER BY id ASC LIMIT $3',
    [roomId, since, limit]
  );
  const rows = result.rows || [];
  const lastId = rows.length ? rows[rows.length - 1].id : since;
  res.json({ ok: true, items: rows, lastId });
});

app.post('/meet/:roomId/recording', requireAdmin, meetingRecordingUpload.single('recording'), async (req, res) => {
  const roomId = req.params.roomId;
  const data = await migrateAppointmentsBookingsMeetingLinks();
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];
  const booking = bookings.find(b => b && b.roomId === roomId) || null;
  if (!booking) return res.status(404).json({ ok: false, error: 'Meeting not found' });

  const adminId = req.session.user && req.session.user.id;
  if (!adminId || booking.adminId !== adminId) return res.status(403).json({ ok: false, error: 'Not allowed' });
  if (!req.file) return res.status(400).json({ ok: false, error: 'No recording uploaded' });

  const absoluteFilePath = req.file && req.file.path ? req.file.path : null;
  let sha256 = null;
  try {
    if (absoluteFilePath && fs.existsSync(absoluteFilePath)) {
      const buf = fs.readFileSync(absoluteFilePath);
      sha256 = crypto.createHash('sha256').update(buf).digest('hex');
    }
  } catch (e) {
    sha256 = null;
  }

  const storedPath = `uploads/meeting-recordings/${req.file.filename}`;
  const recordingsRaw = await db.meetingRecordings();
  const recordings = Array.isArray(recordingsRaw) ? recordingsRaw : [];
  recordings.push({
    id: uuidv4(),
    roomId,
    bookingId: booking.id || null,
    adminId: booking.adminId,
    adminName: booking.adminName || null,
    userId: booking.userId,
    userName: booking.userName || null,
    startAt: booking.startAt || null,
    durationMinutes: booking.durationMinutes || null,
    filePath: storedPath,
    originalFileName: req.file.originalname || 'meeting.webm',
    mimeType: req.file.mimetype || null,
    size: req.file.size || null,
    sha256,
    createdAt: new Date().toISOString()
  });
  await db.saveMeetingRecordings(recordings);

  res.json({ ok: true });
});

app.get('/admin/meeting-recordings', requireSuperAdmin, async (req, res) => {
  const recordingsRaw = await db.meetingRecordings();
  const recordings = (Array.isArray(recordingsRaw) ? recordingsRaw.slice() : []);
  recordings.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  res.render('admin/meeting-recordings', { user: req.session.user, recordings });
});

app.get('/admin/meeting-recordings/:id/verify', requireSuperAdmin, async (req, res) => {
  const id = req.params.id;
  const recordingsRaw = await db.meetingRecordings();
  const recordings = Array.isArray(recordingsRaw) ? recordingsRaw : [];
  const rec = recordings.find(r => r && r.id === id);
  if (!rec) return res.redirect('/admin/meeting-recordings');

  const abs = rec.filePath ? toAbsolutePath(rec.filePath) : null;
  let currentSha256 = null;
  let fileExists = false;
  try {
    if (abs && fs.existsSync(abs)) {
      fileExists = true;
      const buf = fs.readFileSync(abs);
      currentSha256 = crypto.createHash('sha256').update(buf).digest('hex');
    }
  } catch (e) {
    currentSha256 = null;
  }

  const storedSha256 = rec.sha256 || null;
  const match = !!(fileExists && storedSha256 && currentSha256 && storedSha256 === currentSha256);

  res.render('admin/meeting-recording-verify', {
    user: req.session.user,
    recording: rec,
    fileExists,
    storedSha256,
    currentSha256,
    match
  });
});

// Super Admin - Admins management
app.get('/admin/admins', requireSuperAdmin, async (req, res) => {
  const users = await db.users();
  const admins = users.filter(u => u.role === 'admin');
  res.render('admin/admins', {
    admins,
    user: req.session.user,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/admin/admins', requireSuperAdmin, async (req, res) => {
  const name = (req.body.name || '').trim();
  const email = (req.body.email || '').trim().toLowerCase();
  const password = String(req.body.password || '');

  const users = await db.users();

  if (!name || !email || !password) {
    return res.redirect(`/admin/admins?error=${encodeURIComponent('جميع الحقول مطلوبة')}`);
  }
  if (users.some(u => (u.email || '').toLowerCase() === email)) {
    return res.redirect(`/admin/admins?error=${encodeURIComponent('البريد الإلكتروني مستخدم بالفعل')}`);
  }
  if (password.length < 6) {
    return res.redirect(`/admin/admins?error=${encodeURIComponent('كلمة المرور يجب أن تكون 6 أحرف على الأقل')}`);
  }

  const adminPermissions = {
    [ADMIN_PERMISSIONS.projects]: Boolean(req.body.perm_projects),
    [ADMIN_PERMISSIONS.purchases]: Boolean(req.body.perm_purchases),
    [ADMIN_PERMISSIONS.modifications]: Boolean(req.body.perm_modifications),
    [ADMIN_PERMISSIONS.messages]: Boolean(req.body.perm_messages),
    [ADMIN_PERMISSIONS.reviews]: Boolean(req.body.perm_reviews),
    [ADMIN_PERMISSIONS.coupons]: Boolean(req.body.perm_coupons),
    [ADMIN_PERMISSIONS.referrals]: Boolean(req.body.perm_referrals),
    [ADMIN_PERMISSIONS.walletCodes]: Boolean(req.body.perm_walletCodes),
    [ADMIN_PERMISSIONS.walletBalances]: Boolean(req.body.perm_walletBalances),
    [ADMIN_PERMISSIONS.users]: Boolean(req.body.perm_users),
    [ADMIN_PERMISSIONS.appointments]: Boolean(req.body.perm_appointments),
    [ADMIN_PERMISSIONS.subscriptionCoupons]: Boolean(req.body.perm_subscriptionCoupons),
    [ADMIN_PERMISSIONS.subscriptionPlans]: Boolean(req.body.perm_subscriptionPlans),
    [ADMIN_PERMISSIONS.subscriptionReports]: Boolean(req.body.perm_subscriptionReports)
  };

  users.push({
    id: uuidv4(),
    name,
    email,
    password: bcrypt.hashSync(password, 10),
    role: 'admin',
    isSuperAdmin: false,
    adminPermissions,
    createdAt: new Date().toISOString()
  });
  await db.saveUsers(users);

  return res.redirect(`/admin/admins?success=${encodeURIComponent('تم إضافة الأدمن بنجاح')}`);
});

app.post('/admin/admins/:id/permissions', requireSuperAdmin, async (req, res) => {
  const adminId = req.params.id;
  const users = await db.users();
  const idx = users.findIndex(u => u && u.id === adminId && u.role === 'admin');
  if (idx === -1) return res.redirect(`/admin/admins?error=${encodeURIComponent('الأدمن غير موجود')}`);
  if (users[idx].isSuperAdmin) return res.redirect(`/admin/admins?error=${encodeURIComponent('لا يمكن تعديل صلاحيات السوبر أدمن')}`);

  users[idx].adminPermissions = {
    [ADMIN_PERMISSIONS.projects]: Boolean(req.body.perm_projects),
    [ADMIN_PERMISSIONS.purchases]: Boolean(req.body.perm_purchases),
    [ADMIN_PERMISSIONS.modifications]: Boolean(req.body.perm_modifications),
    [ADMIN_PERMISSIONS.messages]: Boolean(req.body.perm_messages),
    [ADMIN_PERMISSIONS.reviews]: Boolean(req.body.perm_reviews),
    [ADMIN_PERMISSIONS.coupons]: Boolean(req.body.perm_coupons),
    [ADMIN_PERMISSIONS.referrals]: Boolean(req.body.perm_referrals),
    [ADMIN_PERMISSIONS.walletCodes]: Boolean(req.body.perm_walletCodes),
    [ADMIN_PERMISSIONS.walletBalances]: Boolean(req.body.perm_walletBalances),
    [ADMIN_PERMISSIONS.users]: Boolean(req.body.perm_users),
    [ADMIN_PERMISSIONS.appointments]: Boolean(req.body.perm_appointments),
    [ADMIN_PERMISSIONS.subscriptionCoupons]: Boolean(req.body.perm_subscriptionCoupons),
    [ADMIN_PERMISSIONS.subscriptionPlans]: Boolean(req.body.perm_subscriptionPlans),
    [ADMIN_PERMISSIONS.subscriptionReports]: Boolean(req.body.perm_subscriptionReports)
  };

  await db.saveUsers(users);

  if (req.session.user && req.session.user.id === users[idx].id) {
    req.session.user.adminPermissions = users[idx].adminPermissions;
  }

  return res.redirect(`/admin/admins?success=${encodeURIComponent('تم حفظ الصلاحيات')}`);
});

// Admin - Add Project
app.get('/admin/projects/new', requireAdminPermission(ADMIN_PERMISSIONS.projects), async (req, res) => {
  res.render('admin/project-form', { project: null, user: req.session.user });
});

app.post('/admin/projects', requireAdminPermission(ADMIN_PERMISSIONS.projects), projectUpload, async (req, res) => {
  try {
    const { title, description, price, category, technologies } = req.body;
    const visibility = (req.body.visibility || 'public').trim();
    
    const projects = await db.projects();
    
    // Handle images with watermark
    let images = [];
    if (req.files && req.files.projectImages) {
      for (const file of req.files.projectImages) {
        try {
          await processImageWithWatermark(file);
          images.push(`uploads/project-images/${file.filename}`);
        } catch (error) {
          console.error('Error processing image:', error);
          // Still add the image even if watermark fails
          images.push(`uploads/project-images/${file.filename}`);
        }
      }
    }
    
    const newProject = {
      id: uuidv4(),
      title,
      description,
      price: parseFloat(price),
      category,
      technologies: technologies ? technologies.split(',').map(t => t.trim()) : [],
      visibility: (visibility === 'basic' || visibility === 'premium') ? visibility : 'public',
      filePath: req.files && req.files.projectFile && req.files.projectFile[0] ? `uploads/${req.files.projectFile[0].filename}` : null,
      originalFileName: req.files && req.files.projectFile && req.files.projectFile[0] ? req.files.projectFile[0].originalname : null,
      images: images,
      createdAt: new Date().toISOString()
    };
    
    projects.push(newProject);
    await db.saveProjects(projects);
    
    res.redirect('/admin');
  } catch (error) {
    console.error('Error creating project:', error);
    res.status(500).render('admin/project-form', { 
      project: null, 
      user: req.session.user,
      error: 'حدث خطأ أثناء حفظ المشروع. يرجى التأكد من حجم الملفات والمحاولة مرة أخرى.'
    });
  }
});

// Admin - Edit Project
app.get('/admin/projects/:id/edit', requireAdminPermission(ADMIN_PERMISSIONS.projects), async (req, res) => {
  const projects = await db.projects();
  const project = projects.find(p => p.id === req.params.id);
  if (!project) return res.status(404).send('Project not found');
  res.render('admin/project-form', { project, user: req.session.user });
});

app.post('/admin/projects/:id', requireAdminPermission(ADMIN_PERMISSIONS.projects), projectUpload, async (req, res) => {
  try {
    const { title, description, price, category, technologies } = req.body;
    const projects = await db.projects();
    const index = projects.findIndex(p => p.id === req.params.id);
    
    if (index === -1) return res.status(404).send('Project not found');
    
    // Handle images
    let images = projects[index].images || [];
    
    // Add new images with watermark
    if (req.files && req.files.projectImages) {
      const newImages = [];
      for (const file of req.files.projectImages) {
        try {
          await processImageWithWatermark(file);
          newImages.push(`uploads/project-images/${file.filename}`);
        } catch (error) {
          console.error('Error processing image:', error);
          // Still add the image even if watermark fails
          newImages.push(`uploads/project-images/${file.filename}`);
        }
      }
      images = [...images, ...newImages];
    }
    
    // Remove images marked for deletion
    const removeImages = Array.isArray(req.body.removeImages) ? req.body.removeImages : 
                        (req.body.removeImages ? [req.body.removeImages] : []);
    if (removeImages.length > 0) {
      images = images.filter(img => !removeImages.includes(img));
      // Delete image files
      removeImages.forEach(imgPath => {
        const absolutePath = toAbsolutePath(imgPath);
        if (absolutePath && fs.existsSync(absolutePath)) {
          fs.unlinkSync(absolutePath);
        }
      });
    }
    
    projects[index] = {
      ...projects[index],
      title,
      description,
      price: parseFloat(price),
      category,
      technologies: technologies ? technologies.split(',').map(t => t.trim()) : [],
      visibility: (((req.body.visibility || projects[index].visibility || 'public').trim() === 'basic' || (req.body.visibility || projects[index].visibility || 'public').trim() === 'premium')
        ? (req.body.visibility || projects[index].visibility || 'public').trim()
        : 'public'),
      filePath: req.files && req.files.projectFile && req.files.projectFile[0] ? `uploads/${req.files.projectFile[0].filename}` : projects[index].filePath,
      originalFileName: req.files && req.files.projectFile && req.files.projectFile[0] ? req.files.projectFile[0].originalname : projects[index].originalFileName,
      images: images
    };
    
    await db.saveProjects(projects);
    res.redirect('/admin');
  } catch (error) {
    console.error('Error updating project:', error);
    const projects = await db.projects();
    const project = projects.find(p => p.id === req.params.id);
    res.status(500).render('admin/project-form', { 
      project, 
      user: req.session.user,
      error: 'حدث خطأ أثناء تحديث المشروع. يرجى التأكد من حجم الملفات والمحاولة مرة أخرى.'
    });
  }
});

// Admin - Delete Project
app.post('/admin/projects/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.projects), async (req, res) => {
  const projects = await db.projects();
  const project = projects.find(p => p.id === req.params.id);
  
  if (project) {
    // Delete project file
    if (project.filePath) {
      const absoluteFilePath = toAbsolutePath(project.filePath);
      if (absoluteFilePath && fs.existsSync(absoluteFilePath)) fs.unlinkSync(absoluteFilePath);
    }
    
    // Delete project images
    if (project.images && project.images.length > 0) {
      project.images.forEach(imgPath => {
        const absolutePath = toAbsolutePath(imgPath);
        if (absolutePath && fs.existsSync(absolutePath)) {
          fs.unlinkSync(absolutePath);
        }
      });
    }
  }
  
  await db.saveProjects(projects.filter(p => p.id !== req.params.id));
  res.redirect('/admin');
});

// Admin - View all purchases
app.get('/admin/purchases', requireAdminPermission(ADMIN_PERMISSIONS.purchases), async (req, res) => {
  const purchases = await db.purchases();
  const users = await db.users();
  const projects = await db.projects();
  res.render('admin/purchases', { purchases, users, projects, user: req.session.user });
});

// Admin - Coupons
app.get('/admin/coupons', requireAdminPermission(ADMIN_PERMISSIONS.coupons), async (req, res) => {
  const coupons = await db.coupons();
  res.render('admin/coupons', { coupons, user: req.session.user, error: null });
});

// Admin - Subscription Coupons
app.get('/admin/subscription-coupons', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), async (req, res) => {
  const coupons = await db.subscriptionCoupons();
  res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: null });
});

app.post('/admin/subscription-coupons', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), async (req, res) => {
  const code = normalizeCouponCode(req.body.code);
  const type = (req.body.type || '').trim();
  const value = Number(req.body.value);
  const usageLimit = req.body.usageLimit ? Number(req.body.usageLimit) : null;
  const expiresAt = parseOptionalIsoDate(req.body.expiresAt);

  const coupons = await db.subscriptionCoupons();

  if (!code) {
    return res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: 'كود الكوبون مطلوب' });
  }
  if (coupons.some(c => normalizeCouponCode(c.code) === code)) {
    return res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: 'الكوبون موجود بالفعل' });
  }
  if (type !== 'percent' && type !== 'fixed') {
    return res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: 'نوع الكوبون غير صحيح' });
  }
  if (!Number.isFinite(value) || value <= 0) {
    return res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: 'قيمة الخصم غير صحيحة' });
  }
  if (type === 'percent' && value > 100) {
    return res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: 'النسبة يجب أن تكون أقل أو تساوي 100' });
  }
  if (usageLimit != null && (!Number.isFinite(usageLimit) || usageLimit < 1)) {
    return res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: 'حد الاستخدام غير صحيح' });
  }

  coupons.push({
    id: uuidv4(),
    code,
    type,
    value,
    active: true,
    usedCount: 0,
    usageLimit: usageLimit != null ? usageLimit : null,
    expiresAt,
    createdAt: new Date().toISOString(),
    lastUsedAt: null
  });

  await db.saveSubscriptionCoupons(coupons);
  res.redirect('/admin/subscription-coupons');
});

app.post('/admin/subscription-coupons/:id/toggle', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), async (req, res) => {
  const coupons = await db.subscriptionCoupons();
  const idx = coupons.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).send('Coupon not found');
  coupons[idx].active = !coupons[idx].active;
  await db.saveSubscriptionCoupons(coupons);
  res.redirect('/admin/subscription-coupons');
});

app.post('/admin/subscription-coupons/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), async (req, res) => {
  const coupons = await db.subscriptionCoupons();
  await db.saveSubscriptionCoupons(coupons.filter(c => c.id !== req.params.id));
  res.redirect('/admin/subscription-coupons');
});

// Admin - Subscription Reports
app.get('/admin/subscription-reports', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionReports), async (req, res) => {
  const users = await db.users();
  const plans = await db.subscriptionPlans();
  const paymentsAll = await db.subscriptionPayments();

  const fromStr = (req.query.from || '').trim();
  const toStr = (req.query.to || '').trim();

  let fromDate = null;
  let toDate = null;
  if (fromStr) {
    const d = new Date(fromStr);
    if (!Number.isNaN(d.getTime())) fromDate = d;
  }
  if (toStr) {
    const d = new Date(toStr);
    if (!Number.isNaN(d.getTime())) {
      d.setHours(23, 59, 59, 999);
      toDate = d;
    }
  }

  const filtered = paymentsAll.filter(p => {
    if (!p || !p.createdAt) return false;
    const t = new Date(p.createdAt);
    if (Number.isNaN(t.getTime())) return false;
    if (fromDate && t < fromDate) return false;
    if (toDate && t > toDate) return false;
    return true;
  });

  const totalRevenue = filtered.reduce((sum, p) => sum + Number(p.amount || 0), 0);
  const totalDiscounts = filtered.reduce((sum, p) => sum + Number(p.discountAmount || 0), 0);
  const uniqueUsers = new Set(filtered.map(p => p.userId).filter(Boolean)).size;

  const rows = filtered
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime())
    .slice(0, 200)
    .map(p => {
      const user = users.find(u => u.id === p.userId) || null;
      const plan = plans.find(pl => pl.id === p.planId) || null;
      return {
        id: p.id,
        userId: p.userId,
        userLabel: user ? (user.name || user.email || user.id) : (p.userId || '-'),
        planId: p.planId,
        planLabel: plan ? plan.name : (p.planId || '-'),
        amount: Number(p.amount || 0),
        priceBefore: p.priceBefore != null ? Number(p.priceBefore) : Number(p.amount || 0),
        discountAmount: Number(p.discountAmount || 0),
        couponCode: p.couponCode || null,
        createdAt: p.createdAt
      };
    });

  res.render('admin/subscription-reports', {
    user: req.session.user,
    filters: { from: fromStr || '', to: toStr || '' },
    summary: {
      totalRevenue: Math.round(totalRevenue * 100) / 100,
      totalDiscounts: Math.round(totalDiscounts * 100) / 100,
      count: filtered.length,
      uniqueUsers
    },
    rows
  });
});

// Admin - Subscription Plans
app.get('/admin/subscription-plans', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionPlans), async (req, res) => {
  const plans = await db.subscriptionPlans();
  res.render('admin/subscription-plans', {
    user: req.session.user,
    plans,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/admin/subscription-plans/:id', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionPlans), async (req, res) => {
  const plans = await db.subscriptionPlans();
  const idx = plans.findIndex(p => p && p.id === req.params.id);
  if (idx === -1) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('الخطة غير موجودة'));

  const name = (req.body.name || '').trim();
  const price = Number(req.body.price);
  const durationDays = Number(req.body.durationDays);
  const currency = (req.body.currency || '').trim();
  const featuresRaw = (req.body.features || '').toString();
  const features = featuresRaw
    .split('\n')
    .map(s => s.trim())
    .filter(Boolean);

  if (!name) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('اسم الخطة مطلوب'));
  if (!Number.isFinite(price) || price <= 0) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('السعر غير صحيح'));
  if (!Number.isFinite(durationDays) || durationDays < 1) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('المدة غير صحيحة'));
  if (!currency) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('العملة مطلوبة'));

  plans[idx] = {
    ...plans[idx],
    name,
    price,
    durationDays,
    currency,
    features
  };

  await db.saveSubscriptionPlans(plans);
  res.redirect('/admin/subscription-plans?success=' + encodeURIComponent('تم حفظ الخطة'));
});

app.post('/admin/subscription-plans/:id/toggle', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionPlans), async (req, res) => {
  const plans = await db.subscriptionPlans();
  const idx = plans.findIndex(p => p && p.id === req.params.id);
  if (idx === -1) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('الخطة غير موجودة'));
  plans[idx].active = !plans[idx].active;
  await db.saveSubscriptionPlans(plans);
  res.redirect('/admin/subscription-plans?success=' + encodeURIComponent('تم تحديث حالة الخطة'));
});

app.post('/admin/coupons', requireAdminPermission(ADMIN_PERMISSIONS.coupons), async (req, res) => {
  const code = normalizeCouponCode(req.body.code);
  const type = (req.body.type || '').trim();
  const value = Number(req.body.value);
  const expiresAt = parseOptionalIsoDate(req.body.expiresAt);
  const usageLimit = req.body.usageLimit ? Number(req.body.usageLimit) : null;

  const coupons = await db.coupons();

  if (!code) {
    return res.render('admin/coupons', { coupons, user: req.session.user, error: 'كود الكوبون مطلوب' });
  }
  if (coupons.some(c => normalizeCouponCode(c.code) === code)) {
    return res.render('admin/coupons', { coupons, user: req.session.user, error: 'الكوبون موجود بالفعل' });
  }
  if (type !== 'percent' && type !== 'fixed') {
    return res.render('admin/coupons', { coupons, user: req.session.user, error: 'نوع الكوبون غير صحيح' });
  }
  if (!Number.isFinite(value) || value <= 0) {
    return res.render('admin/coupons', { coupons, user: req.session.user, error: 'قيمة الخصم غير صحيحة' });
  }
  if (type === 'percent' && value > 100) {
    return res.render('admin/coupons', { coupons, user: req.session.user, error: 'النسبة يجب أن تكون أقل أو تساوي 100' });
  }
  if (usageLimit != null && (!Number.isFinite(usageLimit) || usageLimit < 1)) {
    return res.render('admin/coupons', { coupons, user: req.session.user, error: 'حد الاستخدام غير صحيح' });
  }

  coupons.push({
    id: uuidv4(),
    code,
    type,
    value,
    active: true,
    usedCount: 0,
    usageLimit: usageLimit != null ? usageLimit : null,
    expiresAt,
    createdAt: new Date().toISOString()
  });

  await db.saveCoupons(coupons);
  res.redirect('/admin/coupons');
});

app.post('/admin/coupons/:id/toggle', requireAdminPermission(ADMIN_PERMISSIONS.coupons), async (req, res) => {
  const coupons = await db.coupons();
  const idx = coupons.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).send('Coupon not found');
  coupons[idx].active = !coupons[idx].active;
  await db.saveCoupons(coupons);
  res.redirect('/admin/coupons');
});

app.post('/admin/coupons/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.coupons), async (req, res) => {
  const coupons = await db.coupons();
  await db.saveCoupons(coupons.filter(c => c.id !== req.params.id));
  res.redirect('/admin/coupons');
});

app.get('/admin/reviews', requireAdminPermission(ADMIN_PERMISSIONS.reviews), async (req, res) => {
  const reviews = await db.reviews();
  const users = await db.users();
  const projects = await db.projects();
  res.render('admin/reviews', { reviews, users, projects, user: req.session.user });
});

app.post('/admin/reviews/:id/delete', requireAdmin, async (req, res) => {
  const reviews = await db.reviews();
  await db.saveReviews(reviews.filter(r => r.id !== req.params.id));
  res.redirect('/admin/reviews');
});

// Admin - View all users
app.get('/admin/users', requireAdminPermission(ADMIN_PERMISSIONS.users), async (req, res) => {
  const users = await db.users();
  res.render('admin/users', { users, user: req.session.user, success: req.query.success || null, error: req.query.error || null });
});

app.post('/admin/users/:id/block', requireAdminPermission(ADMIN_PERMISSIONS.users), async (req, res) => {
  try {
    const targetId = req.params.id;
    const { blockType, duration, blockReason } = req.body;
    if (!targetId) return res.redirect('/admin/users?error=مستخدم غير صالح');
    if (!blockType || (blockType !== 'permanent' && blockType !== 'temporary')) {
      return res.redirect('/admin/users?error=نوع الحظر غير صحيح');
    }
    if (!blockReason || !String(blockReason).trim()) {
      return res.redirect('/admin/users?error=اكتب سبب الحظر');
    }

    const users = await db.users();
    const idx = users.findIndex(u => u && u.id === targetId);
    if (idx === -1) return res.redirect('/admin/users?error=المستخدم غير موجود');

    if (users[idx].role !== 'user') {
      return res.redirect('/admin/users?error=لا يمكن حظر هذا الحساب');
    }

    const now = new Date();
    users[idx].isBlocked = true;
    users[idx].blockedReason = String(blockReason).trim();
    users[idx].blockedBy = req.session.user.id;
    users[idx].blockedAt = now.toISOString();

    if (blockType === 'temporary') {
      const days = Number(duration || 1);
      if (!Number.isFinite(days) || days < 1 || days > 3650) {
        return res.redirect('/admin/users?error=مدة الحظر غير صحيحة');
      }
      const until = new Date(now.getTime() + days * 24 * 60 * 60 * 1000);
      users[idx].blockedUntil = until.toISOString();
    } else {
      users[idx].blockedUntil = null;
    }

    await db.saveUsers(users);
    return res.redirect('/admin/users?success=تم حظر المستخدم بنجاح');
  } catch (e) {
    return res.redirect('/admin/users?error=حدث خطأ أثناء حظر المستخدم');
  }
});

app.post('/admin/users/:id/unblock', requireAdminPermission(ADMIN_PERMISSIONS.users), async (req, res) => {
  try {
    const targetId = req.params.id;
    const users = await db.users();
    const idx = users.findIndex(u => u && u.id === targetId);
    if (idx === -1) return res.redirect('/admin/users?error=المستخدم غير موجود');

    const target = users[idx];
    const isPermanent = target.isBlocked && !target.blockedUntil;
    if (isPermanent && !(req.session.user && req.session.user.isSuperAdmin)) {
      return res.redirect('/admin/users?error=لا يمكن فك الحظر الدائم إلا بواسطة السوبر أدمن');
    }

    unblockUserInPlace(target);
    users[idx] = target;
    await db.saveUsers(users);
    return res.redirect('/admin/users?success=تم فك الحظر بنجاح');
  } catch (e) {
    return res.redirect('/admin/users?error=حدث خطأ أثناء فك الحظر');
  }
});

// Admin - Wallet Balances
app.get('/admin/wallet-balances', requireAdminPermission(ADMIN_PERMISSIONS.walletBalances), async (req, res) => {
  const users = await db.users();
  res.render('admin/wallet-balances', { users, user: req.session.user, error: null, success: null });
});

app.post('/admin/wallet-balances/:userId', requireAdminPermission(ADMIN_PERMISSIONS.walletBalances), async (req, res) => {
  const { action } = req.body;
  const amount = Number(req.body.amount);

  if (!['set', 'add', 'subtract'].includes(action)) {
    const users = await db.users();
    return res.render('admin/wallet-balances', { users, user: req.session.user, error: 'عملية غير صحيحة', success: null });
  }
  if (!Number.isFinite(amount) || amount < 0) {
    const users = await db.users();
    return res.render('admin/wallet-balances', { users, user: req.session.user, error: 'قيمة الرصيد غير صحيحة', success: null });
  }

  const users = await db.users();
  const idx = users.findIndex(u => u.id === req.params.userId);
  if (idx === -1) {
    return res.status(404).send('User not found');
  }

  const current = Number(users[idx].walletBalance || 0);
  let next = current;
  if (action === 'set') next = amount;
  if (action === 'add') next = current + amount;
  if (action === 'subtract') next = current - amount;

  if (!Number.isFinite(next)) next = current;
  if (next < 0) {
    return res.render('admin/wallet-balances', { users, user: req.session.user, error: 'لا يمكن أن يصبح الرصيد سالب', success: null });
  }

  users[idx].walletBalance = Math.round(next * 100) / 100;
  await db.saveUsers(users);
  return res.render('admin/wallet-balances', { users, user: req.session.user, error: null, success: 'تم تحديث الرصيد بنجاح' });
});

// Admin - Approve/Reject purchase
app.post('/admin/purchases/:id/approve', requireAdmin, async (req, res) => {
  const purchases = await db.purchases();
  const index = purchases.findIndex(p => p.id === req.params.id);
  if (index === -1) return res.status(404).send('Purchase not found');
  
  purchases[index].status = 'approved';
  purchases[index].approvedAt = new Date().toISOString();
  await db.savePurchases(purchases);

  const approvedPurchase = purchases[index];
  // Create invoice after approval (only once per order)
  try {
    const invoices = await db.invoices();
    const orderId = approvedPurchase.orderId || approvedPurchase.id;
    const already = invoices.some(inv => inv && inv.orderId === orderId);
    if (!already) {
      const users = await db.users();
      const buyer = users.find(u => u && u.id === approvedPurchase.userId) || null;

      const orderPurchases = approvedPurchase.orderId
        ? purchases.filter(p => p && p.orderId === approvedPurchase.orderId)
        : [approvedPurchase];

      const items = orderPurchases.map(p => ({
        purchaseId: p.id,
        projectId: p.projectId,
        projectTitle: p.projectTitle,
        priceBefore: Number(p.priceBefore || p.price || 0),
        discountAmount: Number(p.discountAmount || 0),
        priceAfter: Number(p.price || 0)
      }));

      const totalBefore = Math.round(items.reduce((s, it) => s + Number(it.priceBefore || 0), 0) * 100) / 100;
      const totalDiscount = Math.round(items.reduce((s, it) => s + Number(it.discountAmount || 0), 0) * 100) / 100;
      const totalAfter = Math.round(items.reduce((s, it) => s + Number(it.priceAfter || 0), 0) * 100) / 100;

      const couponCode = approvedPurchase.couponCode || null;

      invoices.push({
        id: uuidv4(),
        invoiceNumber: buildInvoiceNumber(),
        orderId,
        userId: approvedPurchase.userId,
        userName: buyer ? (buyer.name || buyer.email || buyer.id) : (approvedPurchase.userId || null),
        userEmail: buyer ? (buyer.email || null) : null,
        couponCode,
        items,
        totalBefore,
        totalDiscount,
        totalAfter,
        createdAt: new Date().toISOString()
      });
      await db.saveInvoices(invoices);
    }
  } catch (e) {
    // ignore invoice errors
  }
  const referrals = await db.referrals();
  const pendingReferralIndex = referrals.findIndex(
    r => r.referredUserId === approvedPurchase.userId && r.status === 'pending'
  );
  if (pendingReferralIndex !== -1) {
    const referral = referrals[pendingReferralIndex];
    const users = await db.users();
    const referrerIndex = users.findIndex(u => u.id === referral.referrerUserId);
    if (referrerIndex !== -1) {
      users[referrerIndex].walletBalance = Number(users[referrerIndex].walletBalance || 0) + Number(referral.rewardAmount || 0);
      await db.saveUsers(users);
    }

    referrals[pendingReferralIndex].status = 'rewarded';
    referrals[pendingReferralIndex].rewardedAt = new Date().toISOString();
    referrals[pendingReferralIndex].rewardPurchaseId = approvedPurchase.id;
    await db.saveReferrals(referrals);
  }

  await createNotification({
    userId: approvedPurchase.userId,
    title: 'تمت الموافقة على طلبك',
    message: `تمت الموافقة على شراء "${approvedPurchase.projectTitle}" ويمكنك متابعة التفاصيل من مشترياتك.`,
    link: '/my-purchases',
    kind: 'success',
    meta: {
      type: 'purchase_approved',
      purchaseId: approvedPurchase.id
    }
  });

  res.redirect('/admin/purchases');
});

// Invoice PDF (available after approval)
app.get('/invoice/:orderId.pdf', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const orderId = req.params.orderId;
  const invoices = await db.invoices();
  const inv = invoices.find(i => i && i.orderId === orderId && i.userId === req.session.user.id) || null;
  if (!inv) return res.status(404).send('Invoice not found');

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${inv.invoiceNumber || 'invoice'}.pdf"`);
  await renderInvoicePdf({ res, inv, includeEmail: true, includeCoupon: true });
});

// Admin - Referrals
app.get('/admin/referrals', requireAdminPermission(ADMIN_PERMISSIONS.referrals), async (req, res) => {
  const referrals = await db.referrals();
  const users = await db.users();
  res.render('admin/referrals', { referrals, users, user: req.session.user });
});

app.post('/admin/purchases/:id/reject', requireAdmin, async (req, res) => {
  const purchases = await db.purchases();
  const index = purchases.findIndex(p => p.id === req.params.id);
  if (index === -1) return res.status(404).send('Purchase not found');

  const purchase = purchases[index];
  if (purchase.status !== 'rejected') {
    const refundAmount = calculateRefundForRejectedItem({ rejectedPurchase: purchase, allPurchases: purchases });
    if (refundAmount > 0 && !purchase.walletRefundedAt) {
      const users = await db.users();
      const refundUserId = purchase.payerUserId || purchase.userId;
      const userIndex = users.findIndex(u => u.id === refundUserId);
      if (userIndex !== -1) {
        users[userIndex].walletBalance = Math.round((Number(users[userIndex].walletBalance || 0) + refundAmount) * 100) / 100;
        await db.saveUsers(users);
      }
      purchases[index].walletRefundedAt = new Date().toISOString();
      purchases[index].walletRefundAmount = refundAmount;
    }
  }

  purchases[index].status = 'rejected';
  await db.savePurchases(purchases);

  await createNotification({
    userId: purchase.userId,
    title: 'تم رفض أحد طلباتك',
    message: purchase.walletRefundAmount
      ? `تم رفض "${purchase.projectTitle}" وتم رد ${purchase.walletRefundAmount} جنيه إلى المحفظة.`
      : `تم رفض "${purchase.projectTitle}".`,
    link: '/my-purchases',
    kind: 'warning',
    meta: {
      type: 'purchase_rejected',
      purchaseId: purchase.id
    }
  });
  res.redirect('/admin/purchases');
});

// Admin - Wallet Codes
app.get('/admin/wallet-codes', requireAdminPermission(ADMIN_PERMISSIONS.walletCodes), async (req, res) => {
  const walletCodes = await db.walletCodes();
  res.render('admin/wallet-codes', { walletCodes, user: req.session.user, error: null });
});

app.post('/admin/wallet-codes', requireAdminPermission(ADMIN_PERMISSIONS.walletCodes), async (req, res) => {
  const code = normalizeWalletCode(req.body.code);
  const amount = Number(req.body.amount);
  const expiresAt = parseOptionalIsoDate(req.body.expiresAt);
  const usageLimit = req.body.usageLimit ? Number(req.body.usageLimit) : null;

  const walletCodes = await db.walletCodes();

  if (!code) {
    return res.render('admin/wallet-codes', { walletCodes, user: req.session.user, error: 'الكود مطلوب' });
  }
  if (walletCodes.some(c => normalizeWalletCode(c.code) === code)) {
    return res.render('admin/wallet-codes', { walletCodes, user: req.session.user, error: 'الكود موجود بالفعل' });
  }
  if (!Number.isFinite(amount) || amount <= 0) {
    return res.render('admin/wallet-codes', { walletCodes, user: req.session.user, error: 'قيمة الرصيد غير صحيحة' });
  }
  if (usageLimit != null && (!Number.isFinite(usageLimit) || usageLimit < 1)) {
    return res.render('admin/wallet-codes', { walletCodes, user: req.session.user, error: 'حد الاستخدام غير صحيح' });
  }

  walletCodes.push({
    id: uuidv4(),
    code,
    amount,
    active: true,
    usedCount: 0,
    usageLimit: usageLimit != null ? usageLimit : null,
    expiresAt,
    createdAt: new Date().toISOString(),
    lastUsedAt: null
  });

  await db.saveWalletCodes(walletCodes);
  return res.redirect('/admin/wallet-codes');
});

app.post('/admin/wallet-codes/:id/toggle', requireAdmin, async (req, res) => {
  const walletCodes = await db.walletCodes();
  const idx = walletCodes.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).send('Code not found');
  walletCodes[idx].active = !walletCodes[idx].active;
  await db.saveWalletCodes(walletCodes);
  return res.redirect('/admin/wallet-codes');
});

app.post('/admin/wallet-codes/:id/delete', requireAdmin, async (req, res) => {
  const walletCodes = await db.walletCodes();
  await db.saveWalletCodes(walletCodes.filter(c => c.id !== req.params.id));
  return res.redirect('/admin/wallet-codes');
});

// Modification Request - User requests custom changes
app.get('/request-modification/:purchaseId', requireAuth, async (req, res) => {
  const purchases = await db.purchases();
  const purchase = purchases.find(p => p.id === req.params.purchaseId && p.userId === req.session.user.id);
  if (!purchase) return res.status(404).send('Purchase not found');
  
  res.render('request-modification', { purchase, user: req.session.user, error: null });
});

app.post('/request-modification/:purchaseId', requireAuth, async (req, res) => {
  const { description } = req.body;
  const purchases = await db.purchases();
  const purchase = purchases.find(p => p.id === req.params.purchaseId && p.userId === req.session.user.id);
  if (!purchase) return res.status(404).send('Purchase not found');
  
  const modifications = await db.modifications();
  modifications.push({
    id: uuidv4(),
    purchaseId: purchase.id,
    userId: req.session.user.id,
    projectId: purchase.projectId,
    projectTitle: purchase.projectTitle,
    description,
    status: 'pending',
    createdAt: new Date().toISOString()
  });
  
  await db.saveModifications(modifications);
  res.redirect('/my-purchases');
});

app.get('/project-request', requireAuth, async (req, res) => {
  res.redirect('/?info=' + encodeURIComponent('تم إيقاف ميزة طلب الموقع المخصص حالياً'));
});

app.post('/project-request', requireAuth, async (req, res) => {
  res.redirect('/?info=' + encodeURIComponent('تم إيقاف ميزة طلب الموقع المخصص حالياً'));
});

app.get('/admin/project-requests', requireAdmin, async (req, res) => {
  res.redirect('/admin?info=' + encodeURIComponent('تم إيقاف ميزة طلب الموقع المخصص حالياً'));
});

app.post('/admin/project-requests/:id', requireAdmin, async (req, res) => {
  res.redirect('/admin?info=' + encodeURIComponent('تم إيقاف ميزة طلب الموقع المخصص حالياً'));
});

// Admin - View modification requests
app.get('/admin/modifications', requireAdminPermission(ADMIN_PERMISSIONS.modifications), async (req, res) => {
  const modifications = await db.modifications();
  const users = await db.users();
  const purchases = await db.purchases();
  res.render('admin/modifications', { modifications, users, purchases, user: req.session.user });
});

// Admin - Complete modification request
app.post('/admin/modifications/:id/complete', requireAdminPermission(ADMIN_PERMISSIONS.modifications), upload.single('modifiedFile'), async (req, res) => {
  const modifications = await db.modifications();
  const modIndex = modifications.findIndex(m => m.id === req.params.id);
  if (modIndex === -1) return res.status(404).send('Modification request not found');
  
  const modification = modifications[modIndex];
  
  // Update the purchase with the modified file
  const purchases = await db.purchases();
  const purchaseIndex = purchases.findIndex(p => p.id === modification.purchaseId);
  
  if (purchaseIndex !== -1 && req.file) {
    purchases[purchaseIndex].filePath = `uploads/${req.file.filename}`;
    purchases[purchaseIndex].originalFileName = req.file.originalname;
    purchases[purchaseIndex].isModified = true;
    purchases[purchaseIndex].modificationNote = req.body.note || 'Project modified as requested';
    await db.savePurchases(purchases);
  }
  
  modifications[modIndex].status = 'completed';
  modifications[modIndex].completedAt = new Date().toISOString();
  await db.saveModifications(modifications);
  
  res.redirect('/admin/modifications');
});
// ========== MESSAGING SYSTEM ==========

// User - View chat with admin
app.get('/messages', requireAuth, async (req, res) => {
  const messages = (await db.messages()).filter(m => 
    (m.senderId === req.session.user.id && m.receiverId === 'admin') ||
    (m.senderId === 'admin' && m.receiverId === req.session.user.id)
  );
  res.render('messages', { messages, user: req.session.user });
});

app.get('/subscriptions', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const plans = (await db.subscriptionPlans()).filter(p => p && p.active);
  const activeSubscription = await getActiveSubscriptionForUser({ userId: req.session.user.id });
  const activePlan = activeSubscription ? plans.find(p => p.id === activeSubscription.planId) : null;

  res.render('subscriptions', {
    user: req.session.user,
    plans,
    activeSubscription,
    activePlan,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/subscriptions/subscribe', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const planId = req.body.planId;
  const plans = (await db.subscriptionPlans()).filter(p => p && p.active);
  const plan = plans.find(p => p.id === planId);
  if (!plan) return res.redirect('/subscriptions?error=' + encodeURIComponent('الخطة غير صحيحة'));

  const existing = await getActiveSubscriptionForUser({ userId: req.session.user.id });
  if (existing) return res.redirect('/subscriptions?error=' + encodeURIComponent('لديك اشتراك نشط بالفعل'));

  const users = await db.users();
  const idx = users.findIndex(u => u.id === req.session.user.id);
  if (idx === -1) return res.redirect('/subscriptions?error=' + encodeURIComponent('المستخدم غير موجود'));

  const basePrice = Number(plan.price || 0);
  if (!Number.isFinite(basePrice) || basePrice <= 0) {
    return res.redirect('/subscriptions?error=' + encodeURIComponent('سعر الخطة غير صحيح'));
  }

  const couponCode = normalizeCouponCode(req.body.couponCode);
  let appliedCoupon = null;
  let couponDiscountAmount = 0;
  let priceAfterDiscount = basePrice;

  let coupons = null;
  let couponIndex = -1;
  if (couponCode) {
    coupons = await db.subscriptionCoupons();
    couponIndex = coupons.findIndex(c => normalizeCouponCode(c.code) === couponCode);
    const coupon = couponIndex !== -1 ? coupons[couponIndex] : null;
    const eligibility = getSubscriptionCouponEligibility(coupon);

    if (!eligibility.eligible) {
      return res.redirect('/subscriptions?error=' + encodeURIComponent(eligibility.reason || 'كوبون غير صالح'));
    }

    const calc = calculateSubscriptionCouponDiscount({ priceBefore: basePrice, coupon });
    couponDiscountAmount = calc.discountAmount;
    priceAfterDiscount = calc.priceAfter;
    appliedCoupon = coupon;
  }

  const bal = Number(users[idx].walletBalance || 0);
  if (bal < Number(priceAfterDiscount || 0)) {
    return res.redirect('/subscriptions?error=' + encodeURIComponent('الرصيد غير كافي'));
  }

  users[idx].walletBalance = Math.round((bal - Number(priceAfterDiscount || 0)) * 100) / 100;
  await db.saveUsers(users);

  if (appliedCoupon && coupons && couponIndex !== -1) {
    coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
    coupons[couponIndex].lastUsedAt = new Date().toISOString();
    await db.saveSubscriptionCoupons(coupons);
  }

  const now = new Date();
  const end = new Date(now.getTime() + (Number(plan.durationDays || 30) * 24 * 60 * 60 * 1000));

  const subs = await db.subscriptions();
  const sub = {
    id: uuidv4(),
    userId: req.session.user.id,
    planId: plan.id,
    status: 'active',
    currentPeriodStart: now.toISOString(),
    currentPeriodEnd: end.toISOString(),
    canceledAt: null,
    createdAt: now.toISOString()
  };
  subs.push(sub);
  await db.saveSubscriptions(subs);

  const payments = await db.subscriptionPayments();
  payments.push({
    id: uuidv4(),
    subscriptionId: sub.id,
    userId: sub.userId,
    planId: sub.planId,
    amount: Number(priceAfterDiscount || 0),
    currency: plan.currency || 'EGP',
    method: 'wallet',
    priceBefore: basePrice,
    discountAmount: couponDiscountAmount,
    couponCode: appliedCoupon ? normalizeCouponCode(appliedCoupon.code) : null,
    createdAt: now.toISOString()
  });
  await db.saveSubscriptionPayments(payments);

  req.session.user.walletBalance = Number(users[idx].walletBalance || 0);
  req.session.user.subscription = {
    planId: sub.planId,
    status: sub.status,
    currentPeriodEnd: sub.currentPeriodEnd
  };

  res.redirect('/subscriptions?success=' + encodeURIComponent('تم الاشتراك بنجاح'));
});

app.post('/subscriptions/cancel', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const subs = await db.subscriptions();
  const active = await getActiveSubscriptionForUser({ userId: req.session.user.id });
  if (!active) return res.redirect('/subscriptions?error=' + encodeURIComponent('لا يوجد اشتراك نشط'));

  const idx = subs.findIndex(s => s.id === active.id);
  if (idx === -1) return res.redirect('/subscriptions?error=' + encodeURIComponent('لا يوجد اشتراك نشط'));

  subs[idx].status = 'canceled';
  subs[idx].canceledAt = new Date().toISOString();
  await db.saveSubscriptions(subs);

  req.session.user.subscription = null;
  res.redirect('/subscriptions?success=' + encodeURIComponent('تم إلغاء الاشتراك'));
});

// User - Send message to admin
app.post('/messages', requireAuth, async (req, res) => {
  const { content, purchaseId } = req.body;
  
  const messages = await db.messages();
  messages.push({
    id: uuidv4(),
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    receiverId: 'admin',
    content,
    purchaseId: purchaseId || null,
    read: false,
    createdAt: new Date().toISOString()
  });
  
  await db.saveMessages(messages);
  res.redirect('/messages');
});

// Admin - View all conversations
app.get('/admin/messages', requireAdminPermission(ADMIN_PERMISSIONS.messages), async (req, res) => {
  const messages = await db.messages();
  const users = await db.users();
  
  // Group messages by user
  const conversations = {};
  messages.forEach(msg => {
    const userId = msg.senderId === 'admin' ? msg.receiverId : msg.senderId;
    if (!conversations[userId]) {
      conversations[userId] = [];
    }
    conversations[userId].push(msg);
  });
  
  res.render('admin/messages', { conversations, users, user: req.session.user });
});

// Admin - View specific conversation
app.get('/admin/messages/:userId', requireAdmin, async (req, res) => {
  const messages = (await db.messages()).filter(m => 
    (m.senderId === req.params.userId && m.receiverId === 'admin') ||
    (m.senderId === 'admin' && m.receiverId === req.params.userId)
  );
  
  const users = await db.users();
  const chatUser = users.find(u => u.id === req.params.userId);
  
  // Mark messages as read
  const allMessages = await db.messages();
  allMessages.forEach(m => {
    if (m.senderId === req.params.userId && m.receiverId === 'admin') {
      m.read = true;
    }
  });
  await db.saveMessages(allMessages);
  
  res.render('admin/conversation', { messages, chatUser, user: req.session.user });
});

// Admin - Reply to user
app.post('/admin/messages/:userId', requireAdmin, async (req, res) => {
  const { content } = req.body;
  
  const messages = await db.messages();
  messages.push({
    id: uuidv4(),
    senderId: 'admin',
    senderName: 'Admin',
    receiverId: req.params.userId,
    content,
    read: false,
    createdAt: new Date().toISOString()
  });
  
  await db.saveMessages(messages);
  res.redirect(`/admin/messages/${req.params.userId}`);
});

const httpServer = http.createServer(app);
let io = null;

if (!IS_VERCEL) {
  io = new Server(httpServer, {
    cors: {
      origin: ["http://localhost:3000", "http://192.168.8.110:3000", "*"],
      methods: ["GET", "POST"],
      credentials: true
    }
  });

  io.on('connection', (socket) => {
    socket.on('join-room', (roomId) => {
      if (!roomId) return;
      socket.join(roomId);
      socket.to(roomId).emit('peer-joined');
    });

    socket.on('webrtc-offer', ({ roomId, offer }) => {
      if (!roomId || !offer) return;
      socket.to(roomId).emit('webrtc-offer', { offer });
    });

    socket.on('webrtc-answer', ({ roomId, answer }) => {
      if (!roomId || !answer) return;
      socket.to(roomId).emit('webrtc-answer', { answer });
    });

    socket.on('webrtc-ice-candidate', ({ roomId, candidate }) => {
      if (!roomId || !candidate) return;
      socket.to(roomId).emit('webrtc-ice-candidate', { candidate });
    });

    socket.on('leave-room', (roomId) => {
      if (!roomId) return;
      socket.leave(roomId);
      socket.to(roomId).emit('peer-left');
    });

    socket.on('join-admin-team', () => {
      socket.join('admin-team');
    });

    socket.on('admin-team-message', (payload) => {
      try {
        if (!payload || typeof payload !== 'object') return;
        socket.to('admin-team').emit('admin-team-message', payload);
      } catch (e) {
        // ignore
      }
    });
  });

  // Start server
  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`Codentra running on http://localhost:${PORT}`);
    console.log(`Network access: http://192.168.8.110:${PORT}`);
    console.log(`Admin: admin@codentra.com / admin123`);
  });
}

module.exports = app;
module.exports.default = app;
