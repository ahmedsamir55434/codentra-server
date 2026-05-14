require('dotenv').config();

const express = require('express');
const session = require('express-session');
const multer = require('multer');
const bcrypt = require('bcryptjs');
const nodemailer = require('nodemailer');
const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const os = require('os');
const { AsyncLocalStorage } = require('async_hooks');
const mammoth = require('mammoth');
const { v4: uuidv4 } = require('uuid');
const { Server } = require('socket.io');
const PDFDocument = require('pdfkit');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const PptxGenJS = require('pptxgenjs');
const { AccessToken } = require('livekit-server-sdk');
const WatermarkProcessor = require('./utils/watermark');

const app = express();
const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || 'codentra-secret-key-2024';
const AUTH_COOKIE_NAME = 'codentra_auth';
const AUTH_COOKIE_MAX_AGE = 1000 * 60 * 60 * 24 * 7;
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || '').trim();
const GEMINI_MODEL = String(process.env.GEMINI_MODEL || 'gemini-2.5-flash').trim() || 'gemini-2.5-flash';
const LIVEKIT_URL = String(process.env.LIVEKIT_URL || '').trim();
const LIVEKIT_API_KEY = String(process.env.LIVEKIT_API_KEY || '').trim();
const LIVEKIT_API_SECRET = String(process.env.LIVEKIT_API_SECRET || '').trim();
const GOOGLE_CLIENT_ID = String(process.env.GOOGLE_CLIENT_ID || '').trim();
const GOOGLE_CLIENT_SECRET = String(process.env.GOOGLE_CLIENT_SECRET || '').trim();
const GITHUB_CLIENT_ID = String(process.env.GITHUB_CLIENT_ID || '').trim();
const GITHUB_CLIENT_SECRET = String(process.env.GITHUB_CLIENT_SECRET || '').trim();
const ATLOS_API_URL = String(process.env.ATLOS_API_URL || 'https://api.atlos.io/gateway/rest').trim().replace(/\/+$/, '');
const ATLOS_MERCHANT_ID = String(process.env.ATLOS_MERCHANT_ID || process.env.ATLOS_API_KEY || '').trim();
const ATLOS_API_SECRET = String(process.env.ATLOS_API_SECRET || '').trim();
const ATLOS_WEBHOOK_SECRET = String(process.env.ATLOS_WEBHOOK_SECRET || '').trim();
const FX_API_BASE_URL = String(process.env.FX_API_BASE_URL || 'https://api.frankfurter.dev/v1').trim().replace(/\/+$/, '');
const FALLBACK_EGP_TO_USD_RATE = Number(process.env.FALLBACK_EGP_TO_USD_RATE || 0.02);

// CORS middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  if (req.method === 'OPTIONS') {
    res.sendStatus(200);
  } else {
    next();
  }
});

// Initialize watermark processor
const watermarkProcessor = new WatermarkProcessor();
const IS_VERCEL = Boolean(process.env.VERCEL);
const APP_ROOT_DIR = __dirname;
const BUNDLED_DATA_DIR = path.join(APP_ROOT_DIR, 'data');
const BUNDLED_UPLOADS_DIR = path.join(APP_ROOT_DIR, 'uploads');
const BUNDLED_PRIVATE_UPLOADS_DIR = path.join(APP_ROOT_DIR, 'private_uploads');
const AUTH_COOKIE_SECRET = process.env.AUTH_COOKIE_SECRET || `${SESSION_SECRET}-auth`;
const AUTH_COOKIE_OPTIONS = {
  httpOnly: true,
  sameSite: 'lax',
  secure: IS_VERCEL,
  path: '/',
  maxAge: AUTH_COOKIE_MAX_AGE
};

const normalizeStoredPath = (storedPath) => {
  if (!storedPath) return null;
  if (typeof storedPath !== 'string') return null;
  return storedPath.startsWith('/') ? storedPath.slice(1) : storedPath;
};

const toAbsolutePath = (storedPath) => {
  const normalized = normalizeStoredPath(storedPath);
  if (!normalized) return null;

  const candidates = [];

  if (normalized.startsWith('uploads/')) {
    const relativeUploadPath = normalized.slice('uploads/'.length);
    candidates.push(path.join(UPLOADS_DIR, relativeUploadPath));
    if (IS_VERCEL) candidates.push(path.join(BUNDLED_UPLOADS_DIR, relativeUploadPath));
  } else if (normalized.startsWith('private_uploads/')) {
    const relativePrivatePath = normalized.slice('private_uploads/'.length);
    candidates.push(path.join(PRIVATE_UPLOADS_DIR, relativePrivatePath));
    if (IS_VERCEL) candidates.push(path.join(BUNDLED_PRIVATE_UPLOADS_DIR, relativePrivatePath));
  } else {
    candidates.push(path.join(APP_ROOT_DIR, normalized));
  }

  for (const candidate of candidates) {
    if (candidate && fs.existsSync(candidate)) return candidate;
  }

  return candidates[0] || path.join(APP_ROOT_DIR, normalized);
};

const formatMoney = (v) => {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return '0';
  return (Math.round(n * 100) / 100).toFixed(2);
};

const isGoogleAuthConfigured = () => Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET);
const isGitHubAuthConfigured = () => Boolean(GITHUB_CLIENT_ID && GITHUB_CLIENT_SECRET);

const buildGoogleOAuthUrl = ({ mode = 'login', referralCode = '' } = {}) => {
  const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  url.searchParams.set('client_id', GOOGLE_CLIENT_ID);
  url.searchParams.set('redirect_uri', GOOGLE_CALLBACK_URL);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', 'openid email profile');
  url.searchParams.set('access_type', 'offline');
  url.searchParams.set('prompt', 'select_account');
  url.searchParams.set('state', JSON.stringify({
    mode,
    referralCode: normalizeReferralCode(referralCode)
  }));
  return url.toString();
};

const buildGitHubOAuthUrl = ({ mode = 'login', referralCode = '' } = {}) => {
  const url = new URL('https://github.com/login/oauth/authorize');
  url.searchParams.set('client_id', GITHUB_CLIENT_ID);
  url.searchParams.set('redirect_uri', GITHUB_CALLBACK_URL);
  url.searchParams.set('scope', 'read:user user:email');
  url.searchParams.set('state', JSON.stringify({
    mode,
    referralCode: normalizeReferralCode(referralCode)
  }));
  return url.toString();
};

const createGoogleUserRecord = ({ users, profile, referralCheck }) => ({
  id: uuidv4(),
  name: String(profile.name || profile.email || 'Google User').trim(),
  email: String(profile.email || '').trim().toLowerCase(),
  password: null,
  role: 'user',
  authProvider: 'google',
  googleId: String(profile.sub || '').trim() || null,
  referralCode: ensureUniqueReferralCode(users),
  walletBalance: 0,
  walletCardNumber: createWalletCardNumber(users),
  walletPaymentPasswordHash: bcrypt.hashSync(crypto.randomBytes(12).toString('hex'), 10),
  referredBy: referralCheck && referralCheck.referrerUserId
    ? {
        referrerUserId: referralCheck.referrerUserId,
        code: referralCheck.normalized,
        createdAt: new Date().toISOString(),
        rewardedAt: null
      }
    : null,
  loyaltyPoints: 0,
  createdAt: new Date().toISOString()
});

const createGitHubUserRecord = ({ users, profile, referralCheck }) => ({
  id: uuidv4(),
  name: String(profile.name || profile.login || profile.email || 'GitHub User').trim(),
  email: String(profile.email || '').trim().toLowerCase(),
  password: null,
  role: 'user',
  authProvider: 'github',
  githubId: String(profile.id || '').trim() || null,
  githubLogin: String(profile.login || '').trim() || null,
  referralCode: ensureUniqueReferralCode(users),
  walletBalance: 0,
  walletCardNumber: createWalletCardNumber(users),
  walletPaymentPasswordHash: bcrypt.hashSync(crypto.randomBytes(12).toString('hex'), 10),
  referredBy: referralCheck && referralCheck.referrerUserId
    ? {
        referrerUserId: referralCheck.referrerUserId,
        code: referralCheck.normalized,
        createdAt: new Date().toISOString(),
        rewardedAt: null
      }
    : null,
  loyaltyPoints: 0,
  createdAt: new Date().toISOString()
});

let cachedCurrencyRate = {
  egpToUsd: Number.isFinite(FALLBACK_EGP_TO_USD_RATE) && FALLBACK_EGP_TO_USD_RATE > 0 ? FALLBACK_EGP_TO_USD_RATE : 0.02,
  fetchedAt: 0,
  sourceDate: null
};

const roundCurrencyAmount = (value, decimals = 2) => {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return 0;
  const factor = 10 ** decimals;
  return Math.round(amount * factor) / factor;
};

const formatUsdAmount = (value) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(roundCurrencyAmount(value));
};

const getCachedEgpToUsdRate = () => {
  if (Number.isFinite(cachedCurrencyRate.egpToUsd) && cachedCurrencyRate.egpToUsd > 0) {
    return cachedCurrencyRate.egpToUsd;
  }
  return Number.isFinite(FALLBACK_EGP_TO_USD_RATE) && FALLBACK_EGP_TO_USD_RATE > 0 ? FALLBACK_EGP_TO_USD_RATE : 0.02;
};

const convertEgpToUsd = (value, rate = getCachedEgpToUsdRate()) => {
  return roundCurrencyAmount(Number(value || 0) * Number(rate || 0));
};

const convertUsdToEgp = (value, rate = getCachedEgpToUsdRate()) => {
  const safeRate = Number(rate || 0);
  if (!Number.isFinite(safeRate) || safeRate <= 0) return 0;
  return roundCurrencyAmount(Number(value || 0) / safeRate);
};

const buildDisplayMoney = (value, rate = getCachedEgpToUsdRate()) => ({
  egp: roundCurrencyAmount(value),
  usd: convertEgpToUsd(value, rate),
  formatted: formatUsdAmount(convertEgpToUsd(value, rate))
});

const fetchEgpToUsdRate = async () => {
  const now = Date.now();
  if (cachedCurrencyRate.fetchedAt && (now - cachedCurrencyRate.fetchedAt) < (60 * 60 * 1000)) {
    return cachedCurrencyRate;
  }

  try {
    const response = await fetch(`${FX_API_BASE_URL}/latest?base=EGP&symbols=USD`);
    if (!response.ok) throw new Error(`FX_RATE_REQUEST_FAILED:${response.status}`);
    const payload = await response.json();
    const nextRate = Number(payload && payload.rates && payload.rates.USD);
    if (!Number.isFinite(nextRate) || nextRate <= 0) {
      throw new Error('FX_RATE_INVALID');
    }

    cachedCurrencyRate = {
      egpToUsd: nextRate,
      fetchedAt: now,
      sourceDate: payload.date || new Date().toISOString().slice(0, 10)
    };
  } catch (error) {
    if (!cachedCurrencyRate.fetchedAt) {
      cachedCurrencyRate = {
        egpToUsd: getCachedEgpToUsdRate(),
        fetchedAt: now,
        sourceDate: new Date().toISOString().slice(0, 10)
      };
    }
  }

  return cachedCurrencyRate;
};

const buildInvoiceNumber = () => {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const stamp = `${yyyy}${mm}${dd}`;
  return `INV-${stamp}-${String(Date.now()).slice(-6)}`;
};

const INVOICE_FONT_PATH = path.join(__dirname, 'assets', 'fonts', 'NotoNaskhArabic-Regular.ttf');

const formatInvoiceDate = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleString('en-GB');
};

const formatInvoiceDateArabic = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleString('ar-EG');
};

const applyInvoiceFont = (doc) => {
  if (fs.existsSync(INVOICE_FONT_PATH)) {
    doc.font(INVOICE_FONT_PATH);
  }
  return doc;
};

const writeInvoiceField = (doc, arabicLabel, englishLabel, value, options = {}) => {
  const textValue = value == null || value === '' ? '-' : String(value);
  const fontSize = options.fontSize || 11;
  const gap = options.gap == null ? 0.2 : options.gap;

  doc.fontSize(fontSize).text(`${arabicLabel}: ${textValue}`, { align: 'right' });
  doc.text(`${englishLabel}: ${textValue}`, { align: 'left' });
  doc.moveDown(gap);
};

const getInvoiceItems = (inv) => {
  if (Array.isArray(inv && inv.items) && inv.items.length) {
    return inv.items;
  }

  return db
    .purchases()
    .filter(p => p && (p.orderId === inv.orderId || p.id === inv.orderId))
    .map(p => ({
      projectTitle: p.projectTitle || p.projectId || 'Project',
      projectId: p.projectId || null,
      priceBefore: p.priceBefore != null ? p.priceBefore : p.price,
      discountAmount: p.discountAmount != null ? p.discountAmount : 0,
      priceAfter: p.priceAfter != null ? p.priceAfter : p.price
    }));
};

const getInvoicePaymentDetails = (inv) => {
  const directMaskedCard = inv && (inv.walletCardMasked || (inv.payerCardLast4 ? `**** **** **** ${inv.payerCardLast4}` : null));
  if (inv && (inv.payerName || inv.payerEmail || directMaskedCard || inv.paymentMethod)) {
    return {
      paymentMethod: inv.paymentMethod || 'Wallet Card',
      payerName: inv.payerName || '-',
      payerEmail: inv.payerEmail || null,
      payerEmailMasked: inv.payerEmail ? maskEmail(inv.payerEmail) : null,
      walletCardMasked: directMaskedCard || '-'
    };
  }

  const relatedPurchases = db
    .purchases()
    .filter((purchase) => purchase && (purchase.orderId === inv.orderId || purchase.id === inv.orderId));
  const firstPurchase = relatedPurchases[0] || null;
  if (!firstPurchase) {
    return {
      paymentMethod: 'Wallet Card',
      payerName: '-',
      payerEmail: null,
      payerEmailMasked: null,
      walletCardMasked: '-'
    };
  }

  const users = db.users();
  const payer = users.find((user) => user && user.id === firstPurchase.payerUserId) || null;
  return {
    paymentMethod: 'Wallet Card',
    payerName: payer ? (payer.name || payer.email || payer.id) : (firstPurchase.payerUserId || '-'),
    payerEmail: payer ? (payer.email || null) : null,
    payerEmailMasked: payer && payer.email ? maskEmail(payer.email) : null,
    walletCardMasked: firstPurchase.payerCardLast4 ? `**** **** **** ${firstPurchase.payerCardLast4}` : '-'
  };
};

const buildInvoiceViewModel = (inv, options = {}) => {
  const paymentDetails = getInvoicePaymentDetails(inv);
  const items = getInvoiceItems(inv).map((item, index) => ({
    index: index + 1,
    title: item.projectTitle || item.projectId || 'Project',
    priceBefore: Number(item.priceBefore || 0),
    discountAmount: Number(item.discountAmount || 0),
    priceAfter: Number(item.priceAfter || 0)
  }));

  return {
    invoice: inv,
    invoiceDateArabic: formatInvoiceDateArabic(inv.createdAt),
    invoiceDateEnglish: formatInvoiceDate(inv.createdAt),
    paymentDetails: {
      ...paymentDetails,
      payerEmailDisplay: options.includePayerEmail
        ? (paymentDetails.payerEmail || '-')
        : (paymentDetails.payerEmailMasked || '-')
    },
    items,
    totals: {
      totalBefore: Number(inv.totalBefore || 0),
      totalDiscount: Number(inv.totalDiscount || 0),
      totalAfter: Number(inv.totalAfter || 0)
    },
    options: {
      includeEmail: Boolean(options.includeEmail),
      includeCoupon: Boolean(options.includeCoupon),
      includePayerEmail: Boolean(options.includePayerEmail),
      adminMode: Boolean(options.adminMode)
    }
  };
};

const getPurchaseChatContextForUser = (purchaseId, userId) => {
  const purchase = db.purchases().find((item) => item && item.id === purchaseId && item.userId === userId);
  if (!purchase) return null;
  return {
    purchaseId: purchase.id,
    orderId: purchase.orderId || purchase.id,
    projectId: purchase.projectId || null,
    projectTitle: purchase.projectTitle || 'مشروع',
    status: purchase.status || 'pending',
    purchasedAt: purchase.purchasedAt || null,
    price: purchase.price || 0,
    buyerId: purchase.userId,
    payerUserId: purchase.payerUserId || null
  };
};

const getPurchaseChatContextForAdmin = (purchaseId) => {
  const purchase = db.purchases().find((item) => item && item.id === purchaseId);
  if (!purchase) return null;
  return {
    purchaseId: purchase.id,
    orderId: purchase.orderId || purchase.id,
    projectId: purchase.projectId || null,
    projectTitle: purchase.projectTitle || 'مشروع',
    status: purchase.status || 'pending',
    purchasedAt: purchase.purchasedAt || null,
    price: purchase.price || 0,
    buyerId: purchase.userId,
    payerUserId: purchase.payerUserId || null
  };
};

const getCustomProjectRequestForUser = ({ requestId, userId }) => {
  return db.customProjectRequests().find((item) => item && item.id === requestId && item.userId === userId) || null;
};

const getCustomProjectRequestForAdmin = (requestId) => {
  return db.customProjectRequests().find((item) => item && item.id === requestId) || null;
};

const serializeNotificationItem = (entry) => {
  if (!entry) return null;
  return {
    id: entry.id || null,
    type: entry.type || null,
    source: entry.source || 'general',
    title: entry.title || '',
    message: entry.message || '',
    unread: Boolean(entry.unread),
    createdAt: entry.createdAt || null,
    metadata: entry.metadata || {}
  };
};

const serializeCustomProjectMessage = (message, currentUserId) => {
  if (!message) return null;
  return {
    id: message.id,
    content: message.content || '',
    senderId: message.senderId || null,
    senderName: message.senderName || null,
    isMine: message.senderId === currentUserId,
    createdAt: message.createdAt || null
  };
};

const serializeCustomProjectRequest = ({ request, includeMessages = false, currentUserId = null }) => {
  if (!request) return null;

  const payload = {
    id: request.id,
    title: request.title || '',
    projectType: request.projectType || '',
    description: request.description || '',
    budget: request.budget || null,
    timeline: request.timeline || null,
    status: request.status || 'new',
    adminReply: request.adminReply || null,
    quotedPrice: request.quotedPrice != null ? Number(request.quotedPrice) : null,
    quotedTimeline: request.quotedTimeline || null,
    paymentStatus: request.paymentStatus || 'unpaid',
    paidAt: request.paidAt || null,
    fileAvailable: Boolean(request.filePath),
    fileName: request.originalFileName || null,
    createdAt: request.createdAt || null,
    updatedAt: request.updatedAt || null
  };

  if (includeMessages) {
    payload.messages = db.messages()
      .filter((message) => (
        message && message.customProjectRequestId === request.id && (
          (message.senderId === currentUserId && message.receiverId === 'admin') ||
          (message.senderId === 'admin' && message.receiverId === currentUserId)
        )
      ))
      .sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0))
      .map((message) => serializeCustomProjectMessage(message, currentUserId))
      .filter(Boolean);
  }

  return payload;
};

const finalizeCustomProjectPayment = ({ requestId, buyerUserId, payerUserId, skipWalletDebit = false }) => {
  const requests = db.customProjectRequests();
  const index = requests.findIndex((item) => item && item.id === requestId && item.userId === buyerUserId);
  if (index === -1) throw new Error('طلب المشروع غير موجود');

  const users = db.users();
  const buyerIndex = users.findIndex((item) => item && item.id === buyerUserId && item.role === 'user');
  const payerIndex = users.findIndex((item) => item && item.id === payerUserId && item.role === 'user');
  if (buyerIndex === -1 || payerIndex === -1) throw new Error('المستخدم غير موجود');

  const buyer = users[buyerIndex];
  const payer = users[payerIndex];
  const request = requests[index];
  const amount = Math.round(Number(request.quotedPrice || 0) * 100) / 100;

  if (payer.walletCardFrozen) {
    throw new Error('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة');
  }

  if (!(amount > 0)) throw new Error('لم يتم تحديد سعر لهذا الطلب بعد');
  if (request.paymentStatus === 'paid') throw new Error('تم دفع هذا الطلب بالفعل');
  if (!skipWalletDebit) {
    if (Number(payer.walletBalance || 0) < amount) throw new Error('رصيد البطاقة غير كافٍ');
    payer.walletBalance = Math.round((Number(payer.walletBalance || 0) - amount) * 100) / 100;
    users[payerIndex] = payer;
    db.saveUsers(users);
  }

  requests[index] = {
    ...request,
    paymentStatus: 'paid',
    paidAt: new Date().toISOString(),
    paidByUserId: payer.id,
    status: request.status === 'new' ? 'accepted' : request.status,
    updatedAt: new Date().toISOString()
  };
  db.saveCustomProjectRequests(requests);

  try {
    const invoices = db.invoices();
    const invoiceOrderId = `custom-${request.id}`;
    const alreadyExists = invoices.some((item) => item && item.orderId === invoiceOrderId);
    if (!alreadyExists) {
      invoices.push({
        id: uuidv4(),
        invoiceNumber: buildInvoiceNumber(),
        orderId: invoiceOrderId,
        userId: buyer.id,
        userName: buyer.name || buyer.email || buyer.id,
        userEmail: buyer.email || null,
        paymentMethod: 'Wallet Card',
        payerUserId: payer.id,
        payerName: payer.name || payer.email || payer.id,
        payerEmail: payer.email || null,
        walletCardMasked: maskWalletCardNumber(payer.walletCardNumber),
        couponCode: null,
        items: [
          {
            purchaseId: request.id,
            projectId: request.id,
            projectTitle: request.title || 'طلب مشروع مخصص',
            priceBefore: amount,
            discountAmount: 0,
            priceAfter: amount
          }
        ],
        totalBefore: amount,
        totalDiscount: 0,
        totalAfter: amount,
        createdAt: new Date().toISOString()
      });
      db.saveInvoices(invoices);
    }
  } catch (error) {
    // ignore invoice errors
  }

  try {
    const refreshedUsers = db.users();
    const buyerNotifyIndex = refreshedUsers.findIndex((item) => item && item.id === buyer.id && item.role === 'user');
    if (buyerNotifyIndex !== -1) {
      addUserNotificationEntry({
        targetUser: refreshedUsers[buyerNotifyIndex],
        type: 'custom-project-paid',
        title: 'تم دفع طلب المشروع المخصص',
        message: `تم دفع طلب ${request.title || 'المشروع المخصص'} بنجاح.`,
        metadata: {
          customProjectRequestId: request.id,
          amount,
          projectTitle: request.title || null
        }
      });
      db.saveUsers(refreshedUsers);
    }
  } catch (error) {
    // ignore notification errors
  }

  return {
    buyer,
    payer,
    request: requests[index]
  };
};

const renderInvoicePdf = ({ res, inv, includeEmail = false, includeCoupon = false, includePayerEmail = false }) => {
  const doc = new PDFDocument({ size: 'A4', margin: 50 });
  applyInvoiceFont(doc);
  doc.pipe(res);

  const paymentDetails = getInvoicePaymentDetails(inv);

  doc.fontSize(20).text('Codentra', { align: 'center' });
  doc.fontSize(16).text('فاتورة / Invoice', { align: 'center' });
  doc.moveDown(1);

  writeInvoiceField(doc, 'رقم الفاتورة', 'Invoice No', inv.invoiceNumber || '-');
  writeInvoiceField(doc, 'التاريخ', 'Date', `${formatInvoiceDateArabic(inv.createdAt)} / ${formatInvoiceDate(inv.createdAt)}`);
  writeInvoiceField(doc, 'اسم العميل', 'Customer', inv.userName || '-');

  if (includeEmail && inv.userEmail) {
    writeInvoiceField(doc, 'البريد الإلكتروني', 'Email', inv.userEmail);
  }

  writeInvoiceField(doc, 'رقم الطلب', 'Order ID', inv.orderId || '-');
  writeInvoiceField(doc, 'طريقة الدفع', 'Payment Method', paymentDetails.paymentMethod || 'Wallet Card');
  writeInvoiceField(doc, 'تم الدفع من حساب', 'Paid From Account', paymentDetails.payerName || '-');
  writeInvoiceField(doc, 'البطاقة المستخدمة', 'Card Used', paymentDetails.walletCardMasked || '-');

  if (includePayerEmail && paymentDetails.payerEmail) {
    writeInvoiceField(doc, 'بريد الحساب الدافع', 'Payer Email', paymentDetails.payerEmail);
  } else if (paymentDetails.payerEmailMasked) {
    writeInvoiceField(doc, 'بريد الحساب الدافع', 'Payer Email', paymentDetails.payerEmailMasked);
  }

  if (includeCoupon && inv.couponCode) {
    writeInvoiceField(doc, 'كود الخصم', 'Coupon', inv.couponCode);
  }

  doc.moveDown(0.5);
  doc.fontSize(14).text('العناصر / Items', { underline: true, align: 'center' });
  doc.moveDown(0.5);

  const items = getInvoiceItems(inv);
  if (!items.length) {
    doc.fontSize(11).text('لا توجد عناصر في هذه الفاتورة', { align: 'right' });
    doc.text('No items available for this invoice', { align: 'left' });
  } else {
    items.forEach((item, index) => {
      const title = item.projectTitle || item.projectId || 'Project';
      doc.fontSize(11).text(`العنصر ${index + 1}: ${title}`, { align: 'right' });
      doc.text(`Item ${index + 1}: ${title}`, { align: 'left' });
      doc
        .fontSize(10)
        .text(
          `قبل الخصم: ${formatMoney(item.priceBefore)} EGP | الخصم: ${formatMoney(item.discountAmount)} EGP | بعد الخصم: ${formatMoney(item.priceAfter)} EGP`,
          { align: 'right' }
        );
      doc.text(
        `Before: ${formatMoney(item.priceBefore)} EGP | Discount: ${formatMoney(item.discountAmount)} EGP | After: ${formatMoney(item.priceAfter)} EGP`,
        { align: 'left' }
      );
      doc.moveDown(0.4);
    });
  }

  doc.moveDown(0.8);
  doc.fontSize(14).text('الملخص / Summary', { underline: true, align: 'center' });
  doc.moveDown(0.5);
  writeInvoiceField(doc, 'الإجمالي قبل الخصم', 'Total Before', `${formatMoney(inv.totalBefore)} EGP`);
  writeInvoiceField(doc, 'إجمالي الخصم', 'Total Discount', `${formatMoney(inv.totalDiscount)} EGP`);
  writeInvoiceField(doc, 'الإجمالي بعد الخصم', 'Total After', `${formatMoney(inv.totalAfter)} EGP`, {
    fontSize: 13
  });

  doc.end();
};

const HIGH_VALUE_PAYMENT_THRESHOLD = 5000;
const PAYMENT_VERIFICATION_TTL_MS = 10 * 60 * 1000;
const PAYMENT_VERIFICATION_CODE_LENGTH = 6;
const MAX_WALLET_CARD_NOTIFICATIONS = 40;
const MAX_WALLET_CARD_USAGE_LOG = 80;
const MAX_USER_NOTIFICATIONS = 60;
const MAX_WALLET_TOPUPS = 80;
const PRESENTATION_AI_MODEL = process.env.OPENAI_MODEL || 'gpt-5-mini';
const RECENTLY_VIEWED_LIMIT = 6;
const PAYMOB_BASE_URL = String(process.env.PAYMOB_BASE_URL || 'https://accept.paymob.com').replace(/\/+$/, '');
const PAYMOB_SECRET_KEY = String(process.env.PAYMOB_SECRET_KEY || '').trim();
const PAYMOB_PUBLIC_KEY = String(process.env.PAYMOB_PUBLIC_KEY || '').trim();
const PAYMOB_HMAC_SECRET = String(process.env.PAYMOB_HMAC_SECRET || '').trim();
const PAYMOB_INTEGRATION_ID = Number(process.env.PAYMOB_INTEGRATION_ID || 0);
const APP_BASE_URL = String(process.env.APP_BASE_URL || `http://localhost:${PORT}`).replace(/\/+$/, '');
const GOOGLE_CALLBACK_URL = String(process.env.GOOGLE_CALLBACK_URL || `${APP_BASE_URL}/auth/google/callback`).trim();
const GITHUB_CALLBACK_URL = String(process.env.GITHUB_CALLBACK_URL || `${APP_BASE_URL}/auth/github/callback`).trim();

const DEFAULT_PRESENTATION_PLANS = [
  {
    id: 'presentations-monthly',
    name: 'الاشتراك الشهري',
    interval: 'monthly',
    price: 499,
    durationDays: 30,
    generationCredits: 40,
    badge: 'الأكثر طلبًا',
    description: 'أنشئ حتى 40 بريزنتيشن شهريًا مع تحميل PowerPoint جاهز.'
  },
  {
    id: 'presentations-yearly',
    name: 'الاشتراك السنوي',
    interval: 'yearly',
    price: 4499,
    durationDays: 365,
    generationCredits: 600,
    badge: 'أفضل قيمة',
    description: 'حل سنوي للشركات والفرق مع 600 بريزنتيشن وتكلفة أقل لكل عرض.'
  }
];

const normalizeWalletCardNumber = (value) => {
  if (value == null) return '';
  return String(value).replace(/\D+/g, '').slice(0, 16);
};

const formatWalletCardNumber = (value) => {
  const digits = normalizeWalletCardNumber(value);
  if (!digits) return '';
  return digits.replace(/(.{4})/g, '$1 ').trim();
};

const maskWalletCardNumber = (value) => {
  const digits = normalizeWalletCardNumber(value);
  if (!digits) return null;
  return `**** **** **** ${digits.slice(-4)}`;
};

const createWalletCardNumber = (users = []) => {
  const used = new Set(
    users
      .map(u => normalizeWalletCardNumber(u && u.walletCardNumber))
      .filter(Boolean)
  );

  let candidate = '';
  do {
    candidate = Array.from({ length: 16 }, () => Math.floor(Math.random() * 10)).join('');
  } while (used.has(candidate));

  return candidate;
};

const maskEmail = (email) => {
  if (!email || typeof email !== 'string' || !email.includes('@')) return 'البريد المسجل';
  const [local, domain] = email.split('@');
  if (!local || !domain) return 'البريد المسجل';
  const visibleLocal = local.length <= 2 ? `${local[0] || '*'}*` : `${local.slice(0, 2)}${'*'.repeat(Math.max(1, local.length - 2))}`;
  const domainParts = domain.split('.');
  const root = domainParts.shift() || '';
  const tld = domainParts.join('.');
  const visibleRoot = root.length <= 2 ? `${root[0] || '*'}*` : `${root.slice(0, 2)}${'*'.repeat(Math.max(1, root.length - 2))}`;
  return `${visibleLocal}@${visibleRoot}${tld ? `.${tld}` : ''}`;
};

const generatePaymentVerificationCode = () => {
  const min = 10 ** (PAYMENT_VERIFICATION_CODE_LENGTH - 1);
  const max = (10 ** PAYMENT_VERIFICATION_CODE_LENGTH) - 1;
  return String(Math.floor(min + (Math.random() * (max - min + 1))));
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

const findUserByWalletCardNumber = ({ users, walletCardNumber }) => {
  const normalized = normalizeWalletCardNumber(walletCardNumber);
  if (!normalized) return null;
  return users.find(u => u && u.role === 'user' && normalizeWalletCardNumber(u.walletCardNumber) === normalized && !u.walletCardFrozen) || null;
};

// Email configuration - supports both SMTP and Resend API
let cachedMailTransporter = null;

const getMailTransporter = () => {
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 0);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  const from = process.env.SMTP_FROM || process.env.RESEND_FROM_EMAIL;

  if (!host || !port || !user || !pass || !from) {
    throw new Error('SMTP is not configured');
  }

  if (!cachedMailTransporter) {
    cachedMailTransporter = nodemailer.createTransport({
      host,
      port,
      secure: port === 465,
      auth: { user, pass },
      // Add timeout and connection settings
      connectionTimeout: 10000,
      greetingTimeout: 10000,
      socketTimeout: 10000,
      debug: process.env.NODE_ENV === 'development',
      logger: process.env.NODE_ENV === 'development'
    });
  }

  return { transporter: cachedMailTransporter, from };
};

// Verify transporter connection
const verifyMailTransporter = async () => {
  try {
    const { transporter } = getMailTransporter();
    await transporter.verify();
    return { success: true, error: null };
  } catch (error) {
    console.error('Email transporter verification failed:', error.message);
    return { success: false, error: error.message };
  }
};

const isMailConfigured = () => {
  return Boolean(
    (process.env.SMTP_HOST && process.env.SMTP_PORT && process.env.SMTP_USER && process.env.SMTP_PASS) ||
    process.env.RESEND_API_KEY
  ) && Boolean(process.env.SMTP_FROM || process.env.RESEND_FROM_EMAIL);
};

// Send email using Resend API (more reliable than SMTP)
const sendEmailViaResend = async ({ to, subject, text, html }) => {
  const apiKey = process.env.RESEND_API_KEY;
  const from = process.env.RESEND_FROM_EMAIL;
  
  if (!apiKey || !from) {
    throw new Error('Resend API not configured');
  }

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from,
      to: Array.isArray(to) ? to : [to],
      subject,
      text,
      html
    })
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Resend API error: ${error}`);
  }

  return await response.json();
};

const queueNotificationEmail = async ({ user, subject, title, message }) => {
  if (!user || user.role !== 'user' || !user.email) {
    console.log('Email not sent: Invalid user or missing email');
    return { success: false, error: 'Invalid user or missing email' };
  }
  
  if (!isMailConfigured()) {
    console.log('Email not sent: Mail not configured (check SMTP or RESEND_API_KEY)');
    return { success: false, error: 'Mail not configured' };
  }

  const safeTitle = title || subject || 'Codentra';
  const safeMessage = message || '';
  const safeSubject = subject || safeTitle;
  const to = user.email;
  
  const text = `${safeTitle}\n\n${safeMessage}\n\n---\nتم إرسال هذا الإيميل تلقائيًا من Codentra`;
  const html = `
    <div style="font-family:Arial,sans-serif;direction:rtl;text-align:right;max-width:600px;margin:0 auto;padding:20px;border:1px solid #e0e0e0;border-radius:8px;">
      <h2 style="color:#333;margin:0 0 20px 0">${safeTitle}</h2>
      <p style="color:#555;line-height:1.8;font-size:16px">${String(safeMessage).replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br>')}</p>
      <hr style="margin:24px 0;border:none;border-top:1px solid #e0e0e0"/>
      <small style="color:#888;font-size:12px">تم إرسال هذا الإيميل تلقائيًا من Codentra</small>
    </div>
  `;

  try {
    // Try Resend API first (more reliable)
    if (process.env.RESEND_API_KEY) {
      console.log(`Sending email via Resend to: ${to}`);
      const result = await sendEmailViaResend({ to, subject: safeSubject, text, html });
      console.log(`Email sent successfully via Resend:`, result.id);
      return { success: true, provider: 'resend', id: result.id };
    }
    
    // Fallback to SMTP
    if (process.env.SMTP_HOST) {
      console.log(`Sending email via SMTP to: ${to}`);
      const { transporter, from } = getMailTransporter();
      const result = await transporter.sendMail({
        from,
        to,
        subject: safeSubject,
        text,
        html
      });
      console.log(`Email sent successfully via SMTP:`, result.messageId);
      return { success: true, provider: 'smtp', id: result.messageId };
    }
    
    throw new Error('No email provider configured');
  } catch (error) {
    console.error('Failed to send email:', error.message);
    return { success: false, error: error.message };
  }
};

const sendPaymentVerificationCode = async ({ payerUser, buyerUser, code, amount, maskedCardNumber }) => {
  const subject = 'Codentra payment verification code';
  const text = [
    `Hello ${payerUser.name || 'User'},`,
    '',
    `A payment request was created on Codentra using your wallet card ${maskedCardNumber || ''}.`,
    `Buyer account: ${buyerUser && (buyerUser.name || buyerUser.email || buyerUser.id) ? (buyerUser.name || buyerUser.email || buyerUser.id) : 'Unknown user'}`,
    `Amount: ${formatMoney(amount)} EGP`,
    `Verification code: ${code}`,
    '',
    'The code expires in 10 minutes.',
    'If this request was not expected, do not share this code.'
  ].join('\n');
  const html = `
    <div style="font-family:Arial,sans-serif;direction:ltr;max-width:600px;margin:0 auto;padding:20px;border:1px solid #e0e0e0;border-radius:8px;">
      <h2>Codentra payment verification</h2>
      <p>Hello ${payerUser.name || 'User'},</p>
      <p>A payment request was created using your wallet card <strong>${maskedCardNumber || ''}</strong>.</p>
      <p><strong>Buyer:</strong> ${buyerUser && (buyerUser.name || buyerUser.email || buyerUser.id) ? (buyerUser.name || buyerUser.email || buyerUser.id) : 'Unknown user'}</p>
      <p><strong>Amount:</strong> ${formatMoney(amount)} EGP</p>
      <p><strong>Verification code:</strong> <span style="font-size:28px;letter-spacing:4px;color:#007bff;font-weight:bold">${code}</span></p>
      <p>This code expires in 10 minutes. If this request was not expected, do not share this code.</p>
    </div>
  `;

  try {
    // Try Resend API first
    if (process.env.RESEND_API_KEY) {
      console.log(`Sending verification email via Resend to: ${payerUser.email}`);
      const result = await sendEmailViaResend({
        to: payerUser.email,
        subject,
        text,
        html
      });
      console.log('Verification email sent via Resend:', result.id);
      return { success: true, provider: 'resend', id: result.id };
    }
    
    // Fallback to SMTP
    if (process.env.SMTP_HOST) {
      console.log(`Sending verification email via SMTP to: ${payerUser.email}`);
      const { transporter, from } = getMailTransporter();
      const result = await transporter.sendMail({
        from,
        to: payerUser.email,
        subject,
        text,
        html
      });
      console.log('Verification email sent via SMTP:', result.messageId);
      return { success: true, provider: 'smtp', id: result.messageId };
    }
    
    throw new Error('No email provider configured');
  } catch (error) {
    console.error('Failed to send verification email:', error.message);
    throw error;
  }
};

const sanitizeWalletCardSpendingLimit = (value) => {
  if (value == null || value === '') return null;
  const amount = Math.round(Number(value) * 100) / 100;
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return amount;
};

const sanitizeWalletCardSpendingLimitUsd = (value) => {
  if (value == null || value === '') return null;
  const amountUsd = roundCurrencyAmount(value);
  if (!Number.isFinite(amountUsd) || amountUsd <= 0) return null;
  return convertUsdToEgp(amountUsd);
};

const prependLimitedEntry = ({ items, entry, limit }) => {
  const current = Array.isArray(items) ? items : [];
  return [entry, ...current].slice(0, limit);
};

const buildWalletPaymentBuyerLabel = (buyerUser) => {
  if (!buyerUser) return 'مستخدم غير معروف';
  return buyerUser.name || buyerUser.email || buyerUser.id || 'مستخدم غير معروف';
};

const buildWalletPaymentPurposeLabel = ({ kind, payload }) => {
  if (kind === 'single-project') {
    return `شراء المشروع: ${payload && (payload.projectTitle || payload.projectId) ? (payload.projectTitle || payload.projectId) : 'مشروع'}`;
  }

  if (kind === 'cart') {
    const count = Array.isArray(payload && payload.projectTitles)
      ? payload.projectTitles.length
      : Array.isArray(payload && payload.projectIds)
        ? payload.projectIds.length
        : 0;
    return count > 0 ? `شراء ${count} مشروع من السلة` : 'شراء من السلة';
  }

  if (kind === 'presentations-subscription') {
    return `الاشتراك في: ${payload && payload.planName ? payload.planName : 'Codentra Presentations'}`;
  }

  if (kind === 'custom-project') {
    return `دفع طلب مشروع مخصص: ${payload && payload.title ? payload.title : 'طلب مشروع مخصص'}`;
  }

  return 'استخدام بطاقة المحفظة';
};

const addWalletCardNotificationEntry = ({ ownerUser, title, message, metadata = {} }) => {
  if (!ownerUser) return null;
  const entry = {
    id: uuidv4(),
    title,
    message,
    metadata,
    unread: true,
    createdAt: new Date().toISOString()
  };
  ownerUser.walletCardNotifications = prependLimitedEntry({
    items: ownerUser.walletCardNotifications,
    entry,
    limit: MAX_WALLET_CARD_NOTIFICATIONS
  });

  // Optional: send email copy of notifications
  queueNotificationEmail({
    user: ownerUser,
    subject: title || 'إشعار من Codentra',
    title,
    message
  });

  return entry;
};

const addUserNotificationEntry = ({ targetUser, type = 'general', title, message, metadata = {} }) => {
  if (!targetUser || targetUser.role !== 'user') return null;

  const entry = {
    id: uuidv4(),
    type,
    title,
    message,
    metadata,
    unread: true,
    createdAt: new Date().toISOString()
  };

  targetUser.notifications = prependLimitedEntry({
    items: targetUser.notifications,
    entry,
    limit: MAX_USER_NOTIFICATIONS
  });

  // Send email copy of notification (await to catch errors)
  queueNotificationEmail({
    user: targetUser,
    subject: title || 'إشعار من Codentra',
    title,
    message
  }).then(result => {
    if (!result.success) {
      console.error('Failed to send notification email:', result.error);
    }
  });

  return entry;
};

const normalizeUserNotifications = (user) => {
  if (!user || user.role !== 'user') return false;

  let changed = false;

  if (!Array.isArray(user.notifications)) {
    user.notifications = [];
    changed = true;
  }

  if (Array.isArray(user.walletCardNotifications)) {
    user.walletCardNotifications = user.walletCardNotifications.map((entry) => {
      if (entry && typeof entry.unread === 'undefined') {
        changed = true;
        return { ...entry, unread: false };
      }
      return entry;
    });
  }

  if (Array.isArray(user.notifications)) {
    user.notifications = user.notifications.map((entry) => {
      if (entry && typeof entry.unread === 'undefined') {
        changed = true;
        return { ...entry, unread: false };
      }
      return entry;
    });
  }

  return changed;
};

const getNotificationCenterItems = (user) => {
  if (!user || user.role !== 'user') return [];

  const genericItems = Array.isArray(user.notifications)
    ? user.notifications.map((entry) => ({ ...entry, source: 'general' }))
    : [];

  const walletItems = Array.isArray(user.walletCardNotifications)
    ? user.walletCardNotifications.map((entry) => ({ ...entry, source: 'wallet', type: 'wallet-card' }))
    : [];

  return [...genericItems, ...walletItems]
    .filter(Boolean)
    .sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));
};

const getUnreadNotificationCount = (user) => getNotificationCenterItems(user).filter((entry) => entry && entry.unread).length;

const getUserRealtimeSummary = ({ userId }) => {
  const users = db.users();
  const user = users.find((item) => item && item.id === userId && item.role === 'user') || null;
  const messages = db.messages();
  const customRequests = db.customProjectRequests();
  const applications = getCommunityJobApplicationsState();
  const appointmentsData = migrateAppointmentsBookingsMeetingLinks();
  const purchases = db.purchases();
  const topups = db.walletTopups ? db.walletTopups() : [];

  const unreadNotifications = user ? getUnreadNotificationCount(user) : 0;
  const unreadMessages = messages.filter((message) => (
    message &&
    message.senderId === 'admin' &&
    message.receiverId === userId &&
    !message.read
  )).length;
  const unreadCustomProjectMessages = messages.filter((message) => (
    message &&
    message.customProjectRequestId &&
    message.senderId === 'admin' &&
    message.receiverId === userId &&
    !message.read
  )).length;

  const latestPurchaseAt = purchases
    .filter((purchase) => purchase && purchase.userId === userId)
    .map((purchase) => new Date(purchase.purchasedAt || 0).getTime())
    .reduce((max, value) => Math.max(max, value), 0);

  const latestApplicationUpdateAt = applications
    .filter((application) => application && application.userId === userId)
    .map((application) => new Date(application.statusUpdatedAt || application.createdAt || 0).getTime())
    .reduce((max, value) => Math.max(max, value), 0);

  const latestCustomProjectUpdateAt = customRequests
    .filter((request) => request && request.userId === userId)
    .map((request) => new Date(request.updatedAt || request.createdAt || 0).getTime())
    .reduce((max, value) => Math.max(max, value), 0);

  const latestAppointmentUpdateAt = (appointmentsData.bookings || [])
    .filter((booking) => booking && booking.userId === userId)
    .map((booking) => new Date(booking.createdAt || booking.startAt || 0).getTime())
    .reduce((max, value) => Math.max(max, value), 0);

  const latestWalletTopupAt = topups
    .filter((topup) => topup && topup.userId === userId)
    .map((topup) => new Date(topup.updatedAt || topup.paidAt || topup.createdAt || 0).getTime())
    .reduce((max, value) => Math.max(max, value), 0);

  const latestNotificationAt = user
    ? getNotificationCenterItems(user)
        .map((entry) => new Date(entry && entry.createdAt ? entry.createdAt : 0).getTime())
        .reduce((max, value) => Math.max(max, value), 0)
    : 0;

  return {
    unreadNotifications,
    unreadMessages,
    unreadCustomProjectMessages,
    latestNotificationAt: latestNotificationAt || 0,
    latestPurchaseAt: latestPurchaseAt || 0,
    latestApplicationUpdateAt: latestApplicationUpdateAt || 0,
    latestCustomProjectUpdateAt: latestCustomProjectUpdateAt || 0,
    latestAppointmentUpdateAt: latestAppointmentUpdateAt || 0,
    latestWalletTopupAt: latestWalletTopupAt || 0
  };
};

const markAllNotificationsAsRead = (user) => {
  if (!user || user.role !== 'user') return false;

  let changed = false;

  if (Array.isArray(user.notifications)) {
    user.notifications = user.notifications.map((entry) => {
      if (entry && entry.unread) {
        changed = true;
        return { ...entry, unread: false };
      }
      return entry;
    });
  }

  if (Array.isArray(user.walletCardNotifications)) {
    user.walletCardNotifications = user.walletCardNotifications.map((entry) => {
      if (entry && entry.unread) {
        changed = true;
        return { ...entry, unread: false };
      }
      return entry;
    });
  }

  return changed;
};

const isAtlosConfigured = () => Boolean(ATLOS_API_URL && ATLOS_MERCHANT_ID && ATLOS_API_SECRET);

const normalizeWalletTopupUsdAmount = (value) => {
  const amount = roundCurrencyAmount(value);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  if (amount > 10000) return null;
  const egpAmount = convertUsdToEgp(amount);
  if (!Number.isFinite(egpAmount) || egpAmount <= 0) return null;
  return amount;
};

const getWalletTopupStatusLabel = (status) => {
  switch (status) {
    case 'paid':
      return 'تم الدفع';
    case 'failed':
      return 'فشل الدفع';
    case 'processing':
      return 'قيد المعالجة';
    default:
      return 'بانتظار الدفع';
  }
};

const buildWalletTopupReference = () => `topup_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

const createWalletTopupRecord = ({ user, amountUsd, exchangeRate }) => {
  const safeRate = Number(exchangeRate || getCachedEgpToUsdRate());
  const amount = convertUsdToEgp(amountUsd, safeRate);
  return {
    id: uuidv4(),
    reference: buildWalletTopupReference(),
    userId: user.id,
    amount,
    amountUsd: roundCurrencyAmount(amountUsd),
    exchangeRate: safeRate,
    sourceCurrency: 'EGP',
    currency: 'USD',
    gateway: 'atlos',
    status: 'pending',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    gatewayTransactionId: null,
    gatewayPaymentId: null,
    checkoutUrl: null,
    paidAt: null,
    failureReason: null
  };
};

const createAtlosCheckout = async ({ topup, user }) => {
  if (!isAtlosConfigured()) {
    throw new Error('ATLOS_CONFIG_MISSING');
  }

  const payload = {
    MerchantId: ATLOS_MERCHANT_ID,
    OrderId: topup.reference,
    OrderAmount: roundCurrencyAmount(topup.amountUsd || convertEgpToUsd(topup.amount)),
    OrderCurrency: 'USD',
    UserName: user.name || 'Codentra User',
    UserEmail: user.email || null,
    Memo: `Codentra wallet top-up for ${user.email || user.id}`,
    SendEmail: false,
    PostbackUrl: `${APP_BASE_URL}/webhooks/atlos?topupId=${encodeURIComponent(topup.id)}`,
    ReturnUrl: `${APP_BASE_URL}/wallet/topup/return?reference=${encodeURIComponent(topup.reference)}`
  };

  const response = await fetch(`${ATLOS_API_URL}/Invoice/Create`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'ApiSecret': ATLOS_API_SECRET
    },
    body: JSON.stringify(payload)
  });

  const text = await response.text();
  let parsed = null;

  try {
    parsed = text ? JSON.parse(text) : null;
  } catch (error) {
    parsed = null;
  }

  if (!response.ok) {
    throw new Error(parsed && (parsed.error || parsed.message) ? (parsed.error || parsed.message) : `ATLOS_REQUEST_FAILED:${response.status}`);
  }

  const checkoutUrl =
    (parsed && (parsed.PaymentLink || parsed.paymentLink || parsed.checkoutUrl || parsed.url)) ||
    (parsed && parsed.data && (parsed.data.PaymentLink || parsed.data.paymentLink || parsed.data.checkoutUrl || parsed.data.url)) ||
    null;
  const paymentId =
    (parsed && (parsed.Id || parsed.paymentId || parsed.id)) ||
    (parsed && parsed.data && (parsed.data.Id || parsed.data.paymentId || parsed.data.id)) ||
    null;

  if (!checkoutUrl) {
    throw new Error('ATLOS_CHECKOUT_URL_MISSING');
  }

  return {
    paymentId,
    checkoutUrl,
    raw: parsed
  };
};

const finalizeWalletTopup = ({ topupId, gatewayTransactionId = null, status = 'paid', failureReason = null }) => {
  const topups = db.walletTopups();
  const topupIndex = topups.findIndex((item) => item && item.id === topupId);
  if (topupIndex === -1) return { ok: false, reason: 'TOPUP_NOT_FOUND' };

  const topup = topups[topupIndex];
  if (topup.status === 'paid') {
    return { ok: true, topup, alreadyFinalized: true };
  }

  topup.status = status;
  topup.updatedAt = new Date().toISOString();
  topup.gatewayTransactionId = gatewayTransactionId || topup.gatewayTransactionId || null;
  topup.failureReason = failureReason || null;

  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === topup.userId && item.role === 'user');

  if (status === 'paid') {
    topup.paidAt = new Date().toISOString();
    if (userIndex !== -1) {
      const balanceBefore = Number(users[userIndex].walletBalance || 0);
      users[userIndex].walletBalance = Math.round((balanceBefore + Number(topup.amount || 0)) * 100) / 100;
      addUserNotificationEntry({
        targetUser: users[userIndex],
        type: 'wallet-topup-paid',
        title: 'تم شحن الرصيد بنجاح',
        message: `تم إضافة ${formatUsdAmount(topup.amountUsd || convertEgpToUsd(topup.amount))} إلى رصيدك عبر Atlos.`,
        metadata: {
          topupId: topup.id,
          amount: Number(topup.amount || 0),
          amountUsd: Number(topup.amountUsd || convertEgpToUsd(topup.amount)),
          gateway: 'atlos'
        }
      });
      db.saveUsers(users);
    }
  }

  if (status === 'failed' && userIndex !== -1) {
    addUserNotificationEntry({
      targetUser: users[userIndex],
      type: 'wallet-topup-failed',
      title: 'فشل شحن الرصيد',
      message: 'لم تكتمل عملية شحن الرصيد. يمكنك المحاولة مرة أخرى.',
        metadata: {
          topupId: topup.id,
          amount: Number(topup.amount || 0),
          amountUsd: Number(topup.amountUsd || convertEgpToUsd(topup.amount)),
          gateway: 'atlos'
        }
      });
    db.saveUsers(users);
  }

  topups[topupIndex] = topup;
  db.saveWalletTopups(topups);

  return { ok: true, topup, alreadyFinalized: false };
};

const addWalletCardUsageLogEntry = ({ ownerUser, status, amount, buyerUser, purposeLabel, attemptId = null, note = null }) => {
  if (!ownerUser) return null;
  const entry = {
    id: uuidv4(),
    status,
    amount: Math.round(Number(amount || 0) * 100) / 100,
    buyerUserId: buyerUser ? buyerUser.id : null,
    buyerName: buildWalletPaymentBuyerLabel(buyerUser),
    purposeLabel: purposeLabel || 'استخدام بطاقة المحفظة',
    note: note || null,
    attemptId,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  ownerUser.walletCardUsageLog = prependLimitedEntry({
    items: ownerUser.walletCardUsageLog,
    entry,
    limit: MAX_WALLET_CARD_USAGE_LOG
  });
  return entry;
};

const updateWalletCardUsageLogEntry = ({ ownerUser, entryId, patch = {} }) => {
  if (!ownerUser || !entryId || !Array.isArray(ownerUser.walletCardUsageLog)) return false;
  const index = ownerUser.walletCardUsageLog.findIndex((entry) => entry && entry.id === entryId);
  if (index === -1) return false;
  ownerUser.walletCardUsageLog[index] = {
    ...ownerUser.walletCardUsageLog[index],
    ...patch,
    updatedAt: new Date().toISOString()
  };
  return true;
};

const enforceWalletCardSpendingLimit = ({ payerUser, amount }) => {
  const limit = sanitizeWalletCardSpendingLimit(payerUser && payerUser.walletCardSpendingLimit);
  const requestedAmount = Math.round(Number(amount || 0) * 100) / 100;
  if (limit != null && requestedAmount > limit) {
    throw new Error(`المبلغ يتجاوز حد البطاقة المحدد (${formatUsdAmount(convertEgpToUsd(limit))})`);
  }
};

const getWalletIncomingApprovals = ({ ownerUserId }) => {
  if (!ownerUserId) return [];
  purgeExpiredWalletPaymentAttempts();
  return getWalletPaymentAttemptsState()
    .filter((attempt) => attempt && attempt.payerUserId === ownerUserId && !attempt.usedAt && !isPaymentAttemptExpired(attempt))
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
};

const registerWalletPaymentRequestForOwner = ({ payerUserId, buyerUser, amount, kind, payload, attemptId, code, expiresAt }) => {
  const users = db.users();
  const ownerIndex = users.findIndex((user) => user && user.id === payerUserId && user.role === 'user');
  if (ownerIndex === -1) return null;

  const owner = users[ownerIndex];
  if (ensureUserPaymentProfile({ user: owner, users })) {
    users[ownerIndex] = owner;
  }

  const purposeLabel = buildWalletPaymentPurposeLabel({ kind, payload });
  const usageEntry = addWalletCardUsageLogEntry({
    ownerUser: owner,
    status: 'pending_verification',
    amount,
    buyerUser,
    purposeLabel,
    attemptId,
    note: 'بانتظار إدخال كود التحقق لإتمام الدفع'
  });

  addWalletCardNotificationEntry({
    ownerUser: owner,
    title: 'طلب استخدام بطاقتك',
    message: `${buildWalletPaymentBuyerLabel(buyerUser)} طلب استخدام بطاقتك في ${purposeLabel}.`,
    metadata: {
      attemptId,
      amount: Math.round(Number(amount || 0) * 100) / 100,
      verificationCode: code,
      expiresAt,
      purposeLabel
    }
  });

  users[ownerIndex] = owner;
  db.saveUsers(users);

  return usageEntry;
};

const finalizeWalletCardOwnerActivity = ({ payerUserId, buyerUser, amount, kind, payload, attemptId = null, usageLogEntryId = null, status, note = null, notifyTitle, notifyMessage }) => {
  const users = db.users();
  const ownerIndex = users.findIndex((user) => user && user.id === payerUserId && user.role === 'user');
  if (ownerIndex === -1) return false;

  const owner = users[ownerIndex];
  if (ensureUserPaymentProfile({ user: owner, users })) {
    users[ownerIndex] = owner;
  }

  const purposeLabel = buildWalletPaymentPurposeLabel({ kind, payload });
  const updated = usageLogEntryId
    ? updateWalletCardUsageLogEntry({
        ownerUser: owner,
        entryId: usageLogEntryId,
        patch: {
          status,
          note: note || null,
          amount: Math.round(Number(amount || 0) * 100) / 100,
          purposeLabel
        }
      })
    : false;

  if (!updated) {
    addWalletCardUsageLogEntry({
      ownerUser: owner,
      status,
      amount,
      buyerUser,
      purposeLabel,
      attemptId,
      note
    });
  }

  if (notifyTitle && notifyMessage) {
    addWalletCardNotificationEntry({
      ownerUser: owner,
      title: notifyTitle,
      message: notifyMessage,
      metadata: {
        attemptId,
        amount: Math.round(Number(amount || 0) * 100) / 100,
        purposeLabel
      }
    });
  }

  users[ownerIndex] = owner;
  db.saveUsers(users);
  return true;
};

// JWT Helpers
const JWT_SECRET = 'codentra-jwt-secret-2024';
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

const requireApiUserAuth = (req, res, next) => {
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
const RUNTIME_ROOT_DIR = IS_VERCEL ? path.join('/tmp', 'codentra-runtime') : APP_ROOT_DIR;
const DATA_DIR = IS_VERCEL ? path.join(RUNTIME_ROOT_DIR, 'data') : BUNDLED_DATA_DIR;
const UPLOADS_DIR = IS_VERCEL ? path.join(RUNTIME_ROOT_DIR, 'uploads') : BUNDLED_UPLOADS_DIR;
const MEETING_RECORDINGS_DIR = path.join(UPLOADS_DIR, 'meeting-recordings');
const ADMIN_TEAM_UPLOADS_DIR = path.join(UPLOADS_DIR, 'admin-team');
const PRIVATE_UPLOADS_DIR = IS_VERCEL ? path.join(RUNTIME_ROOT_DIR, 'private_uploads') : BUNDLED_PRIVATE_UPLOADS_DIR;
const COMMUNITY_MEDIA_DIR = path.join(UPLOADS_DIR, 'community-media');
const COMMUNITY_CVS_DIR = path.join(PRIVATE_UPLOADS_DIR, 'community-cvs');
const ensureDirectory = (dirPath) => {
  if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
};

const copyFileIfMissing = (sourcePath, targetPath) => {
  if (!sourcePath || !targetPath) return;
  if (!fs.existsSync(sourcePath) || fs.existsSync(targetPath)) return;
  ensureDirectory(path.dirname(targetPath));
  fs.copyFileSync(sourcePath, targetPath);
};

// Ensure directories exist
[DATA_DIR, UPLOADS_DIR, MEETING_RECORDINGS_DIR, ADMIN_TEAM_UPLOADS_DIR, PRIVATE_UPLOADS_DIR, COMMUNITY_MEDIA_DIR, COMMUNITY_CVS_DIR].forEach(ensureDirectory);

if (IS_VERCEL && fs.existsSync(BUNDLED_DATA_DIR)) {
  fs.readdirSync(BUNDLED_DATA_DIR).forEach((entry) => {
    const sourcePath = path.join(BUNDLED_DATA_DIR, entry);
    if (!fs.statSync(sourcePath).isFile()) return;
    copyFileIfMissing(sourcePath, path.join(DATA_DIR, entry));
  });
}

const cloneStoredValue = (value) => JSON.parse(JSON.stringify(value));

const createDefaultAppointmentsState = () => ({ timeSlots: [], bookings: [] });

const createDefaultLoyaltySettingsState = () => ({
  enabled: true,
  pointsPerEGP: 0.1,
  redeem: {
    enabled: true,
    pointsToEGP: 0.1,
    minPoints: 100
  }
});

const normalizeLoyaltySettingsState = (value) => {
  const defaults = createDefaultLoyaltySettingsState();
  const parsed = value && typeof value === 'object' && !Array.isArray(value) ? cloneStoredValue(value) : {};

  if (parsed.enabled === undefined) parsed.enabled = defaults.enabled;
  if (!Number.isFinite(Number(parsed.pointsPerEGP)) || Number(parsed.pointsPerEGP) <= 0) {
    parsed.pointsPerEGP = defaults.pointsPerEGP;
  } else {
    parsed.pointsPerEGP = Number(parsed.pointsPerEGP);
  }

  parsed.redeem = parsed.redeem && typeof parsed.redeem === 'object' && !Array.isArray(parsed.redeem)
    ? parsed.redeem
    : {};

  if (parsed.redeem.enabled === undefined) parsed.redeem.enabled = defaults.redeem.enabled;
  if (!Number.isFinite(Number(parsed.redeem.pointsToEGP)) || Number(parsed.redeem.pointsToEGP) <= 0) {
    parsed.redeem.pointsToEGP = defaults.redeem.pointsToEGP;
  } else {
    parsed.redeem.pointsToEGP = Number(parsed.redeem.pointsToEGP);
  }
  if (!Number.isFinite(Number(parsed.redeem.minPoints)) || Number(parsed.redeem.minPoints) <= 0) {
    parsed.redeem.minPoints = defaults.redeem.minPoints;
  } else {
    parsed.redeem.minPoints = Number(parsed.redeem.minPoints);
  }

  return parsed;
};

const getLoyaltyRedeemSettings = () => {
  const settings = normalizeLoyaltySettingsState(db.loyaltySettings ? db.loyaltySettings() : null);
  return settings && settings.redeem
    ? settings.redeem
    : createDefaultLoyaltySettingsState().redeem;
};

const STORAGE_TABLE_NAME = 'data_store';
const STORAGE_ASSET_TABLE_NAME = 'binary_assets';
const STORAGE_DATASET_NAME = process.env.NEON_DATASET_NAME || 'codentra-server';
const storageRequestContext = new AsyncLocalStorage();
let storageReadyPromise = null;
let storagePersistQueue = Promise.resolve();
let pgPool = null;

const STORAGE_FILE_DEFINITIONS = {
  users: { fileName: 'users.json', createDefault: () => [] },
  projects: { fileName: 'projects.json', createDefault: () => [] },
  purchases: { fileName: 'purchases.json', createDefault: () => [] },
  modifications: { fileName: 'modifications.json', createDefault: () => [] },
  customProjectRequests: { fileName: 'custom-project-requests.json', createDefault: () => [] },
  coupons: { fileName: 'coupons.json', createDefault: () => [] },
  referrals: { fileName: 'referrals.json', createDefault: () => [] },
  walletCodes: { fileName: 'wallet-codes.json', createDefault: () => [] },
  walletTopups: { fileName: 'wallet-topups.json', createDefault: () => [] },
  walletPaymentAttempts: { fileName: 'wallet-payment-attempts.json', createDefault: () => [] },
  reviews: { fileName: 'reviews.json', createDefault: () => [] },
  messages: { fileName: 'messages.json', createDefault: () => [] },
  communityPosts: { fileName: 'community-posts.json', createDefault: () => [] },
  communityJobs: { fileName: 'community-jobs.json', createDefault: () => [] },
  communityJobApplications: { fileName: 'community-job-applications.json', createDefault: () => [] },
  adminTeamMessages: { fileName: 'admin-team-messages.json', createDefault: () => [] },
  carts: { fileName: 'carts.json', createDefault: () => [] },
  invoices: { fileName: 'invoices.json', createDefault: () => [] },
  appointments: { fileName: 'appointments.json', createDefault: createDefaultAppointmentsState },
  meetingRecordings: { fileName: 'meeting-recordings.json', createDefault: () => [] },
  cartReminders: { fileName: 'cart-reminders.json', createDefault: () => [] },
  downloadEvents: { fileName: 'download-events.json', createDefault: () => [] },
  projectViewEvents: { fileName: 'project-view-events.json', createDefault: () => [] },
  fileHealthReports: { fileName: 'file-health-reports.json', createDefault: () => [] },
  adminAuditLog: { fileName: 'admin-audit-log.json', createDefault: () => [] },
  subscriptionPlans: { fileName: 'subscription-plans.json', createDefault: () => [] },
  subscriptions: { fileName: 'subscriptions.json', createDefault: () => [] },
  subscriptionPayments: { fileName: 'subscription-payments.json', createDefault: () => [] },
  subscriptionCoupons: { fileName: 'subscription-coupons.json', createDefault: () => [] },
  presentationPlans: { fileName: 'presentation-plans.json', createDefault: () => [] },
  presentationSubscriptions: { fileName: 'presentation-subscriptions.json', createDefault: () => [] },
  presentationDecks: { fileName: 'presentation-decks.json', createDefault: () => [] },
  presentationPaymentAttempts: { fileName: 'presentation-payment-attempts.json', createDefault: () => [] },
  loyaltySettings: {
    fileName: 'loyalty-settings.json',
    createDefault: createDefaultLoyaltySettingsState,
    normalize: normalizeLoyaltySettingsState
  }
};

const STORAGE_READ_ACCESSORS = {
  users: 'users',
  projects: 'projects',
  purchases: 'purchases',
  modifications: 'modifications',
  customProjectRequests: 'customProjectRequests',
  coupons: 'coupons',
  referrals: 'referrals',
  walletCodes: 'walletCodes',
  walletTopups: 'walletTopups',
  walletPaymentAttempts: 'walletPaymentAttempts',
  reviews: 'reviews',
  messages: 'messages',
  communityPosts: 'communityPosts',
  communityJobs: 'communityJobs',
  communityJobApplications: 'communityJobApplications',
  adminTeamMessages: 'adminTeamMessages',
  carts: 'carts',
  invoices: 'invoices',
  appointments: 'appointments',
  meetingRecordings: 'meetingRecordings',
  cartReminders: 'cartReminders',
  downloadEvents: 'downloadEvents',
  projectViewEvents: 'projectViewEvents',
  fileHealthReports: 'fileHealthReports',
  adminAuditLog: 'adminAuditLog',
  subscriptionPlans: 'subscriptionPlans',
  subscriptions: 'subscriptions',
  subscriptionPayments: 'subscriptionPayments',
  subscriptionCoupons: 'subscriptionCoupons',
  presentationPlans: 'presentationPlans',
  presentationSubscriptions: 'presentationSubscriptions',
  presentationDecks: 'presentationDecks',
  presentationPaymentAttempts: 'presentationPaymentAttempts',
  loyaltySettings: 'loyaltySettings'
};

const STORAGE_WRITE_ACCESSORS = {
  saveUsers: 'users',
  saveProjects: 'projects',
  savePurchases: 'purchases',
  saveModifications: 'modifications',
  saveCustomProjectRequests: 'customProjectRequests',
  saveCoupons: 'coupons',
  saveReferrals: 'referrals',
  saveWalletCodes: 'walletCodes',
  saveWalletTopups: 'walletTopups',
  saveWalletPaymentAttempts: 'walletPaymentAttempts',
  saveReviews: 'reviews',
  saveMessages: 'messages',
  saveCommunityPosts: 'communityPosts',
  saveCommunityJobs: 'communityJobs',
  saveCommunityJobApplications: 'communityJobApplications',
  saveAdminTeamMessages: 'adminTeamMessages',
  saveCarts: 'carts',
  saveInvoices: 'invoices',
  saveAppointments: 'appointments',
  saveMeetingRecordings: 'meetingRecordings',
  saveCartReminders: 'cartReminders',
  saveDownloadEvents: 'downloadEvents',
  saveProjectViewEvents: 'projectViewEvents',
  saveFileHealthReports: 'fileHealthReports',
  saveAdminAuditLog: 'adminAuditLog',
  saveSubscriptionPlans: 'subscriptionPlans',
  saveSubscriptions: 'subscriptions',
  saveSubscriptionPayments: 'subscriptionPayments',
  saveSubscriptionCoupons: 'subscriptionCoupons',
  savePresentationPlans: 'presentationPlans',
  savePresentationSubscriptions: 'presentationSubscriptions',
  savePresentationDecks: 'presentationDecks',
  savePresentationPaymentAttempts: 'presentationPaymentAttempts',
  saveLoyaltySettings: 'loyaltySettings'
};

const usePostgresStorage = () => Boolean(DATABASE_URL);

const readStateValueFromDisk = (definition) => {
  const filePath = path.join(DATA_DIR, definition.fileName);
  const fallback = definition.createDefault();

  if (!fs.existsSync(filePath)) return fallback;

  try {
    const raw = fs.readFileSync(filePath, 'utf8').trim();
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch (error) {
    return fallback;
  }
};

const buildStorageState = ({ seedFromDisk = false, source = null } = {}) => {
  const state = {};

  Object.entries(STORAGE_FILE_DEFINITIONS).forEach(([key, definition]) => {
    const hasSourceValue = source && Object.prototype.hasOwnProperty.call(source, key);
    let value = hasSourceValue
      ? source[key]
      : (seedFromDisk ? readStateValueFromDisk(definition) : definition.createDefault());

    if (value == null) value = definition.createDefault();
    if (definition.normalize) value = definition.normalize(value);
    state[key] = cloneStoredValue(value);
  });

  return state;
};

const writeStateValueToDisk = (key, value) => {
  const definition = STORAGE_FILE_DEFINITIONS[key];
  if (!definition) return;

  const normalizedValue = definition.normalize ? definition.normalize(value) : value;
  const filePath = path.join(DATA_DIR, definition.fileName);
  ensureDirectory(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(normalizedValue, null, 2));
};

const persistRuntimeStateToDisk = () => {
  Object.keys(STORAGE_FILE_DEFINITIONS).forEach((key) => {
    writeStateValueToDisk(key, runtimeDbState[key]);
  });
};

let runtimeDbState = buildStorageState({ seedFromDisk: true });

const getDbPool = () => {
  if (!usePostgresStorage()) return null;
  if (!pgPool) {
    pgPool = new Pool({
      connectionString: DATABASE_URL,
      max: IS_VERCEL ? 3 : 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
    });
  }
  return pgPool;
};

const ensurePostgresStore = async () => {
  const pool = getDbPool();
  if (!pool) return;

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${STORAGE_TABLE_NAME} (
      name TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${STORAGE_ASSET_TABLE_NAME} (
      key TEXT PRIMARY KEY,
      content BYTEA NOT NULL,
      content_type TEXT,
      file_name TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
};

const writeRuntimeStateToNeon = async (state) => {
  const pool = getDbPool();
  if (!pool) return;

  await pool.query(
    `
      INSERT INTO ${STORAGE_TABLE_NAME} (name, data, updated_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (name)
      DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
    `,
    [STORAGE_DATASET_NAME, JSON.stringify(state)]
  );
};

const loadRuntimeStateFromNeon = async () => {
  await ensurePostgresStore();

  const pool = getDbPool();
  const result = await pool.query(
    `SELECT data FROM ${STORAGE_TABLE_NAME} WHERE name = $1 LIMIT 1`,
    [STORAGE_DATASET_NAME]
  );

  if (!result.rowCount) {
    const seededState = buildStorageState({ seedFromDisk: false, source: runtimeDbState });
    await writeRuntimeStateToNeon(seededState);
    return seededState;
  }

  return buildStorageState({ seedFromDisk: false, source: result.rows[0].data });
};

const queueRuntimeStatePersist = () => {
  if (!usePostgresStorage()) return Promise.resolve();

  const snapshot = buildStorageState({ seedFromDisk: false, source: runtimeDbState });
  const persistPromise = storagePersistQueue
    .catch(() => {})
    .then(() => writeRuntimeStateToNeon(snapshot));

  storagePersistQueue = persistPromise.catch((error) => {
    console.error('Failed to persist runtime state to Neon:', error);
    throw error;
  });

  const context = storageRequestContext.getStore();
  if (context) context.pendingPersists.push(persistPromise);

  return persistPromise;
};

const readStorageValue = (key) => {
  const definition = STORAGE_FILE_DEFINITIONS[key];
  if (!definition) return null;

  const currentValue = runtimeDbState && Object.prototype.hasOwnProperty.call(runtimeDbState, key)
    ? runtimeDbState[key]
    : definition.createDefault();

  const normalizedValue = definition.normalize ? definition.normalize(currentValue) : currentValue;
  return cloneStoredValue(normalizedValue == null ? definition.createDefault() : normalizedValue);
};

const saveStorageValue = (key, value) => {
  const definition = STORAGE_FILE_DEFINITIONS[key];
  if (!definition) return null;

  const normalizedValue = definition.normalize ? definition.normalize(value) : value;
  runtimeDbState[key] = cloneStoredValue(normalizedValue == null ? definition.createDefault() : normalizedValue);

  if (usePostgresStorage()) {
    queueRuntimeStatePersist().catch(() => {});
  } else {
    writeStateValueToDisk(key, runtimeDbState[key]);
  }

  return runtimeDbState[key];
};

const ensureStorageReady = () => {
  if (!storageReadyPromise) {
    storageReadyPromise = (async () => {
      if (usePostgresStorage()) {
        runtimeDbState = await loadRuntimeStateFromNeon();
      }

      const changed = applyRuntimeDataFixups(runtimeDbState);
      if (changed) {
        if (usePostgresStorage()) {
          await writeRuntimeStateToNeon(runtimeDbState);
        } else {
          persistRuntimeStateToDisk();
        }
      }

      return runtimeDbState;
    })().catch((error) => {
      storageReadyPromise = null;
      throw error;
    });
  }

  return storageReadyPromise;
};

const refreshRuntimeStateFromNeon = async () => {
  if (!usePostgresStorage()) return runtimeDbState;

  runtimeDbState = await loadRuntimeStateFromNeon();
  const changed = applyRuntimeDataFixups(runtimeDbState);
  if (changed) {
    await writeRuntimeStateToNeon(runtimeDbState);
  }

  return runtimeDbState;
};

const saveBinaryAsset = async ({ key, buffer, mimeType, fileName }) => {
  if (!usePostgresStorage() || !key || !Buffer.isBuffer(buffer)) return false;

  await ensurePostgresStore();
  await getDbPool().query(
    `
      INSERT INTO ${STORAGE_ASSET_TABLE_NAME} (key, content, content_type, file_name, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (key)
      DO UPDATE SET
        content = EXCLUDED.content,
        content_type = EXCLUDED.content_type,
        file_name = EXCLUDED.file_name,
        updated_at = NOW()
    `,
    [key, buffer, mimeType || null, fileName || null]
  );

  return true;
};

const getBinaryAsset = async (key) => {
  if (!usePostgresStorage() || !key) return null;

  await ensurePostgresStore();
  const result = await getDbPool().query(
    `SELECT content, content_type, file_name FROM ${STORAGE_ASSET_TABLE_NAME} WHERE key = $1 LIMIT 1`,
    [key]
  );

  if (!result.rowCount) return null;

  return {
    buffer: result.rows[0].content,
    mimeType: result.rows[0].content_type || null,
    fileName: result.rows[0].file_name || null
  };
};

const db = {};

Object.entries(STORAGE_READ_ACCESSORS).forEach(([accessorName, key]) => {
  db[accessorName] = () => readStorageValue(key);
});

Object.entries(STORAGE_WRITE_ACCESSORS).forEach(([accessorName, key]) => {
  db[accessorName] = (value) => saveStorageValue(key, value);
});

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

const getLoyaltyEarnedPointsForPurchase = ({ amountEGP }) => {
  const settings = db.loyaltySettings();
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

const normalizeOptionalText = (value) => {
  if (typeof value !== 'string') return '';
  return value.trim();
};

const normalizeOptionalDurationDays = (value) => {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1) return null;
  return Math.floor(n);
};

const resolveCouponExpiry = ({ expiresAt, durationDays }) => {
  if (expiresAt) return expiresAt;
  if (!durationDays) return null;
  // Calculate exact duration: days * 24 hours * 60 minutes * 60 seconds * 1000 ms
  const ms = durationDays * 24 * 60 * 60 * 1000;
  const d = new Date(Date.now() + ms);
  return d.toISOString();
};

const describeCouponDuration = (coupon) => {
  const days = normalizeOptionalDurationDays(coupon && coupon.durationDays);
  if (days === 7) return 'لمدة أسبوع';
  if (days === 14) return 'لمدة أسبوعين';
  if (days === 30) return 'لمدة شهر';
  if (days) return `لمدة ${days} يوم`;
  if (coupon && coupon.expiresAt) {
    const expires = new Date(coupon.expiresAt);
    if (!Number.isNaN(expires.getTime())) return `حتى ${expires.toLocaleDateString('ar-EG')}`;
  }
  return 'لفترة محدودة';
};

const buildCouponPromoCard = (coupon) => {
  if (!coupon) return null;
  const benefit = coupon.type === 'fixed' ? `${formatUsdAmount(convertEgpToUsd(coupon.value))} خصم` : `${coupon.value}% خصم`;
  return {
    code: coupon.code,
    benefit,
    occasion: normalizeOptionalText(coupon.occasion),
    durationLabel: describeCouponDuration(coupon),
    expiresAt: coupon.expiresAt || null
  };
};

const getActiveCouponPromoCards = (coupons, eligibilityFn) => {
  return (coupons || [])
    .filter(c => c && c.active)
    .filter(c => (eligibilityFn(c) || {}).eligible)
    .map(buildCouponPromoCard);
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

const getActiveSubscriptionForUser = ({ userId }) => {
  if (!userId) return null;
  const subs = db.subscriptions();
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

const getSubscriptionPlanById = ({ planId }) => {
  if (!planId) return null;
  const plans = db.subscriptionPlans();
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

const normalizeRecentlyViewedProjectIds = (projectIds) => {
  if (!Array.isArray(projectIds)) return [];

  const seen = new Set();
  const normalized = [];

  for (const value of projectIds) {
    if (typeof value !== 'string') continue;
    const projectId = value.trim();
    if (!projectId || seen.has(projectId)) continue;
    seen.add(projectId);
    normalized.push(projectId);
    if (normalized.length >= RECENTLY_VIEWED_LIMIT) break;
  }

  return normalized;
};

const mergeRecentlyViewedProjectIds = (...lists) => {
  const merged = [];
  lists.forEach((list) => {
    normalizeRecentlyViewedProjectIds(list).forEach((projectId) => {
      merged.push(projectId);
    });
  });
  return normalizeRecentlyViewedProjectIds(merged);
};

const prependRecentlyViewedProjectId = (projectIds, projectId) => {
  if (!projectId || typeof projectId !== 'string') {
    return normalizeRecentlyViewedProjectIds(projectIds);
  }

  return normalizeRecentlyViewedProjectIds([projectId.trim(), ...normalizeRecentlyViewedProjectIds(projectIds)]);
};

const getSessionRecentlyViewedProjectIds = (req) => {
  if (!req || !req.session) return [];
  return normalizeRecentlyViewedProjectIds(req.session.recentlyViewedProjectIds);
};

const setSessionRecentlyViewedProjectIds = (req, projectIds) => {
  if (!req || !req.session) return [];
  const normalized = normalizeRecentlyViewedProjectIds(projectIds);
  req.session.recentlyViewedProjectIds = normalized;
  return normalized;
};

const syncRecentlyViewedProjectsForUser = ({ req, userId }) => {
  const sessionIds = getSessionRecentlyViewedProjectIds(req);
  if (!userId) return sessionIds;

  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === userId);
  if (userIndex === -1) return sessionIds;

  const storedIds = normalizeRecentlyViewedProjectIds(users[userIndex].recentlyViewedProjectIds);
  const mergedIds = mergeRecentlyViewedProjectIds(sessionIds, storedIds);
  setSessionRecentlyViewedProjectIds(req, mergedIds);

  if (mergedIds.join('|') !== storedIds.join('|')) {
    users[userIndex].recentlyViewedProjectIds = mergedIds;
    db.saveUsers(users);
  }

  return mergedIds;
};

const recordRecentlyViewedProject = ({ req, projectId }) => {
  const sessionIds = prependRecentlyViewedProjectId(getSessionRecentlyViewedProjectIds(req), projectId);
  setSessionRecentlyViewedProjectIds(req, sessionIds);

  if (!req || !req.session || !req.session.user || !req.session.user.id) {
    return sessionIds;
  }

  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === req.session.user.id);
  if (userIndex === -1) return sessionIds;

  const storedIds = normalizeRecentlyViewedProjectIds(users[userIndex].recentlyViewedProjectIds);
  const nextStoredIds = prependRecentlyViewedProjectId(storedIds, projectId);

  if (nextStoredIds.join('|') !== storedIds.join('|')) {
    users[userIndex].recentlyViewedProjectIds = nextStoredIds;
    db.saveUsers(users);
  }

  return nextStoredIds;
};

const getRecentlyViewedProjects = ({ req, availableProjects }) => {
  if (!Array.isArray(availableProjects) || availableProjects.length === 0) return [];

  let recentIds = getSessionRecentlyViewedProjectIds(req);
  if (req && req.session && req.session.user && req.session.user.id) {
    recentIds = syncRecentlyViewedProjectsForUser({ req, userId: req.session.user.id });
  }

  const visibleProjectsMap = new Map(
    availableProjects.map((project) => [project.id, project])
  );

  return recentIds
    .map((projectId) => visibleProjectsMap.get(projectId))
    .filter(Boolean);
};

const getSubscriberDiscountPercent = ({ sessionUser }) => {
  const tier = getUserSubscriptionTier({ sessionUser });
  if (tier === 'premium') return 20;
  if (tier === 'basic') return 10;
  return 0;
};

const getOrCreateCartForUser = ({ userId }) => {
  const carts = db.carts();
  const idx = carts.findIndex(c => c && c.userId === userId);
  if (idx !== -1) {
    const cart = carts[idx];
    if (!cart.items || !Array.isArray(cart.items)) cart.items = [];
    if (!cart.updatedAt) cart.updatedAt = new Date().toISOString();
    return { carts, cart, cartIndex: idx };
  }
  const cart = { userId, items: [], updatedAt: new Date().toISOString() };
  carts.push(cart);
  return { carts, cart, cartIndex: carts.length - 1 };
};

const MAX_DOWNLOAD_EVENTS = 50000;
const MAX_ADMIN_AUDIT_LOG = 100000;
const MAX_LIVE_FEED_EVENTS = 500;

let liveIo = null;

const emitLiveAdminEvent = (event) => {
  try {
    if (!liveIo) return;
    liveIo.to('admin-team').emit('live-event', event);
  } catch (_) {
    // ignore
  }
};

const addAdminAuditLogEntry = ({ req, action, entity, entityId = null, before = null, after = null, meta = {} }) => {
  try {
    const admin = req && req.session && req.session.user && req.session.user.role === 'admin' ? req.session.user : null;
    if (!admin) return null;

    const entry = {
      id: uuidv4(),
      action: String(action || 'unknown'),
      entity: String(entity || 'unknown'),
      entityId: entityId ? String(entityId) : null,
      adminId: admin.id || null,
      adminEmail: admin.email || null,
      adminName: admin.name || null,
      ip: getRequestIp(req),
      userAgent: normalizeUserAgent(req),
      before,
      after,
      meta,
      createdAt: new Date().toISOString()
    };

    const current = db.adminAuditLog();
    const next = [entry, ...(Array.isArray(current) ? current : [])].slice(0, MAX_ADMIN_AUDIT_LOG);
    db.saveAdminAuditLog(next);

    emitLiveAdminEvent({
      type: 'admin-audit',
      title: 'تعديل إداري',
      message: `${entry.action} (${entry.entity})`,
      severity: 'info',
      data: { entryId: entry.id, action: entry.action, entity: entry.entity, entityId: entry.entityId, adminEmail: entry.adminEmail },
      createdAt: entry.createdAt
    });

    return entry;
  } catch (_) {
    return null;
  }
};

const getRequestIp = (req) => {
  const xf = req && req.headers ? req.headers['x-forwarded-for'] : null;
  if (typeof xf === 'string' && xf.trim()) {
    return xf.split(',')[0].trim();
  }
  if (Array.isArray(xf) && xf.length > 0) return String(xf[0] || '').trim();
  const ra = req && req.socket ? req.socket.remoteAddress : null;
  return ra ? String(ra) : '';
};

const normalizeUserAgent = (req) => {
  const ua = req && req.headers ? req.headers['user-agent'] : null;
  return ua ? String(ua).slice(0, 300) : '';
};

const getSimpleDeviceLabel = (ua) => {
  const s = String(ua || '').toLowerCase();
  if (!s) return 'unknown';
  if (s.includes('iphone') || s.includes('ipad') || s.includes('ios')) return 'ios';
  if (s.includes('android')) return 'android';
  if (s.includes('mac os') || s.includes('macintosh')) return 'mac';
  if (s.includes('windows')) return 'windows';
  if (s.includes('linux')) return 'linux';
  return 'other';
};

const recordDownloadEvent = ({ req, kind, userId, purchaseId = null, projectId = null, meta = {} }) => {
  try {
    const events = db.downloadEvents();
    const ua = normalizeUserAgent(req);
    const entry = {
      id: uuidv4(),
      kind,
      userId,
      purchaseId,
      projectId,
      ip: getRequestIp(req),
      userAgent: ua,
      device: getSimpleDeviceLabel(ua),
      meta,
      createdAt: new Date().toISOString()
    };
    const next = [entry, ...(Array.isArray(events) ? events : [])].slice(0, MAX_DOWNLOAD_EVENTS);
    db.saveDownloadEvents(next);

    emitLiveAdminEvent({
      type: 'download',
      title: 'تحميل',
      message: `تم تحميل ملف (${kind})`,
      severity: 'info',
      data: { userId, purchaseId, projectId, ip: entry.ip, device: entry.device },
      createdAt: entry.createdAt
    });
    return entry;
  } catch (e) {
    return null;
  }
};

const MAX_PROJECT_VIEW_EVENTS = 200000;
const recordProjectViewEvent = ({ req, projectId, sessionUser }) => {
  try {
    const userId = sessionUser && sessionUser.role === 'user' ? sessionUser.id : null;
    const events = db.projectViewEvents();
    const ua = normalizeUserAgent(req);
    const entry = {
      id: uuidv4(),
      projectId,
      userId,
      ip: getRequestIp(req),
      userAgent: ua,
      device: getSimpleDeviceLabel(ua),
      createdAt: new Date().toISOString()
    };
    const next = [entry, ...(Array.isArray(events) ? events : [])].slice(0, MAX_PROJECT_VIEW_EVENTS);
    db.saveProjectViewEvents(next);
    return entry;
  } catch (e) {
    return null;
  }
};

const parseEnvInt = (value, fallback) => {
  const parsed = Number.parseInt(String(value || ''), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const startAbandonedCartRecoveryJob = () => {
  // Note: Vercel serverless functions are not suitable for long-running intervals.
  if (process.env.VERCEL) return;

  const delayMinutes = parseEnvInt(process.env.ABANDONED_CART_DELAY_MINUTES, 60);
  const cooldownHours = parseEnvInt(process.env.ABANDONED_CART_COOLDOWN_HOURS, 24);
  const scanMinutes = parseEnvInt(process.env.ABANDONED_CART_SCAN_MINUTES, 10);

  const delayMs = delayMinutes * 60 * 1000;
  const cooldownMs = cooldownHours * 60 * 60 * 1000;
  const scanMs = scanMinutes * 60 * 1000;

  const scanOnce = () => {
    try {
      const carts = db.carts();
      const users = db.users();
      const reminders = db.cartReminders();

      const usersById = new Map(users.filter(Boolean).map((u) => [u.id, u]));
      const now = Date.now();
      let remindersChanged = false;
      let usersChanged = false;

      carts.forEach((cart) => {
        if (!cart || !cart.userId) return;
        const items = Array.isArray(cart.items) ? cart.items : [];
        if (items.length === 0) return;

        const updatedAtMs = cart.updatedAt ? Date.parse(cart.updatedAt) : NaN;
        if (!Number.isFinite(updatedAtMs)) return;
        if (now - updatedAtMs < delayMs) return;

        const user = usersById.get(cart.userId);
        if (!user || user.role !== 'user') return;

        const existing = reminders
          .filter((r) => r && r.userId === cart.userId)
          .sort((a, b) => Date.parse(String(b.sentAt || 0)) - Date.parse(String(a.sentAt || 0)))[0] || null;

        if (existing) {
          const lastSentMs = existing.sentAt ? Date.parse(existing.sentAt) : 0;
          if (Number.isFinite(lastSentMs) && now - lastSentMs < cooldownMs) return;
          // If we already reminded for the same cart version (same updatedAt), skip.
          if (existing.cartUpdatedAt && existing.cartUpdatedAt === cart.updatedAt) return;
        }

        const count = items.length;
        const title = 'سلتك ما زالت بانتظارك';
        const message = `لديك ${count} ${count === 1 ? 'عنصر' : 'عناصر'} في السلة. أكمل الشراء الآن.`;

        const entry = addUserNotificationEntry({
          targetUser: user,
          type: 'abandoned-cart',
          title,
          message,
          metadata: {
            url: '/cart',
            itemCount: count
          }
        });

        if (entry) {
          usersChanged = true;
          reminders.push({
            id: uuidv4(),
            userId: cart.userId,
            cartUpdatedAt: cart.updatedAt,
            itemCount: count,
            sentAt: new Date().toISOString()
          });
          remindersChanged = true;
        }
      });

      if (usersChanged) db.saveUsers(users);
      if (remindersChanged) db.saveCartReminders(reminders);
    } catch (error) {
      console.error('Abandoned cart recovery scan failed:', error && error.message ? error.message : error);
    }
  };

  // Initial scan shortly after boot, then periodically.
  setTimeout(scanOnce, 15 * 1000);
  setInterval(scanOnce, scanMs);
  console.log(
    `Abandoned cart recovery enabled (delay=${delayMinutes}m, cooldown=${cooldownHours}h, scan=${scanMinutes}m)`
  );
};

const normalizeStoredUploadPath = (value) => {
  const normalized = normalizeStoredPath(value);
  if (!normalized) return null;
  if (normalized.startsWith('uploads/') || normalized.startsWith('private_uploads/')) return normalized;
  return null;
};

const buildFileHealthIssues = () => {
  const issues = [];

  const pushIssue = ({ kind, refId, label, pathValue }) => {
    const storedPath = normalizeStoredUploadPath(pathValue);
    if (!storedPath) return;
    const absolutePath = toAbsolutePath(storedPath);
    if (!absolutePath) return;
    if (!fs.existsSync(absolutePath)) {
      issues.push({
        id: uuidv4(),
        kind,
        refId,
        label,
        storedPath,
        createdAt: new Date().toISOString()
      });
    }
  };

  const projects = db.projects();
  projects.forEach((p) => {
    if (!p) return;
    if (p.filePath) pushIssue({ kind: 'project-file', refId: p.id, label: p.title || p.id, pathValue: p.filePath });
    const images = Array.isArray(p.images) ? p.images : [];
    images.forEach((img) => pushIssue({ kind: 'project-image', refId: p.id, label: p.title || p.id, pathValue: img }));
  });

  const purchases = db.purchases();
  purchases.forEach((p) => {
    if (!p) return;
    if (p.filePath) pushIssue({ kind: 'purchase-file', refId: p.id, label: p.projectTitle || p.projectId || p.id, pathValue: p.filePath });
  });

  const requests = db.customProjectRequests();
  requests.forEach((r) => {
    if (!r) return;
    if (r.filePath) pushIssue({ kind: 'custom-project-file', refId: r.id, label: r.title || r.id, pathValue: r.filePath });
  });

  const recordings = db.meetingRecordings();
  recordings.forEach((rec) => {
    if (!rec) return;
    if (rec.filePath) pushIssue({ kind: 'meeting-recording', refId: rec.id, label: rec.title || rec.id, pathValue: rec.filePath });
  });

  const adminTeamMessages = db.adminTeamMessages();
  adminTeamMessages.forEach((m) => {
    if (!m) return;
    if (m.filePath) pushIssue({ kind: 'admin-team-upload', refId: m.id, label: m.fileName || m.id, pathValue: m.filePath });
  });

  const communityPosts = db.communityPosts();
  communityPosts.forEach((post) => {
    if (!post) return;
    if (post.mediaPath) pushIssue({ kind: 'community-media', refId: post.id, label: post.authorName || post.id, pathValue: post.mediaPath });
  });

  const applications = db.communityJobApplications();
  applications.forEach((app) => {
    if (!app) return;
    if (app.cvPath) pushIssue({ kind: 'community-cv', refId: app.id, label: app.userName || app.id, pathValue: app.cvPath });
  });

  return issues;
};

const runFileHealthCheck = () => {
  try {
    const issues = buildFileHealthIssues();
    const reports = db.fileHealthReports();
    const report = {
      id: uuidv4(),
      issueCount: issues.length,
      issues,
      createdAt: new Date().toISOString()
    };
    const limit = parseEnvInt(process.env.FILE_HEALTH_REPORTS_LIMIT, 30);
    const next = [report, ...(Array.isArray(reports) ? reports : [])].slice(0, limit);
    db.saveFileHealthReports(next);
    return report;
  } catch (e) {
    console.error('File health check failed:', e && e.message ? e.message : e);
    return null;
  }
};

const startFileHealthMonitorJob = () => {
  if (process.env.VERCEL) return;
  const intervalHours = parseEnvInt(process.env.FILE_HEALTH_INTERVAL_HOURS, 6);
  const intervalMs = intervalHours * 60 * 60 * 1000;
  setTimeout(() => runFileHealthCheck(), 20 * 1000);
  setInterval(() => runFileHealthCheck(), intervalMs);
  console.log(`File health monitor enabled (interval=${intervalHours}h)`);
};

const summarizeCart = ({ cart, couponCode, sessionUser }) => {
  const items = (cart && Array.isArray(cart.items)) ? cart.items : [];
  const totalBefore = Math.round(items.reduce((sum, it) => sum + Number(it.price || 0), 0) * 100) / 100;

  let appliedCoupon = null;
  let couponDiscount = 0;
  if (couponCode) {
    const normalized = normalizeCouponCode(couponCode);
    const coupons = db.coupons();
    const coupon = coupons.find(c => normalizeCouponCode(c.code) === normalized) || null;
    const eligibility = getCouponEligibility(coupon);
    if (eligibility.eligible) {
      appliedCoupon = coupon;
      // Convert coupon value from USD to EGP for calculation
      const couponValueEgp = coupon.type === 'fixed' 
        ? convertUsdToEgp(coupon.value)
        : coupon.value;
      const calc = calculateDiscount({ 
        priceBefore: totalBefore, 
        coupon: { ...coupon, value: couponValueEgp }
      });
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

const getProjectSaleState = (project) => {
  const basePrice = Number(project && project.price || 0);
  const saleType = project && project.saleType;
  const saleValue = Number(project && project.saleValue || 0);
  const occasion = normalizeOptionalText(project && project.saleOccasion);
  const expiresAt = project && project.saleExpiresAt ? new Date(project.saleExpiresAt) : null;
  const hasExpiry = expiresAt && !Number.isNaN(expiresAt.getTime());
  const active = Boolean(project && project.saleActive && saleType && saleValue > 0 && (!hasExpiry || expiresAt.getTime() > Date.now()));
  if (!active || basePrice <= 0) {
    return { active: false, basePrice, finalPrice: basePrice, discountAmount: 0, saleType: null, saleValue: 0, occasion: occasion || null, expiresAt: hasExpiry ? expiresAt.toISOString() : null, durationDays: normalizeOptionalDurationDays(project && project.saleDurationDays) };
  }

  let discountAmount = 0;
  if (saleType === 'percent') discountAmount = Math.round((basePrice * (saleValue / 100)) * 100) / 100;
  else if (saleType === 'fixed') discountAmount = saleValue;
  if (!Number.isFinite(discountAmount) || discountAmount < 0) discountAmount = 0;
  if (discountAmount > basePrice) discountAmount = basePrice;
  const finalPrice = Math.round((basePrice - discountAmount) * 100) / 100;

  return {
    active: true,
    basePrice,
    finalPrice,
    discountAmount,
    saleType,
    saleValue,
    occasion: occasion || null,
    expiresAt: hasExpiry ? expiresAt.toISOString() : null,
    durationDays: normalizeOptionalDurationDays(project && project.saleDurationDays),
    durationLabel: describeCouponDuration({ durationDays: project && project.saleDurationDays, expiresAt: hasExpiry ? expiresAt.toISOString() : null })
  };
};

const isProjectDownloadsLocked = (project) => Boolean(project && project.downloadsLocked);

const getProjectDownloadsLockReason = (project) => {
  const reason = project && typeof project.downloadsLockReason === 'string' ? project.downloadsLockReason.trim() : '';
  return reason || null;
};

const decorateProjectPricing = (project) => {
  if (!project) return project;
  const sale = getProjectSaleState(project);
  return {
    ...project,
    finalPrice: sale.finalPrice,
    originalPrice: sale.basePrice,
    sale
  };
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

const ensureUserPaymentProfile = ({ user, users }) => {
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

  if (user.loyaltyPoints == null) {
    user.loyaltyPoints = 0;
    changed = true;
  }

  if (!normalizeWalletCardNumber(user.walletCardNumber)) {
    user.walletCardNumber = createWalletCardNumber(users);
    changed = true;
  } else {
    const normalizedCard = normalizeWalletCardNumber(user.walletCardNumber);
    if (user.walletCardNumber !== normalizedCard) {
      user.walletCardNumber = normalizedCard;
      changed = true;
    }
  }

  if (user.walletPaymentPasswordHash === undefined) {
    user.walletPaymentPasswordHash = null;
    changed = true;
  }

  const normalizedLimit = sanitizeWalletCardSpendingLimit(user.walletCardSpendingLimit);
  if ((user.walletCardSpendingLimit == null && normalizedLimit !== null) || user.walletCardSpendingLimit !== normalizedLimit) {
    user.walletCardSpendingLimit = normalizedLimit;
    changed = true;
  }

  if (!Array.isArray(user.walletCardNotifications)) {
    user.walletCardNotifications = [];
    changed = true;
  }

  if (!Array.isArray(user.notifications)) {
    user.notifications = [];
    changed = true;
  }

  if (!Array.isArray(user.walletCardUsageLog)) {
    user.walletCardUsageLog = [];
    changed = true;
  }

  if (normalizeUserNotifications(user)) {
    changed = true;
  }

  return changed;
};

const buildSessionUser = (user) => {
  if (!user) return null;

  const activeSubscription = user.role === 'user'
    ? getActiveSubscriptionForUser({ userId: user.id })
    : null;

  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    isSuperAdmin: Boolean(user.isSuperAdmin),
    adminPermissions: user.role === 'admin' ? (user.adminPermissions || null) : null,
    referralCode: user.role === 'user' ? (user.referralCode || null) : null,
    walletBalance: Number(user.walletBalance || 0),
    walletCardNumberMasked: user.role === 'user' ? maskWalletCardNumber(user.walletCardNumber) : null,
    hasWalletPaymentPassword: user.role === 'user' ? Boolean(user.walletPaymentPasswordHash) : false,
    walletCardSpendingLimit: user.role === 'user' ? sanitizeWalletCardSpendingLimit(user.walletCardSpendingLimit) : null,
    walletCardFrozen: user.role === 'user' ? Boolean(user.walletCardFrozen) : false,
    loyaltyPoints: user.role === 'user' ? normalizeLoyaltyPoints(user.loyaltyPoints) : 0,
    unreadNotificationsCount: user.role === 'user' ? getUnreadNotificationCount(user) : 0,
    subscription: activeSubscription ? {
      planId: activeSubscription.planId,
      status: activeSubscription.status,
      currentPeriodEnd: activeSubscription.currentPeriodEnd
    } : null
  };
};

const registerPendingReferralSignupReward = ({ users, newUser }) => {
  if (!newUser || !newUser.referredBy) return;

  const referrals = db.referrals();
  referrals.push({
    id: uuidv4(),
    code: newUser.referredBy.code,
    referrerUserId: newUser.referredBy.referrerUserId,
    referredUserId: newUser.id,
    status: 'pending',
    rewardAmount: 100,
    rewardType: 'loyalty-points',
    createdAt: new Date().toISOString(),
    rewardedAt: null,
    rewardPurchaseId: null
  });
  db.saveReferrals(referrals);

  const referrerIndex = users.findIndex((item) => item && item.id === newUser.referredBy.referrerUserId && item.role === 'user');
  if (referrerIndex !== -1) {
    users[referrerIndex].loyaltyPoints = normalizeLoyaltyPoints(Number(users[referrerIndex].loyaltyPoints || 0) + 50);
    addUserNotificationEntry({
      targetUser: users[referrerIndex],
      type: 'referral-signup',
      title: 'مكافأة إحالة جديدة',
      message: 'سجل مستخدم جديد باستخدام كودك، وتمت إضافة 50 نقطة إلى حسابك.',
      metadata: {
        referredUserId: newUser.id,
        rewardAmount: 50,
        rewardType: 'loyalty-points'
      }
    });
  }
};

const parseCookieHeader = (cookieHeader) => {
  if (!cookieHeader || typeof cookieHeader !== 'string') return {};
  return cookieHeader.split(';').reduce((acc, part) => {
    const trimmed = part.trim();
    if (!trimmed) return acc;
    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex === -1) return acc;
    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    if (!key) return acc;
    acc[key] = decodeURIComponent(value);
    return acc;
  }, {});
};

const getAuthCookieToken = (req) => {
  const cookies = parseCookieHeader(req && req.headers ? req.headers.cookie : '');
  return cookies[AUTH_COOKIE_NAME] || null;
};

const setAuthCookie = (res, userId) => {
  if (!res || !userId) return;
  const token = jwt.sign({ userId }, AUTH_COOKIE_SECRET, { expiresIn: '7d' });
  res.cookie(AUTH_COOKIE_NAME, token, AUTH_COOKIE_OPTIONS);
};

const clearAuthCookie = (res) => {
  if (!res) return;
  res.clearCookie(AUTH_COOKIE_NAME, {
    httpOnly: true,
    sameSite: 'lax',
    secure: IS_VERCEL,
    path: '/'
  });
};

const hydrateSessionUserFromAuthCookie = ({ req, res }) => {
  if (!req || !req.session) return;

  const existingSessionUserId = req.session.user && req.session.user.id ? req.session.user.id : null;
  const token = getAuthCookieToken(req);

  if (existingSessionUserId && !token) {
    setAuthCookie(res, existingSessionUserId);
    return;
  }

  if (!token) return;

  let payload = null;
  try {
    payload = jwt.verify(token, AUTH_COOKIE_SECRET);
  } catch (error) {
    if (!existingSessionUserId) clearAuthCookie(res);
    return;
  }

  if (!payload || !payload.userId) {
    if (!existingSessionUserId) clearAuthCookie(res);
    return;
  }

  if (existingSessionUserId === payload.userId) return;

  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === payload.userId);
  if (userIndex === -1) {
    clearAuthCookie(res);
    return;
  }

  const fullUser = users[userIndex];
  let shouldSaveUsers = false;

  if (ensureUserPaymentProfile({ user: fullUser, users })) {
    shouldSaveUsers = true;
  }

  if (isBlockedExpired(fullUser)) {
    unblockUserInPlace(fullUser);
    shouldSaveUsers = true;
  }

  if (shouldSaveUsers) {
    users[userIndex] = fullUser;
    db.saveUsers(users);
  }

  req.session.user = buildSessionUser(fullUser);
};

const COMMUNITY_POST_CONTENT_LIMIT = 4000;
const COMMUNITY_COMMENT_CONTENT_LIMIT = 1000;
const COMMUNITY_JOB_TEXT_LIMIT = 4000;
const COMMUNITY_ABOUT_LIMIT = 2500;
const COMMUNITY_MEDIA_SIZE_LIMIT_MB = 150;
const COMMUNITY_CV_SIZE_LIMIT_MB = 10;
const COMMUNITY_CV_MIME_TYPES = new Set([
  'application/pdf',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
]);
const COMMUNITY_CV_EXTENSIONS = new Set(['.pdf', '.docx']);
const COMMUNITY_APPLICATION_TRACKING_NOTE_LIMIT = 600;
const COMMUNITY_AI_SCREENING_SUMMARY_LIMIT = 300;
const COMMUNITY_AI_SCREENING_REASON_LIMIT = 160;
const COMMUNITY_APPLICATION_STATUSES = [
  {
    key: 'pending',
    label: 'تم الاستلام',
    shortLabel: 'استلام',
    badgeClass: 'pending',
    description: 'تم استلام طلبك وهو في انتظار المراجعة الأولية.',
    order: 0,
    isFinal: false,
    isPositive: false
  },
  {
    key: 'reviewing',
    label: 'قيد المراجعة',
    shortLabel: 'مراجعة',
    badgeClass: 'review',
    description: 'الفريق يراجع بياناتك وسيرتك الذاتية الآن.',
    order: 1,
    isFinal: false,
    isPositive: false
  },
  {
    key: 'shortlisted',
    label: 'تم ترشيحك',
    shortLabel: 'ترشيح',
    badgeClass: 'shortlisted',
    description: 'أنت ضمن القائمة القصيرة المرشحة للخطوة التالية.',
    order: 2,
    isFinal: false,
    isPositive: true
  },
  {
    key: 'interview',
    label: 'مرحلة المقابلة',
    shortLabel: 'مقابلة',
    badgeClass: 'interview',
    description: 'تم نقلك لمرحلة المقابلة أو التقييم العملي.',
    order: 3,
    isFinal: false,
    isPositive: true
  },
  {
    key: 'accepted',
    label: 'قبول مبدئي',
    shortLabel: 'قبول',
    badgeClass: 'approved',
    description: 'تم قبولك مبدئيًا، وبانتظار استكمال الخطوات التالية مع الإدارة.',
    order: 4,
    isFinal: false,
    isPositive: true
  },
  {
    key: 'hired',
    label: 'تم التوظيف',
    shortLabel: 'توظيف',
    badgeClass: 'hired',
    description: 'مبروك، تم تأكيد التوظيف وإغلاق رحلتك بنجاح.',
    order: 5,
    isFinal: true,
    isPositive: true
  },
  {
    key: 'rejected',
    label: 'تم الرفض',
    shortLabel: 'رفض',
    badgeClass: 'rejected',
    description: 'للأسف لم يكتمل القبول في هذه المرحلة.',
    order: 6,
    isFinal: true,
    isPositive: false
  }
];
const COMMUNITY_APPLICATION_STATUS_MAP = new Map(
  COMMUNITY_APPLICATION_STATUSES.map((status) => [status.key, status])
);
const COMMUNITY_AI_DECISION_META = {
  accepted: {
    key: 'accepted',
    label: 'قبول مبدئي AI',
    badgeClass: 'approved',
    description: 'الذكاء الاصطناعي يرى أن السيرة الذاتية مناسبة مبدئيًا للوظيفة.'
  },
  rejected: {
    key: 'rejected',
    label: 'رفض مبدئي AI',
    badgeClass: 'rejected',
    description: 'الذكاء الاصطناعي يرى أن السيرة الذاتية غير مرتبطة بالوظيفة بشكل كافٍ.'
  },
  pending: {
    key: 'pending',
    label: 'جارٍ التقييم',
    badgeClass: 'pending',
    description: 'الذكاء الاصطناعي لم يُكمل التقييم بعد.'
  },
  skipped: {
    key: 'skipped',
    label: 'بدون تقييم AI',
    badgeClass: 'review',
    description: 'لم يتم تشغيل التقييم الذكي على هذا الطلب.'
  },
  error: {
    key: 'error',
    label: 'تعذر التقييم AI',
    badgeClass: 'review',
    description: 'حدثت مشكلة أثناء التقييم الذكي وتم تحويل الطلب للمراجعة اليدوية.'
  }
};

const buildSafeUploadFileName = ({ file, fallbackBaseName }) => {
  const safeOriginal = (file && file.originalname ? file.originalname : fallbackBaseName || 'file')
    .replace(/[^a-zA-Z0-9._-]+/g, '_');
  const ext = path.extname(safeOriginal);
  const base = path.basename(safeOriginal, ext) || fallbackBaseName || 'file';
  return `${uuidv4()}-${base}${ext}`;
};

const safeDeleteFile = (filePath) => {
  if (!filePath) return;
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch (error) {
    // Ignore cleanup failures for optional uploads.
  }
};

const normalizeCommunityPost = (post) => ({
  ...(post || {}),
  likes: Array.isArray(post && post.likes) ? post.likes : [],
  shares: Array.isArray(post && post.shares) ? post.shares : [],
  comments: Array.isArray(post && post.comments) ? post.comments : []
});

const getCommunityApplicationStatusMeta = (statusKey) => {
  return COMMUNITY_APPLICATION_STATUS_MAP.get(statusKey) || COMMUNITY_APPLICATION_STATUS_MAP.get('pending');
};

const normalizeCommunityApplicationStatus = (statusKey) => getCommunityApplicationStatusMeta(statusKey).key;

const getCommunityAiDecisionMeta = (decisionKey) => {
  return COMMUNITY_AI_DECISION_META[decisionKey] || COMMUNITY_AI_DECISION_META.pending;
};

const normalizeCommunityAiDecision = (decisionKey) => getCommunityAiDecisionMeta(decisionKey).key;

const normalizeCommunityAiScreening = (screening) => {
  const normalized = screening && typeof screening === 'object' ? { ...screening } : {};
  normalized.decision = normalizeCommunityAiDecision(normalized.decision || (normalized.recommendedStatus === 'rejected' ? 'rejected' : 'pending'));
  normalized.summary = (normalized.summary || '').toString().trim().slice(0, COMMUNITY_AI_SCREENING_SUMMARY_LIMIT);
  normalized.reasons = Array.isArray(normalized.reasons)
    ? normalized.reasons
      .map((reason) => String(reason || '').trim().slice(0, COMMUNITY_AI_SCREENING_REASON_LIMIT))
      .filter(Boolean)
      .slice(0, 4)
    : [];
  normalized.score = Math.max(0, Math.min(100, Math.round(Number(normalized.score || 0))));
  normalized.evaluatedAt = normalized.evaluatedAt || null;
  normalized.model = (normalized.model || '').toString().trim() || null;
  normalized.error = (normalized.error || '').toString().trim() || null;
  return normalized;
};

const buildCommunityAiScreeningView = (screening) => {
  const normalized = normalizeCommunityAiScreening(screening);
  return {
    ...normalized,
    meta: getCommunityAiDecisionMeta(normalized.decision)
  };
};

const buildCommunityApplicationHistoryEntry = ({
  status,
  note,
  updatedAt,
  updatedById,
  updatedByName,
  actorType
}) => {
  const normalizedStatus = normalizeCommunityApplicationStatus(status);
  return {
    status: normalizedStatus,
    note: (note || '').toString().trim(),
    updatedAt: updatedAt || new Date().toISOString(),
    updatedById: updatedById || null,
    updatedByName: updatedByName || null,
    actorType: actorType || 'system'
  };
};

const normalizeCommunityJobApplication = (application) => {
  const normalized = { ...(application || {}) };
  const currentStatus = normalizeCommunityApplicationStatus(normalized.status);
  const createdAt = normalized.createdAt || new Date().toISOString();
  const rawHistory = Array.isArray(normalized.statusHistory) ? normalized.statusHistory : [];
  const history = rawHistory
    .filter((entry) => entry && typeof entry === 'object')
    .map((entry) => buildCommunityApplicationHistoryEntry({
      status: entry.status,
      note: entry.note,
      updatedAt: entry.updatedAt,
      updatedById: entry.updatedById,
      updatedByName: entry.updatedByName,
      actorType: entry.actorType
    }))
    .sort((a, b) => new Date(a.updatedAt || 0).getTime() - new Date(b.updatedAt || 0).getTime());

  if (!history.length) {
    history.push(buildCommunityApplicationHistoryEntry({
      status: currentStatus,
      note: currentStatus === 'pending' ? 'تم استلام طلب التقديم.' : '',
      updatedAt: normalized.statusUpdatedAt || createdAt,
      updatedById: normalized.userId || null,
      updatedByName: normalized.applicantName || null,
      actorType: 'applicant'
    }));
  }

  normalized.status = currentStatus;
  normalized.createdAt = createdAt;
  normalized.statusHistory = history;
  normalized.statusUpdatedAt = normalized.statusUpdatedAt || history[history.length - 1].updatedAt || createdAt;
  normalized.trackingNote = (normalized.trackingNote || normalized.lastStatusNote || '').toString().trim();
  normalized.hiredAt = normalized.hiredAt || (currentStatus === 'hired' ? normalized.statusUpdatedAt : null);
  normalized.rejectedAt = normalized.rejectedAt || (currentStatus === 'rejected' ? normalized.statusUpdatedAt : null);
  normalized.aiScreening = normalizeCommunityAiScreening(normalized.aiScreening);

  return normalized;
};

const buildCommunityApplicationProgress = (statusKey, statusHistory = []) => {
  const currentMeta = getCommunityApplicationStatusMeta(statusKey);
  const completedStatuses = new Set(
    (Array.isArray(statusHistory) ? statusHistory : [])
      .map((entry) => normalizeCommunityApplicationStatus(entry && entry.status))
      .filter((status) => status && status !== 'rejected')
  );

  return COMMUNITY_APPLICATION_STATUSES
    .filter((status) => status.key !== 'rejected')
    .map((status) => ({
      ...status,
      isCurrent: currentMeta.key !== 'rejected' && status.key === currentMeta.key,
      isDone: currentMeta.key === 'rejected'
        ? completedStatuses.has(status.key)
        : status.order < currentMeta.order,
      isUpcoming: currentMeta.key === 'rejected'
        ? !completedStatuses.has(status.key)
        : status.order > currentMeta.order
    }));
};

const getCommunityPostsState = () => {
  const posts = db.communityPosts();
  return Array.isArray(posts) ? posts.map(normalizeCommunityPost) : [];
};

const saveCommunityPostsState = (posts) => {
  db.saveCommunityPosts(Array.isArray(posts) ? posts.map(normalizeCommunityPost) : []);
};

const getCommunityJobsState = () => {
  const jobs = db.communityJobs();
  return Array.isArray(jobs) ? jobs : [];
};

const saveCommunityJobsState = (jobs) => {
  db.saveCommunityJobs(Array.isArray(jobs) ? jobs : []);
};

const getCommunityJobApplicationsState = () => {
  const applications = db.communityJobApplications();
  return Array.isArray(applications) ? applications.map(normalizeCommunityJobApplication) : [];
};

const saveCommunityJobApplicationsState = (applications) => {
  db.saveCommunityJobApplications(
    Array.isArray(applications) ? applications.map(normalizeCommunityJobApplication) : []
  );
};

const isCommunityMemberSession = (sessionUser) => Boolean(sessionUser && sessionUser.role === 'user');

const requireCommunityMember = (req, res, next) => {
  if (!req.session.user) return res.redirect('/login');
  if (!isCommunityMemberSession(req.session.user)) {
    return res.redirect('/community?error=' + encodeURIComponent('التفاعل والتقديم متاحان للحسابات العادية فقط'));
  }
  next();
};

const isValidCommunityMediaFile = (file) => {
  if (!file) return false;
  return typeof file.mimetype === 'string' && (file.mimetype.startsWith('image/') || file.mimetype.startsWith('video/'));
};

const getCommunityMediaType = (file) => {
  if (!file || typeof file.mimetype !== 'string') return null;
  if (file.mimetype.startsWith('video/')) return 'video';
  if (file.mimetype.startsWith('image/')) return 'image';
  return null;
};

const isValidCommunityCvFile = (file) => {
  if (!file) return false;
  const ext = path.extname(file.originalname || '').toLowerCase();
  return COMMUNITY_CV_MIME_TYPES.has(file.mimetype) || COMMUNITY_CV_EXTENSIONS.has(ext);
};

const buildRedirectUrl = ({ pathName, hash, error, success }) => {
  const params = new URLSearchParams();
  if (error) params.set('error', error);
  if (success) params.set('success', success);
  const query = params.toString();
  return `${pathName}${query ? `?${query}` : ''}${hash || ''}`;
};

const getCommunityPostHash = (postId) => `#post-${postId}`;

const buildCommunityJobApplicationsMap = (applications) => {
  const map = new Map();
  for (const application of applications) {
    if (!application || !application.jobId) continue;
    if (!map.has(application.jobId)) map.set(application.jobId, []);
    map.get(application.jobId).push(application);
  }
  return map;
};

const extractGeminiTextResponse = (payload) => {
  const parts = payload?.candidates?.[0]?.content?.parts;
  if (!Array.isArray(parts)) return null;
  const textPart = parts.find((part) => typeof part?.text === 'string' && part.text.trim());
  return textPart ? textPart.text.trim() : null;
};

const parseGeminiJsonResponse = (responseText) => {
  const rawText = String(responseText || '').trim();
  if (!rawText) throw new Error('gemini_empty_text');

  const cleaned = rawText
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();

  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  const jsonText = jsonMatch ? jsonMatch[0] : cleaned;
  return JSON.parse(jsonText);
};

const COMMUNITY_AI_STOP_WORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'have', 'has', 'your', 'you', 'will', 'are', 'our',
  'الى', 'إلى', 'على', 'في', 'من', 'عن', 'مع', 'هذه', 'هذا', 'ذلك', 'التي', 'الذي', 'كما', 'ثم', 'بعد',
  'قبل', 'لدى', 'عند', 'بين', 'حول', 'وقد', 'تم', 'يتم', 'يكون', 'تكون', 'ضمن', 'خبرة', 'سنوات', 'سنة',
  'وظيفة', 'مطلوب', 'شركة', 'codentra'
].map((token) => token.toLowerCase()));

const tokenizeCommunityScreeningText = (text) => {
  const matches = String(text || '').toLowerCase().match(/[a-z0-9+#.-]+|[\u0600-\u06FF0-9+#.-]+/g) || [];
  return matches.filter((token) => token.length > 1 && !COMMUNITY_AI_STOP_WORDS.has(token));
};

const getCommunityKeywordCandidates = (job) => {
  const weighted = [];
  const pushTokens = (text, weight = 1) => {
    tokenizeCommunityScreeningText(text).forEach((token) => {
      for (let i = 0; i < weight; i += 1) weighted.push(token);
    });
  };

  pushTokens(job && job.title, 3);
  pushTokens(job && job.description, 1);

  const unique = [];
  const seen = new Set();
  for (const token of weighted) {
    if (seen.has(token)) continue;
    seen.add(token);
    unique.push(token);
  }

  return unique.slice(0, 18);
};

const buildCommunityLocalAiScreening = ({ job, applicant, cvText, sourceLabel, failureReason }) => {
  const jobKeywords = getCommunityKeywordCandidates(job);
  const applicantTokens = new Set(
    tokenizeCommunityScreeningText(`${applicant && applicant.about ? applicant.about : ''}\n${cvText || ''}`)
  );
  const matchedKeywords = jobKeywords.filter((token) => applicantTokens.has(token));
  const scoreBase = Math.max(4, Math.min(jobKeywords.length, 12));
  const score = Math.max(0, Math.min(100, Math.round((matchedKeywords.length / scoreBase) * 100)));
  const decision = matchedKeywords.length >= 3 || score >= 35 ? 'accepted' : 'rejected';
  const missingKeywords = jobKeywords.filter((token) => !applicantTokens.has(token)).slice(0, 3);

  const reasons = [];
  if (matchedKeywords.length) {
    reasons.push(`تم العثور على تقاطع واضح مع متطلبات الوظيفة في: ${matchedKeywords.slice(0, 4).join('، ')}`);
  }
  if (decision === 'rejected' && missingKeywords.length) {
    reasons.push(`السيرة لا تُظهر كلمات أو مهارات أساسية متوقعة مثل: ${missingKeywords.join('، ')}`);
  }
  if (failureReason === 'gemini_unavailable') {
    reasons.push('تم استخدام التقييم الاحتياطي لأن Gemini غير مفعل حاليًا.');
  } else if (failureReason) {
    reasons.push('تم استخدام التقييم الاحتياطي لأن Gemini لم يُكمل التحليل هذه المرة.');
  }
  if (!reasons.length) {
    reasons.push('تم تحليل السيرة الذاتية نصيًا ومقارنتها بمتطلبات الوظيفة.');
  }

  return normalizeCommunityAiScreening({
    decision,
    score,
    summary: decision === 'accepted'
      ? 'تم قبولك مبدئيًا بعد التحليل الآلي للسيرة الذاتية ومقارنتها بمتطلبات الوظيفة.'
      : 'تم رفض الطلب مبدئيًا لأن التحليل الآلي لم يجد صلة كافية بين السيرة الذاتية ومتطلبات الوظيفة.',
    reasons,
    evaluatedAt: new Date().toISOString(),
    model: failureReason ? `fallback:${sourceLabel}` : `heuristic:${sourceLabel}`,
    error: failureReason || null
  });
};

const getCommunityAiScreeningSchema = () => ({
  type: 'object',
  additionalProperties: false,
  properties: {
    decision: {
      type: 'string',
      enum: ['accepted', 'rejected']
    },
    score: {
      type: 'integer',
      minimum: 0,
      maximum: 100
    },
    summary: {
      type: 'string'
    },
    reasons: {
      type: 'array',
      items: {
        type: 'string'
      },
      minItems: 1,
      maxItems: 4
    }
  },
  required: ['decision', 'score', 'summary', 'reasons']
});

const buildCommunityAiPromptText = ({ job, applicant, fileName, sourceLabel, cvText, plainJsonOnly = false }) => [
  'أنت مسؤول توظيف أولي في Codentra.',
  'قيّم السيرة الذاتية المرفوعة مقارنة بالوظيفة المعروضة.',
  'أعطِ قرارًا ثنائيًا فقط: accepted أو rejected.',
  'اختر rejected فقط إذا كانت السيرة الذاتية بعيدة بوضوح عن الوظيفة أو تفتقد الحد الأدنى من الصلة المطلوبة.',
  'اختر accepted إذا كانت هناك صلة معقولة أو قابلية للانتقال للمرحلة التالية.',
  'اكتب النتيجة بالعربية المختصرة والواضحة.',
  plainJsonOnly ? 'أعد JSON فقط بدون markdown أو شرح إضافي.' : 'أعد JSON فقط وفق المخطط المطلوب.',
  '',
  `اسم الوظيفة: ${job.title}`,
  `الوصف الوظيفي: ${job.description}`,
  `الأجر: ${job.salary}`,
  `سنوات الخبرة المطلوبة: ${formatYearsOfExperience(job.experienceYears)}`,
  '',
  `بيانات المتقدم: الاسم ${applicant.name}، العمر ${applicant.age}`,
  `نبذة المتقدم: ${applicant.about}`,
  `اسم الملف: ${fileName || 'cv'}`,
  `صيغة السيرة الذاتية المستخدمة في التقييم: ${sourceLabel}`,
  '',
  'نص السيرة الذاتية:',
  String(cvText || '').slice(0, 18000),
  plainJsonOnly ? 'استخدم هذا الشكل حرفيًا: {"decision":"accepted","score":75,"summary":"...","reasons":["..."]}' : ''
].filter(Boolean).join('\n');

const requestCommunityGeminiScreening = async ({ cvText, sourceLabel, job, applicant, fileName, useSchema }) => {
  const generationConfig = {
    temperature: 0.1
  };

  if (useSchema) {
    generationConfig.responseMimeType = 'application/json';
    generationConfig.responseJsonSchema = getCommunityAiScreeningSchema();
  }

  const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-goog-api-key': GEMINI_API_KEY
    },
    body: JSON.stringify({
      contents: [
        {
          role: 'user',
          parts: [
            {
              text: buildCommunityAiPromptText({
                job,
                applicant,
                fileName,
                sourceLabel,
                cvText,
                plainJsonOnly: !useSchema
              })
            }
          ]
        }
      ],
      generationConfig
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`gemini_http_${response.status}:${errorText}`);
  }

  const payload = await response.json();
  const responseText = extractGeminiTextResponse(payload);
  if (!responseText) {
    throw new Error(`gemini_empty_response:${JSON.stringify(payload?.promptFeedback || {})}`);
  }

  return parseGeminiJsonResponse(responseText);
};

const extractCommunityCvText = async ({ cvBuffer, mimeType, fileName }) => {
  const normalizedMimeType = String(mimeType || '').toLowerCase();
  const normalizedFileName = String(fileName || '').toLowerCase();

  if (normalizedMimeType === 'application/pdf' || normalizedFileName.endsWith('.pdf')) {
    const extraction = await pdfParse(cvBuffer);
    const extractedText = String(extraction?.text || '').replace(/\s+/g, ' ').trim();
    if (!extractedText) {
      throw new Error('empty_pdf_text');
    }

    return {
      cvText: extractedText,
      sourceLabel: 'PDF'
    };
  }

  if (
    normalizedMimeType === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
    normalizedFileName.endsWith('.docx')
  ) {
    const extraction = await mammoth.extractRawText({ buffer: cvBuffer });
    const extractedText = String(extraction?.value || '').replace(/\s+/g, ' ').trim();
    if (!extractedText) {
      throw new Error('empty_docx_text');
    }

    return {
      cvText: extractedText,
      sourceLabel: 'DOCX'
    };
  }

  throw new Error('unsupported_cv_ai_format');
};

const screenCommunityCvWithGemini = async ({ job, applicant, cvBuffer, mimeType, fileName }) => {
  if (!GEMINI_API_KEY) {
    return normalizeCommunityAiScreening({
      decision: 'skipped',
      summary: 'لم يتم تفعيل التقييم الذكي بعد.',
      reasons: []
    });
  }

  if (!Buffer.isBuffer(cvBuffer) || !cvBuffer.length) {
    throw new Error('missing_cv_buffer');
  }

  const cvContent = await extractCommunityCvText({ cvBuffer, mimeType, fileName });

  if (!GEMINI_API_KEY) {
    return buildCommunityLocalAiScreening({
      job,
      applicant,
      cvText: cvContent.cvText,
      sourceLabel: cvContent.sourceLabel,
      failureReason: 'gemini_unavailable'
    });
  }

  let parsed;

  try {
    parsed = await requestCommunityGeminiScreening({
      cvText: cvContent.cvText,
      sourceLabel: cvContent.sourceLabel,
      job,
      applicant,
      fileName,
      useSchema: true
    });
  } catch (primaryError) {
    try {
      parsed = await requestCommunityGeminiScreening({
        cvText: cvContent.cvText,
        sourceLabel: cvContent.sourceLabel,
        job,
        applicant,
        fileName,
        useSchema: false
      });
    } catch (fallbackError) {
      return buildCommunityLocalAiScreening({
        job,
        applicant,
        cvText: cvContent.cvText,
        sourceLabel: cvContent.sourceLabel,
        failureReason: `gemini_screening_failed:primary=${String(primaryError && primaryError.message ? primaryError.message : primaryError)};fallback=${String(fallbackError && fallbackError.message ? fallbackError.message : fallbackError)}`
      });
    }
  }

  return normalizeCommunityAiScreening({
    decision: parsed.decision,
    score: parsed.score,
    summary: parsed.summary,
    reasons: parsed.reasons,
    evaluatedAt: new Date().toISOString(),
    model: GEMINI_MODEL
  });
};

const formatYearsOfExperience = (value) => {
  const years = Number(value || 0);
  if (!Number.isFinite(years)) return '0 سنة';
  if (years === 0) return 'بدون خبرة';
  if (Number.isInteger(years)) return `${years} سنة`;
  return `${years} سنة`;
};

const getWalletPaymentAttemptsState = () => {
  const attempts = db.walletPaymentAttempts();
  return Array.isArray(attempts) ? attempts : [];
};

const saveWalletPaymentAttemptsState = (attempts) => {
  db.saveWalletPaymentAttempts(Array.isArray(attempts) ? attempts : []);
};

function applyRuntimeDataFixups(state) {
  if (!state || typeof state !== 'object') return false;

  let changed = false;
  const arrayKeys = [
    'users',
    'projects',
    'purchases',
    'modifications',
    'coupons',
    'referrals',
    'walletCodes',
    'walletPaymentAttempts',
    'reviews',
    'messages',
    'communityPosts',
    'communityJobs',
    'communityJobApplications',
    'adminTeamMessages',
    'carts',
    'invoices',
    'meetingRecordings',
    'subscriptionPlans',
    'subscriptions',
    'subscriptionPayments',
    'subscriptionCoupons',
    'presentationPlans',
    'presentationSubscriptions',
    'presentationDecks',
    'presentationPaymentAttempts'
  ];

  arrayKeys.forEach((key) => {
    if (!Array.isArray(state[key])) {
      state[key] = [];
      changed = true;
    }
  });

  if (!state.appointments || typeof state.appointments !== 'object' || Array.isArray(state.appointments)) {
    state.appointments = createDefaultAppointmentsState();
    changed = true;
  } else {
    if (!Array.isArray(state.appointments.timeSlots)) {
      state.appointments.timeSlots = [];
      changed = true;
    }
    if (!Array.isArray(state.appointments.bookings)) {
      state.appointments.bookings = [];
      changed = true;
    }
  }

  const normalizedPosts = state.communityPosts.map(normalizeCommunityPost);
  if (JSON.stringify(normalizedPosts) !== JSON.stringify(state.communityPosts)) {
    state.communityPosts = normalizedPosts;
    changed = true;
  }

  const normalizedApplications = state.communityJobApplications.map(normalizeCommunityJobApplication);
  if (JSON.stringify(normalizedApplications) !== JSON.stringify(state.communityJobApplications)) {
    state.communityJobApplications = normalizedApplications;
    changed = true;
  }

  const normalizedLoyaltySettings = normalizeLoyaltySettingsState(state.loyaltySettings);
  if (JSON.stringify(normalizedLoyaltySettings) !== JSON.stringify(state.loyaltySettings)) {
    state.loyaltySettings = normalizedLoyaltySettings;
    changed = true;
  }

  if (!state.users.find((user) => user && user.email === 'admin@codentra.com')) {
    state.users.push({
      id: uuidv4(),
      name: 'Admin',
      email: 'admin@codentra.com',
      password: bcrypt.hashSync('admin123', 10),
      role: 'admin',
      isSuperAdmin: true,
      createdAt: new Date().toISOString()
    });
    changed = true;
  }

  state.users.forEach((user) => {
    if (ensureUserPaymentProfile({ user, users: state.users })) {
      changed = true;
    }
  });

  return changed;
}

const purgeExpiredWalletPaymentAttempts = () => {
  const attempts = getWalletPaymentAttemptsState();
  const nextAttempts = attempts.filter((attempt) => {
    if (!attempt || attempt.usedAt) return false;
    if (!isPaymentAttemptExpired(attempt)) return true;

    finalizeWalletCardOwnerActivity({
      payerUserId: attempt.payerUserId,
      buyerUser: { id: attempt.buyerUserId, name: attempt.buyerDisplayName || null, email: null },
      amount: attempt.amount,
      kind: attempt.kind,
      payload: attempt.payload || {},
      attemptId: attempt.id,
      usageLogEntryId: attempt.usageLogEntryId || null,
      status: 'expired',
      note: 'انتهت صلاحية طلب الدفع قبل إدخال كود التحقق',
      notifyTitle: 'انتهت صلاحية طلب على بطاقتك',
      notifyMessage: `انتهت صلاحية طلب الدفع الخاص بـ ${attempt.purposeLabel || 'استخدام بطاقة المحفظة'} قبل إتمامه.`
    });
    return false;
  });
  if (nextAttempts.length !== attempts.length) {
    saveWalletPaymentAttemptsState(nextAttempts);
  }
};

const createWalletPaymentAttempt = async ({ buyerUser, payerUser, amount, kind, payload }) => {
  purgeExpiredWalletPaymentAttempts();

  const code = generatePaymentVerificationCode();
  const purposeLabel = buildWalletPaymentPurposeLabel({ kind, payload });
  const attempt = {
    id: uuidv4(),
    buyerUserId: buyerUser.id,
    buyerDisplayName: buildWalletPaymentBuyerLabel(buyerUser),
    payerUserId: payerUser.id,
    payerCardLast4: normalizeWalletCardNumber(payerUser.walletCardNumber).slice(-4),
    amount: Math.round(Number(amount || 0) * 100) / 100,
    kind,
    payload: payload || {},
    purposeLabel,
    ownerVisibleCode: code,
    codeHash: hashPaymentVerificationCode(code),
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + PAYMENT_VERIFICATION_TTL_MS).toISOString(),
    usedAt: null
  };

  const attempts = getWalletPaymentAttemptsState();
  const usageEntry = registerWalletPaymentRequestForOwner({
    payerUserId: payerUser.id,
    buyerUser,
    amount: attempt.amount,
    kind,
    payload: payload || {},
    attemptId: attempt.id,
    code,
    expiresAt: attempt.expiresAt
  });
  attempt.usageLogEntryId = usageEntry ? usageEntry.id : null;
  attempts.push(attempt);
  saveWalletPaymentAttemptsState(attempts);

  try {
    await sendPaymentVerificationCode({
      payerUser,
      buyerUser,
      code,
      amount: attempt.amount,
      maskedCardNumber: maskWalletCardNumber(payerUser.walletCardNumber)
    });
  } catch (error) {
    // The in-app notification center is now the primary delivery channel.
  }

  return attempt;
};

const getWalletPaymentAttemptById = (attemptId) => {
  if (!attemptId) return null;
  const attempts = getWalletPaymentAttemptsState();
  return attempts.find(attempt => attempt && attempt.id === attemptId) || null;
};

const markWalletPaymentAttemptUsed = (attemptId) => {
  const attempts = getWalletPaymentAttemptsState();
  const index = attempts.findIndex(attempt => attempt && attempt.id === attemptId);
  if (index === -1) return false;
  attempts[index].usedAt = new Date().toISOString();
  saveWalletPaymentAttemptsState(attempts);
  return true;
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

const cloneDeep = (value) => JSON.parse(JSON.stringify(value));

const ensurePresentationPlans = () => {
  const current = db.presentationPlans();
  if (Array.isArray(current) && current.length) return current;
  const seeded = cloneDeep(DEFAULT_PRESENTATION_PLANS);
  db.savePresentationPlans(seeded);
  return seeded;
};

const getActivePresentationSubscriptionForUser = ({ userId }) => {
  if (!userId) return null;
  const subscriptions = db.presentationSubscriptions();
  const now = Date.now();
  return subscriptions
    .filter((subscription) => {
      if (!subscription || subscription.userId !== userId || subscription.status !== 'active') return false;
      const end = new Date(subscription.currentPeriodEnd || 0).getTime();
      return end > now;
    })
    .sort((a, b) => new Date(b.currentPeriodEnd || 0).getTime() - new Date(a.currentPeriodEnd || 0).getTime())[0] || null;
};

const getPresentationPlanById = ({ planId }) => {
  const plans = ensurePresentationPlans();
  return plans.find((plan) => plan && plan.id === planId) || null;
};

const getPresentationIncomingApprovals = ({ ownerUserId }) => {
  const attempts = db.presentationPaymentAttempts();
  return attempts
    .filter((attempt) => attempt && attempt.payerUserId === ownerUserId && !attempt.usedAt && !isPaymentAttemptExpired(attempt))
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
};

const getPresentationDecksForUser = ({ userId, limit = 8 }) => {
  const decks = db.presentationDecks();
  return decks
    .filter((deck) => deck && deck.userId === userId)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime())
    .slice(0, limit);
};

const purgeExpiredPresentationPaymentAttempts = () => {
  const attempts = db.presentationPaymentAttempts();
  const nextAttempts = attempts.filter((attempt) => {
    if (!attempt || attempt.usedAt) return false;
    if (!isPaymentAttemptExpired(attempt)) return true;

    finalizeWalletCardOwnerActivity({
      payerUserId: attempt.payerUserId,
      buyerUser: { id: attempt.buyerUserId, name: attempt.buyerDisplayName || null, email: null },
      amount: attempt.amount,
      kind: 'presentations-subscription',
      payload: { planName: attempt.planName || 'Codentra Presentations' },
      attemptId: attempt.id,
      usageLogEntryId: attempt.usageLogEntryId || null,
      status: 'expired',
      note: 'انتهت صلاحية طلب الاشتراك قبل إدخال كود التحقق',
      notifyTitle: 'انتهت صلاحية طلب على بطاقتك',
      notifyMessage: 'انتهت صلاحية طلب الدفع الخاص باشتراك Codentra Presentations قبل إتمامه.'
    });
    return false;
  });
  if (nextAttempts.length !== attempts.length) {
    db.savePresentationPaymentAttempts(nextAttempts);
  }
};

const getPresentationPaymentAttemptById = ({ attemptId }) => {
  if (!attemptId) return null;
  purgeExpiredPresentationPaymentAttempts();
  const attempts = db.presentationPaymentAttempts();
  return attempts.find((attempt) => attempt && attempt.id === attemptId) || null;
};

const markPresentationPaymentAttemptUsed = ({ attemptId }) => {
  const attempts = db.presentationPaymentAttempts();
  const index = attempts.findIndex((attempt) => attempt && attempt.id === attemptId);
  if (index === -1) return false;
  attempts[index].usedAt = new Date().toISOString();
  db.savePresentationPaymentAttempts(attempts);
  return true;
};

const createPresentationPaymentAttempt = ({ buyerUser, payerUser, plan }) => {
  purgeExpiredPresentationPaymentAttempts();

  const amount = Number(plan.price || 0);
  enforceWalletCardSpendingLimit({ payerUser, amount });
  if (Number(payerUser.walletBalance || 0) < amount) {
    throw new Error('رصيد البطاقة غير كافٍ');
  }

  if (amount > HIGH_VALUE_PAYMENT_THRESHOLD && !payerUser.walletPaymentPasswordHash) {
    throw new Error('صاحب البطاقة لم يضبط كلمة مرور البطاقة بعد');
  }

  const code = generatePaymentVerificationCode();
  const attempts = db.presentationPaymentAttempts();
  const attempt = {
    id: uuidv4(),
    buyerUserId: buyerUser.id,
    buyerDisplayName: buildWalletPaymentBuyerLabel(buyerUser),
    payerUserId: payerUser.id,
    planId: plan.id,
    planName: plan.name,
    amount,
    requiresPassword: amount > HIGH_VALUE_PAYMENT_THRESHOLD,
    purposeLabel: buildWalletPaymentPurposeLabel({ kind: 'presentations-subscription', payload: { planName: plan.name } }),
    ownerVisibleCode: code,
    codeHash: hashPaymentVerificationCode(code),
    payerCardLast4: normalizeWalletCardNumber(payerUser.walletCardNumber).slice(-4),
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + PAYMENT_VERIFICATION_TTL_MS).toISOString(),
    usedAt: null
  };

  const usageEntry = registerWalletPaymentRequestForOwner({
    payerUserId: payerUser.id,
    buyerUser,
    amount,
    kind: 'presentations-subscription',
    payload: { planName: plan.name, planId: plan.id },
    attemptId: attempt.id,
    code,
    expiresAt: attempt.expiresAt
  });
  attempt.usageLogEntryId = usageEntry ? usageEntry.id : null;

  attempts.push(attempt);
  db.savePresentationPaymentAttempts(attempts);
  return attempt;
};

const upsertPresentationSubscription = ({ userId, plan, payerUserId }) => {
  const subscriptions = db.presentationSubscriptions();
  const now = new Date();
  const activeIndex = subscriptions.findIndex((subscription) => {
    if (!subscription || subscription.userId !== userId || subscription.status !== 'active') return false;
    const end = new Date(subscription.currentPeriodEnd || 0);
    return !Number.isNaN(end.getTime()) && end > now;
  });

  if (activeIndex !== -1) {
    const current = subscriptions[activeIndex];
    const currentEnd = new Date(current.currentPeriodEnd || 0);
    const baseTime = Math.max(now.getTime(), currentEnd.getTime());
    subscriptions[activeIndex] = {
      ...current,
      planId: plan.id,
      interval: plan.interval,
      currentPeriodEnd: new Date(baseTime + (Number(plan.durationDays || 0) * 24 * 60 * 60 * 1000)).toISOString(),
      remainingCredits: Number(current.remainingCredits || 0) + Number(plan.generationCredits || 0),
      lastAmountPaid: Number(plan.price || 0),
      lastPayerUserId: payerUserId,
      updatedAt: new Date().toISOString()
    };
    db.savePresentationSubscriptions(subscriptions);
    return subscriptions[activeIndex];
  }

  const subscription = {
    id: uuidv4(),
    userId,
    planId: plan.id,
    interval: plan.interval,
    status: 'active',
    currentPeriodStart: now.toISOString(),
    currentPeriodEnd: new Date(now.getTime() + (Number(plan.durationDays || 0) * 24 * 60 * 60 * 1000)).toISOString(),
    remainingCredits: Number(plan.generationCredits || 0),
    createdAt: now.toISOString(),
    updatedAt: now.toISOString(),
    lastAmountPaid: Number(plan.price || 0),
    lastPayerUserId: payerUserId
  };
  subscriptions.push(subscription);
  db.savePresentationSubscriptions(subscriptions);
  return subscription;
};

const consumePresentationCredit = ({ subscriptionId }) => {
  const subscriptions = db.presentationSubscriptions();
  const index = subscriptions.findIndex((subscription) => subscription && subscription.id === subscriptionId);
  if (index === -1) throw new Error('الاشتراك غير موجود');
  if (Number(subscriptions[index].remainingCredits || 0) <= 0) {
    throw new Error('لا توجد عمليات إنشاء متبقية');
  }

  subscriptions[index].remainingCredits = Number(subscriptions[index].remainingCredits || 0) - 1;
  subscriptions[index].updatedAt = new Date().toISOString();
  db.savePresentationSubscriptions(subscriptions);
  return subscriptions[index];
};

const countPresentationSlides = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return 7;
  return Math.min(12, Math.max(4, Math.round(amount)));
};

const buildFallbackPresentation = ({ topic, audience, tone, purpose, slideCount, language }) => {
  const isArabic = language !== 'en';
  const titles = isArabic
    ? ['مقدمة', 'التحدي', 'الرؤية', 'الحل', 'خطة التنفيذ', 'القيمة', 'الخطوات التالية', 'الخاتمة']
    : ['Introduction', 'Problem', 'Vision', 'Solution', 'Execution Plan', 'Value', 'Next Steps', 'Closing'];

  const slides = Array.from({ length: slideCount }, (_, index) => {
    const title = titles[index] || (isArabic ? `شريحة ${index + 1}` : `Slide ${index + 1}`);
    return {
      title,
      bullets: isArabic
        ? [
            `الموضوع الرئيسي: ${topic}`,
            `الجمهور المستهدف: ${audience || 'عملاء أو إدارة'}`,
            `النبرة المطلوبة: ${tone || 'احترافية'}`,
            `هدف هذه الشريحة: ${purpose || 'شرح الفكرة'}`
          ]
        : [
            `Main topic: ${topic}`,
            `Audience: ${audience || 'clients or decision makers'}`,
            `Tone: ${tone || 'professional'}`,
            `Goal: ${purpose || 'explain the idea'}`
          ],
      speakerNotes: isArabic
        ? `اربط هذه الشريحة بالهدف العام للعرض: ${purpose || topic}.`
        : `Connect this slide to the main objective: ${purpose || topic}.`
    };
  });

  return {
    title: isArabic ? `عرض تقديمي: ${topic}` : `${topic} Presentation`,
    subtitle: isArabic ? 'تم إنشاؤه بواسطة Codentra Presentations' : 'Generated by Codentra Presentations',
    theme: tone || (isArabic ? 'احترافي' : 'Professional'),
    slides
  };
};

const normalizeGeneratedPresentation = (deck, options) => {
  const fallback = buildFallbackPresentation(options);
  if (!deck || typeof deck !== 'object') return fallback;

  const rawSlides = Array.isArray(deck.slides) ? deck.slides : fallback.slides;
  return {
    title: String(deck.title || fallback.title),
    subtitle: String(deck.subtitle || fallback.subtitle),
    theme: String(deck.theme || fallback.theme),
    slides: rawSlides.slice(0, options.slideCount).map((slide, index) => ({
      title: String((slide && slide.title) || fallback.slides[index]?.title || `Slide ${index + 1}`),
      bullets: Array.isArray(slide && slide.bullets) && slide.bullets.length
        ? slide.bullets.slice(0, 5).map((bullet) => String(bullet))
        : (fallback.slides[index]?.bullets || []),
      speakerNotes: String((slide && slide.speakerNotes) || fallback.slides[index]?.speakerNotes || '')
    }))
  };
};

const generatePresentationWithOpenAI = async (options) => {
  if (!process.env.OPENAI_API_KEY) {
    return buildFallbackPresentation(options);
  }

  const schema = {
    name: 'presentation_deck',
    strict: true,
    schema: {
      type: 'object',
      properties: {
        title: { type: 'string' },
        subtitle: { type: 'string' },
        theme: { type: 'string' },
        slides: {
          type: 'array',
          minItems: options.slideCount,
          maxItems: options.slideCount,
          items: {
            type: 'object',
            properties: {
              title: { type: 'string' },
              bullets: {
                type: 'array',
                minItems: 3,
                maxItems: 5,
                items: { type: 'string' }
              },
              speakerNotes: { type: 'string' }
            },
            required: ['title', 'bullets', 'speakerNotes'],
            additionalProperties: false
          }
        }
      },
      required: ['title', 'subtitle', 'theme', 'slides'],
      additionalProperties: false
    }
  };

  const systemPrompt = options.language === 'en'
    ? 'You are a presentation strategist for Codentra. Return only valid JSON matching the provided schema.'
    : 'أنت خبير عروض تقديمية لشركة Codentra. أعد فقط JSON صالحًا مطابقًا للمخطط المطلوب.';

  const userPrompt = options.language === 'en'
    ? `Create a presentation about: ${options.topic}. Audience: ${options.audience || 'general'}. Goal: ${options.purpose || 'present an idea'}. Tone: ${options.tone || 'professional'}. Required slide count: ${options.slideCount}.`
    : `أنشئ عرضًا تقديميًا عن: ${options.topic}. الجمهور: ${options.audience || 'عام'}. الهدف: ${options.purpose || 'عرض فكرة'}. النبرة: ${options.tone || 'احترافية'}. عدد الشرائح المطلوب: ${options.slideCount}.`;

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: PRESENTATION_AI_MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt }
      ],
      response_format: {
        type: 'json_schema',
        json_schema: schema
      }
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenAI request failed: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  const content = data?.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error('OpenAI did not return content');
  }

  return normalizeGeneratedPresentation(JSON.parse(content), options);
};

const generatePresentationDeck = async (options) => {
  try {
    return await generatePresentationWithOpenAI(options);
  } catch (error) {
    console.error('Presentation AI fallback:', error.message);
    return buildFallbackPresentation(options);
  }
};

const sanitizePresentationFileName = (value) => {
  return String(value || 'presentation')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-')
    .slice(0, 80) || 'presentation';
};

const buildPresentationPptxFile = async (deck) => {
  const pptx = new PptxGenJS();
  pptx.layout = 'LAYOUT_WIDE';
  pptx.author = 'Codentra';
  pptx.company = 'Codentra';
  pptx.subject = deck.title;
  pptx.title = deck.title;
  pptx.lang = deck.language === 'en' ? 'en-US' : 'ar-SA';

  const cover = pptx.addSlide();
  cover.background = { color: 'F6F2EA' };
  cover.addText(deck.title, {
    x: 0.6, y: 1.1, w: 12.0, h: 0.7,
    fontFace: 'Arial', fontSize: 24, bold: true, color: '132238',
    align: deck.language === 'en' ? 'left' : 'right',
    rtlMode: deck.language !== 'en'
  });
  cover.addText(deck.subtitle || 'Generated by Codentra Presentations', {
    x: 0.6, y: 2.0, w: 12.0, h: 0.6,
    fontFace: 'Arial', fontSize: 13, color: '5B6472',
    align: deck.language === 'en' ? 'left' : 'right',
    rtlMode: deck.language !== 'en'
  });

  deck.slides.forEach((slideData, index) => {
    const slide = pptx.addSlide();
    slide.background = { color: 'FFFFFF' };
    slide.addText(slideData.title, {
      x: 0.6, y: 0.5, w: 12.0, h: 0.5,
      fontFace: 'Arial', fontSize: 20, bold: true, color: '132238',
      align: deck.language === 'en' ? 'left' : 'right',
      rtlMode: deck.language !== 'en'
    });
    slide.addText(
      slideData.bullets.map((bullet) => ({
        text: bullet,
        options: {
          bullet: { indent: 18 },
          breakLine: true
        }
      })),
      {
        x: 0.9, y: 1.35, w: 11.4, h: 4.7,
        fontFace: 'Arial', fontSize: 16, color: '233044',
        paraSpaceAfterPt: 12,
        align: deck.language === 'en' ? 'left' : 'right',
        rtlMode: deck.language !== 'en',
        valign: 'top'
      }
    );
    slide.addText(`Slide ${index + 1}`, {
      x: 0.6, y: 6.75, w: 12.0, h: 0.3,
      fontFace: 'Arial', fontSize: 10, color: '7B8794',
      align: deck.language === 'en' ? 'left' : 'right'
    });
  });

  const fileName = `${sanitizePresentationFileName(deck.title)}-${deck.id}.pptx`;
  const filePath = path.join(os.tmpdir(), fileName);
  await pptx.writeFile({ fileName: filePath });
  return { fileName, filePath };
};

ensureStorageReady().catch((error) => {
  console.error('Storage warmup failed:', error);
});

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));
app.get('/uploads/*', (req, res, next) => {
  const requestedPath = req.params[0];
  if (!requestedPath || requestedPath.includes('..')) return next();

  const absolutePath = toAbsolutePath(`uploads/${requestedPath}`);
  if (!absolutePath || !fs.existsSync(absolutePath)) return next();

  return res.sendFile(absolutePath);
});
app.use('/uploads', express.static(UPLOADS_DIR));

app.use((req, res, next) => {
  storageRequestContext.run({ pendingPersists: [] }, () => {
    const context = storageRequestContext.getStore();
    const originalEnd = res.end.bind(res);
    let endIntercepted = false;

    res.end = (...args) => {
      if (endIntercepted) return;
      endIntercepted = true;

      const pendingPersists = context ? context.pendingPersists.slice() : [];
      if (!pendingPersists.length) {
        return originalEnd(...args);
      }

      Promise.allSettled(pendingPersists).then((results) => {
        const failedPersist = results.find((result) => result.status === 'rejected');
        if (failedPersist && !res.headersSent) {
          res.statusCode = 500;
          return originalEnd('Storage persistence error');
        }

        return originalEnd(...args);
      });
    };

    next();
  });
});

app.use(async (req, res, next) => {
  try {
    await ensureStorageReady();
    await refreshRuntimeStateFromNeon();
    next();
  } catch (error) {
    console.error('Storage initialization error:', error);
    if (!res.headersSent) {
      res.status(500).send('Storage connection error');
    }
  }
});

if (IS_VERCEL) {
  app.set('trust proxy', 1);
}

app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { 
    maxAge: AUTH_COOKIE_MAX_AGE,
    sameSite: 'lax',
    secure: IS_VERCEL
  }
}));

app.use((req, res, next) => {
  try {
    hydrateSessionUserFromAuthCookie({ req, res });
  } catch (error) {
    // ignore auth cookie hydration failures
  }
  next();
});

app.use(async (req, res, next) => {
  try {
    const currencyRate = await fetchEgpToUsdRate();
    res.locals.displayCurrencyCode = 'USD';
    res.locals.displayCurrencyRate = currencyRate.egpToUsd;
    res.locals.displayCurrencyDate = currencyRate.sourceDate;
    res.locals.displayMoney = (amount) => buildDisplayMoney(amount, currencyRate.egpToUsd).formatted;
    res.locals.displayMoneyValue = (amount) => buildDisplayMoney(amount, currencyRate.egpToUsd).usd.toFixed(2);
    res.locals.formatUsdAmount = (amount) => formatUsdAmount(amount);
    res.locals.convertEgpToUsd = (amount) => convertEgpToUsd(amount, currencyRate.egpToUsd);
  } catch (error) {
    res.locals.displayCurrencyCode = 'USD';
    res.locals.displayCurrencyRate = getCachedEgpToUsdRate();
    res.locals.displayCurrencyDate = new Date().toISOString().slice(0, 10);
    res.locals.displayMoney = (amount) => buildDisplayMoney(amount).formatted;
    res.locals.displayMoneyValue = (amount) => buildDisplayMoney(amount).usd.toFixed(2);
    res.locals.formatUsdAmount = (amount) => formatUsdAmount(amount);
    res.locals.convertEgpToUsd = (amount) => convertEgpToUsd(amount);
  }
  next();
});

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

app.use((req, res, next) => {
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

    const users = db.users();
    const idx = users.findIndex(u => u && u.id === req.session.user.id);
    if (idx === -1) return next();

    const fullUser = users[idx];

    if (ensureUserPaymentProfile({ user: fullUser, users })) {
      users[idx] = fullUser;
      db.saveUsers(users);
    }

    if (isBlockedExpired(fullUser)) {
      unblockUserInPlace(fullUser);
      users[idx] = fullUser;
      db.saveUsers(users);
    }

    if (fullUser.isBlocked) {
      if (allowWhileBlocked) return next();
      return res.redirect('/blocked');
    }

    req.session.user = buildSessionUser(fullUser);
  } catch (e) {
    // ignore
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

const communityMediaStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, COMMUNITY_MEDIA_DIR),
  filename: (req, file, cb) => cb(null, buildSafeUploadFileName({ file, fallbackBaseName: 'community-media' }))
});
const communityMediaUpload = multer({
  storage: communityMediaStorage,
  limits: { fileSize: COMMUNITY_MEDIA_SIZE_LIMIT_MB * 1024 * 1024 }
});

const communityCvStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, COMMUNITY_CVS_DIR),
  filename: (req, file, cb) => cb(null, buildSafeUploadFileName({ file, fallbackBaseName: 'community-cv' }))
});
const communityCvUpload = multer({
  storage: communityCvStorage,
  limits: { fileSize: COMMUNITY_CV_SIZE_LIMIT_MB * 1024 * 1024 }
});

const getCommunityUploadErrorMessage = ({ error, kind }) => {
  if (!error) return null;
  if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
    if (kind === 'post') return `حجم الصورة أو الفيديو يجب ألا يتجاوز ${COMMUNITY_MEDIA_SIZE_LIMIT_MB}MB`;
    if (kind === 'cv') return `حجم السيرة الذاتية يجب ألا يتجاوز ${COMMUNITY_CV_SIZE_LIMIT_MB}MB`;
  }
  return kind === 'cv'
    ? 'تعذر رفع السيرة الذاتية. حاول مرة أخرى.'
    : 'تعذر رفع ملف البوست. حاول مرة أخرى.';
};

const communityPostUploadHandler = (req, res, next) => {
  communityMediaUpload.single('media')(req, res, (error) => {
    if (error) {
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        error: getCommunityUploadErrorMessage({ error, kind: 'post' })
      }));
    }
    next();
  });
};

const communityCvUploadHandler = (req, res, next) => {
  communityCvUpload.single('cv')(req, res, (error) => {
    if (error) {
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: getCommunityUploadErrorMessage({ error, kind: 'cv' })
      }));
    }
    next();
  });
};

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
const requireAuth = (req, res, next) => {
  if (!req.session.user) return res.redirect('/login');
  next();
};

const requireAdmin = (req, res, next) => {
  if (!req.session.user || req.session.user.role !== 'admin') return res.redirect('/');
  next();
};

const requireSuperAdmin = (req, res, next) => {
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
  return (req, res, next) => {
    if (!req.session.user || req.session.user.role !== 'admin') return res.redirect('/');
    if (!hasAdminPermission({ sessionUser: req.session.user, permission })) {
      return res.redirect('/admin?error=' + encodeURIComponent('ليس لديك صلاحية لهذه الصفحة'));
    }
    next();
  };
};

// Routes

// Home - Projects listing
app.get('/', (req, res) => {
  const projects = db.projects()
    .filter(p => isProjectVisibleToUser({ project: p, sessionUser: req.session.user }))
    .map(decorateProjectPricing);
  const approvedPurchases = db.purchases().filter((p) => p && p.status === 'approved' && p.projectId);
  const salesCountByProjectId = new Map();
  const latestPurchaseAtByProjectId = new Map();
  approvedPurchases.forEach((purchase) => {
    const projectId = purchase.projectId;
    salesCountByProjectId.set(projectId, (salesCountByProjectId.get(projectId) || 0) + 1);
    const ts = new Date(purchase.purchasedAt || purchase.createdAt || 0).getTime();
    if (!Number.isNaN(ts) && ts > 0) {
      const prev = latestPurchaseAtByProjectId.get(projectId) || 0;
      if (ts > prev) latestPurchaseAtByProjectId.set(projectId, ts);
    }
  });
  const maxSales = Math.max(0, ...Array.from(salesCountByProjectId.values()));
  const topSellerProjectIds = maxSales > 0
    ? Array.from(salesCountByProjectId.entries()).filter(([, count]) => count === maxSales).map(([projectId]) => projectId)
    : [];

  const nowMs = Date.now();
  const formatSince = (diffMs) => {
    const minutes = Math.max(1, Math.floor(diffMs / 60000));
    if (minutes < 60) return `آخر شراء منذ ${minutes} دقيقة`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `آخر شراء منذ ${hours} ساعة`;
    const days = Math.floor(hours / 24);
    return `آخر شراء منذ ${days} يوم`;
  };

  const projectsWithTrustSignals = projects.map((project) => {
    const latestPurchaseAt = latestPurchaseAtByProjectId.get(project.id) || null;
    return {
      ...project,
      trustSignals: {
        isTopSeller: topSellerProjectIds.includes(project.id),
        salesCount: salesCountByProjectId.get(project.id) || 0,
        lastPurchaseText: latestPurchaseAt ? formatSince(Math.max(0, nowMs - latestPurchaseAt)) : null
      }
    };
  });

  const recentProjects = getRecentlyViewedProjects({ req, availableProjects: projects })
    .map(decorateProjectPricing);
  let wishlistProjectIds = [];
  if (req.session.user && req.session.user.role === 'user') {
    const currentUser = db.users().find((u) => u && u.id === req.session.user.id && u.role === 'user');
    wishlistProjectIds = currentUser && Array.isArray(currentUser.wishlistProjectIds) ? currentUser.wishlistProjectIds : [];
  }
  res.render('index', { projects: projectsWithTrustSignals, recentProjects, user: req.session.user, wishlistProjectIds });
});

app.get('/wishlist', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const users = db.users();
  const currentUser = users.find((u) => u && u.id === req.session.user.id && u.role === 'user');
  const wishlistIds = currentUser && Array.isArray(currentUser.wishlistProjectIds) ? currentUser.wishlistProjectIds : [];
  const projects = db.projects()
    .filter((p) => wishlistIds.includes(p.id))
    .filter((p) => isProjectVisibleToUser({ project: p, sessionUser: req.session.user }))
    .map(decorateProjectPricing);
  res.render('wishlist', { user: req.session.user, projects, wishlistProjectIds: wishlistIds });
});

app.post('/wishlist/toggle', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/login');
  const projectId = String(req.body.projectId || '').trim();
  if (!projectId) return res.redirect(req.get('referer') || '/');

  const users = db.users();
  const userIndex = users.findIndex((u) => u && u.id === req.session.user.id && u.role === 'user');
  if (userIndex === -1) return res.redirect(req.get('referer') || '/');

  const wishlist = Array.isArray(users[userIndex].wishlistProjectIds) ? users[userIndex].wishlistProjectIds : [];
  const exists = wishlist.includes(projectId);
  users[userIndex].wishlistProjectIds = exists ? wishlist.filter((id) => id !== projectId) : [...wishlist, projectId];
  db.saveUsers(users);
  req.session.user = buildSessionUser(users[userIndex]);
  return res.redirect(req.get('referer') || '/');
});

app.get('/compare', (req, res) => {
  const raw = String(req.query.ids || '');
  const ids = raw.split(',').map((id) => id.trim()).filter(Boolean).slice(0, 3);
  const allProjects = db.projects()
    .filter((p) => isProjectVisibleToUser({ project: p, sessionUser: req.session.user }))
    .map(decorateProjectPricing);
  const projects = ids.map((id) => allProjects.find((p) => p.id === id)).filter(Boolean);

  const ratingsByProjectId = new Map();
  const reviews = db.reviews();
  projects.forEach((project) => {
    const projectReviews = reviews.filter((r) => r && r.projectId === project.id);
    const avgRating = projectReviews.length
      ? (projectReviews.reduce((sum, r) => sum + Number(r.rating || 0), 0) / projectReviews.length)
      : 0;
    ratingsByProjectId.set(project.id, { avgRating, count: projectReviews.length });
  });

  res.render('compare', { user: req.session.user, projects, ratingsByProjectId });
});

app.get('/community', (req, res) => {
  const currentUser = req.session.user || null;
  const posts = getCommunityPostsState()
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime())
    .map(post => {
      const likes = Array.isArray(post.likes) ? post.likes : [];
      const shares = Array.isArray(post.shares) ? post.shares : [];
      const comments = Array.isArray(post.comments) ? post.comments : [];
      return {
        ...post,
        likes,
        shares,
        comments: comments
          .slice()
          .sort((a, b) => new Date(a.createdAt || 0).getTime() - new Date(b.createdAt || 0).getTime()),
        likeCount: likes.length,
        shareCount: shares.length,
        commentCount: comments.length,
        isLikedByCurrentUser: Boolean(currentUser && likes.some(item => item && item.userId === currentUser.id)),
        isSharedByCurrentUser: Boolean(currentUser && shares.some(item => item && item.userId === currentUser.id))
      };
    });

  const jobs = getCommunityJobsState()
    .filter(job => job && job.isActive !== false)
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  const applications = getCommunityJobApplicationsState();
  const applicationsByJobId = buildCommunityJobApplicationsMap(applications);
  const currentUserApplications = applications
    .filter(application => currentUser && application && application.userId === currentUser.id)
    .slice()
    .sort((a, b) => new Date(b.statusUpdatedAt || b.createdAt || 0).getTime() - new Date(a.statusUpdatedAt || a.createdAt || 0).getTime());
  const currentUserApplicationsByJobId = new Map(currentUserApplications.map((application) => [application.jobId, application]));

  const jobsForView = jobs.map(job => ({
    ...job,
    applicationCount: (applicationsByJobId.get(job.id) || []).length,
    applicationStatus: currentUserApplicationsByJobId.has(job.id)
      ? {
          ...currentUserApplicationsByJobId.get(job.id),
          statusMeta: getCommunityApplicationStatusMeta(currentUserApplicationsByJobId.get(job.id).status),
          progressSteps: buildCommunityApplicationProgress(
            currentUserApplicationsByJobId.get(job.id).status,
            currentUserApplicationsByJobId.get(job.id).statusHistory
          ),
          aiScreeningView: buildCommunityAiScreeningView(currentUserApplicationsByJobId.get(job.id).aiScreening)
        }
      : null,
    hasApplied: currentUserApplicationsByJobId.has(job.id),
    yearsLabel: formatYearsOfExperience(job.experienceYears)
  }));

  res.render('community', {
    user: currentUser,
    posts,
    jobs: jobsForView,
    myApplications: currentUserApplications.map((application) => ({
      ...application,
      statusMeta: getCommunityApplicationStatusMeta(application.status),
      progressSteps: buildCommunityApplicationProgress(application.status, application.statusHistory),
      aiScreeningView: buildCommunityAiScreeningView(application.aiScreening)
    })),
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/community/posts', requireAdmin, communityPostUploadHandler, (req, res) => {
  const content = (req.body.content || '').toString().trim();
  const file = req.file || null;

  if (content.length > COMMUNITY_POST_CONTENT_LIMIT) {
    if (file) safeDeleteFile(file.path);
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      error: `نص البوست طويل جدًا. الحد الأقصى ${COMMUNITY_POST_CONTENT_LIMIT} حرف`
    }));
  }

  if (file && !isValidCommunityMediaFile(file)) {
    safeDeleteFile(file.path);
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      error: 'ارفع صورة أو فيديو فقط في بوست الكوميونتي'
    }));
  }

  if (!content && !file) {
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      error: 'اكتب نصًا أو ارفع صورة أو فيديو قبل النشر'
    }));
  }

  const posts = getCommunityPostsState();
  posts.unshift({
    id: uuidv4(),
    authorId: req.session.user.id,
    authorName: req.session.user.name,
    authorEmail: req.session.user.email,
    authorRole: 'admin',
    authorIsSuperAdmin: Boolean(req.session.user.isSuperAdmin),
    content,
    mediaType: file ? getCommunityMediaType(file) : null,
    mediaPath: file ? `uploads/community-media/${file.filename}` : null,
    mediaOriginalName: file ? file.originalname : null,
    likes: [],
    shares: [],
    comments: [],
    createdAt: new Date().toISOString()
  });
  saveCommunityPostsState(posts);

  res.redirect(buildRedirectUrl({
    pathName: '/community',
    success: 'تم نشر البوست في الكوميونتي بنجاح'
  }));
});

app.post('/community/posts/:id/like', requireCommunityMember, (req, res) => {
  const posts = getCommunityPostsState();
  const postIndex = posts.findIndex(post => post && post.id === req.params.id);
  if (postIndex === -1) {
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: getCommunityPostHash(req.params.id),
      error: 'البوست غير موجود'
    }));
  }

  const likes = Array.isArray(posts[postIndex].likes) ? posts[postIndex].likes : [];
  const existingIndex = likes.findIndex(item => item && item.userId === req.session.user.id);
  if (existingIndex === -1) {
    likes.push({
      userId: req.session.user.id,
      userName: req.session.user.name,
      createdAt: new Date().toISOString()
    });
  } else {
    likes.splice(existingIndex, 1);
  }
  posts[postIndex].likes = likes;
  saveCommunityPostsState(posts);

  res.redirect(buildRedirectUrl({
    pathName: '/community',
    hash: getCommunityPostHash(req.params.id),
    success: existingIndex === -1 ? 'تم تسجيل الإعجاب' : 'تم إلغاء الإعجاب'
  }));
});

app.post('/community/posts/:id/comments', requireCommunityMember, (req, res) => {
  const content = (req.body.content || '').toString().trim();
  if (!content) {
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: getCommunityPostHash(req.params.id),
      error: 'اكتب تعليقًا أولًا'
    }));
  }

  if (content.length > COMMUNITY_COMMENT_CONTENT_LIMIT) {
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: getCommunityPostHash(req.params.id),
      error: `التعليق طويل جدًا. الحد الأقصى ${COMMUNITY_COMMENT_CONTENT_LIMIT} حرف`
    }));
  }

  const posts = getCommunityPostsState();
  const postIndex = posts.findIndex(post => post && post.id === req.params.id);
  if (postIndex === -1) {
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: getCommunityPostHash(req.params.id),
      error: 'البوست غير موجود'
    }));
  }

  posts[postIndex].comments.push({
    id: uuidv4(),
    userId: req.session.user.id,
    userName: req.session.user.name,
    content,
    createdAt: new Date().toISOString()
  });
  saveCommunityPostsState(posts);

  res.redirect(buildRedirectUrl({
    pathName: '/community',
    hash: getCommunityPostHash(req.params.id),
    success: 'تم إضافة التعليق'
  }));
});

app.post('/community/posts/:id/share', requireCommunityMember, (req, res) => {
  const posts = getCommunityPostsState();
  const postIndex = posts.findIndex(post => post && post.id === req.params.id);
  if (postIndex === -1) {
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: getCommunityPostHash(req.params.id),
      error: 'البوست غير موجود'
    }));
  }

  const shares = Array.isArray(posts[postIndex].shares) ? posts[postIndex].shares : [];
  const existingShare = shares.find(item => item && item.userId === req.session.user.id);
  if (!existingShare) {
    shares.push({
      userId: req.session.user.id,
      userName: req.session.user.name,
      createdAt: new Date().toISOString()
    });
    posts[postIndex].shares = shares;
    saveCommunityPostsState(posts);
  }

  res.redirect(buildRedirectUrl({
    pathName: '/community',
    hash: getCommunityPostHash(req.params.id),
    success: 'تم تسجيل المشاركة. يمكنك الآن إرسال الرابط لصاحبك'
  }));
});

app.post('/community/jobs/:id/apply', requireCommunityMember, communityCvUploadHandler, async (req, res) => {
  const file = req.file || null;

  try {
    const jobs = getCommunityJobsState();
    const job = jobs.find(item => item && item.id === req.params.id);

    if (!job || job.isActive === false) {
      if (file) safeDeleteFile(file.path);
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: 'الوظيفة غير متاحة حاليًا'
      }));
    }

    if (!file) {
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: 'ارفع السيرة الذاتية أولًا'
      }));
    }

    if (!isValidCommunityCvFile(file)) {
      safeDeleteFile(file.path);
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: 'صيغة السيرة الذاتية يجب أن تكون PDF أو DOCX'
      }));
    }

    const name = (req.body.name || '').toString().trim();
    const about = (req.body.about || '').toString().trim();
    const age = Number(req.body.age || 0);

    if (!name || !Number.isFinite(age) || age <= 0 || !about) {
      safeDeleteFile(file.path);
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: 'أكمل الاسم والعمر ونبذة عنك قبل التقديم'
      }));
    }

    if (about.length > COMMUNITY_ABOUT_LIMIT) {
      safeDeleteFile(file.path);
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: `النبذة طويلة جدًا. الحد الأقصى ${COMMUNITY_ABOUT_LIMIT} حرف`
      }));
    }

    const applications = getCommunityJobApplicationsState();
    const alreadyApplied = applications.find(application =>
      application &&
      application.jobId === job.id &&
      application.userId === req.session.user.id
    );

    if (alreadyApplied) {
      safeDeleteFile(file.path);
      return res.redirect(buildRedirectUrl({
        pathName: '/community',
        hash: '#jobs',
        error: 'لقد قدمت على هذه الوظيفة بالفعل'
      }));
    }

    const applicationId = uuidv4();
    const relativeCvPath = path.relative(__dirname, file.path).split(path.sep).join('/');
    const cvBuffer = fs.readFileSync(file.path);
    let cvFilePath = relativeCvPath;
    let cvStorageKey = null;

    if (usePostgresStorage()) {
      cvStorageKey = `community-cv:${applicationId}`;
      await saveBinaryAsset({
        key: cvStorageKey,
        buffer: cvBuffer,
        mimeType: file.mimetype,
        fileName: file.originalname
      });
      safeDeleteFile(file.path);
      cvFilePath = null;
    }

    const now = new Date().toISOString();
    const application = {
      id: applicationId,
      jobId: job.id,
      jobTitle: job.title,
      userId: req.session.user.id,
      userEmail: req.session.user.email,
      applicantName: name,
      applicantAge: age,
      about,
      cvFilePath,
      cvStorageKey,
      cvOriginalName: file.originalname,
      cvMimeType: file.mimetype,
      status: 'pending',
      trackingNote: 'تم استلام طلبك وبانتظار مراجعة فريق Codentra.',
      statusUpdatedAt: now,
      statusHistory: [
        buildCommunityApplicationHistoryEntry({
          status: 'pending',
          note: 'تم استلام طلب التقديم.',
          updatedAt: now,
          updatedById: req.session.user.id,
          updatedByName: req.session.user.name,
          actorType: 'applicant'
        })
      ],
      aiScreening: normalizeCommunityAiScreening({
        decision: 'pending',
        summary: 'جارٍ تحليل السيرة الذاتية وتقييمها...',
        reasons: [],
        evaluatedAt: now,
        model: GEMINI_MODEL || 'local-fallback'
      }),
      createdAt: now
    };
    try {
      const aiScreening = await screenCommunityCvWithGemini({
        job,
        applicant: { name, age, about },
        cvBuffer,
        mimeType: file.mimetype,
        fileName: file.originalname
      });

      application.aiScreening = aiScreening;
      application.statusUpdatedAt = aiScreening.evaluatedAt || new Date().toISOString();

      if (aiScreening.decision === 'accepted') {
        application.status = 'accepted';
        application.trackingNote = aiScreening.summary || 'تم قبولك مبدئيًا بعد التحليل الآلي، وبانتظار استكمال الإدارة للخطوة التالية.';
        application.statusHistory.push(buildCommunityApplicationHistoryEntry({
          status: 'accepted',
          note: `تم قبول الطلب مبدئيًا عبر التحليل الآلي.${aiScreening.summary ? ` ${aiScreening.summary}` : ''}`.trim(),
          updatedAt: application.statusUpdatedAt,
          updatedByName: 'Codentra AI',
          actorType: 'system'
        }));
      } else if (aiScreening.decision === 'rejected') {
        application.status = 'rejected';
        application.trackingNote = aiScreening.summary || 'تم رفض الطلب مبدئيًا لعدم ارتباط السيرة الذاتية بالوظيفة بشكل كافٍ.';
        application.rejectedAt = application.statusUpdatedAt;
        application.statusHistory.push(buildCommunityApplicationHistoryEntry({
          status: 'rejected',
          note: `تم رفض الطلب مبدئيًا عبر التحليل الآلي.${aiScreening.summary ? ` ${aiScreening.summary}` : ''}`.trim(),
          updatedAt: application.statusUpdatedAt,
          updatedByName: 'Codentra AI',
          actorType: 'system'
        }));
      }
    } catch (aiError) {
      console.error('Community AI screening error:', aiError);
      const aiErrorMessage = String(aiError && aiError.message ? aiError.message : aiError);
      const unsupportedFormat = aiErrorMessage.includes('unsupported_cv_ai_format');
      const emptyDocxText = aiErrorMessage.includes('empty_docx_text');
      const emptyPdfText = aiErrorMessage.includes('empty_pdf_text');
      const fallbackSummary = unsupportedFormat
        ? 'التقييم الآلي يدعم حاليًا ملفات PDF و DOCX فقط.'
        : emptyDocxText
          ? 'تعذر قراءة محتوى ملف DOCX، لذلك لم يكتمل التقييم الآلي.'
          : emptyPdfText
            ? 'تعذر استخراج نص واضح من ملف PDF، لذلك لم يكتمل التقييم الآلي.'
            : 'تعذر تنفيذ التقييم الآلي لهذه السيرة الذاتية.';
      application.aiScreening = normalizeCommunityAiScreening({
        decision: 'error',
        summary: fallbackSummary,
        reasons: [],
        evaluatedAt: new Date().toISOString(),
        model: GEMINI_MODEL || 'local-fallback',
        error: aiErrorMessage
      });
      application.trackingNote = fallbackSummary;
    }

    applications.unshift(application);
    saveCommunityJobApplicationsState(applications);

    let submitMessage = 'تم إرسال طلب التقديم بنجاح';
    if (application.aiScreening.decision === 'accepted') {
      submitMessage = 'تم إرسال طلبك وحصل على قبول مبدئي من التحليل الآلي';
    } else if (application.aiScreening.decision === 'rejected') {
      submitMessage = 'تم إرسال طلبك لكن التحليل الآلي أعطى رفضًا مبدئيًا، ويمكن للإدارة مراجعته';
    } else if (application.aiScreening.decision === 'error') {
      submitMessage = 'تم إرسال طلبك لكن لم يكتمل التحليل الآلي لهذه السيرة الذاتية';
    }

    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: '#jobs',
      success: submitMessage
    }));
  } catch (error) {
    if (file) safeDeleteFile(file.path);
    console.error('Community job application error:', error);
    return res.redirect(buildRedirectUrl({
      pathName: '/community',
      hash: '#jobs',
      error: 'تعذر إرسال طلب التقديم الآن. حاول مرة أخرى'
    }));
  }
});

// Auth routes
app.get('/login', (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('login', {
    error: req.query.error || null,
    user: null,
    googleAuthEnabled: isGoogleAuthConfigured(),
    githubAuthEnabled: isGitHubAuthConfigured()
  });
});

app.get('/blocked', (req, res) => {
  try {
    if (!req.session || !req.session.user || !req.session.user.id) {
      return res.redirect('/login');
    }
    const users = db.users();
    const u = users.find(x => x && x.id === req.session.user.id);
    if (!u) return res.redirect('/login');

    if (isBlockedExpired(u)) {
      const idx = users.findIndex(x => x && x.id === u.id);
      unblockUserInPlace(u);
      if (idx !== -1) users[idx] = u;
      db.saveUsers(users);
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

app.post('/login', (req, res) => {
  const { email, password } = req.body;
  const users = db.users();
  const user = users.find(u => u.email === email);
  
  if (!user || !user.password || !bcrypt.compareSync(password, user.password)) {
    return res.render('login', {
      error: user && !user.password ? 'هذا الحساب مرتبط بتسجيل اجتماعي. استخدم جوجل أو GitHub.' : 'Invalid email or password',
      user: null,
      googleAuthEnabled: isGoogleAuthConfigured(),
      githubAuthEnabled: isGitHubAuthConfigured()
    });
  }

  const currentIndex = users.findIndex(u => u && u.id === user.id);
  let shouldSaveUsers = false;

  if (ensureUserPaymentProfile({ user, users })) {
    shouldSaveUsers = true;
  }

  if (isBlockedExpired(user)) {
    unblockUserInPlace(user);
    shouldSaveUsers = true;
  }

  if (user.isBlocked) {
    if (shouldSaveUsers && currentIndex !== -1) {
      users[currentIndex] = user;
      db.saveUsers(users);
    }
    req.session.user = buildSessionUser(user);
    setAuthCookie(res, user.id);
    return res.redirect('/blocked');
  }

  if (user.role === 'admin') {
    if (user.isSuperAdmin == null) {
      user.isSuperAdmin = false;
      shouldSaveUsers = true;
    }
    if (user.email === 'admin@codentra.com' && user.isSuperAdmin !== true) {
      user.isSuperAdmin = true;
      shouldSaveUsers = true;
    }
  }

  if (shouldSaveUsers && currentIndex !== -1) {
    users[currentIndex] = user;
    db.saveUsers(users);
  }

  req.session.user = buildSessionUser(user);
  setAuthCookie(res, user.id);
  syncRecentlyViewedProjectsForUser({ req, userId: user.id });
  res.redirect(user.role === 'admin' ? '/admin' : '/');
});

app.get('/register', (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('register', {
    error: req.query.error || null,
    user: null,
    referralPrefill: req.query.ref || '',
    googleAuthEnabled: isGoogleAuthConfigured(),
    githubAuthEnabled: isGitHubAuthConfigured()
  });
});

app.post('/register', (req, res) => {
  const { name, email, password, walletPassword } = req.body;
  const referralInput = normalizeReferralCode(req.body.referralCode);
  const users = db.users();

  if (!walletPassword || String(walletPassword).trim().length < 4) {
    return res.render('register', { error: 'كلمة مرور البطاقة يجب ألا تقل عن 4 أحرف أو أرقام', user: null, referralPrefill: referralInput, googleAuthEnabled: isGoogleAuthConfigured(), githubAuthEnabled: isGitHubAuthConfigured() });
  }
  
  if (users.find(u => u.email === email)) {
    return res.render('register', { error: 'Email already registered', user: null, referralPrefill: referralInput, googleAuthEnabled: isGoogleAuthConfigured(), githubAuthEnabled: isGitHubAuthConfigured() });
  }

  const referralCheck = validateReferralCodeForUser({ users, code: referralInput, targetUserId: null });
  if (!referralCheck.valid) {
    return res.render('register', { error: referralCheck.reason, user: null, referralPrefill: referralInput, googleAuthEnabled: isGoogleAuthConfigured(), githubAuthEnabled: isGitHubAuthConfigured() });
  }
  
  const newUser = {
    id: uuidv4(),
    name,
    email,
    password: bcrypt.hashSync(password, 10),
    role: 'user',
    referralCode: ensureUniqueReferralCode(users),
    walletBalance: 0,
    walletCardNumber: createWalletCardNumber(users),
    walletPaymentPasswordHash: bcrypt.hashSync(String(walletPassword), 10),
    referredBy: referralCheck.referrerUserId
      ? {
          referrerUserId: referralCheck.referrerUserId,
          code: referralCheck.normalized,
          createdAt: new Date().toISOString(),
          rewardedAt: null
        }
      : null,
    loyaltyPoints: 0,
    createdAt: new Date().toISOString()
  };

  registerPendingReferralSignupReward({ users, newUser });
  
  users.push(newUser);
  db.saveUsers(users);

  req.session.user = buildSessionUser(newUser);
  setAuthCookie(res, newUser.id);
  syncRecentlyViewedProjectsForUser({ req, userId: newUser.id });
  res.redirect('/');
});

app.get('/auth/google', (req, res) => {
  if (req.session.user) return res.redirect('/');
  if (!isGoogleAuthConfigured()) {
    return res.redirect('/login?error=' + encodeURIComponent('تسجيل الدخول عبر جوجل غير مفعّل بعد'));
  }

  const mode = String(req.query.mode || 'login').trim() === 'register' ? 'register' : 'login';
  const referralCode = mode === 'register' ? normalizeReferralCode(req.query.ref || req.query.referralCode || '') : '';
  return res.redirect(buildGoogleOAuthUrl({ mode, referralCode }));
});

app.get('/auth/github', (req, res) => {
  if (req.session.user) return res.redirect('/');
  if (!isGitHubAuthConfigured()) {
    return res.redirect('/login?error=' + encodeURIComponent('تسجيل الدخول عبر GitHub غير مفعّل بعد'));
  }

  const mode = String(req.query.mode || 'login').trim() === 'register' ? 'register' : 'login';
  const referralCode = mode === 'register' ? normalizeReferralCode(req.query.ref || req.query.referralCode || '') : '';
  return res.redirect(buildGitHubOAuthUrl({ mode, referralCode }));
});

app.get('/auth/google/callback', async (req, res) => {
  if (!isGoogleAuthConfigured()) {
    return res.redirect('/login?error=' + encodeURIComponent('تسجيل الدخول عبر جوجل غير مفعّل بعد'));
  }

  if (req.query.error) {
    return res.redirect('/login?error=' + encodeURIComponent('تم إلغاء تسجيل الدخول عبر جوجل'));
  }

  const code = String(req.query.code || '').trim();
  if (!code) {
    return res.redirect('/login?error=' + encodeURIComponent('تعذر إكمال تسجيل الدخول عبر جوجل'));
  }

  let parsedState = {};
  if (req.query.state) {
    try {
      parsedState = JSON.parse(String(req.query.state));
    } catch (error) {
      parsedState = {};
    }
  }

  try {
    const tokenResponse = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        redirect_uri: GOOGLE_CALLBACK_URL,
        grant_type: 'authorization_code'
      }).toString()
    });

    const tokenPayload = await tokenResponse.json();
    if (!tokenResponse.ok || !tokenPayload.access_token) {
      return res.redirect('/login?error=' + encodeURIComponent('تعذر التحقق من حساب جوجل'));
    }

    const profileResponse = await fetch('https://openidconnect.googleapis.com/v1/userinfo', {
      headers: { Authorization: `Bearer ${tokenPayload.access_token}` }
    });
    const profile = await profileResponse.json();
    if (!profileResponse.ok || !profile || !profile.sub || !profile.email) {
      return res.redirect('/login?error=' + encodeURIComponent('تعذر قراءة بيانات حساب جوجل'));
    }

    if (profile.email_verified === false) {
      return res.redirect('/login?error=' + encodeURIComponent('حساب جوجل يجب أن يكون ببريد موثّق'));
    }

    const mode = String(parsedState.mode || 'login');
    const referralCode = String(parsedState.referralCode || '');

    const users = db.users();
    let user = users.find((item) => item && item.googleId && item.googleId === profile.sub) || null;
    let didMutateUsers = false;

    if (!user) {
      const existingByEmail = users.find((item) => item && item.email && item.email.toLowerCase() === String(profile.email).trim().toLowerCase()) || null;
      if (existingByEmail) {
        existingByEmail.googleId = profile.sub;
        existingByEmail.authProvider = existingByEmail.authProvider || 'google';
        if (!existingByEmail.name && profile.name) {
          existingByEmail.name = String(profile.name).trim();
        }
        user = existingByEmail;
        didMutateUsers = true;
      } else {
        const referralCheck = validateReferralCodeForUser({ users, code: referralCode, targetUserId: null });
        if (!referralCheck.valid) {
          return res.redirect('/register?error=' + encodeURIComponent(referralCheck.reason || 'كود الإحالة غير صالح'));
        }
        user = createGoogleUserRecord({ users, profile, referralCheck });
        registerPendingReferralSignupReward({ users, newUser: user });
        users.push(user);
        didMutateUsers = true;
      }
    }

    const currentIndex = users.findIndex((item) => item && item.id === user.id);
    let shouldSaveUsers = false;

    if (ensureUserPaymentProfile({ user, users })) shouldSaveUsers = true;
    if (isBlockedExpired(user)) {
      unblockUserInPlace(user);
      shouldSaveUsers = true;
    }

    if (currentIndex !== -1) {
      users[currentIndex] = user;
      if (shouldSaveUsers || didMutateUsers) {
        db.saveUsers(users);
      }
    } else if (didMutateUsers || shouldSaveUsers) {
      db.saveUsers(users);
    }

    req.session.user = buildSessionUser(user);
    setAuthCookie(res, user.id);
    syncRecentlyViewedProjectsForUser({ req, userId: user.id });

    if (user.isBlocked) {
      return res.redirect('/blocked');
    }

    return res.redirect(user.role === 'admin' ? '/admin' : '/');
  } catch (error) {
    return res.redirect('/login?error=' + encodeURIComponent('حدث خطأ أثناء تسجيل الدخول عبر جوجل'));
  }
});

app.get('/auth/github/callback', async (req, res) => {
  if (!isGitHubAuthConfigured()) {
    return res.redirect('/login?error=' + encodeURIComponent('تسجيل الدخول عبر GitHub غير مفعّل بعد'));
  }

  if (req.query.error) {
    return res.redirect('/login?error=' + encodeURIComponent('تم إلغاء تسجيل الدخول عبر GitHub'));
  }

  const code = String(req.query.code || '').trim();
  if (!code) {
    return res.redirect('/login?error=' + encodeURIComponent('تعذر إكمال تسجيل الدخول عبر GitHub'));
  }

  let parsedState = {};
  if (req.query.state) {
    try {
      parsedState = JSON.parse(String(req.query.state));
    } catch (error) {
      parsedState = {};
    }
  }

  try {
    const tokenResponse = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({
        client_id: GITHUB_CLIENT_ID,
        client_secret: GITHUB_CLIENT_SECRET,
        code,
        redirect_uri: GITHUB_CALLBACK_URL
      })
    });

    const tokenPayload = await tokenResponse.json();
    if (!tokenResponse.ok || !tokenPayload.access_token) {
      return res.redirect('/login?error=' + encodeURIComponent('تعذر التحقق من حساب GitHub'));
    }

    const profileResponse = await fetch('https://api.github.com/user', {
      headers: {
        'Authorization': `Bearer ${tokenPayload.access_token}`,
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'Codentra'
      }
    });
    const profile = await profileResponse.json();
    if (!profileResponse.ok || !profile || !profile.id) {
      return res.redirect('/login?error=' + encodeURIComponent('تعذر قراءة بيانات حساب GitHub'));
    }

    let email = profile.email || null;
    if (!email) {
      const emailsResponse = await fetch('https://api.github.com/user/emails', {
        headers: {
          'Authorization': `Bearer ${tokenPayload.access_token}`,
          'Accept': 'application/vnd.github+json',
          'User-Agent': 'Codentra'
        }
      });
      const emails = await emailsResponse.json();
      if (emailsResponse.ok && Array.isArray(emails)) {
        const primary = emails.find((item) => item && item.primary && item.verified) || emails.find((item) => item && item.verified) || emails[0];
        email = primary && primary.email ? primary.email : null;
      }
    }

    if (!email) {
      return res.redirect('/login?error=' + encodeURIComponent('حساب GitHub يجب أن يحتوي على بريد إلكتروني متاح'));
    }

    const referralCode = String(parsedState.referralCode || '');
    const users = db.users();
    let user = users.find((item) => item && item.githubId && String(item.githubId) === String(profile.id)) || null;
    let didMutateUsers = false;

    if (!user) {
      const existingByEmail = users.find((item) => item && item.email && item.email.toLowerCase() === String(email).trim().toLowerCase()) || null;
      if (existingByEmail) {
        existingByEmail.githubId = String(profile.id);
        existingByEmail.githubLogin = String(profile.login || '').trim() || existingByEmail.githubLogin || null;
        existingByEmail.authProvider = existingByEmail.authProvider || 'github';
        if (!existingByEmail.name && profile.name) {
          existingByEmail.name = String(profile.name).trim();
        }
        user = existingByEmail;
        didMutateUsers = true;
      } else {
        const referralCheck = validateReferralCodeForUser({ users, code: referralCode, targetUserId: null });
        if (!referralCheck.valid) {
          return res.redirect('/register?error=' + encodeURIComponent(referralCheck.reason || 'كود الإحالة غير صالح'));
        }
        user = createGitHubUserRecord({
          users,
          profile: { ...profile, email },
          referralCheck
        });
        registerPendingReferralSignupReward({ users, newUser: user });
        users.push(user);
        didMutateUsers = true;
      }
    }

    const currentIndex = users.findIndex((item) => item && item.id === user.id);
    let shouldSaveUsers = false;
    if (ensureUserPaymentProfile({ user, users })) shouldSaveUsers = true;
    if (isBlockedExpired(user)) {
      unblockUserInPlace(user);
      shouldSaveUsers = true;
    }

    if (currentIndex !== -1) {
      users[currentIndex] = user;
      if (didMutateUsers || shouldSaveUsers) db.saveUsers(users);
    } else if (didMutateUsers || shouldSaveUsers) {
      db.saveUsers(users);
    }

    req.session.user = buildSessionUser(user);
    setAuthCookie(res, user.id);
    syncRecentlyViewedProjectsForUser({ req, userId: user.id });

    if (user.isBlocked) {
      return res.redirect('/blocked');
    }

    return res.redirect(user.role === 'admin' ? '/admin' : '/');
  } catch (error) {
    return res.redirect('/login?error=' + encodeURIComponent('حدث خطأ أثناء تسجيل الدخول عبر GitHub'));
  }
});

app.get('/logout', (req, res) => {
  clearAuthCookie(res);
  if (!req.session) return res.redirect('/');
  req.session.destroy(() => {
    res.redirect('/');
  });
});

// API Routes for Mobile App
app.post('/api/auth/login', (req, res) => {
  const { email, password } = req.body;
  const users = db.users();
  const user = users.find(u => u.email === email);
  
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }

  const token = generateToken(user);
  res.json({ token, user: { id: user.id, name: user.name, email: user.email, role: user.role } });
});

app.post('/api/auth/register', (req, res) => {
  const name = String(req.body.name || '').trim();
  const email = String(req.body.email || '').trim().toLowerCase();
  const password = String(req.body.password || '');
  const walletPassword = String(req.body.walletPassword || '');
  const referralInput = normalizeReferralCode(req.body.referralCode);
  const users = db.users();

  if (!name || !email || !password) {
    return res.status(400).json({ error: 'Missing required fields' });
  }
  if (!walletPassword || walletPassword.trim().length < 4) {
    return res.status(400).json({ error: 'كلمة مرور البطاقة يجب ألا تقل عن 4 أحرف أو أرقام' });
  }
  if (users.find(u => u.email === email)) {
    return res.status(400).json({ error: 'Email already registered' });
  }

  const referralCheck = validateReferralCodeForUser({ users, code: referralInput, targetUserId: null });
  if (!referralCheck.valid) {
    return res.status(400).json({ error: referralCheck.reason });
  }

  const newUser = {
    id: uuidv4(),
    name,
    email,
    password: bcrypt.hashSync(password, 10),
    role: 'user',
    referralCode: ensureUniqueReferralCode(users),
    walletBalance: 0,
    walletCardNumber: createWalletCardNumber(users),
    walletPaymentPasswordHash: bcrypt.hashSync(walletPassword, 10),
    referredBy: referralCheck.referrerUserId
      ? {
          referrerUserId: referralCheck.referrerUserId,
          code: referralCheck.normalized,
          createdAt: new Date().toISOString(),
          rewardedAt: null
        }
      : null,
    loyaltyPoints: 0,
    createdAt: new Date().toISOString()
  };

  if (newUser.referredBy) {
    const referrals = db.referrals();
    referrals.push({
      id: uuidv4(),
      code: newUser.referredBy.code,
      referrerUserId: newUser.referredBy.referrerUserId,
      referredUserId: newUser.id,
      status: 'pending',
      rewardAmount: 100,
      rewardType: 'loyalty-points',
      createdAt: new Date().toISOString(),
      rewardedAt: null,
      rewardPurchaseId: null
    });
    db.saveReferrals(referrals);

    const referrerIndex = users.findIndex((item) => item && item.id === newUser.referredBy.referrerUserId && item.role === 'user');
    if (referrerIndex !== -1) {
      users[referrerIndex].loyaltyPoints = normalizeLoyaltyPoints(Number(users[referrerIndex].loyaltyPoints || 0) + 50);
      addUserNotificationEntry({
        targetUser: users[referrerIndex],
        type: 'referral-signup',
        title: 'مكافأة إحالة جديدة',
        message: 'سجل مستخدم جديد باستخدام كودك، وتمت إضافة 50 نقطة إلى حسابك.',
        metadata: {
          referredUserId: newUser.id,
          rewardAmount: 50,
          rewardType: 'loyalty-points'
        }
      });
    }
  }

  users.push(newUser);
  db.saveUsers(users);

  const token = generateToken(newUser);
  res.status(201).json({
    token,
    user: {
      id: newUser.id,
      name: newUser.name,
      email: newUser.email,
      role: newUser.role,
      walletBalance: 0,
      loyaltyPoints: 0,
      referralCode: newUser.referralCode
    }
  });
});

app.get('/api/me', requireApiUserAuth, (req, res) => {
  const users = db.users();
  const user = users.find(u => u.id === req.apiUser.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  res.json({
    user: {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role,
      walletBalance: Number(user.walletBalance || 0),
      loyaltyPoints: normalizeLoyaltyPoints(user.loyaltyPoints),
      referralCode: user.referralCode || null,
      unreadNotificationsCount: getUnreadNotificationCount(user),
      walletCardNumberMasked: maskWalletCardNumber(user.walletCardNumber),
      walletCardSpendingLimit: sanitizeWalletCardSpendingLimit(user.walletCardSpendingLimit),
      subscription: buildSessionUser(user).subscription
    }
  });
});

app.get('/api/projects', requireApiUserAuth, (req, res) => {
  const projects = db.projects().filter(p => isProjectVisibleToUser({ project: p, sessionUser: { role: 'user', id: req.apiUser.id } })).map(decorateProjectPricing);
  res.json({ projects });
});

app.get('/api/projects/:id', requireApiUserAuth, (req, res) => {
  const projects = db.projects();
  const project = decorateProjectPricing(projects.find(p => p.id === req.params.id));
  if (!project) return res.status(404).json({ error: 'Project not found' });
  if (!isProjectVisibleToUser({ project, sessionUser: { role: 'user', id: req.apiUser.id } })) {
    return res.status(403).json({ error: 'Not allowed' });
  }
  res.json({ project });
});

app.get('/api/cart', requireApiUserAuth, (req, res) => {
  const { cart } = getOrCreateCartForUser({ userId: req.apiUser.id });
  res.json({ cart });
});

app.get('/api/cart/summary', requireApiUserAuth, (req, res) => {
  const { cart } = getOrCreateCartForUser({ userId: req.apiUser.id });
  const couponCode = normalizeCouponCode(req.query.couponCode);
  const summary = summarizeCart({ cart, couponCode, sessionUser: { role: 'user', id: req.apiUser.id } });
  res.json({ summary });
});

app.post('/api/cart/add', requireApiUserAuth, (req, res) => {
  const { projectId } = req.body;
  if (!projectId) return res.status(400).json({ error: 'Missing projectId' });
  
  const projects = db.projects();
  const project = projects.find(p => p && p.id === projectId);
  if (!project) return res.status(404).json({ error: 'Project not found' });
  if (!isProjectVisibleToUser({ project, sessionUser: { role: 'user', id: req.apiUser.id } })) {
    return res.status(403).json({ error: 'Not allowed' });
  }

  const purchases = db.purchases();
  const alreadyPurchased = purchases.some(p => p.userId === req.apiUser.id && p.projectId === projectId);
  if (alreadyPurchased) return res.status(400).json({ error: 'Already purchased' });

  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.apiUser.id });
  const items = Array.isArray(cart.items) ? cart.items : [];
  const exists = items.some(it => it && it.projectId === projectId);
  if (!exists) {
    items.push({
      projectId: project.id,
      projectTitle: project.title,
      price: Number(getProjectSaleState(project).finalPrice || 0),
      createdAt: new Date().toISOString()
    });
  }
  carts[cartIndex] = { ...cart, items, updatedAt: new Date().toISOString() };
  db.saveCarts(carts);
  res.json({ cart: carts[cartIndex] });
});

app.post('/api/cart/remove', requireApiUserAuth, (req, res) => {
  const { projectId } = req.body;
  if (!projectId) return res.status(400).json({ error: 'Missing projectId' });
  
  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.apiUser.id });
  const nextItems = (Array.isArray(cart.items) ? cart.items : []).filter(it => it && it.projectId !== projectId);
  carts[cartIndex] = { ...cart, items: nextItems, updatedAt: new Date().toISOString() };
  db.saveCarts(carts);
  res.json({ cart: carts[cartIndex] });
});

app.post('/api/cart/checkout', requireApiUserAuth, (req, res) => {
  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.apiUser.id });
  const items = (cart && Array.isArray(cart.items)) ? cart.items : [];
  if (items.length === 0) return res.status(400).json({ error: 'Cart empty' });

  const users = db.users();
  const currentUserIndex = users.findIndex(u => u.id === req.apiUser.id);
  const currentUser = currentUserIndex !== -1 ? users[currentUserIndex] : null;
  if (!currentUser) return res.status(404).json({ error: 'User not found' });

  const projects = db.projects();
  const purchases = db.purchases();

  const couponCode = normalizeCouponCode(req.body.couponCode);
  const summary = summarizeCart({ cart, couponCode, sessionUser: { role: 'user', id: req.apiUser.id } });
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

  const earnedPoints = getLoyaltyEarnedPointsForPurchase({ amountEGP: totalAfter });
  if (earnedPoints > 0) {
    currentUser.loyaltyPoints = normalizeLoyaltyPoints(currentUser.loyaltyPoints) + earnedPoints;
  }

  // increment coupon usage (if valid)
  if (summary.appliedCoupon) {
    const coupons = db.coupons();
    const idx = coupons.findIndex(c => c && normalizeCouponCode(c.code) === normalizeCouponCode(summary.appliedCoupon.code));
    if (idx !== -1) {
      coupons[idx].usedCount = Number(coupons[idx].usedCount || 0) + 1;
      db.saveCoupons(coupons);
    }
  }

  users[currentUserIndex] = currentUser;
  db.saveUsers(users);

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
      fingerprintCode: uuidv4(),
      downloadLocked: false,
      downloadLockReason: null,
      downloadLockedAt: null,
      userId: req.apiUser.id,
      projectId: project.id,
      projectTitle: project.title,
      price: Math.max(0, Math.round((Number(project.price || 0) - perItemDiscount) * 100) / 100),
      priceBefore: Number(project.price || 0),
    projectSaleDiscountAmount: Number(projectSale.discountAmount || 0),
    projectSaleOccasion: projectSale.occasion || null,
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

  db.savePurchases(purchases);
  carts[cartIndex] = { ...cart, items: [], updatedAt: new Date().toISOString() };
  db.saveCarts(carts);
  res.json({ orderId, message: 'Order created' });
});

app.get('/api/purchases', requireApiUserAuth, (req, res) => {
  const purchases = db.purchases().filter(p => p.userId === req.apiUser.id);
  const invoices = db.invoices();
  res.json({ purchases, invoices });
});

app.get('/api/notifications', requireApiUserAuth, (req, res) => {
  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

  const currentUser = users[userIndex];
  if (ensureUserPaymentProfile({ user: currentUser, users })) {
    db.saveUsers(users);
  }

  if (String(req.query.markRead || '').trim() === '1') {
    if (markAllNotificationsAsRead(currentUser)) {
      db.saveUsers(users);
    }
  }

  return res.json({
    notifications: getNotificationCenterItems(currentUser).map(serializeNotificationItem).filter(Boolean),
    unreadCount: getUnreadNotificationCount(currentUser)
  });
});

app.get('/api/loyalty', requireApiUserAuth, (req, res) => {
  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (!currentUser) return res.status(404).json({ error: 'User not found' });

  const redeem = getLoyaltyRedeemSettings();
  return res.json({
    loyaltyPoints: normalizeLoyaltyPoints(currentUser.loyaltyPoints),
    walletBalance: Number(currentUser.walletBalance || 0),
    minPoints: normalizeLoyaltyPoints(redeem.minPoints),
    egpPerPoint: Number(redeem.egpPerPoint || 0)
  });
});

app.get('/api/live/summary', requireApiUserAuth, (req, res) => {
  return res.json({
    summary: getUserRealtimeSummary({ userId: req.apiUser.id }),
    serverTime: Date.now()
  });
});

app.post('/api/device/register', requireApiUserAuth, (req, res) => {
  const pushToken = String(req.body.pushToken || '').trim();
  const platform = String(req.body.platform || 'ios').trim() || 'ios';
  if (!pushToken) return res.status(400).json({ error: 'pushToken is required' });

  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

  const devices = Array.isArray(users[userIndex].pushDevices) ? users[userIndex].pushDevices : [];
  const existingIndex = devices.findIndex((item) => item && item.pushToken === pushToken);
  const payload = {
    pushToken,
    platform,
    updatedAt: new Date().toISOString()
  };

  if (existingIndex === -1) {
    devices.push(payload);
  } else {
    devices[existingIndex] = { ...devices[existingIndex], ...payload };
  }

  users[userIndex].pushDevices = devices;
  db.saveUsers(users);
  return res.json({ success: true });
});

app.post('/api/device/unregister', requireApiUserAuth, (req, res) => {
  const pushToken = String(req.body.pushToken || '').trim();
  if (!pushToken) return res.status(400).json({ error: 'pushToken is required' });

  const users = db.users();
  const userIndex = users.findIndex((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

  const devices = Array.isArray(users[userIndex].pushDevices) ? users[userIndex].pushDevices : [];
  users[userIndex].pushDevices = devices.filter((item) => item && item.pushToken !== pushToken);
  db.saveUsers(users);
  return res.json({ success: true });
});

app.get('/api/messages', requireApiUserAuth, (req, res) => {
  const messages = db.messages()
    .filter((message) => message && (
      (message.senderId === req.apiUser.id && message.receiverId === 'admin') ||
      (message.senderId === 'admin' && message.receiverId === req.apiUser.id)
    ))
    .sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0))
    .map((message) => ({
      id: message.id,
      content: message.content || '',
      senderId: message.senderId || null,
      senderName: message.senderName || null,
      receiverId: message.receiverId || null,
      isMine: message.senderId === req.apiUser.id,
      purchaseId: message.purchaseId || null,
      customProjectRequestId: message.customProjectRequestId || null,
      projectTitle: message.projectTitle || null,
      createdAt: message.createdAt || null,
      read: Boolean(message.read)
    }));

  return res.json({ messages });
});

app.post('/api/messages', requireApiUserAuth, (req, res) => {
  const content = String(req.body.content || '').trim();
  if (!content) return res.status(400).json({ error: 'محتوى الرسالة مطلوب' });

  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (!currentUser) return res.status(404).json({ error: 'User not found' });

  const message = {
    id: uuidv4(),
    senderId: currentUser.id,
    senderName: currentUser.name,
    receiverId: 'admin',
    content,
    read: false,
    createdAt: new Date().toISOString()
  };

  const messages = db.messages();
  messages.push(message);
  db.saveMessages(messages);

  return res.status(201).json({
    message: {
      id: message.id,
      content: message.content,
      senderId: message.senderId,
      senderName: message.senderName,
      receiverId: message.receiverId,
      isMine: true,
      createdAt: message.createdAt,
      read: false
    }
  });
});

app.get('/api/purchases/:id/messages', requireApiUserAuth, (req, res) => {
  const purchaseChat = getPurchaseChatContextForUser(req.params.id, req.apiUser.id);
  if (!purchaseChat) return res.status(404).json({ error: 'Purchase not found' });

  const messages = db.messages()
    .filter((message) => (
      message &&
      message.purchaseId === purchaseChat.purchaseId && (
        (message.senderId === req.apiUser.id && message.receiverId === 'admin') ||
        (message.senderId === 'admin' && message.receiverId === req.apiUser.id)
      )
    ))
    .sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0))
    .map((message) => ({
      id: message.id,
      content: message.content || '',
      senderId: message.senderId || null,
      senderName: message.senderName || null,
      receiverId: message.receiverId || null,
      isMine: message.senderId === req.apiUser.id,
      purchaseId: message.purchaseId || null,
      projectTitle: message.projectTitle || purchaseChat.projectTitle,
      createdAt: message.createdAt || null,
      read: Boolean(message.read)
    }));

  const allMessages = db.messages();
  let changed = false;
  allMessages.forEach((message) => {
    if (message && message.purchaseId === purchaseChat.purchaseId && message.senderId === 'admin' && message.receiverId === req.apiUser.id && !message.read) {
      message.read = true;
      changed = true;
    }
  });
  if (changed) db.saveMessages(allMessages);

  return res.json({ purchase: purchaseChat, messages });
});

app.post('/api/purchases/:id/messages', requireApiUserAuth, (req, res) => {
  const purchaseChat = getPurchaseChatContextForUser(req.params.id, req.apiUser.id);
  if (!purchaseChat) return res.status(404).json({ error: 'Purchase not found' });

  const content = String(req.body.content || '').trim();
  if (!content) return res.status(400).json({ error: 'محتوى الرسالة مطلوب' });

  const messages = db.messages();
  const newMessage = {
    id: uuidv4(),
    senderId: req.apiUser.id,
    senderName: req.apiUser.name,
    receiverId: 'admin',
    content,
    read: false,
    purchaseId: purchaseChat.purchaseId,
    orderId: purchaseChat.orderId,
    projectTitle: purchaseChat.projectTitle,
    createdAt: new Date().toISOString()
  };
  messages.push(newMessage);
  db.saveMessages(messages);

  return res.status(201).json({ message: {
    id: newMessage.id,
    content: newMessage.content,
    senderId: newMessage.senderId,
    senderName: newMessage.senderName,
    receiverId: newMessage.receiverId,
    isMine: true,
    purchaseId: newMessage.purchaseId,
    projectTitle: newMessage.projectTitle,
    createdAt: newMessage.createdAt,
    read: false
  }});
});

app.get('/api/purchases/:id/download-url', requireApiUserAuth, (req, res) => {
  const purchase = db.purchases().find((item) => item && item.id === req.params.id && item.userId === req.apiUser.id);
  if (!purchase) return res.status(404).json({ error: 'Purchase not found' });
  if (purchase.status !== 'approved') return res.status(400).json({ error: 'الملف غير متاح للتحميل بعد' });
  if (purchase.downloadLocked) {
    return res.status(403).json({ error: purchase.downloadLockReason || 'تم قفل هذه النسخة من المشروع بواسطة الأدمن' });
  }

  if (purchase.projectId) {
    const project = db.projects().find((p) => p && p.id === purchase.projectId);
    if (project && isProjectDownloadsLocked(project)) {
      return res.status(403).json({ error: getProjectDownloadsLockReason(project) || 'تم قفل تنزيلات هذا المشروع مؤقتاً' });
    }
  }

  return res.json({
    url: `/api/download/${purchase.id}`,
    fileName: getDownloadFileName(purchase)
  });
});

app.get('/api/download/:purchaseId', (req, res) => {
  const token = String(req.query.token || '').trim();
  const decoded = token ? verifyToken(token) : null;
  if (!decoded || decoded.role !== 'user') {
    return res.status(401).json({ error: 'Invalid token' });
  }

  const purchase = db.purchases().find((item) => item && item.id === req.params.purchaseId && item.userId === decoded.id);
  if (!purchase) return res.status(404).json({ error: 'Purchase not found' });
  if (purchase.status !== 'approved') return res.status(400).json({ error: 'File not available' });
  if (purchase.downloadLocked) {
    return res.status(403).json({ error: purchase.downloadLockReason || 'Download locked' });
  }

  if (purchase.projectId) {
    const project = db.projects().find((p) => p && p.id === purchase.projectId);
    if (project && isProjectDownloadsLocked(project)) {
      return res.status(403).json({ error: getProjectDownloadsLockReason(project) || 'Project downloads are locked' });
    }
  }

  const absoluteFilePath = path.resolve(__dirname, purchase.filePath);
  if (!fs.existsSync(absoluteFilePath)) {
    return res.status(404).json({ error: 'File not found' });
  }

  recordDownloadEvent({
    req,
    kind: 'purchase',
    userId: decoded.id,
    purchaseId: purchase.id,
    projectId: purchase.projectId || null,
    meta: { via: 'api-token' }
  });

  return res.download(absoluteFilePath, getDownloadFileName(purchase));
});

app.get('/api/community', requireApiUserAuth, (req, res) => {
  const posts = getCommunityPostsState()
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime())
    .map((post) => {
      const likes = Array.isArray(post.likes) ? post.likes : [];
      const shares = Array.isArray(post.shares) ? post.shares : [];
      const comments = Array.isArray(post.comments) ? post.comments : [];
      return {
        id: post.id,
        authorName: post.authorName || 'Admin',
        authorRole: post.authorRole || 'admin',
        content: post.content || '',
        mediaType: post.mediaType || null,
        mediaPath: post.mediaPath || null,
        likeCount: likes.length,
        shareCount: shares.length,
        commentCount: comments.length,
        isLikedByCurrentUser: Boolean(likes.some((item) => item && item.userId === req.apiUser.id)),
        createdAt: post.createdAt || null,
        comments: comments.map((comment) => ({
          id: comment.id,
          userName: comment.userName || '',
          content: comment.content || '',
          createdAt: comment.createdAt || null
        }))
      };
    });

  const applications = getCommunityJobApplicationsState();
  const myApplications = applications
    .filter((item) => item && item.userId === req.apiUser.id)
    .sort((a, b) => new Date(b.statusUpdatedAt || b.createdAt || 0).getTime() - new Date(a.statusUpdatedAt || a.createdAt || 0).getTime());
  const myApplicationsByJobId = new Map(myApplications.map((item) => [item.jobId, item]));

  const jobs = getCommunityJobsState()
    .filter((job) => job && job.isActive !== false)
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime())
    .map((job) => {
      const app = myApplicationsByJobId.get(job.id) || null;
      return {
        id: job.id,
        title: job.title || '',
        salary: job.salary || '',
        description: job.description || '',
        experienceYears: Number(job.experienceYears || 0),
        hasApplied: Boolean(app),
        applicationStatus: app ? app.status : null,
        trackingNote: app ? (app.trackingNote || null) : null,
        createdAt: job.createdAt || null
      };
    });

  return res.json({
    posts,
    jobs,
    myApplications: myApplications.map((application) => ({
      id: application.id,
      jobId: application.jobId,
      jobTitle: application.jobTitle || '',
      status: application.status || 'received',
      trackingNote: application.trackingNote || null,
      createdAt: application.createdAt || null,
      statusUpdatedAt: application.statusUpdatedAt || null
    }))
  });
});

app.post('/api/community/posts/:id/like', requireApiUserAuth, (req, res) => {
  const posts = getCommunityPostsState();
  const postIndex = posts.findIndex((post) => post && post.id === req.params.id);
  if (postIndex === -1) return res.status(404).json({ error: 'Post not found' });

  const likes = Array.isArray(posts[postIndex].likes) ? posts[postIndex].likes : [];
  const existing = likes.findIndex((entry) => entry && entry.userId === req.apiUser.id);
  if (existing === -1) {
    likes.push({
      userId: req.apiUser.id,
      userName: req.apiUser.name,
      createdAt: new Date().toISOString()
    });
  } else {
    likes.splice(existing, 1);
  }

  posts[postIndex].likes = likes;
  saveCommunityPostsState(posts);
  return res.json({
    success: true,
    likeCount: likes.length,
    isLikedByCurrentUser: existing === -1
  });
});

app.post('/api/community/posts/:id/comments', requireApiUserAuth, (req, res) => {
  const content = String(req.body.content || '').trim();
  if (!content) return res.status(400).json({ error: 'محتوى التعليق مطلوب' });

  const posts = getCommunityPostsState();
  const postIndex = posts.findIndex((post) => post && post.id === req.params.id);
  if (postIndex === -1) return res.status(404).json({ error: 'Post not found' });

  const comments = Array.isArray(posts[postIndex].comments) ? posts[postIndex].comments : [];
  const comment = {
    id: uuidv4(),
    userId: req.apiUser.id,
    userName: req.apiUser.name,
    content,
    createdAt: new Date().toISOString()
  };
  comments.push(comment);
  posts[postIndex].comments = comments;
  saveCommunityPostsState(posts);

  return res.status(201).json({
    comment: {
      id: comment.id,
      userName: comment.userName,
      content: comment.content,
      createdAt: comment.createdAt
    },
    commentCount: comments.length
  });
});

app.post('/api/community/posts/:id/share', requireApiUserAuth, (req, res) => {
  const posts = getCommunityPostsState();
  const postIndex = posts.findIndex((post) => post && post.id === req.params.id);
  if (postIndex === -1) return res.status(404).json({ error: 'Post not found' });

  const shares = Array.isArray(posts[postIndex].shares) ? posts[postIndex].shares : [];
  shares.push({
    id: uuidv4(),
    userId: req.apiUser.id,
    userName: req.apiUser.name,
    createdAt: new Date().toISOString()
  });
  posts[postIndex].shares = shares;
  saveCommunityPostsState(posts);

  return res.json({ success: true, shareCount: shares.length });
});

app.post('/api/community/jobs/:id/apply', requireApiUserAuth, communityCvUploadHandler, async (req, res) => {
  const file = req.file || null;

  try {
    const jobs = getCommunityJobsState();
    const job = jobs.find((item) => item && item.id === req.params.id);
    if (!job || job.isActive === false) {
      if (file) safeDeleteFile(file.path);
      return res.status(404).json({ error: 'الوظيفة غير متاحة حاليًا' });
    }

    if (!file) return res.status(400).json({ error: 'ارفع السيرة الذاتية أولًا' });
    if (!isValidCommunityCvFile(file)) {
      safeDeleteFile(file.path);
      return res.status(400).json({ error: 'صيغة السيرة الذاتية يجب أن تكون PDF أو DOCX' });
    }

    const users = db.users();
    const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
    if (!currentUser) {
      safeDeleteFile(file.path);
      return res.status(404).json({ error: 'User not found' });
    }

    const name = String(req.body.name || currentUser.name || '').trim();
    const about = String(req.body.about || '').trim();
    const age = Number(req.body.age || 0);
    if (!name || !Number.isFinite(age) || age <= 0 || !about) {
      safeDeleteFile(file.path);
      return res.status(400).json({ error: 'أكمل الاسم والعمر ونبذة عنك قبل التقديم' });
    }
    if (about.length > COMMUNITY_ABOUT_LIMIT) {
      safeDeleteFile(file.path);
      return res.status(400).json({ error: `النبذة طويلة جدًا. الحد الأقصى ${COMMUNITY_ABOUT_LIMIT} حرف` });
    }

    const applications = getCommunityJobApplicationsState();
    const alreadyApplied = applications.find((application) => application && application.jobId === job.id && application.userId === currentUser.id);
    if (alreadyApplied) {
      safeDeleteFile(file.path);
      return res.status(400).json({ error: 'لقد قدمت على هذه الوظيفة بالفعل' });
    }

    const applicationId = uuidv4();
    const relativeCvPath = path.relative(__dirname, file.path).split(path.sep).join('/');
    const cvBuffer = fs.readFileSync(file.path);
    let cvFilePath = relativeCvPath;
    let cvStorageKey = null;

    if (usePostgresStorage()) {
      cvStorageKey = `community-cv:${applicationId}`;
      await saveBinaryAsset({
        key: cvStorageKey,
        buffer: cvBuffer,
        mimeType: file.mimetype,
        fileName: file.originalname
      });
      safeDeleteFile(file.path);
      cvFilePath = null;
    }

    const now = new Date().toISOString();
    const application = {
      id: applicationId,
      jobId: job.id,
      jobTitle: job.title,
      userId: currentUser.id,
      userEmail: currentUser.email,
      applicantName: name,
      applicantAge: age,
      about,
      cvFilePath,
      cvStorageKey,
      cvOriginalName: file.originalname,
      cvMimeType: file.mimetype,
      status: 'pending',
      trackingNote: 'تم استلام طلبك وبانتظار مراجعة فريق Codentra.',
      statusUpdatedAt: now,
      statusHistory: [
        buildCommunityApplicationHistoryEntry({
          status: 'pending',
          note: 'تم استلام طلب التقديم.',
          updatedAt: now,
          updatedById: currentUser.id,
          updatedByName: currentUser.name,
          actorType: 'applicant'
        })
      ],
      aiScreening: normalizeCommunityAiScreening({
        decision: 'pending',
        summary: 'جارٍ تحليل السيرة الذاتية وتقييمها...',
        reasons: [],
        evaluatedAt: now,
        model: GEMINI_MODEL || 'local-fallback'
      }),
      createdAt: now
    };

    try {
      const aiScreening = await screenCommunityCvWithGemini({
        job,
        applicant: { name, age, about },
        cvBuffer,
        mimeType: file.mimetype,
        fileName: file.originalname
      });

      application.aiScreening = aiScreening;
      application.statusUpdatedAt = aiScreening.evaluatedAt || new Date().toISOString();

      if (aiScreening.decision === 'accepted') {
        application.status = 'accepted';
        application.trackingNote = aiScreening.summary || 'تم قبولك مبدئيًا بعد التحليل الآلي، وبانتظار استكمال الإدارة للخطوة التالية.';
        application.statusHistory.push(buildCommunityApplicationHistoryEntry({
          status: 'accepted',
          note: `تم قبول الطلب مبدئيًا عبر التحليل الآلي.${aiScreening.summary ? ` ${aiScreening.summary}` : ''}`.trim(),
          updatedAt: application.statusUpdatedAt,
          updatedByName: 'Codentra AI',
          actorType: 'system'
        }));
      } else if (aiScreening.decision === 'rejected') {
        application.status = 'rejected';
        application.trackingNote = aiScreening.summary || 'تم رفض الطلب مبدئيًا لعدم ارتباط السيرة الذاتية بالوظيفة بشكل كافٍ.';
        application.rejectedAt = application.statusUpdatedAt;
        application.statusHistory.push(buildCommunityApplicationHistoryEntry({
          status: 'rejected',
          note: `تم رفض الطلب مبدئيًا عبر التحليل الآلي.${aiScreening.summary ? ` ${aiScreening.summary}` : ''}`.trim(),
          updatedAt: application.statusUpdatedAt,
          updatedByName: 'Codentra AI',
          actorType: 'system'
        }));
      }
    } catch (aiError) {
      const aiErrorMessage = String(aiError && aiError.message ? aiError.message : aiError);
      const unsupportedFormat = aiErrorMessage.includes('unsupported_cv_ai_format');
      const emptyDocxText = aiErrorMessage.includes('empty_docx_text');
      const emptyPdfText = aiErrorMessage.includes('empty_pdf_text');
      const fallbackSummary = unsupportedFormat
        ? 'التقييم الآلي يدعم حاليًا ملفات PDF و DOCX فقط.'
        : emptyDocxText
          ? 'تعذر قراءة محتوى ملف DOCX، لذلك لم يكتمل التقييم الآلي.'
          : emptyPdfText
            ? 'تعذر استخراج نص واضح من ملف PDF، لذلك لم يكتمل التقييم الآلي.'
            : 'تعذر تنفيذ التقييم الآلي لهذه السيرة الذاتية.';
      application.aiScreening = normalizeCommunityAiScreening({
        decision: 'error',
        summary: fallbackSummary,
        reasons: [],
        evaluatedAt: new Date().toISOString(),
        model: GEMINI_MODEL || 'local-fallback',
        error: aiErrorMessage
      });
      application.trackingNote = fallbackSummary;
    }

    applications.unshift(application);
    saveCommunityJobApplicationsState(applications);

    return res.status(201).json({
      success: true,
      application: {
        id: application.id,
        jobId: application.jobId,
        jobTitle: application.jobTitle,
        status: application.status,
        trackingNote: application.trackingNote,
        createdAt: application.createdAt,
        statusUpdatedAt: application.statusUpdatedAt,
        aiScreening: application.aiScreening
      }
    });
  } catch (error) {
    if (file) safeDeleteFile(file.path);
    console.error('Community job application API error:', error);
    return res.status(500).json({ error: 'تعذر إرسال طلب التقديم الآن. حاول مرة أخرى' });
  }
});

app.get('/api/appointments', requireApiUserAuth, (req, res) => {
  const admins = getAdminUsers();
  const data = migrateAppointmentsBookingsMeetingLinks();
  const availableSlots = (data.timeSlots || [])
    .filter((slot) => slot && slot.status === 'available')
    .filter(isValidFutureSlot)
    .sort((a, b) => new Date(a.startAt).getTime() - new Date(b.startAt).getTime());
  const adminsById = new Map(admins.map((admin) => [admin.id, admin]));

  return res.json({
    slots: availableSlots
      .filter((slot) => adminsById.has(slot.adminId))
      .map((slot) => ({
        id: slot.id,
        adminId: slot.adminId,
        adminName: adminsById.get(slot.adminId)?.name || 'Admin',
        startAt: slot.startAt,
        durationMinutes: Number(slot.durationMinutes || 30)
      }))
  });
});

app.post('/api/appointments/book', requireApiUserAuth, (req, res) => {
  const slotId = String(req.body.slotId || '').trim();
  const notes = String(req.body.notes || '').trim();
  if (!slotId) return res.status(400).json({ error: 'اختر ميعاد للحجز' });

  const admins = getAdminUsers();
  const adminsById = new Map(admins.map((admin) => [admin.id, admin]));
  const data = db.appointments();
  const timeSlots = Array.isArray(data.timeSlots) ? data.timeSlots : [];
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];

  const slotIndex = timeSlots.findIndex((slot) => slot && slot.id === slotId);
  if (slotIndex === -1) return res.status(404).json({ error: 'الموعد غير موجود' });

  const slot = timeSlots[slotIndex];
  if (!adminsById.has(slot.adminId)) return res.status(400).json({ error: 'الأدمن غير موجود' });
  if (slot.status !== 'available') return res.status(400).json({ error: 'الموعد غير متاح' });
  if (!isValidFutureSlot(slot)) return res.status(400).json({ error: 'الموعد انتهى' });

  timeSlots[slotIndex] = { ...slot, status: 'booked' };
  const roomId = buildMeetingRoomId({ adminId: slot.adminId, userId: req.apiUser.id, slotId: slot.id });
  const booking = {
    id: uuidv4(),
    userId: req.apiUser.id,
    userName: req.apiUser.name,
    adminId: slot.adminId,
    adminName: adminsById.get(slot.adminId)?.name || 'Admin',
    slotId: slot.id,
    startAt: slot.startAt,
    durationMinutes: Number(slot.durationMinutes || 30),
    roomId,
    meetingLink: `/meet/${roomId}`,
    notes: notes || null,
    status: 'confirmed',
    createdAt: new Date().toISOString()
  };

  bookings.push(booking);
  db.saveAppointments({ timeSlots, bookings });

  return res.status(201).json({ booking });
});

app.get('/api/my-appointments', requireApiUserAuth, (req, res) => {
  const data = migrateAppointmentsBookingsMeetingLinks();
  const bookings = (data.bookings || [])
    .filter((booking) => booking && booking.userId === req.apiUser.id)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime())
    .map((booking) => ({
      id: booking.id,
      adminId: booking.adminId,
      adminName: booking.adminName || 'Admin',
      slotId: booking.slotId,
      startAt: booking.startAt,
      durationMinutes: Number(booking.durationMinutes || 30),
      roomId: booking.roomId || null,
      meetingLink: booking.meetingLink || null,
      notes: booking.notes || null,
      status: booking.status || 'confirmed',
      createdAt: booking.createdAt || null
    }));

  return res.json({ bookings });
});

app.get('/api/custom-projects', requireApiUserAuth, (req, res) => {
  const requests = db.customProjectRequests()
    .filter((item) => item && item.userId === req.apiUser.id)
    .sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0))
    .map((request) => serializeCustomProjectRequest({ request, currentUserId: req.apiUser.id }))
    .filter(Boolean);

  return res.json({ requests });
});

app.get('/api/custom-projects/:id', requireApiUserAuth, (req, res) => {
  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.apiUser.id });
  if (!requestItem) return res.status(404).json({ error: 'طلب المشروع غير موجود' });

  const allMessages = db.messages();
  let changed = false;
  allMessages.forEach((message) => {
    if (message && message.customProjectRequestId === requestItem.id && message.senderId === 'admin' && message.receiverId === req.apiUser.id && !message.read) {
      message.read = true;
      changed = true;
    }
  });
  if (changed) db.saveMessages(allMessages);

  return res.json({
    request: serializeCustomProjectRequest({ request: requestItem, includeMessages: true, currentUserId: req.apiUser.id })
  });
});

app.post('/api/custom-projects', requireApiUserAuth, (req, res) => {
  const title = String(req.body.title || '').trim();
  const projectType = String(req.body.projectType || '').trim();
  const description = String(req.body.description || '').trim();
  const budget = String(req.body.budget || '').trim();
  const timeline = String(req.body.timeline || '').trim();

  if (!title || !projectType || !description) {
    return res.status(400).json({ error: 'اسم المشروع والنوع والوصف مطلوبين' });
  }

  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (!currentUser) return res.status(404).json({ error: 'User not found' });

  const requests = db.customProjectRequests();
  const createdRequest = {
    id: uuidv4(),
    userId: currentUser.id,
    userName: currentUser.name,
    userEmail: currentUser.email,
    title,
    projectType,
    description,
    budget: budget || null,
    timeline: timeline || null,
    status: 'new',
    adminReply: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  requests.unshift(createdRequest);
  db.saveCustomProjectRequests(requests);

  return res.status(201).json({
    request: serializeCustomProjectRequest({ request: createdRequest, currentUserId: currentUser.id })
  });
});

app.post('/api/custom-projects/:id/messages', requireApiUserAuth, (req, res) => {
  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.apiUser.id });
  if (!requestItem) return res.status(404).json({ error: 'طلب المشروع غير موجود' });

  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (!currentUser) return res.status(404).json({ error: 'User not found' });

  const content = String(req.body.content || '').trim();
  if (!content) {
    return res.status(400).json({ error: 'محتوى الرسالة مطلوب' });
  }

  const message = {
    id: uuidv4(),
    senderId: currentUser.id,
    senderName: currentUser.name,
    receiverId: 'admin',
    content,
    read: false,
    customProjectRequestId: requestItem.id,
    projectTitle: requestItem.title,
    createdAt: new Date().toISOString()
  };

  const messages = db.messages();
  messages.push(message);
  db.saveMessages(messages);

  return res.status(201).json({
    message: serializeCustomProjectMessage(message, currentUser.id)
  });
});

app.post('/api/custom-projects/:id/pay-wallet', requireApiUserAuth, (req, res) => {
  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.apiUser.id });
  if (!requestItem) return res.status(404).json({ error: 'طلب المشروع غير موجود' });

  const amount = Math.round(Number(requestItem.quotedPrice || 0) * 100) / 100;
  if (!(amount > 0)) return res.status(400).json({ error: 'لم يتم تحديد السعر بعد' });
  if (requestItem.paymentStatus === 'paid') return res.status(400).json({ error: 'تم دفع الطلب بالفعل' });

  const users = db.users();
  const buyerIndex = users.findIndex((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (buyerIndex === -1) return res.status(404).json({ error: 'User not found' });

  const currentBalance = Number(users[buyerIndex].walletBalance || 0);
  if (currentBalance < amount) {
    return res.status(400).json({ error: 'رصيد المحفظة غير كافٍ' });
  }

  users[buyerIndex].walletBalance = Math.round((currentBalance - amount) * 100) / 100;
  db.saveUsers(users);

  try {
    finalizeCustomProjectPayment({
      requestId: requestItem.id,
      buyerUserId: req.apiUser.id,
      payerUserId: req.apiUser.id,
      skipWalletDebit: true
    });
  } catch (error) {
    users[buyerIndex].walletBalance = currentBalance;
    db.saveUsers(users);
    return res.status(400).json({ error: error.message || 'تعذر إتمام الدفع' });
  }

  const refreshedRequest = getCustomProjectRequestForUser({ requestId: requestItem.id, userId: req.apiUser.id });
  const refreshedUser = db.users().find((item) => item && item.id === req.apiUser.id && item.role === 'user');

  return res.json({
    success: true,
    request: serializeCustomProjectRequest({ request: refreshedRequest, currentUserId: req.apiUser.id }),
    newBalance: Number((refreshedUser && refreshedUser.walletBalance) || 0)
  });
});

app.get('/api/custom-projects/:id/download-url', requireApiUserAuth, (req, res) => {
  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.apiUser.id });
  if (!requestItem) return res.status(404).json({ error: 'طلب المشروع غير موجود' });
  if (!requestItem.filePath) return res.status(400).json({ error: 'الملف غير متاح بعد' });

  return res.json({
    url: `/api/custom-projects/${requestItem.id}/download`,
    fileName: requestItem.originalFileName || path.basename(requestItem.filePath)
  });
});

app.get('/api/custom-projects/:id/download', (req, res) => {
  const authHeader = req.headers.authorization;
  const tokenFromQuery = req.query.token;
  const token = authHeader?.startsWith('Bearer ')
    ? authHeader.split(' ')[1]
    : tokenFromQuery;

  if (!token) return res.status(401).json({ error: 'Missing or invalid token' });
  const decoded = verifyToken(token);
  if (!decoded || decoded.role !== 'user') return res.status(401).json({ error: 'Invalid token' });

  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: decoded.id });
  if (!requestItem || !requestItem.filePath) return res.status(404).json({ error: 'File not found' });

  const absolutePath = path.resolve(__dirname, requestItem.filePath);
  if (!fs.existsSync(absolutePath)) return res.status(404).json({ error: 'File not found' });

  recordDownloadEvent({
    req,
    kind: 'custom-project',
    userId: decoded.id,
    purchaseId: requestItem.id,
    projectId: null,
    meta: { via: 'api-token' }
  });

  return res.download(absolutePath, requestItem.originalFileName || path.basename(absolutePath));
});

// Mobile API - Invoice PDF
app.get('/api/invoice/:orderId.pdf', (req, res) => {
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
  const invoices = db.invoices();
  const inv = invoices.find(i => i && i.orderId === orderId && i.userId === decoded.id) || null;
  if (!inv) return res.status(404).send('Invoice not found');

  res.render('invoice', buildInvoiceViewModel(inv, { includeEmail: true, includeCoupon: true }));
});

// Mobile API - Subscriptions
app.get('/api/subscriptions/plans', requireApiUserAuth, (req, res) => {
  const plans = db.subscriptionPlans().filter(p => p && p.active);
  const coupons = db.subscriptionCoupons().filter(c => c && c.active);
  const activeSubscription = getActiveSubscriptionForUser({ userId: req.apiUser.id });
  res.json({ plans, coupons, activeSubscription });
});

app.post('/api/subscriptions/subscribe', requireApiUserAuth, (req, res) => {
  const { planId, couponCode } = req.body;
  
  const plans = db.subscriptionPlans().filter(p => p && p.active);
  const plan = plans.find(p => p.id === planId);
  if (!plan) return res.status(400).json({ error: 'الخطة غير صحيحة' });

  const existing = getActiveSubscriptionForUser({ userId: req.apiUser.id });
  if (existing) return res.status(400).json({ error: 'لديك اشتراك نشط بالفعل' });

  const users = db.users();
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
    coupons = db.subscriptionCoupons();
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
  db.saveUsers(users);

  if (appliedCoupon && coupons && couponIndex !== -1) {
    coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
    coupons[couponIndex].lastUsedAt = new Date().toISOString();
    db.saveSubscriptionCoupons(coupons);
  }

  const now = new Date();
  const end = new Date(now.getTime() + (Number(plan.durationDays || 30) * 24 * 60 * 60 * 1000));

  const subs = db.subscriptions();
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
  db.saveSubscriptions(subs);

  const payments = db.subscriptionPayments();
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
  db.saveSubscriptionPayments(payments);

  res.json({ success: true, subscription: sub, newBalance: users[idx].walletBalance });
});

app.post('/api/subscriptions/cancel', requireApiUserAuth, (req, res) => {
  const subs = db.subscriptions();
  const active = getActiveSubscriptionForUser({ userId: req.apiUser.id });
  if (!active) return res.status(400).json({ error: 'لا يوجد اشتراك نشط' });

  const idx = subs.findIndex(s => s.id === active.id);
  if (idx === -1) return res.status(400).json({ error: 'لا يوجد اشتراك نشط' });

  subs[idx].status = 'canceled';
  subs[idx].canceledAt = new Date().toISOString();
  db.saveSubscriptions(subs);

  res.json({ success: true });
});

// Mobile API - Wallet Code Redeem
app.post('/api/wallet/redeem', requireApiUserAuth, (req, res) => {
  const code = normalizeWalletCode(req.body.code);
  if (!code) return res.status(400).json({ error: 'الكود مطلوب' });

  const walletCodes = db.walletCodes();
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

  const users = db.users();
  const userIndex = users.findIndex(u => u.id === req.apiUser.id);
  if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

  users[userIndex].walletBalance = Number(users[userIndex].walletBalance || 0) + amount;
  db.saveUsers(users);

  walletCodes[idx].usedCount = Number(walletCodes[idx].usedCount || 0) + 1;
  walletCodes[idx].lastUsedAt = new Date().toISOString();
  db.saveWalletCodes(walletCodes);

  res.json({ 
    success: true, 
    amount, 
    newBalance: users[userIndex].walletBalance,
    message: 'تم إضافة الرصيد بنجاح'
  });
});

app.get('/api/wallet/topups', requireApiUserAuth, (req, res) => {
  const topups = db.walletTopups()
    .filter((topup) => topup && topup.userId === req.apiUser.id)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());

  return res.json({ topups });
});

app.post('/api/wallet/topup/start', requireApiUserAuth, async (req, res) => {
  const amountUsd = normalizeWalletTopupUsdAmount(req.body.amount);
  if (!amountUsd) {
    return res.status(400).json({ error: 'مبلغ الشحن يجب أن يكون أكبر من صفر' });
  }

  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (!currentUser) return res.status(404).json({ error: 'المستخدم غير موجود' });
  if (!isAtlosConfigured()) return res.status(400).json({ error: 'إعدادات Atlos غير مكتملة بعد' });

  const topups = db.walletTopups();
  const topup = createWalletTopupRecord({ user: currentUser, amountUsd, exchangeRate: res.locals.displayCurrencyRate });
  topups.push(topup);
  db.saveWalletTopups(topups);

  try {
    const checkout = await createAtlosCheckout({ topup, user: currentUser });
    const refreshedTopups = db.walletTopups();
    const topupIndex = refreshedTopups.findIndex((item) => item && item.id === topup.id);
    if (topupIndex !== -1) {
      refreshedTopups[topupIndex] = {
        ...refreshedTopups[topupIndex],
        gatewayPaymentId: checkout.paymentId,
        checkoutUrl: checkout.checkoutUrl,
        updatedAt: new Date().toISOString()
      };
      db.saveWalletTopups(refreshedTopups);
    }

    return res.status(201).json({
      success: true,
      topupId: topup.id,
      checkoutUrl: checkout.checkoutUrl
    });
  } catch (error) {
    finalizeWalletTopup({ topupId: topup.id, status: 'failed', failureReason: error.message || 'ATLOS_ERROR' });
    return res.status(500).json({ error: 'تعذر بدء عملية الدفع عبر Atlos الآن' });
  }
});

app.get('/api/modifications', requireApiUserAuth, (req, res) => {
  const modifications = db.modifications()
    .filter((item) => item && item.userId === req.apiUser.id)
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  return res.json({ modifications });
});

app.post('/api/modifications/:purchaseId', requireApiUserAuth, (req, res) => {
  const description = String(req.body.description || '').trim();
  if (!description) return res.status(400).json({ error: 'وصف التعديل مطلوب' });

  const purchases = db.purchases();
  const purchase = purchases.find((item) => item && item.id === req.params.purchaseId && item.userId === req.apiUser.id);
  if (!purchase) return res.status(404).json({ error: 'Purchase not found' });

  const modifications = db.modifications();
  const modification = {
    id: uuidv4(),
    purchaseId: purchase.id,
    userId: req.apiUser.id,
    projectId: purchase.projectId,
    projectTitle: purchase.projectTitle,
    description,
    status: 'pending',
    createdAt: new Date().toISOString()
  };
  modifications.push(modification);
  db.saveModifications(modifications);

  return res.status(201).json({ modification });
});

app.get('/api/presentations', requireApiUserAuth, (req, res) => {
  const plans = ensurePresentationPlans();
  const activeSubscription = getActivePresentationSubscriptionForUser({ userId: req.apiUser.id });
  const activePlan = activeSubscription ? plans.find((plan) => plan.id === activeSubscription.planId) || null : null;
  const decks = getPresentationDecksForUser({ userId: req.apiUser.id, limit: 20 });
  const incomingApprovals = getPresentationIncomingApprovals({ ownerUserId: req.apiUser.id });

  return res.json({
    plans,
    activeSubscriptionCouponPromos,
    activeSubscription,
    activePlan,
    decks,
    incomingApprovals
  });
});

app.post('/api/presentations/subscribe/:planId', requireApiUserAuth, (req, res) => {
  const plan = getPresentationPlanById({ planId: req.params.planId });
  if (!plan) return res.status(400).json({ error: 'الخطة غير موجودة' });

  const users = db.users();
  const buyerIndex = users.findIndex((item) => item && item.id === req.apiUser.id && item.role === 'user');
  if (buyerIndex === -1) return res.status(404).json({ error: 'المستخدم غير موجود' });
  const buyer = users[buyerIndex];
  if (ensureUserPaymentProfile({ user: buyer, users })) {
    users[buyerIndex] = buyer;
    db.saveUsers(users);
  }

  const amount = Number(plan.price || 0);
  try {
    enforceWalletCardSpendingLimit({ payerUser: buyer, amount });
  } catch (limitError) {
    return res.status(400).json({ error: limitError.message || 'تم تجاوز حد البطاقة' });
  }
  if (Number(buyer.walletBalance || 0) < amount) {
    return res.status(400).json({ error: 'رصيد البطاقة غير كافٍ' });
  }

  buyer.walletBalance = Math.round((Number(buyer.walletBalance || 0) - amount) * 100) / 100;
  users[buyerIndex] = buyer;
  db.saveUsers(users);

  const subscription = upsertPresentationSubscription({ userId: buyer.id, plan, payerUserId: buyer.id });
  finalizeWalletCardOwnerActivity({
    payerUserId: buyer.id,
    buyerUser: buyer,
    amount,
    kind: 'presentations-subscription',
    payload: { planName: plan.name, planId: plan.id },
    status: 'completed',
    note: 'تم تفعيل اشتراك Codentra Presentations من التطبيق',
    notifyTitle: 'تم استخدام بطاقتك',
    notifyMessage: `استخدمت بطاقتك في الاشتراك بخطة ${plan.name}.`
  });

  return res.json({ success: true, subscription, newBalance: buyer.walletBalance });
});

app.post('/api/presentations/generate', requireApiUserAuth, async (req, res) => {
  const activeSubscription = getActivePresentationSubscriptionForUser({ userId: req.apiUser.id });
  if (!activeSubscription) return res.status(400).json({ error: 'تحتاج اشتراكًا نشطًا للبدء' });
  if (Number(activeSubscription.remainingCredits || 0) <= 0) {
    return res.status(400).json({ error: 'لا توجد عمليات إنشاء متبقية في اشتراكك' });
  }

  const topic = String(req.body.topic || '').trim();
  const audience = String(req.body.audience || '').trim();
  const tone = String(req.body.tone || '').trim();
  const purpose = String(req.body.purpose || '').trim();
  const language = req.body.language === 'en' ? 'en' : 'ar';
  const slideCount = countPresentationSlides(req.body.slideCount);
  if (!topic) return res.status(400).json({ error: 'اكتب موضوع العرض أولًا' });

  const generated = await generatePresentationDeck({ topic, audience, tone, purpose, language, slideCount });
  const decks = db.presentationDecks();
  const deck = {
    id: uuidv4(),
    userId: req.apiUser.id,
    subscriptionId: activeSubscription.id,
    topic,
    audience,
    tone,
    purpose,
    language,
    title: generated.title,
    subtitle: generated.subtitle,
    theme: generated.theme,
    slides: generated.slides,
    createdAt: new Date().toISOString(),
    modelUsed: process.env.OPENAI_API_KEY ? PRESENTATION_AI_MODEL : 'local-fallback'
  };
  decks.push(deck);
  db.savePresentationDecks(decks);
  consumePresentationCredit({ subscriptionId: activeSubscription.id });

  return res.status(201).json({ deck });
});

app.get('/api/presentations/decks/:deckId/download-url', requireApiUserAuth, (req, res) => {
  const decks = db.presentationDecks();
  const deck = decks.find((item) => item && item.id === req.params.deckId && item.userId === req.apiUser.id);
  if (!deck) return res.status(404).json({ error: 'العرض غير موجود' });
  return res.json({
    url: `/api/presentations/decks/${deck.id}/download.pptx`,
    fileName: `${slugify(deck.title || deck.topic || 'presentation') || 'presentation'}.pptx`
  });
});

app.get('/api/presentations/decks/:deckId/download.pptx', async (req, res, next) => {
  const authHeader = req.headers.authorization;
  const tokenFromQuery = req.query.token;
  const token = authHeader?.startsWith('Bearer ')
    ? authHeader.split(' ')[1]
    : tokenFromQuery;
  if (!token) return res.status(401).json({ error: 'Missing or invalid token' });
  const decoded = verifyToken(token);
  if (!decoded || decoded.role !== 'user') return res.status(401).json({ error: 'Invalid token' });

  try {
    const decks = db.presentationDecks();
    const deck = decks.find((item) => item && item.id === req.params.deckId && item.userId === decoded.id);
    if (!deck) return res.status(404).json({ error: 'العرض غير موجود' });
    const { fileName, filePath } = await buildPresentationPptxFile(deck);
    return res.download(filePath, fileName, () => {
      fs.unlink(filePath, () => {});
    });
  } catch (error) {
    return next(error);
  }
});

// Project detail
app.get('/project/:id', (req, res) => {
  const projects = db.projects();
  const project = decorateProjectPricing(projects.find(p => p.id === req.params.id));
  if (!project) return res.status(404).send('Project not found');

  if (!isProjectVisibleToUser({ project, sessionUser: req.session.user })) {
    return res.status(403).send('Not allowed');
  }

  // Used by admin "AI Pricing" to estimate view-to-purchase conversion.
  recordProjectViewEvent({ req, projectId: project.id, sessionUser: req.session.user });

  recordRecentlyViewedProject({ req, projectId: project.id });
  
  let hasPurchased = false;
  let canReview = false;
  let existingReview = null;
  if (req.session.user) {
    const purchases = db.purchases();
    hasPurchased = purchases.some(p => p.userId === req.session.user.id && p.projectId === project.id);

    const hasApprovedPurchase = purchases.some(p => p.userId === req.session.user.id && p.projectId === project.id && p.status === 'approved');
    const reviews = db.reviews();
    existingReview = reviews.find(r => r.userId === req.session.user.id && r.projectId === project.id) || null;
    canReview = hasApprovedPurchase && !existingReview;
  }

  const projectReviews = db.reviews().filter(r => r.projectId === project.id);
  const avgRating = projectReviews.length
    ? (projectReviews.reduce((sum, r) => sum + Number(r.rating || 0), 0) / projectReviews.length)
    : 0;

  let canApplyReferral = false;
  let referralPrefill = '';
  if (req.session.user) {
    const users = db.users();
    const currentUser = users.find(u => u.id === req.session.user.id);
    const purchases = db.purchases();
    const hasAnyPurchase = purchases.some(p => p.userId === req.session.user.id);
    canApplyReferral = !!currentUser && !currentUser.referredBy && !hasAnyPurchase;
    referralPrefill = req.query.ref || '';
  }
  
  const activeCouponPromos = getActiveCouponPromoCards(db.coupons(), getCouponEligibility);

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
    referralPrefill,
    activeCouponPromos
  });
});

app.post('/project/:id/reviews', requireAuth, (req, res) => {
  const projects = db.projects();
  const project = decorateProjectPricing(projects.find(p => p.id === req.params.id));
  if (!project) return res.status(404).send('Project not found');

  const rating = Number(req.body.rating);
  const comment = (req.body.comment || '').trim();

  if (!Number.isFinite(rating) || rating < 1 || rating > 5) {
    return res.status(400).send('Invalid rating');
  }

  if (!comment) {
    return res.status(400).send('Comment is required');
  }

  const purchases = db.purchases();
  const hasApprovedPurchase = purchases.some(
    p => p.userId === req.session.user.id && p.projectId === project.id && p.status === 'approved'
  );

  if (!hasApprovedPurchase) {
    return res.status(403).send('You can review only after an approved purchase');
  }

  const reviews = db.reviews();
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

  db.saveReviews(reviews);
  res.redirect(`/project/${project.id}`);
});

const finalizeSingleProjectPurchase = ({ buyerUserId, payerUserId, projectId, couponCode, referralCode }) => {
  const users = db.users();
  const buyerIndex = users.findIndex(u => u && u.id === buyerUserId && u.role === 'user');
  const payerIndex = users.findIndex(u => u && u.id === payerUserId && u.role === 'user');
  if (buyerIndex === -1) throw new Error('المشتري غير موجود');
  if (payerIndex === -1) throw new Error('صاحب البطاقة غير موجود');

  const buyer = users[buyerIndex];
  const payer = users[payerIndex];
  let shouldSaveUsers = false;

  if (ensureUserPaymentProfile({ user: buyer, users })) shouldSaveUsers = true;
  if (ensureUserPaymentProfile({ user: payer, users })) shouldSaveUsers = true;

  if (payer.walletCardFrozen) {
    throw new Error('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة');
  }

  const projects = db.projects();
  const project = projects.find(p => p && p.id === projectId);
  if (!project) throw new Error('المشروع غير موجود');

  const purchases = db.purchases();
  const alreadyPurchased = purchases.find(p => p && p.userId === buyer.id && p.projectId === project.id);
  if (alreadyPurchased) throw new Error('تم شراء هذا المشروع بالفعل');

  if (!isProjectVisibleToUser({ project, sessionUser: buildSessionUser(buyer) })) {
    throw new Error('هذا المشروع غير متاح لهذا الحساب');
  }

  const normalizedReferral = normalizeReferralCode(referralCode);
  if (normalizedReferral) {
    const hasAnyPurchase = purchases.some(p => p && p.userId === buyer.id);
    const canApplyReferralNow = !buyer.referredBy && !hasAnyPurchase;
    if (!canApplyReferralNow) {
      throw new Error('لا يمكنك استخدام كود الإحالة الآن');
    }

    const referralCheck = validateReferralCodeForUser({ users, code: normalizedReferral, targetUserId: buyer.id });
    if (!referralCheck.valid) {
      throw new Error(referralCheck.reason || 'كود الإحالة غير صحيح');
    }

    buyer.referredBy = {
      referrerUserId: referralCheck.referrerUserId,
      code: referralCheck.normalized,
      createdAt: new Date().toISOString(),
      rewardedAt: null
    };
    shouldSaveUsers = true;

    const referrals = db.referrals();
    const alreadyRecorded = referrals.some(r => r && r.referredUserId === buyer.id);
    if (!alreadyRecorded) {
      referrals.push({
        id: uuidv4(),
        code: referralCheck.normalized,
        referrerUserId: referralCheck.referrerUserId,
        referredUserId: buyer.id,
        status: 'pending',
        rewardAmount: 100,
        rewardType: 'loyalty-points',
        createdAt: new Date().toISOString(),
        rewardedAt: null,
        rewardPurchaseId: null
      });
      db.saveReferrals(referrals);
    }
  }

  const normalizedCouponCode = normalizeCouponCode(couponCode);
  let appliedCoupon = null;
  const projectSale = getProjectSaleState(project);
  let discountAmount = Number(projectSale.discountAmount || 0);
  let priceAfterDiscount = Number(projectSale.finalPrice || 0);
  let coupons = null;
  let couponIndex = -1;

  if (normalizedCouponCode) {
    coupons = db.coupons();
    couponIndex = coupons.findIndex(c => c && normalizeCouponCode(c.code) === normalizedCouponCode);
    const coupon = couponIndex !== -1 ? coupons[couponIndex] : null;
    const eligibility = getCouponEligibility(coupon);
    if (!eligibility.eligible) {
      throw new Error(eligibility.reason || 'الكوبون غير صالح');
    }

    const calc = calculateDiscount({ priceBefore: priceAfterDiscount, coupon });
    discountAmount = Math.round((Number(discountAmount || 0) + Number(calc.discountAmount || 0)) * 100) / 100;
    priceAfterDiscount = calc.priceAfter;
    appliedCoupon = coupon;
  }

  const subPercent = getSubscriberDiscountPercent({ sessionUser: buildSessionUser(buyer) });
  if (subPercent > 0) {
    const base = Number(priceAfterDiscount || 0);
    const subscriberDiscount = Math.round((base * (subPercent / 100)) * 100) / 100;
    discountAmount = Math.round((Number(discountAmount || 0) + subscriberDiscount) * 100) / 100;
    priceAfterDiscount = Math.round((base - subscriberDiscount) * 100) / 100;
  }

  const payerWalletBalance = Number(payer.walletBalance || 0);
  enforceWalletCardSpendingLimit({ payerUser: payer, amount: Number(priceAfterDiscount || 0) });
  if (payerWalletBalance < Number(priceAfterDiscount || 0)) {
    throw new Error('رصيد البطاقة غير كافٍ');
  }

  payer.walletBalance = Math.round((payerWalletBalance - Number(priceAfterDiscount || 0)) * 100) / 100;
  shouldSaveUsers = true;

  const earnedPoints = getLoyaltyEarnedPointsForPurchase({ amountEGP: Number(priceAfterDiscount || 0) });
  if (earnedPoints > 0) {
    buyer.loyaltyPoints = normalizeLoyaltyPoints(buyer.loyaltyPoints) + earnedPoints;
    shouldSaveUsers = true;
  }

  if (shouldSaveUsers) {
    users[buyerIndex] = buyer;
    users[payerIndex] = payer;
    db.saveUsers(users);
  }

  if (appliedCoupon && coupons && couponIndex !== -1) {
    coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
    db.saveCoupons(coupons);
  }

  purchases.push({
    id: uuidv4(),
    fingerprintCode: uuidv4(),
    downloadLocked: false,
    downloadLockReason: null,
    downloadLockedAt: null,
    userId: buyer.id,
    payerUserId: payer.id,
    payerCardLast4: normalizeWalletCardNumber(payer.walletCardNumber).slice(-4),
    projectId: project.id,
    projectTitle: project.title,
    price: priceAfterDiscount,
    priceBefore: Number(project.price || 0),
    projectSaleDiscountAmount: Number(projectSale.discountAmount || 0),
    projectSaleOccasion: projectSale.occasion || null,
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

  db.savePurchases(purchases);
  return { buyer, payer };
};

const finalizeCartPurchase = ({ buyerUserId, payerUserId, projectIds, couponCode }) => {
  const users = db.users();
  const buyerIndex = users.findIndex(u => u && u.id === buyerUserId && u.role === 'user');
  const payerIndex = users.findIndex(u => u && u.id === payerUserId && u.role === 'user');
  if (buyerIndex === -1) throw new Error('المشتري غير موجود');
  if (payerIndex === -1) throw new Error('صاحب البطاقة غير موجود');

  const buyer = users[buyerIndex];
  const payer = users[payerIndex];
  let shouldSaveUsers = false;

  if (ensureUserPaymentProfile({ user: buyer, users })) shouldSaveUsers = true;
  if (ensureUserPaymentProfile({ user: payer, users })) shouldSaveUsers = true;

  if (payer.walletCardFrozen) {
    throw new Error('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة');
  }

  const normalizedProjectIds = Array.isArray(projectIds) ? projectIds.filter(Boolean) : [];
  if (!normalizedProjectIds.length) throw new Error('لا توجد عناصر لإتمام الدفع');

  const projects = db.projects();
  const purchases = db.purchases();
  const buyerSession = buildSessionUser(buyer);

  const cartItems = normalizedProjectIds.map(projectId => {
    const project = projects.find(p => p && p.id === projectId);
    if (!project) throw new Error('يوجد مشروع غير موجود في عملية الدفع');
    if (!isProjectVisibleToUser({ project, sessionUser: buyerSession })) {
      throw new Error('يوجد مشروع غير مسموح لك بشرائه');
    }
    const alreadyPurchased = purchases.some(p => p && p.userId === buyer.id && p.projectId === project.id);
    if (alreadyPurchased) throw new Error('يوجد مشروع تم شراؤه مسبقاً');
    return {
      projectId: project.id,
      projectTitle: project.title,
      price: Number(getProjectSaleState(project).finalPrice || 0),
      project
    };
  });

  const summary = summarizeCart({
    cart: { items: cartItems.map(item => ({ projectId: item.projectId, projectTitle: item.projectTitle, price: item.price })) },
    couponCode,
    sessionUser: buyerSession
  });
  const totalAfter = Number(summary.totalAfter || 0);
  if (!(totalAfter > 0)) throw new Error('إجمالي الدفع غير صحيح');

  const payerWalletBalance = Number(payer.walletBalance || 0);
  enforceWalletCardSpendingLimit({ payerUser: payer, amount: totalAfter });
  if (payerWalletBalance < totalAfter) {
    throw new Error('رصيد البطاقة غير كافٍ');
  }

  payer.walletBalance = Math.round((payerWalletBalance - totalAfter) * 100) / 100;
  shouldSaveUsers = true;

  const earnedPoints = getLoyaltyEarnedPointsForPurchase({ amountEGP: totalAfter });
  if (earnedPoints > 0) {
    buyer.loyaltyPoints = normalizeLoyaltyPoints(buyer.loyaltyPoints) + earnedPoints;
    shouldSaveUsers = true;
  }

  if (summary.appliedCoupon) {
    const coupons = db.coupons();
    const couponIndex = coupons.findIndex(c => c && normalizeCouponCode(c.code) === normalizeCouponCode(summary.appliedCoupon.code));
    if (couponIndex !== -1) {
      coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
      db.saveCoupons(coupons);
    }
  }

  if (shouldSaveUsers) {
    users[buyerIndex] = buyer;
    users[payerIndex] = payer;
    db.saveUsers(users);
  }

  const orderId = uuidv4();
  const totalDiscount = Math.round((Number(summary.couponDiscount || 0) + Number(summary.subscriberDiscount || 0)) * 100) / 100;
  const perItemDiscount = cartItems.length ? Math.round((totalDiscount / cartItems.length) * 100) / 100 : 0;
  const perItemDebit = cartItems.length ? Math.round((totalAfter / cartItems.length) * 100) / 100 : 0;

  cartItems.forEach(item => {
    purchases.push({
      id: uuidv4(),
      orderId,
      fingerprintCode: uuidv4(),
      downloadLocked: false,
      downloadLockReason: null,
      downloadLockedAt: null,
      userId: buyer.id,
      payerUserId: payer.id,
      payerCardLast4: normalizeWalletCardNumber(payer.walletCardNumber).slice(-4),
      projectId: item.projectId,
      projectTitle: item.projectTitle,
      price: Math.max(0, Math.round((Number(item.price || 0) - perItemDiscount) * 100) / 100),
      priceBefore: Number(item.project && item.project.price || item.price || 0),
      discountAmount: Math.round((Number((item.project && getProjectSaleState(item.project).discountAmount) || 0) + Number(perItemDiscount || 0)) * 100) / 100,
      couponCode: summary.appliedCoupon ? normalizeCouponCode(summary.appliedCoupon.code) : null,
      walletDebitAmount: perItemDebit,
      walletRefundedAt: null,
      loyaltyPointsEarned: null,
      status: 'pending',
      purchasedAt: new Date().toISOString(),
      filePath: item.project.filePath,
      originalFileName: item.project.originalFileName || null
    });
  });

  db.savePurchases(purchases);

  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: buyer.id });
  const targetIds = new Set(normalizedProjectIds);
  const nextItems = (Array.isArray(cart.items) ? cart.items : []).filter(item => item && !targetIds.has(item.projectId));
  carts[cartIndex] = { ...cart, items: nextItems, updatedAt: new Date().toISOString() };
  db.saveCarts(carts);

  return { buyer, payer };
};

// Purchase - creates pending payment verification request
app.post('/purchase/:id', requireAuth, async (req, res) => {
  try {
    const projects = db.projects();
    const project = projects.find(p => p && p.id === req.params.id);
    if (!project) return res.status(404).send('Project not found');

    const users = db.users();
    const buyer = users.find(u => u && u.id === req.session.user.id && u.role === 'user');
    if (!buyer) return res.status(404).send('User not found');
    if (ensureUserPaymentProfile({ user: buyer, users })) {
      db.saveUsers(users);
    }

    if (!isProjectVisibleToUser({ project, sessionUser: buildSessionUser(buyer) })) {
      return res.status(403).send('Not allowed');
    }

    const purchases = db.purchases();
    const alreadyPurchased = purchases.find(p => p && p.userId === buyer.id && p.projectId === project.id);
    if (alreadyPurchased) {
      return res.redirect('/my-purchases');
    }

    const couponCode = normalizeCouponCode(req.body.couponCode);
    const projectSale = getProjectSaleState(project);
    let priceAfterDiscount = Number(projectSale.finalPrice || 0);
    if (couponCode) {
      const coupons = db.coupons();
      const coupon = coupons.find(c => c && normalizeCouponCode(c.code) === couponCode) || null;
      const eligibility = getCouponEligibility(coupon);
      if (!eligibility.eligible) {
        return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent(eligibility.reason || 'كوبون غير صالح')}`);
      }
      const calc = calculateDiscount({ priceBefore: priceAfterDiscount, coupon });
      priceAfterDiscount = calc.priceAfter;
    }

    const subPercent = getSubscriberDiscountPercent({ sessionUser: buildSessionUser(buyer) });
    if (subPercent > 0) {
      priceAfterDiscount = Math.round((Number(priceAfterDiscount || 0) * (1 - (subPercent / 100))) * 100) / 100;
    }

    const enteredCardNumber = normalizeWalletCardNumber(req.body.walletCardNumber) || normalizeWalletCardNumber(buyer.walletCardNumber);
    const payer = findUserByWalletCardNumber({ users, walletCardNumber: enteredCardNumber });
    if (!payer) {
      return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('رقم بطاقة المحفظة غير صحيح')}`);
    }
    if (payer.walletCardFrozen) {
      return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة')}`);
    }
    if (ensureUserPaymentProfile({ user: payer, users })) {
      db.saveUsers(users);
    }

    if (Number(payer.walletBalance || 0) < Number(priceAfterDiscount || 0)) {
      return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('رصيد البطاقة غير كافٍ')}`);
    }

    try {
      enforceWalletCardSpendingLimit({ payerUser: payer, amount: Number(priceAfterDiscount || 0) });
    } catch (limitError) {
      return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent(limitError.message || 'تم تجاوز حد البطاقة')}`);
    }

    if (Number(priceAfterDiscount || 0) > HIGH_VALUE_PAYMENT_THRESHOLD && !payer.walletPaymentPasswordHash) {
      return res.redirect(`/project/${project.id}?couponError=${encodeURIComponent('صاحب البطاقة لم يضبط كلمة مرور البطاقة بعد')}`);
    }

    if (payer.id === buyer.id) {
      const result = finalizeSingleProjectPurchase({
        buyerUserId: buyer.id,
        payerUserId: payer.id,
        projectId: project.id,
        couponCode,
        referralCode: normalizeReferralCode(req.body.referralCode)
      });

      finalizeWalletCardOwnerActivity({
        payerUserId: payer.id,
        buyerUser: buyer,
        amount: priceAfterDiscount,
        kind: 'single-project',
        payload: {
          projectId: project.id,
          projectTitle: project.title,
          couponCode,
          referralCode: normalizeReferralCode(req.body.referralCode)
        },
        status: 'completed',
        note: 'تم الدفع مباشرة باستخدام بطاقتك الشخصية',
        notifyTitle: 'تم استخدام بطاقتك',
        notifyMessage: `استخدمت بطاقتك في شراء المشروع: ${project.title}.`
      });

      if (result && result.buyer) {
        req.session.user = buildSessionUser(result.buyer);
      }

      return res.redirect('/my-purchases?paymentSuccess=' + encodeURIComponent('تم الدفع مباشرة باستخدام بطاقتك'));
    }

    const attempt = await createWalletPaymentAttempt({
      buyerUser: buyer,
      payerUser: payer,
      amount: priceAfterDiscount,
      kind: 'single-project',
      payload: {
        projectId: project.id,
        projectTitle: project.title,
        couponCode,
        referralCode: normalizeReferralCode(req.body.referralCode)
      }
    });

    return res.redirect(`/payment/verify/${attempt.id}`);
  } catch (error) {
    const message = error && error.message === 'SMTP is not configured'
      ? 'خدمة إرسال البريد غير مفعلة. أضف إعدادات SMTP أولاً.'
      : 'تعذر إرسال كود التحقق الآن';
    return res.redirect(`/project/${req.params.id}?couponError=${encodeURIComponent(message)}`);
  }
});

// Cart
app.get('/cart', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = db.users();
  const currentUser = users.find(u => u.id === req.session.user.id);
  if (currentUser) {
    if (ensureUserPaymentProfile({ user: currentUser, users })) {
      db.saveUsers(users);
    }
    req.session.user = buildSessionUser(currentUser);
  }

  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.session.user.id });
  // Do NOT bump updatedAt on mere page view; updatedAt is used to detect abandoned carts.
  if (!cart.updatedAt) {
    carts[cartIndex] = { ...cart, updatedAt: new Date().toISOString() };
    db.saveCarts(carts);
  }

  const couponCode = (req.query.couponCode || '').toString();
  const summary = summarizeCart({ cart, couponCode, sessionUser: req.session.user });
  const activeCouponPromos = getActiveCouponPromoCards(db.coupons(), getCouponEligibility);

  res.render('cart', {
    user: req.session.user,
    cart,
    summary,
    couponCode,
    activeCouponPromos,
    walletCardNumber: currentUser ? formatWalletCardNumber(currentUser.walletCardNumber) : '',
    error: req.query.error || null
  });
});

// API to validate coupon for cart
app.post('/api/cart/validate-coupon', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') {
    return res.status(403).json({ valid: false, message: 'Unauthorized' });
  }

  const { couponCode } = req.body;
  if (!couponCode || !couponCode.trim()) {
    return res.json({ valid: false, message: 'الرجاء إدخال كود الخصم' });
  }

  const { cart } = getOrCreateCartForUser({ userId: req.session.user.id });
  const items = (cart && Array.isArray(cart.items)) ? cart.items : [];

  if (items.length === 0) {
    return res.json({ valid: false, message: 'السلة فارغة' });
  }

  const totalBefore = Math.round(items.reduce((sum, it) => sum + Number(it.price || 0), 0) * 100) / 100;

  const normalized = normalizeCouponCode(couponCode);
  const coupons = db.coupons();
  const coupon = coupons.find(c => normalizeCouponCode(c.code) === normalized) || null;

  if (!coupon) {
    return res.json({ valid: false, message: 'كود الخصم غير موجود' });
  }

  const eligibility = getCouponEligibility(coupon);
  if (!eligibility.eligible) {
    return res.json({ valid: false, message: eligibility.reason || 'الكوبون غير صالح' });
  }

  // Convert coupon value from USD to EGP for calculation (cart prices are in EGP)
  const couponValueEgp = coupon.type === 'fixed' 
    ? convertUsdToEgp(coupon.value)
    : coupon.value;
  const calc = calculateDiscount({ 
    priceBefore: totalBefore, 
    coupon: { ...coupon, value: couponValueEgp }
  });
  const discountAmount = Math.round(Number(calc.discountAmount || 0) * 100) / 100;
  const totalAfter = Math.round((totalBefore - discountAmount) * 100) / 100;

  // Convert discount back to USD for display
  const discountUsd = convertEgpToUsd(discountAmount);
  const totalBeforeUsd = convertEgpToUsd(totalBefore);
  const totalAfterUsd = convertEgpToUsd(totalAfter);

  res.json({
    valid: true,
    discountAmount: discountUsd,
    totalBefore: totalBeforeUsd,
    totalAfter: totalAfterUsd,
    couponType: coupon.type,
    couponValue: coupon.value,
    message: `تم تطبيق خصم ${coupon.type === 'percent' ? coupon.value + '%' : '$' + coupon.value}`
  });
});

// Test email configuration endpoint
app.post('/api/test-email', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'admin') {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    // Check email configuration
    const isConfigured = isMailConfigured();
    const config = {
      smtp: {
        host: process.env.SMTP_HOST || null,
        port: process.env.SMTP_PORT || null,
        user: process.env.SMTP_USER ? '***' : null,
        from: process.env.SMTP_FROM || null,
        configured: Boolean(process.env.SMTP_HOST && process.env.SMTP_PORT && process.env.SMTP_USER && process.env.SMTP_PASS)
      },
      resend: {
        apiKey: process.env.RESEND_API_KEY ? '***' : null,
        from: process.env.RESEND_FROM_EMAIL || null,
        configured: Boolean(process.env.RESEND_API_KEY && process.env.RESEND_FROM_EMAIL)
      }
    };

    if (!isConfigured) {
      return res.json({
        configured: false,
        config,
        message: 'Email not configured. Set SMTP_* or RESEND_API_KEY + RESEND_FROM_EMAIL in .env'
      });
    }

    // Test sending email to admin
    const testResult = await queueNotificationEmail({
      user: { role: 'user', email: req.session.user.email },
      subject: 'Codentra - اختبار إرسال الإيميل',
      title: 'اختبار إرسال الإيميل',
      message: 'إذا رأيت هذا الإيميل فإن إعدادات البريد تعمل بشكل صحيح! 🎉'
    });

    res.json({
      configured: true,
      config,
      testResult,
      message: testResult.success ? 'Email sent successfully! Check your inbox.' : 'Email configuration exists but sending failed.'
    });
  } catch (error) {
    console.error('Email test error:', error);
    res.status(500).json({ error: 'Failed to test email: ' + error.message });
  }
});

app.post('/cart/add', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const projectId = (req.body.projectId || '').toString();
  if (!projectId) return res.redirect('/?error=' + encodeURIComponent('مشروع غير صحيح'));

  const projects = db.projects();
  const project = projects.find(p => p && p.id === projectId);
  if (!project) return res.status(404).send('Project not found');
  if (!isProjectVisibleToUser({ project, sessionUser: req.session.user })) {
    return res.status(403).send('Not allowed');
  }

  const purchases = db.purchases();
  const alreadyPurchased = purchases.some(p => p.userId === req.session.user.id && p.projectId === projectId);
  if (alreadyPurchased) return res.redirect(`/project/${projectId}`);

  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.session.user.id });
  const items = Array.isArray(cart.items) ? cart.items : [];
  const exists = items.some(it => it && it.projectId === projectId);
  if (!exists) {
    items.push({
      projectId: project.id,
      projectTitle: project.title,
      price: Number(getProjectSaleState(project).finalPrice || 0),
      createdAt: new Date().toISOString()
    });
  }

  carts[cartIndex] = { ...cart, items, updatedAt: new Date().toISOString() };
  db.saveCarts(carts);
  res.redirect('/cart');
});

app.post('/cart/remove', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const projectId = (req.body.projectId || '').toString();
  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.session.user.id });
  const nextItems = (Array.isArray(cart.items) ? cart.items : []).filter(it => it && it.projectId !== projectId);
  carts[cartIndex] = { ...cart, items: nextItems, updatedAt: new Date().toISOString() };
  db.saveCarts(carts);
  res.redirect('/cart');
});

app.post('/cart/clear', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const { carts, cart, cartIndex } = getOrCreateCartForUser({ userId: req.session.user.id });
  carts[cartIndex] = { ...cart, items: [], updatedAt: new Date().toISOString() };
  db.saveCarts(carts);
  res.redirect('/cart');
});

app.post('/cart/checkout', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  try {
    const { cart } = getOrCreateCartForUser({ userId: req.session.user.id });
    const items = (cart && Array.isArray(cart.items)) ? cart.items : [];
    if (items.length === 0) return res.redirect('/cart?error=' + encodeURIComponent('السلة فارغة'));

    const users = db.users();
    const buyer = users.find(u => u && u.id === req.session.user.id && u.role === 'user');
    if (!buyer) return res.status(404).send('User not found');
    if (ensureUserPaymentProfile({ user: buyer, users })) {
      db.saveUsers(users);
    }

    const projects = db.projects();
    const purchases = db.purchases();
    const couponCode = normalizeCouponCode(req.body.couponCode);
    const summary = summarizeCart({ cart, couponCode, sessionUser: buildSessionUser(buyer) });
    const totalAfter = Number(summary.totalAfter || 0);
    if (!(totalAfter > 0)) return res.redirect('/cart?error=' + encodeURIComponent('إجمالي غير صحيح'));

    for (const item of items) {
      const project = projects.find(p => p && p.id === item.projectId);
      if (!project) return res.redirect('/cart?error=' + encodeURIComponent('يوجد مشروع غير موجود بالسلة'));
      if (!isProjectVisibleToUser({ project, sessionUser: buildSessionUser(buyer) })) {
        return res.redirect('/cart?error=' + encodeURIComponent('يوجد مشروع غير مسموح لك بشرائه'));
      }
      const alreadyPurchased = purchases.some(p => p && p.userId === buyer.id && p.projectId === item.projectId);
      if (alreadyPurchased) {
        return res.redirect('/cart?error=' + encodeURIComponent('يوجد مشروع تم شراؤه مسبقاً'));
      }
    }

    const enteredCardNumber = normalizeWalletCardNumber(req.body.walletCardNumber) || normalizeWalletCardNumber(buyer.walletCardNumber);
    const payer = findUserByWalletCardNumber({ users, walletCardNumber: enteredCardNumber });
    if (!payer) {
      return res.redirect('/cart?error=' + encodeURIComponent('رقم بطاقة المحفظة غير صحيح'));
    }
    if (payer.walletCardFrozen) {
      return res.redirect('/cart?error=' + encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة'));
    }
    if (ensureUserPaymentProfile({ user: payer, users })) {
      db.saveUsers(users);
    }

    if (Number(payer.walletBalance || 0) < totalAfter) {
      return res.redirect('/cart?error=' + encodeURIComponent('رصيد البطاقة غير كافٍ'));
    }

    try {
      enforceWalletCardSpendingLimit({ payerUser: payer, amount: totalAfter });
    } catch (limitError) {
      return res.redirect('/cart?error=' + encodeURIComponent(limitError.message || 'تم تجاوز حد البطاقة'));
    }

    if (totalAfter > HIGH_VALUE_PAYMENT_THRESHOLD && !payer.walletPaymentPasswordHash) {
      return res.redirect('/cart?error=' + encodeURIComponent('صاحب البطاقة لم يضبط كلمة مرور البطاقة بعد'));
    }

    if (payer.id === buyer.id) {
      const result = finalizeCartPurchase({
        buyerUserId: buyer.id,
        payerUserId: payer.id,
        projectIds: items.map(item => item.projectId),
        couponCode
      });

      finalizeWalletCardOwnerActivity({
        payerUserId: payer.id,
        buyerUser: buyer,
        amount: totalAfter,
        kind: 'cart',
        payload: {
          projectIds: items.map(item => item.projectId),
          projectTitles: items.map(item => item.projectTitle),
          couponCode
        },
        status: 'completed',
        note: 'تم الدفع مباشرة باستخدام بطاقتك الشخصية',
        notifyTitle: 'تم استخدام بطاقتك',
        notifyMessage: `استخدمت بطاقتك في شراء ${items.length} مشروع من السلة.`
      });

      if (result && result.buyer) {
        req.session.user = buildSessionUser(result.buyer);
      }

      return res.redirect('/my-purchases?paymentSuccess=' + encodeURIComponent('تم الدفع مباشرة باستخدام بطاقتك'));
    }

    const attempt = await createWalletPaymentAttempt({
      buyerUser: buyer,
      payerUser: payer,
      amount: totalAfter,
      kind: 'cart',
      payload: {
        projectIds: items.map(item => item.projectId),
        projectTitles: items.map(item => item.projectTitle),
        couponCode
      }
    });

    return res.redirect(`/payment/verify/${attempt.id}`);
  } catch (error) {
    const message = error && error.message === 'SMTP is not configured'
      ? 'خدمة إرسال البريد غير مفعلة. أضف إعدادات SMTP أولاً.'
      : 'تعذر إرسال كود التحقق الآن';
    return res.redirect('/cart?error=' + encodeURIComponent(message));
  }
});

app.get('/payment/verify/:attemptId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const attempt = getWalletPaymentAttemptById(req.params.attemptId);
  if (!attempt || attempt.buyerUserId !== req.session.user.id) {
    return res.redirect('/cart?error=' + encodeURIComponent('طلب التحقق غير موجود'));
  }

  if (attempt.usedAt) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('تم استخدام طلب التحقق بالفعل'));
  }

  if (isPaymentAttemptExpired(attempt)) {
    return res.redirect('/cart?error=' + encodeURIComponent('انتهت صلاحية كود التحقق. حاول مرة أخرى'));
  }

  const users = db.users();
  const payer = users.find(u => u && u.id === attempt.payerUserId) || null;
  if (!payer) {
    return res.redirect('/cart?error=' + encodeURIComponent('صاحب البطاقة غير موجود'));
  }

  return res.render('payment-verify', {
    user: req.session.user,
    attempt,
    amount: Number(attempt.amount || 0),
    payerEmailMasked: maskEmail(payer.email),
    payerName: payer.name || payer.email || 'صاحب البطاقة',
    walletCardMasked: maskWalletCardNumber(payer.walletCardNumber),
    requiresPassword: Number(attempt.amount || 0) > HIGH_VALUE_PAYMENT_THRESHOLD,
    error: req.query.error || null
  });
});

app.post('/payment/verify/:attemptId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const attempt = getWalletPaymentAttemptById(req.params.attemptId);
  if (!attempt || attempt.buyerUserId !== req.session.user.id) {
    return res.redirect('/cart?error=' + encodeURIComponent('طلب التحقق غير موجود'));
  }

  if (attempt.usedAt) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('تم استخدام طلب التحقق بالفعل'));
  }

  if (isPaymentAttemptExpired(attempt)) {
    return res.redirect('/cart?error=' + encodeURIComponent('انتهت صلاحية كود التحقق. حاول مرة أخرى'));
  }

  const verificationCode = String(req.body.code || '').trim();
  if (!verificationCode || hashPaymentVerificationCode(verificationCode) !== attempt.codeHash) {
    return res.redirect(`/payment/verify/${attempt.id}?error=${encodeURIComponent('كود التحقق غير صحيح')}`);
  }

  const users = db.users();
  const payer = users.find(u => u && u.id === attempt.payerUserId && u.role === 'user');
  if (!payer) {
    return res.redirect('/cart?error=' + encodeURIComponent('صاحب البطاقة غير موجود'));
  }
  if (payer.walletCardFrozen) {
    return res.redirect('/cart?error=' + encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة'));
  }

  if (Number(attempt.amount || 0) > HIGH_VALUE_PAYMENT_THRESHOLD) {
    const paymentPassword = String(req.body.paymentPassword || '');
    if (!payer.walletPaymentPasswordHash) {
      return res.redirect(`/payment/verify/${attempt.id}?error=${encodeURIComponent('صاحب البطاقة لم يضبط كلمة مرور البطاقة بعد')}`);
    }
    if (!paymentPassword || !bcrypt.compareSync(paymentPassword, payer.walletPaymentPasswordHash)) {
      return res.redirect(`/payment/verify/${attempt.id}?error=${encodeURIComponent('كلمة مرور البطاقة غير صحيحة')}`);
    }
  }

  try {
    let result = null;

    if (attempt.kind === 'single-project') {
      result = finalizeSingleProjectPurchase({
        buyerUserId: attempt.buyerUserId,
        payerUserId: attempt.payerUserId,
        projectId: attempt.payload && attempt.payload.projectId,
        couponCode: attempt.payload && attempt.payload.couponCode,
        referralCode: attempt.payload && attempt.payload.referralCode
      });
    } else if (attempt.kind === 'cart') {
      result = finalizeCartPurchase({
        buyerUserId: attempt.buyerUserId,
        payerUserId: attempt.payerUserId,
        projectIds: attempt.payload && attempt.payload.projectIds,
        couponCode: attempt.payload && attempt.payload.couponCode
      });
    } else if (attempt.kind === 'custom-project') {
      result = finalizeCustomProjectPayment({
        requestId: attempt.payload && attempt.payload.customProjectRequestId,
        buyerUserId: attempt.buyerUserId,
        payerUserId: attempt.payerUserId
      });
    } else {
      throw new Error('نوع عملية الدفع غير مدعوم');
    }

    finalizeWalletCardOwnerActivity({
      payerUserId: attempt.payerUserId,
      buyerUser: { id: attempt.buyerUserId, name: attempt.buyerDisplayName || null, email: null },
      amount: attempt.amount,
      kind: attempt.kind,
      payload: attempt.payload || {},
      attemptId: attempt.id,
      usageLogEntryId: attempt.usageLogEntryId || null,
      status: 'completed',
      note: 'تم إدخال كود التحقق وإتمام الدفع بنجاح',
      notifyTitle: 'تم استخدام بطاقتك بنجاح',
      notifyMessage: `اكتمل الدفع الخاص بـ ${attempt.purposeLabel || 'استخدام بطاقة المحفظة'} بنجاح.`
    });

    markWalletPaymentAttemptUsed(attempt.id);

    if (result && result.buyer) {
      req.session.user = buildSessionUser(result.buyer);
    }

    return res.redirect('/my-purchases?paymentSuccess=' + encodeURIComponent('تم تأكيد الدفع وإرسال الطلب بنجاح'));
  } catch (error) {
    const fallbackTarget = attempt.kind === 'single-project' && attempt.payload && attempt.payload.projectId
      ? `/project/${attempt.payload.projectId}`
      : '/cart';
    const message = error && error.message ? error.message : 'تعذر إتمام الدفع بعد التحقق';
    return res.redirect(`/payment/verify/${attempt.id}?error=${encodeURIComponent(message)}&back=${encodeURIComponent(fallbackTarget)}`);
  }
});

// My purchases
app.get('/my-purchases', requireAuth, (req, res) => {
  const users = db.users();
  const currentUser = users.find(u => u.id === req.session.user.id);
  if (currentUser) {
    if (ensureUserPaymentProfile({ user: currentUser, users })) {
      db.saveUsers(users);
    }
    req.session.user = buildSessionUser(currentUser);
  }
  const purchases = db.purchases().filter(p => p.userId === req.session.user.id);
  const invoices = db.invoices().filter(i => i && i.userId === req.session.user.id);
  const subscriptionPlans = db.subscriptionPlans();
  const subscriptions = db.subscriptions().filter(s => s && s.userId === req.session.user.id);
  const subscriptionPayments = db.subscriptionPayments().filter(p => p && p.userId === req.session.user.id);
  const walletTopups = db.walletTopups()
    .filter((topup) => topup && topup.userId === req.session.user.id)
    .sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0))
    .slice(0, MAX_WALLET_TOPUPS);
  const incomingApprovals = currentUser ? getWalletIncomingApprovals({ ownerUserId: currentUser.id }) : [];
  res.render('my-purchases', {
    purchases,
    invoices,
    user: req.session.user,
    referralLink: currentUser && currentUser.referralCode ? `${req.protocol}://${req.get('host')}/register?ref=${encodeURIComponent(currentUser.referralCode)}` : '',
    redeemError: req.query.redeemError || null,
    redeemSuccess: req.query.redeemSuccess || null,
    walletCardError: req.query.walletCardError || null,
    walletCardSuccess: req.query.walletCardSuccess || null,
    paymentError: req.query.paymentError || null,
    paymentSuccess: req.query.paymentSuccess || null,
    walletCardNumber: currentUser ? formatWalletCardNumber(currentUser.walletCardNumber) : null,
    walletCardSpendingLimit: currentUser ? sanitizeWalletCardSpendingLimit(currentUser.walletCardSpendingLimit) : null,
    walletCardNotifications: currentUser && Array.isArray(currentUser.walletCardNotifications) ? currentUser.walletCardNotifications.slice(0, 12) : [],
    walletCardUsageLog: currentUser && Array.isArray(currentUser.walletCardUsageLog) ? currentUser.walletCardUsageLog.slice(0, 20) : [],
    walletCardFrozen: currentUser ? Boolean(currentUser.walletCardFrozen) : false,
    walletTopups,
    atlosEnabled: isAtlosConfigured(),
    incomingApprovals,
    subscriptionPlans,
    subscriptions,
    subscriptionPayments
  });
});

app.get('/notifications', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.session.user.id && item.role === 'user');
  if (!currentUser) return res.redirect('/');

  if (ensureUserPaymentProfile({ user: currentUser, users })) {
    db.saveUsers(users);
  }

  if (markAllNotificationsAsRead(currentUser)) {
    db.saveUsers(users);
  }

  req.session.user = buildSessionUser(currentUser);

  res.render('notifications', {
    user: req.session.user,
    notifications: getNotificationCenterItems(currentUser)
  });
});

app.get('/custom-project', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const requests = db.customProjectRequests()
    .filter((item) => item && item.userId === req.session.user.id)
    .sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));

  res.render('custom-project', {
    user: req.session.user,
    requests,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.get('/custom-project/:id', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.session.user.id });
  if (!requestItem) {
    return res.redirect('/custom-project?error=' + encodeURIComponent('طلب المشروع غير موجود'));
  }

  const messages = db.messages().filter((message) => (
    message && message.customProjectRequestId === requestItem.id && (
      (message.senderId === req.session.user.id && message.receiverId === 'admin') ||
      (message.senderId === 'admin' && message.receiverId === req.session.user.id)
    )
  ));

  const allMessages = db.messages();
  let changed = false;
  allMessages.forEach((message) => {
    if (message && message.customProjectRequestId === requestItem.id && message.senderId === 'admin' && message.receiverId === req.session.user.id && !message.read) {
      message.read = true;
      changed = true;
    }
  });
  if (changed) db.saveMessages(allMessages);

  res.render('custom-project-details', {
    user: req.session.user,
    requestItem,
    messages,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/custom-project', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const title = String(req.body.title || '').trim();
  const projectType = String(req.body.projectType || '').trim();
  const description = String(req.body.description || '').trim();
  const budget = String(req.body.budget || '').trim();
  const timeline = String(req.body.timeline || '').trim();

  if (!title || !projectType || !description) {
    return res.redirect('/custom-project?error=' + encodeURIComponent('اسم المشروع والنوع والوصف مطلوبين'));
  }

  const requests = db.customProjectRequests();
  requests.unshift({
    id: uuidv4(),
    userId: req.session.user.id,
    userName: req.session.user.name,
    userEmail: req.session.user.email,
    title,
    projectType,
    description,
    budget: budget || null,
    timeline: timeline || null,
    status: 'new',
    adminReply: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  });
  db.saveCustomProjectRequests(requests);

  return res.redirect('/custom-project?success=' + encodeURIComponent('تم إرسال طلب المشروع بنجاح'));
});

app.post('/custom-project/:id/messages', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.session.user.id });
  if (!requestItem) {
    return res.redirect('/custom-project?error=' + encodeURIComponent('طلب المشروع غير موجود'));
  }

  const content = String(req.body.content || '').trim();
  if (!content) {
    return res.redirect(`/custom-project/${requestItem.id}`);
  }

  const messages = db.messages();
  messages.push({
    id: uuidv4(),
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    receiverId: 'admin',
    content,
    read: false,
    customProjectRequestId: requestItem.id,
    projectTitle: requestItem.title,
    createdAt: new Date().toISOString()
  });
  db.saveMessages(messages);

  return res.redirect(`/custom-project/${requestItem.id}?success=${encodeURIComponent('تم إرسال الرسالة')}`);
});

app.post('/custom-project/:id/pay', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.session.user.id });
  if (!requestItem) {
    return res.redirect('/custom-project?error=' + encodeURIComponent('طلب المشروع غير موجود'));
  }

  const amount = Math.round(Number(requestItem.quotedPrice || 0) * 100) / 100;
  if (!(amount > 0)) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('لم يتم تحديد السعر بعد')}`);
  }

  if (requestItem.paymentStatus === 'paid') {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('تم دفع الطلب بالفعل')}`);
  }

  const users = db.users();
  const buyer = users.find((item) => item && item.id === req.session.user.id && item.role === 'user');
  if (!buyer) return res.redirect('/');

  const enteredCardNumber = normalizeWalletCardNumber(req.body.walletCardNumber) || normalizeWalletCardNumber(buyer.walletCardNumber);
  const payer = findUserByWalletCardNumber({ users, walletCardNumber: enteredCardNumber });
  if (!payer) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('رقم بطاقة المحفظة غير صحيح')}`);
  }

  if (payer.walletCardFrozen) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة')}`);
  }

  if (Number(payer.walletBalance || 0) < amount) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('رصيد البطاقة غير كافٍ')}`);
  }

  try {
    enforceWalletCardSpendingLimit({ payerUser: payer, amount });
  } catch (limitError) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent(limitError.message || 'تم تجاوز حد البطاقة')}`);
  }

  if (payer.id === buyer.id) {
    finalizeCustomProjectPayment({
      requestId: requestItem.id,
      buyerUserId: buyer.id,
      payerUserId: payer.id
    });

    finalizeWalletCardOwnerActivity({
      payerUserId: payer.id,
      buyerUser: buyer,
      amount,
      kind: 'custom-project',
      payload: { customProjectRequestId: requestItem.id, title: requestItem.title },
      status: 'completed',
      note: 'تم الدفع مباشرة باستخدام بطاقتك الشخصية',
      notifyTitle: 'تم استخدام بطاقتك',
      notifyMessage: `استخدمت بطاقتك في دفع طلب المشروع المخصص: ${requestItem.title}.`
    });

    return res.redirect(`/custom-project/${requestItem.id}?success=${encodeURIComponent('تم دفع الطلب بنجاح')}`);
  }

  try {
    const attempt = await createWalletPaymentAttempt({
      buyerUser: buyer,
      payerUser: payer,
      amount,
      kind: 'custom-project',
      payload: {
        customProjectRequestId: requestItem.id,
        title: requestItem.title
      }
    });

    return res.redirect(`/payment/verify/${attempt.id}`);
  } catch (error) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('تعذر بدء عملية الدفع الآن')}`);
  }
});

app.post('/custom-project/:id/pay-wallet', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.id, userId: req.session.user.id });
  if (!requestItem) {
    return res.redirect('/custom-project?error=' + encodeURIComponent('طلب المشروع غير موجود'));
  }

  const amount = Math.round(Number(requestItem.quotedPrice || 0) * 100) / 100;
  if (!(amount > 0)) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('لم يتم تحديد السعر بعد')}`);
  }

  if (requestItem.paymentStatus === 'paid') {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('تم دفع الطلب بالفعل')}`);
  }

  const users = db.users();
  const buyerIndex = users.findIndex((item) => item && item.id === req.session.user.id && item.role === 'user');
  if (buyerIndex === -1) return res.redirect('/');

  const buyer = users[buyerIndex];
  if (buyer.walletCardFrozen) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة')}`);
  }

  const currentBalance = Number(buyer.walletBalance || 0);
  if (currentBalance < amount) {
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent('رصيد المحفظة غير كافٍ')}`);
  }

  users[buyerIndex].walletBalance = Math.round((currentBalance - amount) * 100) / 100;
  db.saveUsers(users);

  try {
    finalizeCustomProjectPayment({
      requestId: requestItem.id,
      buyerUserId: buyer.id,
      payerUserId: buyer.id,
      skipWalletDebit: true
    });
  } catch (error) {
    users[buyerIndex].walletBalance = currentBalance;
    db.saveUsers(users);
    return res.redirect(`/custom-project/${requestItem.id}?error=${encodeURIComponent(error.message || 'تعذر إتمام الدفع')}`);
  }

  const refreshedUsers = db.users();
  const refreshedBuyer = refreshedUsers.find((item) => item && item.id === buyer.id && item.role === 'user');
  if (refreshedBuyer) {
    req.session.user = buildSessionUser(refreshedBuyer);
  }

  return res.redirect(`/custom-project/${requestItem.id}?success=${encodeURIComponent('تم الدفع من رصيد المحفظة بنجاح')}`);
});

app.post('/wallet/topup/start', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const amountUsd = normalizeWalletTopupUsdAmount(req.body.amount);
  if (!amountUsd) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('مبلغ الشحن يجب أن يكون أكبر من صفر'));
  }

  const users = db.users();
  const currentUser = users.find((item) => item && item.id === req.session.user.id && item.role === 'user');
  if (!currentUser) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('المستخدم غير موجود'));
  }

  if (!isAtlosConfigured()) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('إعدادات Atlos غير مكتملة بعد'));
  }

  const topups = db.walletTopups();
  const topup = createWalletTopupRecord({ user: currentUser, amountUsd, exchangeRate: res.locals.displayCurrencyRate });
  topups.push(topup);
  db.saveWalletTopups(topups);

  try {
    const checkout = await createAtlosCheckout({ topup, user: currentUser });
    const refreshedTopups = db.walletTopups();
    const topupIndex = refreshedTopups.findIndex((item) => item && item.id === topup.id);
    if (topupIndex !== -1) {
      refreshedTopups[topupIndex] = {
        ...refreshedTopups[topupIndex],
        gatewayPaymentId: checkout.paymentId,
        checkoutUrl: checkout.checkoutUrl,
        updatedAt: new Date().toISOString()
      };
      db.saveWalletTopups(refreshedTopups);
    }

    return res.redirect(checkout.checkoutUrl);
  } catch (error) {
    finalizeWalletTopup({ topupId: topup.id, status: 'failed', failureReason: error.message || 'ATLOS_ERROR' });
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('تعذر بدء عملية الدفع عبر Atlos الآن'));
  }
});

app.get('/wallet/topup/return', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const reference = String(req.query.reference || req.query.OrderId || req.query.orderId || '').trim();
  const status = String(req.query.status || req.query.success || '').toLowerCase();
  const canceled = String(req.query.cancel || req.query.canceled || req.query.cancelled || '').toLowerCase();
  const pending = String(req.query.pending || '').toLowerCase();
  const transactionId = String(req.query.id || req.query.transaction_id || req.query.payment_id || '').trim() || null;

  if (!reference) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('تعذر تحديد عملية الشحن'));
  }

  const topups = db.walletTopups();
  const topup = topups.find((item) => item && item.reference === reference);
  if (!topup) {
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('عملية الشحن غير موجودة'));
  }

  if (status === 'success' || status === 'paid' || status === 'true' || req.query.success === true) {
    finalizeWalletTopup({ topupId: topup.id, gatewayTransactionId: transactionId, status: 'paid' });
    return res.redirect('/my-purchases?paymentSuccess=' + encodeURIComponent('تم شحن الرصيد بنجاح'));
  }

  const isCanceled = canceled === 'true' || canceled === '1' || canceled === 'yes' || canceled === 'y' || status === 'cancel' || status === 'canceled' || status === 'cancelled';

  if (isCanceled) {
    finalizeWalletTopup({ topupId: topup.id, gatewayTransactionId: transactionId, status: 'failed', failureReason: 'CANCELED_BY_USER' });
    return res.redirect('/my-purchases?paymentError=' + encodeURIComponent('تم إلغاء عملية الدفع. لم يتم خصم أي مبلغ.'));
  }

  if (status === 'pending' || pending === 'true') {
    return res.redirect('/my-purchases?paymentSuccess=' + encodeURIComponent('تم فتح صفحة الدفع، وسيتم تحديث الرصيد بعد تأكيد Atlos'));
  }

  const failureReason = status === 'cancel' || status === 'canceled' || status === 'cancelled'
    ? 'CANCELED_BY_USER'
    : 'RETURN_MARKED_FAILED';

  finalizeWalletTopup({ topupId: topup.id, gatewayTransactionId: transactionId, status: 'failed', failureReason });

  const message = isCanceled
    ? 'تم إلغاء عملية الدفع. لم يتم خصم أي مبلغ.'
    : 'فشلت عملية الدفع. حالة العملية: عملية غير ناجحة.';

  return res.redirect('/my-purchases?paymentError=' + encodeURIComponent(message));
});

app.post('/webhooks/atlos', (req, res) => {
  try {
    const providedSecret = String(req.get('x-atlos-secret') || req.get('x-webhook-secret') || '').trim();
    if (ATLOS_WEBHOOK_SECRET && providedSecret && providedSecret !== ATLOS_WEBHOOK_SECRET) {
      return res.status(200).json({ ok: false, skipped: true });
    }

    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const payload = body.data && typeof body.data === 'object' ? body.data : body;
    const numericStatus = Number(payload.Status != null ? payload.Status : payload.statusCode);
    const success = Boolean(
      payload.success === true ||
      numericStatus === 100 ||
      String(payload.status || '').toLowerCase() === 'success' ||
      String(payload.status || '').toLowerCase() === 'paid' ||
      String(payload.success || '').toLowerCase() === 'true' ||
      String(payload.txn_response_code || '') === 'APPROVED'
    );

    const merchantOrderId = String(
      payload.OrderId ||
      payload.reference ||
      payload.merchantReference ||
      payload.merchant_order_id ||
      payload.orderId ||
      ''
    );

    if (!merchantOrderId) {
      return res.status(200).json({ ok: true, skipped: true });
    }

    const topups = db.walletTopups();
    const topup = topups.find((item) => item && item.reference === merchantOrderId);
    if (!topup) {
      return res.status(200).json({ ok: true, skipped: true });
    }

    finalizeWalletTopup({
      topupId: topup.id,
      gatewayTransactionId: String(payload.paymentId || payload.id || payload.transaction_id || '') || null,
      status: success ? 'paid' : 'failed',
      failureReason: success ? null : 'ATLOS_WEBHOOK_FAILED'
    });

    return res.status(200).json({ ok: true });
  } catch (error) {
    return res.status(200).json({ ok: false });
  }
});

app.get('/loyalty', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = db.users();
  const currentUser = users.find(u => u.id === req.session.user.id);
  if (!currentUser) return res.redirect('/?error=' + encodeURIComponent('المستخدم غير موجود'));

  const settings = db.loyaltySettings();
  const redeem = (settings && settings.redeem) ? settings.redeem : {};
  const minPoints = normalizeLoyaltyPoints(redeem.minPoints);
  const pointsToEGP = Number(redeem.pointsToEGP || 0);

  req.session.user = {
    ...req.session.user,
    walletBalance: Number(currentUser.walletBalance || 0),
    loyaltyPoints: normalizeLoyaltyPoints(currentUser.loyaltyPoints)
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

app.post('/loyalty/redeem', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = db.users();
  const idx = users.findIndex(u => u.id === req.session.user.id);
  if (idx === -1) return res.redirect('/loyalty?error=' + encodeURIComponent('المستخدم غير موجود'));

  const settings = db.loyaltySettings();
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
  db.saveUsers(users);

  req.session.user = {
    ...req.session.user,
    walletBalance: Number(users[idx].walletBalance || 0),
    loyaltyPoints: normalizeLoyaltyPoints(users[idx].loyaltyPoints)
  };

  res.redirect('/loyalty?success=' + encodeURIComponent('تم الاستبدال بنجاح'));
});

// Wallet - redeem top-up code
app.post('/wallet/redeem', requireAuth, (req, res) => {
  const code = normalizeWalletCode(req.body.code);
  if (!code) return res.redirect(`/my-purchases?redeemError=${encodeURIComponent('الكود مطلوب')}`);

  const walletCodes = db.walletCodes();
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

  const users = db.users();
  const userIndex = users.findIndex(u => u.id === req.session.user.id);
  if (userIndex === -1) return res.status(404).send('User not found');

  users[userIndex].walletBalance = Number(users[userIndex].walletBalance || 0) + amount;
  db.saveUsers(users);

  walletCodes[idx].usedCount = Number(walletCodes[idx].usedCount || 0) + 1;
  walletCodes[idx].lastUsedAt = new Date().toISOString();
  db.saveWalletCodes(walletCodes);

  req.session.user = {
    ...req.session.user,
    walletBalance: Number(users[userIndex].walletBalance || 0)
  };

  return res.redirect(`/my-purchases?redeemSuccess=${encodeURIComponent('تم إضافة الرصيد بنجاح')}`);
});

app.post('/wallet-card/password', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const nextPassword = String(req.body.walletPassword || '').trim();
  const rawLimit = String(req.body.walletCardSpendingLimit || '').trim();
  const hasPasswordUpdate = nextPassword.length > 0;
  const hasLimitUpdate = req.body.walletCardSpendingLimit !== undefined;
  const nextLimit = rawLimit === '' ? null : sanitizeWalletCardSpendingLimitUsd(rawLimit);

  if (!hasPasswordUpdate && !hasLimitUpdate) {
    return res.redirect(`/my-purchases?walletCardError=${encodeURIComponent('لم يتم إرسال أي إعدادات جديدة للبطاقة')}`);
  }

  if (hasPasswordUpdate && nextPassword.length < 4) {
    return res.redirect(`/my-purchases?walletCardError=${encodeURIComponent('كلمة مرور البطاقة يجب ألا تقل عن 4 أحرف أو أرقام')}`);
  }

  if (rawLimit !== '' && nextLimit == null) {
    return res.redirect(`/my-purchases?walletCardError=${encodeURIComponent('حد البطاقة يجب أن يكون رقمًا أكبر من صفر')}`);
  }

  const users = db.users();
  const userIndex = users.findIndex(u => u && u.id === req.session.user.id && u.role === 'user');
  if (userIndex === -1) return res.status(404).send('User not found');

  if (ensureUserPaymentProfile({ user: users[userIndex], users })) {
    // profile fields are updated in place before saving
  }

  if (hasPasswordUpdate) {
    users[userIndex].walletPaymentPasswordHash = bcrypt.hashSync(nextPassword, 10);
  }

  if (hasLimitUpdate) {
    users[userIndex].walletCardSpendingLimit = nextLimit;
  }

  db.saveUsers(users);
  req.session.user = buildSessionUser(users[userIndex]);

  return res.redirect(`/my-purchases?walletCardSuccess=${encodeURIComponent('تم تحديث إعدادات البطاقة بنجاح')}`);
});

app.post('/wallet-card/freeze', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const users = db.users();
  const userIndex = users.findIndex(u => u && u.id === req.session.user.id && u.role === 'user');
  if (userIndex === -1) return res.status(404).send('User not found');

  const nextFrozen = !users[userIndex].walletCardFrozen;
  users[userIndex].walletCardFrozen = nextFrozen;
  db.saveUsers(users);
  req.session.user = buildSessionUser(users[userIndex]);

  const message = nextFrozen
    ? 'تم تجميد البطاقة بنجاح. لا يمكن لأحد استخدامها الآن.'
    : 'تم فك تجميد البطاقة بنجاح. يمكن استخدامها الآن.';
  return res.redirect(`/my-purchases?walletCardSuccess=${encodeURIComponent(message)}`);
});

// Protected download - only approved purchases can download
app.get('/download/:purchaseId', requireAuth, (req, res) => {
  const purchases = db.purchases();
  const purchase = purchases.find(p => p.id === req.params.purchaseId && p.userId === req.session.user.id);
  
  if (!purchase || !purchase.filePath) {
    return res.status(403).send('Access denied or file not available');
  }

  if (purchase.downloadLocked) {
    return res.status(403).send(purchase.downloadLockReason || 'تم قفل هذه النسخة من المشروع بواسطة الأدمن');
  }

  if (purchase.projectId) {
    const project = db.projects().find((p) => p && p.id === purchase.projectId);
    if (project && isProjectDownloadsLocked(project)) {
      return res.status(403).send(getProjectDownloadsLockReason(project) || 'تم قفل تنزيلات هذا المشروع مؤقتاً');
    }
  }
  
  if (purchase.status !== 'approved') {
    return res.status(403).send('Purchase not approved yet. Please wait for admin approval.');
  }
  
  const absoluteFilePath = toAbsolutePath(purchase.filePath);
  if (!absoluteFilePath || !fs.existsSync(absoluteFilePath)) {
    return res.status(404).send('File not found');
  }
  
  const downloadFileName = getDownloadFileName(purchase);
  recordDownloadEvent({
    req,
    kind: 'purchase',
    userId: req.session.user.id,
    purchaseId: purchase.id,
    projectId: purchase.projectId || null,
    meta: { via: 'session-download' }
  });
  res.download(absoluteFilePath, downloadFileName);
});

app.get('/custom-project/download/:requestId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const requestItem = getCustomProjectRequestForUser({ requestId: req.params.requestId, userId: req.session.user.id });
  if (!requestItem) return res.status(404).send('Custom project request not found');
  if (!requestItem.filePath) return res.status(404).send('File not found');

  const absolutePath = toAbsolutePath(requestItem.filePath);
  if (!absolutePath || !fs.existsSync(absolutePath)) return res.status(404).send('File not found');

  return res.download(absolutePath, requestItem.originalFileName || path.basename(absolutePath));
});

app.get('/admin/custom-projects/:id/download', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const requestItem = getCustomProjectRequestForAdmin(req.params.id);
  if (!requestItem) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('طلب المشروع غير موجود'));
  }
  if (!requestItem.filePath) {
    return res.redirect(`/admin/custom-projects/${req.params.id}?error=${encodeURIComponent('لا يوجد ملف مرفوع لهذا الطلب بعد')}`);
  }

  const absolutePath = toAbsolutePath(requestItem.filePath);
  if (!absolutePath || !fs.existsSync(absolutePath)) {
    return res.redirect(`/admin/custom-projects/${req.params.id}?error=${encodeURIComponent('ملف المشروع غير موجود على الخادم')}`);
  }

  return res.download(absolutePath, requestItem.originalFileName || path.basename(absolutePath));
});

// Admin Dashboard
app.get('/admin', requireAdmin, (req, res) => {
  const users = db.users();
  const projects = db.projects();
  const purchases = db.purchases();

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

// Admin Net Profit
app.get('/admin/net-profit', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const purchases = db.purchases();

  const now = new Date();
  const startOfDay = new Date(now);
  startOfDay.setHours(0, 0, 0, 0);

  const period = String(req.query.period || 'month');
  const getStart = () => {
    if (period === 'day') return startOfDay;
    if (period === 'week') {
      const d = new Date(startOfDay);
      d.setDate(d.getDate() - 6);
      return d;
    }
    if (period === 'year') {
      const d = new Date(startOfDay);
      d.setMonth(0, 1);
      return d;
    }
    // month (default)
    const d = new Date(startOfDay);
    d.setDate(1);
    return d;
  };

  const rangeStart = getStart();
  const rangeEnd = now;

  const approvedPurchases = purchases
    .filter((p) => p && p.status === 'approved')
    .filter((p) => {
      const d = p.approvedAt || p.purchasedAt || p.createdAt;
      const t = d ? Date.parse(d) : NaN;
      return Number.isFinite(t) && t >= rangeStart.getTime() && t <= rangeEnd.getTime();
    });

  const sum = (arr, selector) => arr.reduce((acc, item) => acc + Number(selector(item) || 0), 0);

  // NOTE: purchases are stored in EGP, but displayMoney() renders in the site display currency (USD).
  const grossSales = Math.round(sum(approvedPurchases, (p) => p.price) * 100) / 100;
  const grossBefore = Math.round(sum(approvedPurchases, (p) => p.priceBefore) * 100) / 100;
  const discounts = Math.round(sum(approvedPurchases, (p) => p.discountAmount) * 100) / 100;

  const refundedPurchases = approvedPurchases.filter((p) => p && p.walletRefundedAt);
  const refunds = Math.round(sum(refundedPurchases, (p) => p.walletDebitAmount || p.price || 0) * 100) / 100;

  const feePercent = Number(process.env.PAYMENT_FEE_PERCENT || 0);
  const feeFixedEgp = Number(process.env.PAYMENT_FEE_FIXED_EGP || 0);
  const feePercentSafe = Number.isFinite(feePercent) && feePercent >= 0 ? feePercent : 0;
  const feeFixedSafe = Number.isFinite(feeFixedEgp) && feeFixedEgp >= 0 ? feeFixedEgp : 0;

  // Estimate gateway fees only for purchases that were not paid by wallet.
  const estimatedFees = Math.round(
    approvedPurchases.reduce((acc, p) => {
      const paidByWallet = Number(p.walletDebitAmount || 0) > 0;
      if (paidByWallet) return acc;
      const amount = Number(p.price || 0);
      const percentFee = amount * (feePercentSafe / 100);
      return acc + percentFee + feeFixedSafe;
    }, 0) * 100
  ) / 100;

  const netProfit = Math.round((grossSales - refunds - estimatedFees) * 100) / 100;

  // Daily breakdown (last N days in the period)
  const byDay = new Map();
  const dayKey = (iso) => {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    d.setHours(0, 0, 0, 0);
    return d.toISOString().slice(0, 10);
  };

  for (const p of approvedPurchases) {
    const t = p.approvedAt || p.purchasedAt || p.createdAt;
    const key = t ? dayKey(t) : null;
    if (!key) continue;
    if (!byDay.has(key)) byDay.set(key, { day: key, count: 0, gross: 0, refunds: 0, fees: 0, net: 0 });
    const row = byDay.get(key);
    row.count += 1;
    row.gross += Number(p.price || 0);
    if (p.walletRefundedAt) row.refunds += Number(p.walletDebitAmount || p.price || 0);
    const paidByWallet = Number(p.walletDebitAmount || 0) > 0;
    if (!paidByWallet) {
      row.fees += Number(p.price || 0) * (feePercentSafe / 100) + feeFixedSafe;
    }
  }
  const dailyRows = Array.from(byDay.values())
    .map((r) => {
      const gross = Math.round(r.gross * 100) / 100;
      const refunds = Math.round(r.refunds * 100) / 100;
      const fees = Math.round(r.fees * 100) / 100;
      const net = Math.round((gross - refunds - fees) * 100) / 100;
      return { ...r, gross, refunds, fees, net };
    })
    .sort((a, b) => (a.day < b.day ? 1 : -1));

  res.render('admin/net-profit', {
    user: req.session.user,
    period,
    rangeStart: rangeStart.toISOString(),
    rangeEnd: rangeEnd.toISOString(),
    metrics: {
      grossSales,
      grossBefore,
      discounts,
      refunds,
      estimatedFees,
      netProfit
    },
    feeConfig: {
      feePercent: feePercentSafe,
      feeFixedEgp: feeFixedSafe
    },
    dailyRows
  });
});

// Admin Leak Radar (download anomaly detection)
app.get('/admin/leak-radar', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const users = db.users();
  const projects = db.projects();
  const purchases = db.purchases();
  const events = db.downloadEvents();

  const windowHours = parseEnvInt(process.env.LEAK_RADAR_WINDOW_HOURS, 24);
  const mediumDownloadThreshold = parseEnvInt(process.env.LEAK_RADAR_MEDIUM_DOWNLOADS, 5);
  const highDownloadThreshold = parseEnvInt(process.env.LEAK_RADAR_HIGH_DOWNLOADS, 10);
  const windowMs = windowHours * 60 * 60 * 1000;
  const since = Date.now() - windowMs;

  const userLabelById = new Map(
    users.filter(Boolean).map((u) => [u.id, u.email || u.name || u.id])
  );
  const projectTitleById = new Map(
    projects.filter(Boolean).map((p) => [p.id, p.title || p.id])
  );

  const approvedPurchaseById = new Map(
    purchases
      .filter((p) => p && p.status === 'approved')
      .map((p) => [p.id, p])
  );

  const recent = (Array.isArray(events) ? events : []).filter((e) => {
    const t = e && e.createdAt ? Date.parse(e.createdAt) : NaN;
    if (!Number.isFinite(t) || t < since) return false;
    return e && e.userId;
  });

  const keyFor = (e) => `${e.userId}::${e.projectId || ''}`;
  const groups = new Map();
  for (const e of recent) {
    const p = e.purchaseId ? approvedPurchaseById.get(e.purchaseId) : null;
    // Ignore malformed events that don't map to a purchase for "purchase" kind.
    if (e.kind === 'purchase' && !p) continue;
    const key = keyFor(e);
    if (!groups.has(key)) {
      groups.set(key, { userId: e.userId, projectId: e.projectId || null, ips: new Set(), devices: new Set(), count: 0, lastAt: e.createdAt });
    }
    const g = groups.get(key);
    g.count += 1;
    if (e.ip) g.ips.add(e.ip);
    if (e.device) g.devices.add(e.device);
    if (e.createdAt && (!g.lastAt || Date.parse(e.createdAt) > Date.parse(g.lastAt))) g.lastAt = e.createdAt;
  }

  const alerts = [];
  for (const g of groups.values()) {
    const ipCount = g.ips.size;
    const deviceCount = g.devices.size;
    const downloadCount = g.count;

    let severity = null;
    if (ipCount >= 3 || deviceCount >= 3 || downloadCount >= highDownloadThreshold) severity = 'high';
    else if (ipCount >= 2 || deviceCount >= 2 || downloadCount >= mediumDownloadThreshold) severity = 'medium';
    if (!severity) continue;

    alerts.push({
      severity,
      userId: g.userId,
      userLabel: userLabelById.get(g.userId) || g.userId,
      projectId: g.projectId,
      projectTitle: projectTitleById.get(g.projectId) || (g.projectId || 'غير معروف'),
      downloadCount,
      ipList: Array.from(g.ips).slice(0, 6),
      deviceList: Array.from(g.devices).slice(0, 6),
      lastAt: g.lastAt
    });
  }

  alerts.sort((a, b) => {
    const sev = (s) => (s === 'high' ? 2 : 1);
    return (sev(b.severity) - sev(a.severity)) || (Date.parse(b.lastAt) - Date.parse(a.lastAt));
  });

  res.render('admin/leak-radar', {
    user: req.session.user,
    alerts: alerts.slice(0, 200),
    windowHours,
    mediumDownloadThreshold,
    highDownloadThreshold
  });
});

// Admin AI Pricing (internal heuristic recommendations)
app.get('/admin/ai-pricing', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects().filter(Boolean);
  const purchases = db.purchases().filter((p) => p && p.status === 'approved');
  const views = db.projectViewEvents();

  const windowDays = parseEnvInt(process.env.AI_PRICING_WINDOW_DAYS, 30);
  const since = Date.now() - windowDays * 24 * 60 * 60 * 1000;

  const recentViews = (Array.isArray(views) ? views : []).filter((v) => {
    const t = v && v.createdAt ? Date.parse(v.createdAt) : NaN;
    return Number.isFinite(t) && t >= since && v.projectId;
  });
  const recentPurchases = purchases.filter((p) => {
    const d = p.approvedAt || p.purchasedAt || p.createdAt;
    const t = d ? Date.parse(d) : NaN;
    return Number.isFinite(t) && t >= since;
  });

  const viewCountByProject = new Map();
  for (const v of recentViews) {
    viewCountByProject.set(v.projectId, (viewCountByProject.get(v.projectId) || 0) + 1);
  }
  const purchaseCountByProject = new Map();
  const revenueByProject = new Map();
  for (const p of recentPurchases) {
    const id = p.projectId;
    if (!id) continue;
    purchaseCountByProject.set(id, (purchaseCountByProject.get(id) || 0) + 1);
    revenueByProject.set(id, (revenueByProject.get(id) || 0) + Number(p.price || 0));
  }

  const recommend = ({ viewsCount, purchasesCount, conversion }) => {
    // Heuristics tuned for software products:
    // - If lots of views with weak conversion => price probably high or page not convincing.
    // - If conversion strong and purchases decent => can increase slightly.
    if (viewsCount < 50 && purchasesCount < 2) {
      return { action: 'hold', label: 'ثبّت السعر', reason: 'بيانات قليلة خلال الفترة الحالية.' };
    }
    if (viewsCount >= 200 && conversion < 1.2) {
      return { action: 'decrease', label: 'خفض 10% - 20%', reason: 'Views عالية وتحويل ضعيف: جرّب خفض بسيط لرفع التحويل.' };
    }
    if (viewsCount >= 100 && conversion < 2.0) {
      return { action: 'decrease', label: 'خفض 5% - 10%', reason: 'تحويل أقل من المتوقع مقابل عدد Views جيد.' };
    }
    if (conversion >= 4.0 && purchasesCount >= 5) {
      return { action: 'increase', label: 'رفع 5% - 10%', reason: 'تحويل قوي ومبيعات جيدة: يمكن رفع السعر تدريجياً.' };
    }
    if (conversion >= 3.0 && purchasesCount >= 3) {
      return { action: 'increase', label: 'رفع 3% - 5%', reason: 'تحويل جيد جدًا: جرّب رفع بسيط مع مراقبة التحويل.' };
    }
    return { action: 'hold', label: 'ثبّت السعر', reason: 'الأداء متوازن خلال الفترة الحالية.' };
  };

  const rows = projects
    .filter((p) => p && p.id)
    .map((p) => {
      const viewsCount = Number(viewCountByProject.get(p.id) || 0);
      const purchasesCount = Number(purchaseCountByProject.get(p.id) || 0);
      const conversion = viewsCount > 0 ? (purchasesCount / viewsCount) * 100 : 0;
      const revenueEgp = Math.round(Number(revenueByProject.get(p.id) || 0) * 100) / 100;
      const priceEgp = Math.round(Number(p.price || 0) * 100) / 100;
      return {
        projectId: p.id,
        title: p.title || p.id,
        views: viewsCount,
        purchases: purchasesCount,
        conversionPercent: conversion,
        revenueEgp,
        priceEgp,
        recommendation: recommend({ viewsCount, purchasesCount, conversion })
      };
    })
    .sort((a, b) => (b.revenueEgp - a.revenueEgp) || (b.purchases - a.purchases) || (b.views - a.views));

  res.render('admin/ai-pricing', {
    user: req.session.user,
    windowDays,
    rows
  });
});

// Admin File Health (missing files / broken refs)
app.get('/admin/file-health', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const reports = db.fileHealthReports();
  const latestReport = Array.isArray(reports) && reports.length ? reports[0] : null;
  res.render('admin/file-health', {
    user: req.session.user,
    reports: Array.isArray(reports) ? reports : [],
    latestReport
  });
});

app.post('/admin/file-health/run', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  runFileHealthCheck();
  res.redirect('/admin/file-health');
});

app.get('/admin/live-feed', requireAdmin, (req, res) => {
  const audit = db.adminAuditLog();
  const seedEvents = (Array.isArray(audit) ? audit : [])
    .slice(0, 50)
    .map((entry) => ({
      type: 'admin-audit',
      title: 'تعديل إداري',
      message: `${entry.action} (${entry.entity})`,
      data: {
        adminEmail: entry.adminEmail || null,
        entityId: entry.entityId || null,
        meta: entry.meta || null
      },
      createdAt: entry.createdAt
    }));
  res.render('admin/live-feed', { user: req.session.user, seedEvents, maxEvents: MAX_LIVE_FEED_EVENTS });
});

// Admin Payments Inbox (failures / disputes / refunds)
app.get('/admin/payments-inbox', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const users = db.users();
  const purchases = db.purchases();
  const topups = db.walletTopups();

  const userLabelById = new Map(users.filter(Boolean).map((u) => [u.id, u.email || u.name || u.id]));

  const rejectedPurchases = purchases
    .filter((p) => p && p.status === 'rejected')
    .slice()
    .sort((a, b) => Date.parse(b.walletRefundedAt || b.purchasedAt || 0) - Date.parse(a.walletRefundedAt || a.purchasedAt || 0))
    .slice(0, 200)
    .map((p) => ({
      id: p.id,
      orderId: p.orderId || p.id,
      userId: p.userId,
      userLabel: userLabelById.get(p.userId) || p.userId,
      projectTitle: p.projectTitle || p.projectId || '-',
      amount: Number(p.price || 0),
      refunded: Boolean(p.walletRefundedAt),
      at: p.walletRefundedAt || p.purchasedAt || null
    }));

  const refundRows = purchases
    .filter((p) => p && p.walletRefundedAt)
    .slice()
    .sort((a, b) => Date.parse(b.walletRefundedAt || 0) - Date.parse(a.walletRefundedAt || 0))
    .slice(0, 200)
    .map((p) => ({
      id: p.id,
      orderId: p.orderId || p.id,
      userId: p.userId,
      userLabel: userLabelById.get(p.userId) || p.userId,
      projectTitle: p.projectTitle || p.projectId || '-',
      amount: Number(p.walletRefundAmount || p.walletDebitAmount || p.price || 0),
      at: p.walletRefundedAt
    }));

  const failedTopups = (Array.isArray(topups) ? topups : [])
    .filter((t) => t && t.status === 'failed')
    .slice()
    .sort((a, b) => Date.parse(b.updatedAt || b.createdAt || 0) - Date.parse(a.updatedAt || a.createdAt || 0))
    .slice(0, 200)
    .map((t) => ({
      id: t.id,
      reference: t.reference || null,
      userId: t.userId,
      userLabel: userLabelById.get(t.userId) || t.userId,
      amount: Number(t.amount || 0),
      currency: t.currency || null,
      gateway: t.gateway || null,
      failureReason: t.failureReason || null,
      updatedAt: t.updatedAt || t.createdAt || null
    }));

  res.render('admin/payments-inbox', {
    user: req.session.user,
    refundRows,
    failedTopups,
    rejectedPurchases
  });
});

app.get('/admin/community/jobs', requireSuperAdmin, (req, res) => {
  const jobs = getCommunityJobsState()
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  const applications = getCommunityJobApplicationsState()
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  const applicationsByJobId = buildCommunityJobApplicationsMap(applications);

  const jobsWithApplications = jobs.map(job => ({
    ...job,
    yearsLabel: formatYearsOfExperience(job.experienceYears),
    applications: (applicationsByJobId.get(job.id) || []).slice().map((application) => ({
      ...application,
      statusMeta: getCommunityApplicationStatusMeta(application.status),
      progressSteps: buildCommunityApplicationProgress(application.status, application.statusHistory),
      latestHistory: application.statusHistory[application.statusHistory.length - 1] || null,
      aiScreeningView: buildCommunityAiScreeningView(application.aiScreening)
    }))
  }));

  const statusCounts = applications.reduce((counts, application) => {
    const status = normalizeCommunityApplicationStatus(application.status);
    counts[status] = (counts[status] || 0) + 1;
    return counts;
  }, {});

  const stats = {
    totalJobs: jobs.length,
    activeJobs: jobs.filter(job => job && job.isActive !== false).length,
    totalApplications: applications.length,
    inProgressApplications: applications.filter((application) => !getCommunityApplicationStatusMeta(application.status).isFinal).length,
    hiredApplications: (statusCounts.hired || 0) + (statusCounts.accepted || 0),
    rejectedApplications: statusCounts.rejected || 0
  };

  res.render('admin/community-jobs', {
    user: req.session.user,
    jobs: jobsWithApplications,
    stats,
    applicationStatuses: COMMUNITY_APPLICATION_STATUSES,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/admin/community/jobs', requireSuperAdmin, (req, res) => {
  const title = (req.body.title || '').toString().trim();
  const salary = (req.body.salary || '').toString().trim();
  const description = (req.body.description || '').toString().trim();
  const experienceYears = Number(req.body.experienceYears || 0);

  if (!title || !salary || !description) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('اسم الوظيفة والوصف والأجر حقول مطلوبة'));
  }

  if (!Number.isFinite(experienceYears) || experienceYears < 0) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('عدد سنوات الخبرة غير صحيح'));
  }

  if (title.length > 200 || salary.length > 120 || description.length > COMMUNITY_JOB_TEXT_LIMIT) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('تحقق من طول البيانات المدخلة للوظيفة'));
  }

  const jobs = getCommunityJobsState();
  jobs.unshift({
    id: uuidv4(),
    title,
    salary,
    description,
    experienceYears,
    createdById: req.session.user.id,
    createdByName: req.session.user.name,
    isActive: true,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  });
  saveCommunityJobsState(jobs);

  res.redirect('/admin/community/jobs?success=' + encodeURIComponent('تم إضافة الوظيفة بنجاح'));
});

app.post('/admin/community/jobs/:id/toggle', requireSuperAdmin, (req, res) => {
  const jobs = getCommunityJobsState();
  const jobIndex = jobs.findIndex(job => job && job.id === req.params.id);
  if (jobIndex === -1) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('الوظيفة غير موجودة'));
  }

  jobs[jobIndex].isActive = jobs[jobIndex].isActive === false;
  jobs[jobIndex].updatedAt = new Date().toISOString();
  saveCommunityJobsState(jobs);

  res.redirect('/admin/community/jobs?success=' + encodeURIComponent(
    jobs[jobIndex].isActive ? 'تم فتح الوظيفة للتقديم' : 'تم إغلاق الوظيفة'
  ));
});

app.post('/admin/community/applications/:id/status', requireSuperAdmin, (req, res) => {
  const nextStatus = normalizeCommunityApplicationStatus(req.body.status);
  const note = (req.body.note || '').toString().trim();
  const applications = getCommunityJobApplicationsState();
  const applicationIndex = applications.findIndex((item) => item && item.id === req.params.id);

  if (applicationIndex === -1) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('طلب التقديم غير موجود'));
  }

  if (!COMMUNITY_APPLICATION_STATUS_MAP.has(nextStatus)) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('حالة التقديم غير صحيحة'));
  }

  if (note.length > COMMUNITY_APPLICATION_TRACKING_NOTE_LIMIT) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent(`ملاحظة المتابعة يجب ألا تتجاوز ${COMMUNITY_APPLICATION_TRACKING_NOTE_LIMIT} حرف`));
  }

  const application = normalizeCommunityJobApplication(applications[applicationIndex]);
  const now = new Date().toISOString();
  const hasStatusChanged = application.status !== nextStatus;
  const hasNoteChanged = note !== (application.trackingNote || '');

  if (!hasStatusChanged && !hasNoteChanged) {
    return res.redirect('/admin/community/jobs?success=' + encodeURIComponent('لا توجد تغييرات جديدة على حالة الطلب'));
  }

  application.status = nextStatus;
  application.statusUpdatedAt = now;
  application.trackingNote = note;
  application.statusHistory.push(buildCommunityApplicationHistoryEntry({
    status: nextStatus,
    note,
    updatedAt: now,
    updatedById: req.session.user.id,
    updatedByName: req.session.user.name,
    actorType: 'admin'
  }));

  if (nextStatus === 'hired') {
    application.hiredAt = now;
    application.rejectedAt = null;
  } else if (nextStatus === 'rejected') {
    application.rejectedAt = now;
    application.hiredAt = null;
  } else {
    application.hiredAt = null;
    application.rejectedAt = null;
  }

  applications[applicationIndex] = application;
  saveCommunityJobApplicationsState(applications);

  return res.redirect('/admin/community/jobs?success=' + encodeURIComponent('تم تحديث حالة طلب التقديم بنجاح'));
});

app.get('/admin/community/applications/:id/cv', requireSuperAdmin, async (req, res) => {
  const applications = getCommunityJobApplicationsState();
  let application = applications.find(item => item && item.id === req.params.id);
  if (!application || (!application.cvFilePath && !application.cvStorageKey)) {
    return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('السيرة الذاتية غير موجودة'));
  }

  const absolutePath = application.cvFilePath ? toAbsolutePath(application.cvFilePath) : null;
  if (absolutePath && fs.existsSync(absolutePath)) {
    if (usePostgresStorage() && !application.cvStorageKey) {
      try {
        const cvStorageKey = `community-cv:${application.id}`;
        await saveBinaryAsset({
          key: cvStorageKey,
          buffer: fs.readFileSync(absolutePath),
          mimeType: application.cvMimeType,
          fileName: application.cvOriginalName
        });

        const applicationIndex = applications.findIndex(item => item && item.id === application.id);
        if (applicationIndex !== -1) {
          applications[applicationIndex] = {
            ...applications[applicationIndex],
            cvStorageKey
          };
          saveCommunityJobApplicationsState(applications);
          application = applications[applicationIndex];
        }
      } catch (error) {
        console.error('Community CV asset migration error:', error);
      }
    }

    return res.download(absolutePath, application.cvOriginalName || path.basename(absolutePath));
  }

  if (application.cvStorageKey) {
    try {
      const asset = await getBinaryAsset(application.cvStorageKey);
      if (asset && asset.buffer) {
        if (application.cvMimeType || asset.mimeType) {
          res.type(application.cvMimeType || asset.mimeType);
        }
        res.attachment(application.cvOriginalName || asset.fileName || `cv-${application.id}`);
        return res.send(asset.buffer);
      }
    } catch (error) {
      console.error('Community CV asset read error:', error);
    }
  }

  return res.redirect('/admin/community/jobs?error=' + encodeURIComponent('ملف السيرة الذاتية غير موجود على الخادم'));
});

// Admin Team - Group chat for admins
app.get('/admin/team', requireAdmin, (req, res) => {
  const messages = db.adminTeamMessages()
    .slice()
    .sort((a, b) => new Date(a.createdAt || 0).getTime() - new Date(b.createdAt || 0).getTime());
  res.render('admin/team', { user: req.session.user, messages, error: req.query.error || null });
});

app.post('/admin/team/message', requireAdmin, (req, res) => {
  const content = (req.body.content || '').toString().trim();
  if (!content) return res.redirect('/admin/team?error=' + encodeURIComponent('اكتب رسالة'));

  const messages = db.adminTeamMessages();
  const msg = {
    id: uuidv4(),
    type: 'text',
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    content,
    createdAt: new Date().toISOString()
  };
  messages.push(msg);
  db.saveAdminTeamMessages(messages);

  io.to('admin-team').emit('admin-team-message', msg);
  res.redirect('/admin/team');
});

app.post('/admin/team/upload', requireAdmin, adminTeamUpload.single('file'), (req, res) => {
  if (!req.file) return res.redirect('/admin/team?error=' + encodeURIComponent('لم يتم رفع الملف'));

  const storedPath = `uploads/admin-team/${req.file.filename}`;
  const messages = db.adminTeamMessages();

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
  db.saveAdminTeamMessages(messages);

  io.to('admin-team').emit('admin-team-message', msg);
  res.redirect('/admin/team');
});

app.post('/admin/team/meeting', requireAdmin, (req, res) => {
  const roomId = `${uuidv4()}`;
  const meetingLink = `/meet/${roomId}`;

  const messages = db.adminTeamMessages();
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
  db.saveAdminTeamMessages(messages);

  io.to('admin-team').emit('admin-team-message', msg);
  res.redirect('/admin/team');
});

const isValidFutureSlot = (slot) => {
  if (!slot) return false;
  const startAt = slot.startAt ? new Date(slot.startAt) : null;
  if (!startAt || Number.isNaN(startAt.getTime())) return false;
  return startAt.getTime() > Date.now();
};

const getAdminUsers = () => {
  return db.users().filter(u => u.role === 'admin');
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

const getMeetingIceServers = () => {
  const fallback = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ];
  const raw = String(process.env.WEBRTC_ICE_SERVERS || '').trim();
  if (!raw) return fallback;

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || !parsed.length) return fallback;
    const sanitized = parsed.filter((entry) => entry && typeof entry === 'object' && entry.urls);
    return sanitized.length ? sanitized : fallback;
  } catch (error) {
    return fallback;
  }
};

const migrateAppointmentsBookingsMeetingLinks = () => {
  const data = db.appointments();
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
    db.saveAppointments({ timeSlots, bookings: nextBookings });
    return { timeSlots, bookings: nextBookings };
  }
  return { timeSlots, bookings };
};

const isLiveKitConfigured = () => Boolean(LIVEKIT_URL && LIVEKIT_API_KEY && LIVEKIT_API_SECRET);

const getMeetingAccessContext = ({ roomId, sessionUser }) => {
  const userId = sessionUser && sessionUser.id ? String(sessionUser.id) : '';
  const role = sessionUser && sessionUser.role ? String(sessionUser.role) : '';

  const appointmentsData = migrateAppointmentsBookingsMeetingLinks();
  const booking = (appointmentsData.bookings || []).find((item) => item && item.roomId === roomId) || null;

  const isTeamRoom = (db.adminTeamMessages() || []).some(
    (item) => item && item.type === 'meeting' && item.roomId === roomId
  );

  const bookingAllowed = Boolean(
    booking &&
    userId &&
    (
      String(booking.userId || '') === userId ||
      String(booking.adminId || '') === userId
    )
  );

  const teamAllowed = Boolean(isTeamRoom && role === 'admin');

  let returnUrl = '/my-appointments';
  if (booking && String(booking.adminId || '') === userId) {
    returnUrl = '/admin/appointments';
  } else if (teamAllowed) {
    returnUrl = '/admin/team';
  }

  return {
    booking,
    isTeamRoom,
    allowed: bookingAllowed || teamAllowed,
    returnUrl
  };
};

const createLiveKitMeetingToken = ({ roomId, user }) => {
  const token = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
    identity: String(user.id || `guest-${uuidv4()}`),
    name: String(user.name || 'Codentra User'),
    metadata: JSON.stringify({
      userId: String(user.id || ''),
      role: String(user.role || 'user')
    }),
    ttl: '2h'
  });

  token.addGrant({
    roomJoin: true,
    room: roomId,
    canPublish: true,
    canPublishData: true,
    canSubscribe: true
  });

  return token.toJwt();
};

// ========== APPOINTMENTS SYSTEM ==========

// User - Browse available admin slots
app.get('/appointments', requireAuth, (req, res) => {
  const admins = getAdminUsers();
  const data = migrateAppointmentsBookingsMeetingLinks();

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
app.post('/appointments/book', requireAuth, (req, res) => {
  const { slotId, notes } = req.body;
  if (!slotId) return res.redirect('/appointments?error=اختر ميعاد للحجز');

  const admins = getAdminUsers();
  const adminsById = new Map(admins.map(a => [a.id, a]));

  const data = db.appointments();
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

  db.saveAppointments({ timeSlots, bookings });
  res.redirect('/my-appointments?success=تم الحجز بنجاح');
});

// User - View my appointments
app.get('/my-appointments', requireAuth, (req, res) => {
  const data = migrateAppointmentsBookingsMeetingLinks();
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
app.get('/admin/appointments', requireAdminPermission(ADMIN_PERMISSIONS.appointments), (req, res) => {
  const data = migrateAppointmentsBookingsMeetingLinks();
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
app.post('/admin/appointments/slots', requireAdminPermission(ADMIN_PERMISSIONS.appointments), (req, res) => {
  const { date, time, durationMinutes } = req.body;
  if (!date || !time) return res.redirect('/admin/appointments?error=أدخل التاريخ والوقت');

  const duration = Number(durationMinutes || 30);
  if (!Number.isFinite(duration) || duration < 15 || duration > 240) {
    return res.redirect('/admin/appointments?error=مدة الجلسة غير صحيحة');
  }

  const startAt = new Date(`${date}T${time}:00`);
  if (Number.isNaN(startAt.getTime())) return res.redirect('/admin/appointments?error=التاريخ أو الوقت غير صحيح');
  if (startAt.getTime() <= Date.now() + 60 * 1000) return res.redirect('/admin/appointments?error=اختر وقت في المستقبل');

  const data = db.appointments();
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

  db.saveAppointments({ timeSlots, bookings });
  res.redirect('/admin/appointments?success=تم إضافة الموعد');
});

// Admin - Delete available slot
app.post('/admin/appointments/slots/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.appointments), (req, res) => {
  const slotId = req.params.id;
  const data = db.appointments();
  const timeSlots = Array.isArray(data.timeSlots) ? data.timeSlots : [];
  const bookings = Array.isArray(data.bookings) ? data.bookings : [];

  const adminId = req.session.user.id;
  const slotIndex = timeSlots.findIndex(s => s && s.id === slotId && s.adminId === adminId);
  if (slotIndex === -1) return res.redirect('/admin/appointments?error=الموعد غير موجود');

  const slot = timeSlots[slotIndex];
  if (slot.status !== 'available') return res.redirect('/admin/appointments?error=لا يمكن حذف موعد محجوز');

  timeSlots.splice(slotIndex, 1);
  db.saveAppointments({ timeSlots, bookings });
  res.redirect('/admin/appointments?success=تم حذف الموعد');
});

app.get('/meet/:roomId', requireAuth, (req, res) => {
  const roomId = req.params.roomId;
  const access = getMeetingAccessContext({ roomId, sessionUser: req.session.user });
  if (!access.booking && !access.isTeamRoom) return res.status(404).send('Meeting not found');
  if (!access.allowed) return res.status(403).send('Not allowed');

  res.render('meet', {
    user: req.session.user,
    roomId,
    currentUserId: req.session.user && req.session.user.id ? req.session.user.id : '',
    returnUrl: access.returnUrl,
    liveKitEnabled: isLiveKitConfigured()
  });
});

app.get('/meet/:roomId/token', requireAuth, async (req, res) => {
  const roomId = req.params.roomId;
  const access = getMeetingAccessContext({ roomId, sessionUser: req.session.user });
  if (!access.booking && !access.isTeamRoom) return res.status(404).json({ ok: false, error: 'Meeting not found' });
  if (!access.allowed) return res.status(403).json({ ok: false, error: 'Not allowed' });
  if (!isLiveKitConfigured()) {
    return res.status(503).json({
      ok: false,
      error: 'LiveKit is not configured',
      required: ['LIVEKIT_URL', 'LIVEKIT_API_KEY', 'LIVEKIT_API_SECRET']
    });
  }

  try {
    const token = await createLiveKitMeetingToken({
      roomId,
      user: req.session.user || {}
    });
    return res.json({
      ok: true,
      roomId,
      url: LIVEKIT_URL,
      token
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: 'Failed to create meeting token'
    });
  }
});

app.get('/meet/:roomId/signals', requireAuth, (req, res) => {
  return res.status(410).json({ ok: false, error: 'Legacy meeting signaling disabled. Use LiveKit.' });
});

app.post('/meet/:roomId/signals', requireAuth, (req, res) => {
  return res.status(410).json({ ok: false, error: 'Legacy meeting signaling disabled. Use LiveKit.' });
});

app.post('/meet/:roomId/recording', requireAdmin, meetingRecordingUpload.single('recording'), (req, res) => {
  const roomId = req.params.roomId;
  const access = getMeetingAccessContext({ roomId, sessionUser: req.session.user });
  if (!access.booking) return res.status(404).json({ ok: false, error: 'Meeting not found' });
  if (!access.allowed || !access.booking.adminId || access.booking.adminId !== req.session.user.id) {
    return res.status(403).json({ ok: false, error: 'Not allowed' });
  }
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
  const recordings = db.meetingRecordings();
  recordings.push({
    id: uuidv4(),
    roomId,
    bookingId: access.booking.id || null,
    adminId: access.booking.adminId,
    adminName: access.booking.adminName || null,
    userId: access.booking.userId,
    userName: access.booking.userName || null,
    startAt: access.booking.startAt || null,
    durationMinutes: access.booking.durationMinutes || null,
    filePath: storedPath,
    originalFileName: req.file.originalname || 'meeting.webm',
    mimeType: req.file.mimetype || null,
    size: req.file.size || null,
    sha256,
    createdAt: new Date().toISOString()
  });
  db.saveMeetingRecordings(recordings);

  res.json({ ok: true });
});

app.get('/admin/meeting-recordings', requireSuperAdmin, (req, res) => {
  const recordings = db.meetingRecordings()
    .slice()
    .sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
  res.render('admin/meeting-recordings', { user: req.session.user, recordings });
});

app.get('/admin/meeting-recordings/:id/verify', requireSuperAdmin, (req, res) => {
  const id = req.params.id;
  const recordings = db.meetingRecordings();
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
app.get('/admin/admins', requireSuperAdmin, (req, res) => {
  const users = db.users();
  const admins = users.filter(u => u.role === 'admin');
  res.render('admin/admins', {
    admins,
    user: req.session.user,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/admin/admins', requireSuperAdmin, (req, res) => {
  const name = (req.body.name || '').trim();
  const email = (req.body.email || '').trim().toLowerCase();
  const password = String(req.body.password || '');

  const users = db.users();

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
  db.saveUsers(users);

  return res.redirect(`/admin/admins?success=${encodeURIComponent('تم إضافة الأدمن بنجاح')}`);
});

app.post('/admin/admins/:id/permissions', requireSuperAdmin, (req, res) => {
  const adminId = req.params.id;
  const users = db.users();
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

  db.saveUsers(users);

  if (req.session.user && req.session.user.id === users[idx].id) {
    req.session.user.adminPermissions = users[idx].adminPermissions;
  }

  return res.redirect(`/admin/admins?success=${encodeURIComponent('تم حفظ الصلاحيات')}`);
});

// Admin - Add Project
app.get('/admin/projects/new', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  res.render('admin/project-form', { project: null, user: req.session.user });
});

app.post('/admin/projects', requireAdminPermission(ADMIN_PERMISSIONS.projects), projectUpload, async (req, res) => {
  try {
    const { title, description, price, category, technologies } = req.body;
    const priceUsd = Number(price);
    const visibility = (req.body.visibility || 'public').trim();
    const saleType = (req.body.saleType || '').trim();
    const saleValue = Number(req.body.saleValue || 0);
    const saleOccasion = normalizeOptionalText(req.body.saleOccasion);
    const saleDurationDays = normalizeOptionalDurationDays(req.body.saleDurationDays);
    const saleExpiresAt = resolveCouponExpiry({ expiresAt: parseOptionalIsoDate(req.body.saleExpiresAt), durationDays: saleDurationDays });
    const saleActive = Boolean(req.body.saleActive) && (saleType === 'percent' || saleType === 'fixed') && Number.isFinite(saleValue) && saleValue > 0;
    
    const projects = db.projects();
    
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
      price: convertUsdToEgp(priceUsd),
      category,
      technologies: technologies ? technologies.split(',').map(t => t.trim()) : [],
      visibility: (visibility === 'basic' || visibility === 'premium') ? visibility : 'public',
      downloadsLocked: Boolean(req.body.downloadsLocked),
      downloadsLockReason: normalizeOptionalText(req.body.downloadsLockReason) || null,
      downloadsLockedAt: Boolean(req.body.downloadsLocked) ? new Date().toISOString() : null,
      saleActive,
      saleType: saleActive ? saleType : null,
      saleValue: saleActive ? saleValue : 0,
      saleOccasion: saleOccasion || null,
      saleDurationDays,
      saleExpiresAt: saleActive ? saleExpiresAt : null,
      filePath: req.files && req.files.projectFile && req.files.projectFile[0] ? `uploads/${req.files.projectFile[0].filename}` : null,
      originalFileName: req.files && req.files.projectFile && req.files.projectFile[0] ? req.files.projectFile[0].originalname : null,
      images: images,
      createdAt: new Date().toISOString()
    };
    
    projects.push(newProject);
    db.saveProjects(projects);

    addAdminAuditLogEntry({
      req,
      action: 'create_project',
      entity: 'project',
      entityId: newProject.id,
      before: null,
      after: { title: newProject.title, price: newProject.price, visibility: newProject.visibility || 'public' }
    });
    emitLiveAdminEvent({
      type: 'project-create',
      title: 'إضافة مشروع',
      message: `تمت إضافة مشروع: ${newProject.title}`,
      severity: 'success',
      data: { projectId: newProject.id },
      createdAt: new Date().toISOString()
    });
    
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
app.get('/admin/projects/:id/edit', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects();
  const project = decorateProjectPricing(projects.find(p => p.id === req.params.id));
  if (!project) return res.status(404).send('Project not found');
  res.render('admin/project-form', { project, user: req.session.user });
});

app.post('/admin/projects/:id', requireAdminPermission(ADMIN_PERMISSIONS.projects), projectUpload, async (req, res) => {
  try {
    const { title, description, price, category, technologies } = req.body;
    const priceUsd = Number(price);
    const saleType = (req.body.saleType || '').trim();
    const saleValue = Number(req.body.saleValue || 0);
    const saleOccasion = normalizeOptionalText(req.body.saleOccasion);
    const saleDurationDays = normalizeOptionalDurationDays(req.body.saleDurationDays);
    const saleExpiresAt = resolveCouponExpiry({ expiresAt: parseOptionalIsoDate(req.body.saleExpiresAt), durationDays: saleDurationDays });
    const saleActive = Boolean(req.body.saleActive) && (saleType === 'percent' || saleType === 'fixed') && Number.isFinite(saleValue) && saleValue > 0;
    const projects = db.projects();
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
    
    const before = {
      title: projects[index].title,
      price: projects[index].price,
      visibility: projects[index].visibility || 'public',
      saleActive: Boolean(projects[index].saleActive),
      saleType: projects[index].saleType || null,
      saleValue: Number(projects[index].saleValue || 0),
      downloadsLocked: Boolean(projects[index].downloadsLocked),
      downloadsLockReason: projects[index].downloadsLockReason || null
    };

    projects[index] = {
      ...projects[index],
      title,
      description,
      price: convertUsdToEgp(priceUsd),
      category,
      technologies: technologies ? technologies.split(',').map(t => t.trim()) : [],
      visibility: (((req.body.visibility || projects[index].visibility || 'public').trim() === 'basic' || (req.body.visibility || projects[index].visibility || 'public').trim() === 'premium')
        ? (req.body.visibility || projects[index].visibility || 'public').trim()
        : 'public'),
      downloadsLocked: Boolean(req.body.downloadsLocked),
      downloadsLockReason: normalizeOptionalText(req.body.downloadsLockReason) || null,
      downloadsLockedAt: Boolean(req.body.downloadsLocked) ? (projects[index].downloadsLockedAt || new Date().toISOString()) : null,
      saleActive,
      saleType: saleActive ? saleType : null,
      saleValue: saleActive ? saleValue : 0,
      saleOccasion: saleOccasion || null,
      saleDurationDays,
      saleExpiresAt: saleActive ? saleExpiresAt : null,
      filePath: req.files && req.files.projectFile && req.files.projectFile[0] ? `uploads/${req.files.projectFile[0].filename}` : projects[index].filePath,
      originalFileName: req.files && req.files.projectFile && req.files.projectFile[0] ? req.files.projectFile[0].originalname : projects[index].originalFileName,
      images: images
    };
    
    db.saveProjects(projects);

    addAdminAuditLogEntry({
      req,
      action: 'update_project',
      entity: 'project',
      entityId: projects[index].id,
      before,
      after: {
        title: projects[index].title,
        price: projects[index].price,
        visibility: projects[index].visibility || 'public',
        saleActive: Boolean(projects[index].saleActive),
        saleType: projects[index].saleType || null,
        saleValue: Number(projects[index].saleValue || 0),
        downloadsLocked: Boolean(projects[index].downloadsLocked),
        downloadsLockReason: projects[index].downloadsLockReason || null
      }
    });

    emitLiveAdminEvent({
      type: 'project-update',
      title: 'تعديل مشروع',
      message: `تم تحديث المشروع: ${projects[index].title}`,
      severity: 'info',
      data: { projectId: projects[index].id },
      createdAt: new Date().toISOString()
    });
    res.redirect('/admin');
  } catch (error) {
    console.error('Error updating project:', error);
    const projects = db.projects();
    const project = projects.find(p => p.id === req.params.id);
    res.status(500).render('admin/project-form', { 
      project, 
      user: req.session.user,
      error: 'حدث خطأ أثناء تحديث المشروع. يرجى التأكد من حجم الملفات والمحاولة مرة أخرى.'
    });
  }
});

// Admin - Delete Project
app.post('/admin/projects/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects();
  const project = decorateProjectPricing(projects.find(p => p.id === req.params.id));
  
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
  
  db.saveProjects(projects.filter(p => p.id !== req.params.id));

  addAdminAuditLogEntry({
    req,
    action: 'delete_project',
    entity: 'project',
    entityId: req.params.id,
    before: project ? { title: project.title, price: project.price } : null,
    after: null
  });
  emitLiveAdminEvent({
    type: 'project-delete',
    title: 'حذف مشروع',
    message: `تم حذف مشروع: ${project ? project.title : req.params.id}`,
    severity: 'warning',
    data: { projectId: req.params.id },
    createdAt: new Date().toISOString()
  });
  res.redirect('/admin');
});

// Admin - View all purchases
app.get('/admin/purchases', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const purchases = db.purchases();
  const users = db.users();
  const projects = db.projects();
  const invoices = db.invoices();
  res.render('admin/purchases', { purchases, users, projects, invoices, user: req.session.user });
});

app.post('/admin/projects/:id/lock-downloads', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects();
  const idx = projects.findIndex((p) => p && p.id === req.params.id);
  if (idx === -1) return res.status(404).send('Project not found');
  const before = { downloadsLocked: Boolean(projects[idx].downloadsLocked), downloadsLockReason: projects[idx].downloadsLockReason || null };
  const reason = normalizeOptionalText(req.body.reason) || 'تم قفل تنزيلات هذا المشروع بواسطة الأدمن';
  projects[idx].downloadsLocked = true;
  projects[idx].downloadsLockReason = reason;
  projects[idx].downloadsLockedAt = new Date().toISOString();
  db.saveProjects(projects);
  addAdminAuditLogEntry({
    req,
    action: 'lock_project_downloads',
    entity: 'project',
    entityId: projects[idx].id,
    before,
    after: { downloadsLocked: true, downloadsLockReason: projects[idx].downloadsLockReason, downloadsLockedAt: projects[idx].downloadsLockedAt }
  });
  emitLiveAdminEvent({
    type: 'project-lock',
    title: 'قفل مشروع',
    message: `تم قفل تنزيلات المشروع: ${projects[idx].title || projects[idx].id}`,
    severity: 'warning',
    data: { projectId: projects[idx].id },
    createdAt: new Date().toISOString()
  });
  res.redirect('/admin');
});

app.post('/admin/projects/:id/unlock-downloads', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects();
  const idx = projects.findIndex((p) => p && p.id === req.params.id);
  if (idx === -1) return res.status(404).send('Project not found');
  const before = { downloadsLocked: Boolean(projects[idx].downloadsLocked), downloadsLockReason: projects[idx].downloadsLockReason || null };
  projects[idx].downloadsLocked = false;
  projects[idx].downloadsLockReason = null;
  projects[idx].downloadsLockedAt = null;
  db.saveProjects(projects);
  addAdminAuditLogEntry({
    req,
    action: 'unlock_project_downloads',
    entity: 'project',
    entityId: projects[idx].id,
    before,
    after: { downloadsLocked: false }
  });
  emitLiveAdminEvent({
    type: 'project-unlock',
    title: 'فك قفل مشروع',
    message: `تم فك قفل تنزيلات المشروع: ${projects[idx].title || projects[idx].id}`,
    severity: 'success',
    data: { projectId: projects[idx].id },
    createdAt: new Date().toISOString()
  });
  res.redirect('/admin');
});

app.get('/admin/custom-projects', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const requests = db.customProjectRequests().sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));
  res.render('admin/custom-projects', {
    user: req.session.user,
    requests,
    success: req.query.success || null,
    error: req.query.error || null
  });
});

app.get('/admin/custom-projects/:id', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const requestItem = getCustomProjectRequestForAdmin(req.params.id);
  if (!requestItem) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('الطلب غير موجود'));
  }

  const messages = db.messages().filter((message) => (
    message && message.customProjectRequestId === requestItem.id && (
      (message.senderId === requestItem.userId && message.receiverId === 'admin') ||
      (message.senderId === 'admin' && message.receiverId === requestItem.userId)
    )
  ));

  const allMessages = db.messages();
  let changed = false;
  allMessages.forEach((message) => {
    if (message && message.customProjectRequestId === requestItem.id && message.senderId === requestItem.userId && message.receiverId === 'admin' && !message.read) {
      message.read = true;
      changed = true;
    }
  });
  if (changed) db.saveMessages(allMessages);

  res.render('admin/custom-project-details', {
    user: req.session.user,
    requestItem,
    messages,
    success: req.query.success || null,
    error: req.query.error || null
  });
});

app.post('/admin/custom-projects/:id', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const requests = db.customProjectRequests();
  const index = requests.findIndex((item) => item && item.id === req.params.id);
  if (index === -1) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('الطلب غير موجود'));
  }

  const status = String(req.body.status || '').trim();
  const adminReply = String(req.body.adminReply || '').trim();
  const quotedPrice = req.body.quotedPrice === undefined || req.body.quotedPrice === '' ? null : Math.round(Number(req.body.quotedPrice) * 100) / 100;
  const quotedTimeline = String(req.body.quotedTimeline || '').trim();
  const allowedStatuses = new Set(['new', 'reviewing', 'quoted', 'accepted', 'completed', 'rejected']);
  if (!allowedStatuses.has(status)) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('الحالة غير صحيحة'));
  }
  if (quotedPrice !== null && (!Number.isFinite(quotedPrice) || quotedPrice <= 0)) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('السعر غير صحيح'));
  }

  requests[index] = {
    ...requests[index],
    status,
    adminReply: adminReply || null,
    quotedPrice,
    quotedTimeline: quotedTimeline || null,
    updatedAt: new Date().toISOString()
  };
  db.saveCustomProjectRequests(requests);

  try {
    const users = db.users();
    const userIndex = users.findIndex((item) => item && item.id === requests[index].userId && item.role === 'user');
    if (userIndex !== -1) {
      const extraMessage = requests[index].quotedPrice
        ? ` السعر المحدد حاليًا ${formatMoney(requests[index].quotedPrice)} جنيه${requests[index].quotedTimeline ? ` والمدة ${requests[index].quotedTimeline}` : ''}.`
        : '';
      const statusLabelMap = {
        new: 'تم استلام الطلب',
        reviewing: 'قيد المراجعة',
        quoted: 'تم إرسال رد على الطلب',
        accepted: 'تم قبول الطلب',
        rejected: 'تم رفض الطلب'
      };
      addUserNotificationEntry({
        targetUser: users[userIndex],
        type: 'custom-project-update',
        title: 'تحديث على طلب مشروعك المخصص',
        message: `${statusLabelMap[status] || 'تم تحديث الطلب'}: ${requests[index].title}.${extraMessage}`,
        metadata: {
          customProjectRequestId: requests[index].id,
          projectTitle: requests[index].title,
          amount: requests[index].quotedPrice || null
        }
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore notification errors
  }

  return res.redirect('/admin/custom-projects?success=' + encodeURIComponent('تم تحديث الطلب'));
});

app.post('/admin/custom-projects/:id/messages', requireAdminPermission(ADMIN_PERMISSIONS.messages), (req, res) => {
  const requestItem = getCustomProjectRequestForAdmin(req.params.id);
  if (!requestItem) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('الطلب غير موجود'));
  }

  const content = String(req.body.content || '').trim();
  if (!content) {
    return res.redirect(`/admin/custom-projects/${requestItem.id}`);
  }

  const messages = db.messages();
  messages.push({
    id: uuidv4(),
    senderId: 'admin',
    senderName: 'Admin',
    receiverId: requestItem.userId,
    content,
    read: false,
    customProjectRequestId: requestItem.id,
    projectTitle: requestItem.title,
    createdAt: new Date().toISOString()
  });
  db.saveMessages(messages);

  try {
    const users = db.users();
    const userIndex = users.findIndex((item) => item && item.id === requestItem.userId && item.role === 'user');
    if (userIndex !== -1) {
      addUserNotificationEntry({
        targetUser: users[userIndex],
        type: 'custom-project-message',
        title: 'رسالة جديدة بخصوص مشروعك المخصص',
        message: `لديك رسالة جديدة بخصوص طلب ${requestItem.title}.`,
        metadata: {
          customProjectRequestId: requestItem.id,
          projectTitle: requestItem.title
        }
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore
  }

  return res.redirect(`/admin/custom-projects/${requestItem.id}?success=${encodeURIComponent('تم إرسال الرسالة')}`);
});

app.post('/admin/custom-projects/:id/upload', requireAdminPermission(ADMIN_PERMISSIONS.purchases), upload.single('deliveryFile'), (req, res) => {
  const requests = db.customProjectRequests();
  const index = requests.findIndex((item) => item && item.id === req.params.id);
  if (index === -1) {
    return res.redirect('/admin/custom-projects?error=' + encodeURIComponent('الطلب غير موجود'));
  }

  if (!req.file) {
    return res.redirect(`/admin/custom-projects/${req.params.id}?error=${encodeURIComponent('اختر ملفًا أولًا')}`);
  }

  requests[index] = {
    ...requests[index],
    filePath: `uploads/${req.file.filename}`,
    originalFileName: req.file.originalname,
    fileUploadedAt: new Date().toISOString(),
    status: 'completed',
    updatedAt: new Date().toISOString()
  };
  db.saveCustomProjectRequests(requests);

  try {
    const users = db.users();
    const userIndex = users.findIndex((item) => item && item.id === requests[index].userId && item.role === 'user');
    if (userIndex !== -1) {
      addUserNotificationEntry({
        targetUser: users[userIndex],
        type: 'custom-project-file',
        title: 'تم رفع ملف مشروعك المخصص',
        message: `تم رفع الملف النهائي لطلب ${requests[index].title} ويمكنك تحميله الآن.`,
        metadata: {
          customProjectRequestId: requests[index].id,
          projectTitle: requests[index].title
        }
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore
  }

  return res.redirect(`/admin/custom-projects/${req.params.id}?success=${encodeURIComponent('تم رفع الملف بنجاح')}`);
});


// Admin - Product Sales (per-product temporary discounts)
app.get('/admin/sales', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects();
  const decorated = projects.map(p => decorateProjectPricing(p));
  const now = Date.now();
  const active = decorated.filter(p => p && p.sale && p.sale.active);
  const expired = decorated.filter(p => p && p.sale && !p.sale.active && (p.saleOccasion || p.saleExpiresAt || p.saleDurationDays || p.saleValue));

  expired.sort((a, b) => {
    const ta = a.saleExpiresAt ? new Date(a.saleExpiresAt).getTime() : 0;
    const tb = b.saleExpiresAt ? new Date(b.saleExpiresAt).getTime() : 0;
    return tb - ta;
  });

  res.render('admin/sales', { user: req.session.user, activeSales: active, expiredSales: expired, now });
});

app.post('/admin/sales/:id/disable', requireAdminPermission(ADMIN_PERMISSIONS.projects), (req, res) => {
  const projects = db.projects();
  const idx = projects.findIndex(p => p && p.id === req.params.id);
  if (idx === -1) return res.redirect('/admin/sales');
  projects[idx] = {
    ...projects[idx],
    saleActive: false,
    saleType: null,
    saleValue: 0,
    saleOccasion: null,
    saleDurationDays: null,
    saleExpiresAt: null
  };
  db.saveProjects(projects);
  res.redirect('/admin/sales');
});

// Admin - Coupons
app.get('/admin/coupons', requireAdminPermission(ADMIN_PERMISSIONS.coupons), (req, res) => {
  const coupons = db.coupons();
  res.render('admin/coupons', { coupons, user: req.session.user, error: null });
});

// Admin - Subscription Coupons
app.get('/admin/subscription-coupons', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), (req, res) => {
  const coupons = db.subscriptionCoupons();
  res.render('admin/subscription-coupons', { coupons, user: req.session.user, error: null });
});

app.post('/admin/subscription-coupons', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), (req, res) => {
  const code = normalizeCouponCode(req.body.code);
  const type = (req.body.type || '').trim();
  const value = Number(req.body.value);
  const usageLimit = req.body.usageLimit ? Number(req.body.usageLimit) : null;
  const occasion = normalizeOptionalText(req.body.occasion);
  const durationDays = normalizeOptionalDurationDays(req.body.durationDays);
  const expiresAt = resolveCouponExpiry({ expiresAt: parseOptionalIsoDate(req.body.expiresAt), durationDays });

  const coupons = db.subscriptionCoupons();

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
    occasion: occasion || null,
    durationDays,
    expiresAt,
    createdAt: new Date().toISOString(),
    lastUsedAt: null
  });

  db.saveSubscriptionCoupons(coupons);
  res.redirect('/admin/subscription-coupons');
});

app.post('/admin/subscription-coupons/:id/toggle', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), (req, res) => {
  const coupons = db.subscriptionCoupons();
  const idx = coupons.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).send('Coupon not found');
  coupons[idx].active = !coupons[idx].active;
  db.saveSubscriptionCoupons(coupons);
  res.redirect('/admin/subscription-coupons');
});

app.post('/admin/subscription-coupons/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionCoupons), (req, res) => {
  const coupons = db.subscriptionCoupons();
  db.saveSubscriptionCoupons(coupons.filter(c => c.id !== req.params.id));
  res.redirect('/admin/subscription-coupons');
});

// Admin - Subscription Reports
app.get('/admin/subscription-reports', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionReports), (req, res) => {
  const users = db.users();
  const plans = db.subscriptionPlans();
  const paymentsAll = db.subscriptionPayments();

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
app.get('/admin/subscription-plans', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionPlans), (req, res) => {
  const plans = db.subscriptionPlans();
  res.render('admin/subscription-plans', {
    user: req.session.user,
    plans,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/admin/subscription-plans/:id', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionPlans), (req, res) => {
  const plans = db.subscriptionPlans();
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

  db.saveSubscriptionPlans(plans);
  res.redirect('/admin/subscription-plans?success=' + encodeURIComponent('تم حفظ الخطة'));
});

app.post('/admin/subscription-plans/:id/toggle', requireAdminPermission(ADMIN_PERMISSIONS.subscriptionPlans), (req, res) => {
  const plans = db.subscriptionPlans();
  const idx = plans.findIndex(p => p && p.id === req.params.id);
  if (idx === -1) return res.redirect('/admin/subscription-plans?error=' + encodeURIComponent('الخطة غير موجودة'));
  plans[idx].active = !plans[idx].active;
  db.saveSubscriptionPlans(plans);
  res.redirect('/admin/subscription-plans?success=' + encodeURIComponent('تم تحديث حالة الخطة'));
});

app.post('/admin/coupons', requireAdminPermission(ADMIN_PERMISSIONS.coupons), (req, res) => {
  const code = normalizeCouponCode(req.body.code);
  const type = (req.body.type || '').trim();
  const value = Number(req.body.value);
  const occasion = normalizeOptionalText(req.body.occasion);
  const durationDays = normalizeOptionalDurationDays(req.body.durationDays);
  const expiresAt = resolveCouponExpiry({ expiresAt: parseOptionalIsoDate(req.body.expiresAt), durationDays });
  const usageLimit = req.body.usageLimit ? Number(req.body.usageLimit) : null;

  const coupons = db.coupons();

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
    occasion: occasion || null,
    durationDays,
    expiresAt,
    createdAt: new Date().toISOString()
  });

  db.saveCoupons(coupons);
  res.redirect('/admin/coupons');
});

app.post('/admin/coupons/:id/toggle', requireAdminPermission(ADMIN_PERMISSIONS.coupons), (req, res) => {
  const coupons = db.coupons();
  const idx = coupons.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).send('Coupon not found');
  coupons[idx].active = !coupons[idx].active;
  db.saveCoupons(coupons);
  res.redirect('/admin/coupons');
});

app.post('/admin/coupons/:id/delete', requireAdminPermission(ADMIN_PERMISSIONS.coupons), (req, res) => {
  const coupons = db.coupons();
  db.saveCoupons(coupons.filter(c => c.id !== req.params.id));
  res.redirect('/admin/coupons');
});

app.get('/admin/reviews', requireAdminPermission(ADMIN_PERMISSIONS.reviews), (req, res) => {
  const reviews = db.reviews();
  const users = db.users();
  const projects = db.projects();
  res.render('admin/reviews', { reviews, users, projects, user: req.session.user });
});

app.post('/admin/reviews/:id/delete', requireAdmin, (req, res) => {
  const reviews = db.reviews();
  db.saveReviews(reviews.filter(r => r.id !== req.params.id));
  res.redirect('/admin/reviews');
});

// Admin - View all users
app.get('/admin/users', requireAdminPermission(ADMIN_PERMISSIONS.users), (req, res) => {
  const users = db.users();
  res.render('admin/users', { users, user: req.session.user, success: req.query.success || null, error: req.query.error || null });
});

app.post('/admin/users/:id/block', requireAdminPermission(ADMIN_PERMISSIONS.users), (req, res) => {
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

    const users = db.users();
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

    db.saveUsers(users);
    return res.redirect('/admin/users?success=تم حظر المستخدم بنجاح');
  } catch (e) {
    return res.redirect('/admin/users?error=حدث خطأ أثناء حظر المستخدم');
  }
});

app.post('/admin/users/:id/unblock', requireAdminPermission(ADMIN_PERMISSIONS.users), (req, res) => {
  try {
    const targetId = req.params.id;
    const users = db.users();
    const idx = users.findIndex(u => u && u.id === targetId);
    if (idx === -1) return res.redirect('/admin/users?error=المستخدم غير موجود');

    const target = users[idx];
    const isPermanent = target.isBlocked && !target.blockedUntil;
    if (isPermanent && !(req.session.user && req.session.user.isSuperAdmin)) {
      return res.redirect('/admin/users?error=لا يمكن فك الحظر الدائم إلا بواسطة السوبر أدمن');
    }

    unblockUserInPlace(target);
    users[idx] = target;
    db.saveUsers(users);
    return res.redirect('/admin/users?success=تم فك الحظر بنجاح');
  } catch (e) {
    return res.redirect('/admin/users?error=حدث خطأ أثناء فك الحظر');
  }
});

// Admin - Wallet Balances
app.get('/admin/wallet-balances', requireAdminPermission(ADMIN_PERMISSIONS.walletBalances), (req, res) => {
  const users = db.users();
  res.render('admin/wallet-balances', { users, user: req.session.user, error: null, success: null });
});

app.post('/admin/wallet-balances/:userId', requireAdminPermission(ADMIN_PERMISSIONS.walletBalances), (req, res) => {
  const { action } = req.body;
  const amountUsd = Number(req.body.amount);

  if (!['set', 'add', 'subtract'].includes(action)) {
    const users = db.users();
    return res.render('admin/wallet-balances', { users, user: req.session.user, error: 'عملية غير صحيحة', success: null });
  }
  if (!Number.isFinite(amountUsd) || amountUsd < 0) {
    const users = db.users();
    return res.render('admin/wallet-balances', { users, user: req.session.user, error: 'قيمة الرصيد غير صحيحة', success: null });
  }

  const users = db.users();
  const idx = users.findIndex(u => u.id === req.params.userId);
  if (idx === -1) {
    return res.status(404).send('User not found');
  }

  const amount = convertUsdToEgp(amountUsd);
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
  db.saveUsers(users);
  return res.render('admin/wallet-balances', { users, user: req.session.user, error: null, success: 'تم تحديث الرصيد بنجاح' });
});

// Admin - Approve/Reject purchase
app.post('/admin/purchases/:id/approve', requireAdmin, (req, res) => {
  const purchases = db.purchases();
  const index = purchases.findIndex(p => p.id === req.params.id);
  if (index === -1) return res.status(404).send('Purchase not found');
  
  const before = { status: purchases[index].status, approvedAt: purchases[index].approvedAt || null };
  purchases[index].status = 'approved';
  purchases[index].approvedAt = new Date().toISOString();
  db.savePurchases(purchases);
  addAdminAuditLogEntry({
    req,
    action: 'approve_purchase',
    entity: 'purchase',
    entityId: purchases[index].id,
    before,
    after: { status: purchases[index].status, approvedAt: purchases[index].approvedAt },
    meta: { projectId: purchases[index].projectId || null, userId: purchases[index].userId || null }
  });
  emitLiveAdminEvent({
    type: 'purchase-approve',
    title: 'موافقة شراء',
    message: `تمت الموافقة على شراء: ${purchases[index].projectTitle || purchases[index].projectId || purchases[index].id}`,
    severity: 'success',
    data: { purchaseId: purchases[index].id, projectId: purchases[index].projectId || null, userId: purchases[index].userId || null },
    createdAt: new Date().toISOString()
  });

  const approvedPurchase = purchases[index];
  // Create invoice after approval (only once per order)
  try {
    const invoices = db.invoices();
    const orderId = approvedPurchase.orderId || approvedPurchase.id;
    const already = invoices.some(inv => inv && inv.orderId === orderId);
    if (!already) {
      const users = db.users();
      const buyer = users.find(u => u && u.id === approvedPurchase.userId) || null;
      const payer = users.find(u => u && u.id === approvedPurchase.payerUserId) || null;

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
        paymentMethod: 'Wallet Card',
        payerUserId: approvedPurchase.payerUserId || null,
        payerName: payer ? (payer.name || payer.email || payer.id) : (approvedPurchase.payerUserId || null),
        payerEmail: payer ? (payer.email || null) : null,
        walletCardMasked: approvedPurchase.payerCardLast4 ? `**** **** **** ${approvedPurchase.payerCardLast4}` : null,
        couponCode,
        items,
        totalBefore,
        totalDiscount,
        totalAfter,
        createdAt: new Date().toISOString()
      });
      db.saveInvoices(invoices);
    }
  } catch (e) {
    // ignore invoice errors
  }

  try {
    const users = db.users();
    const buyerIndex = users.findIndex((item) => item && item.id === approvedPurchase.userId && item.role === 'user');
    if (buyerIndex !== -1) {
      addUserNotificationEntry({
        targetUser: users[buyerIndex],
        type: 'purchase-approved',
        title: 'تمت الموافقة على طلبك',
        message: `تمت الموافقة على شراء ${approvedPurchase.projectTitle || 'المشروع'} ويمكنك متابعة الطلب من مشترياتي.`,
        metadata: {
          purchaseId: approvedPurchase.id,
          orderId: approvedPurchase.orderId || approvedPurchase.id,
          projectTitle: approvedPurchase.projectTitle || null
        }
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore notification errors
  }
  const referrals = db.referrals();
  const pendingReferralIndex = referrals.findIndex(
    r => r.referredUserId === approvedPurchase.userId && r.status === 'pending'
  );
  if (pendingReferralIndex !== -1) {
    const referral = referrals[pendingReferralIndex];
    const users = db.users();
    const referrerIndex = users.findIndex(u => u.id === referral.referrerUserId);
    if (referrerIndex !== -1) {
      const rewardAmount = Number(referral.rewardAmount || 0);
      const rewardType = String(referral.rewardType || 'loyalty-points');
      if (rewardType === 'wallet-balance') {
        users[referrerIndex].walletBalance = Number(users[referrerIndex].walletBalance || 0) + rewardAmount;
      } else {
        users[referrerIndex].loyaltyPoints = normalizeLoyaltyPoints(Number(users[referrerIndex].loyaltyPoints || 0) + rewardAmount);
      }
      db.saveUsers(users);
    }

    referrals[pendingReferralIndex].status = 'rewarded';
    referrals[pendingReferralIndex].rewardedAt = new Date().toISOString();
    referrals[pendingReferralIndex].rewardPurchaseId = approvedPurchase.id;
    db.saveReferrals(referrals);
  }

  res.redirect('/admin/purchases');
});

app.post('/admin/purchases/:id/upload', requireAdminPermission(ADMIN_PERMISSIONS.purchases), upload.single('modifiedFile'), (req, res) => {
  const purchases = db.purchases();
  const purchaseIndex = purchases.findIndex((purchase) => purchase && purchase.id === req.params.id);
  if (purchaseIndex === -1) return res.status(404).send('Purchase not found');

  if (!req.file) {
    return res.status(400).send('Modified file is required');
  }

  purchases[purchaseIndex].filePath = `uploads/${req.file.filename}`;
  purchases[purchaseIndex].originalFileName = req.file.originalname;
  purchases[purchaseIndex].isModified = true;
  purchases[purchaseIndex].modifiedAt = new Date().toISOString();
  purchases[purchaseIndex].modificationNote = req.body.note || purchases[purchaseIndex].modificationNote || 'Admin uploaded the final project file';
  db.savePurchases(purchases);

  try {
    const users = db.users();
    const buyerIndex = users.findIndex((item) => item && item.id === purchases[purchaseIndex].userId && item.role === 'user');
    if (buyerIndex !== -1) {
      addUserNotificationEntry({
        targetUser: users[buyerIndex],
        type: 'purchase-file-uploaded',
        title: 'تم رفع ملف المشروع',
        message: `الأدمن رفع الملف النهائي لمشروع ${purchases[purchaseIndex].projectTitle || 'المشروع'} ويمكنك تحميله الآن.`,
        metadata: {
          purchaseId: purchases[purchaseIndex].id,
          orderId: purchases[purchaseIndex].orderId || purchases[purchaseIndex].id,
          projectTitle: purchases[purchaseIndex].projectTitle || null
        }
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore notification errors
  }

  res.redirect('/admin/purchases');
});

app.post('/admin/purchases/:id/lock-download', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const purchases = db.purchases();
  const idx = purchases.findIndex((p) => p && p.id === req.params.id);
  if (idx === -1) return res.status(404).send('Purchase not found');

  const before = { downloadLocked: Boolean(purchases[idx].downloadLocked), downloadLockReason: purchases[idx].downloadLockReason || null };
  const reason = (req.body.reason || '').toString().trim();
  purchases[idx].downloadLocked = true;
  purchases[idx].downloadLockReason = reason || 'تم قفل هذه النسخة بواسطة الأدمن';
  purchases[idx].downloadLockedAt = new Date().toISOString();
  db.savePurchases(purchases);

  addAdminAuditLogEntry({
    req,
    action: 'lock_download',
    entity: 'purchase',
    entityId: purchases[idx].id,
    before,
    after: { downloadLocked: true, downloadLockReason: purchases[idx].downloadLockReason, downloadLockedAt: purchases[idx].downloadLockedAt },
    meta: { projectId: purchases[idx].projectId || null, userId: purchases[idx].userId || null }
  });
  emitLiveAdminEvent({
    type: 'lock',
    title: 'قفل نسخة',
    message: `تم قفل نسخة شراء (${purchases[idx].projectTitle || purchases[idx].projectId || purchases[idx].id})`,
    severity: 'warning',
    data: { purchaseId: purchases[idx].id, projectId: purchases[idx].projectId || null, userId: purchases[idx].userId || null },
    createdAt: new Date().toISOString()
  });
  res.redirect('/admin/purchases');
});

app.post('/admin/purchases/:id/unlock-download', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const purchases = db.purchases();
  const idx = purchases.findIndex((p) => p && p.id === req.params.id);
  if (idx === -1) return res.status(404).send('Purchase not found');

  const before = { downloadLocked: Boolean(purchases[idx].downloadLocked), downloadLockReason: purchases[idx].downloadLockReason || null };
  purchases[idx].downloadLocked = false;
  purchases[idx].downloadLockReason = null;
  purchases[idx].downloadLockedAt = null;
  db.savePurchases(purchases);

  addAdminAuditLogEntry({
    req,
    action: 'unlock_download',
    entity: 'purchase',
    entityId: purchases[idx].id,
    before,
    after: { downloadLocked: false },
    meta: { projectId: purchases[idx].projectId || null, userId: purchases[idx].userId || null }
  });
  emitLiveAdminEvent({
    type: 'unlock',
    title: 'فك قفل نسخة',
    message: `تم فك قفل نسخة شراء (${purchases[idx].projectTitle || purchases[idx].projectId || purchases[idx].id})`,
    severity: 'success',
    data: { purchaseId: purchases[idx].id, projectId: purchases[idx].projectId || null, userId: purchases[idx].userId || null },
    createdAt: new Date().toISOString()
  });
  res.redirect('/admin/purchases');
});

// Invoice PDF (available after approval)
app.get('/invoice/:orderId.pdf', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');
  const orderId = req.params.orderId;
  const invoices = db.invoices();
  const inv = invoices.find(i => i && i.orderId === orderId && i.userId === req.session.user.id) || null;
  if (!inv) return res.status(404).send('Invoice not found');

  res.render('invoice', buildInvoiceViewModel(inv, { includeEmail: true, includeCoupon: true }));
});

app.get('/admin/invoice/:orderId.pdf', requireAdminPermission(ADMIN_PERMISSIONS.purchases), (req, res) => {
  const orderId = req.params.orderId;
  const invoices = db.invoices();
  const inv = invoices.find(i => i && i.orderId === orderId) || null;
  if (!inv) return res.status(404).send('Invoice not found');

  res.render('invoice', buildInvoiceViewModel(inv, { includeEmail: true, includeCoupon: true, includePayerEmail: true, adminMode: true }));
});

// Admin - Referrals
app.get('/admin/referrals', requireAdminPermission(ADMIN_PERMISSIONS.referrals), (req, res) => {
  const referrals = db.referrals();
  const users = db.users();
  res.render('admin/referrals', { referrals, users, user: req.session.user });
});

app.post('/admin/purchases/:id/reject', requireAdmin, (req, res) => {
  const purchases = db.purchases();
  const index = purchases.findIndex(p => p.id === req.params.id);
  if (index === -1) return res.status(404).send('Purchase not found');

  const before = { status: purchases[index].status, walletRefundedAt: purchases[index].walletRefundedAt || null, walletRefundAmount: purchases[index].walletRefundAmount || null };
  const purchase = purchases[index];
  if (purchase.status !== 'rejected') {
    const refundAmount = calculateRefundForRejectedItem({ rejectedPurchase: purchase, allPurchases: purchases });
    if (refundAmount > 0 && !purchase.walletRefundedAt) {
      const users = db.users();
      const refundUserId = purchase.payerUserId || purchase.userId;
      const userIndex = users.findIndex(u => u.id === refundUserId);
      if (userIndex !== -1) {
        users[userIndex].walletBalance = Math.round((Number(users[userIndex].walletBalance || 0) + refundAmount) * 100) / 100;
        db.saveUsers(users);
      }
      purchases[index].walletRefundedAt = new Date().toISOString();
      purchases[index].walletRefundAmount = refundAmount;
    }
  }

  purchases[index].status = 'rejected';
  db.savePurchases(purchases);

  addAdminAuditLogEntry({
    req,
    action: 'reject_purchase',
    entity: 'purchase',
    entityId: purchases[index].id,
    before,
    after: { status: purchases[index].status, walletRefundedAt: purchases[index].walletRefundedAt || null, walletRefundAmount: purchases[index].walletRefundAmount || null },
    meta: { projectId: purchases[index].projectId || null, userId: purchases[index].userId || null }
  });
  emitLiveAdminEvent({
    type: 'purchase-reject',
    title: 'رفض شراء',
    message: `تم رفض شراء: ${purchases[index].projectTitle || purchases[index].projectId || purchases[index].id}`,
    severity: 'warning',
    data: { purchaseId: purchases[index].id, projectId: purchases[index].projectId || null, userId: purchases[index].userId || null },
    createdAt: new Date().toISOString()
  });
  res.redirect('/admin/purchases');
});

// Admin - Wallet Codes
app.get('/admin/wallet-codes', requireAdminPermission(ADMIN_PERMISSIONS.walletCodes), (req, res) => {
  const walletCodes = db.walletCodes();
  res.render('admin/wallet-codes', { walletCodes, user: req.session.user, error: null });
});

app.post('/admin/wallet-codes', requireAdminPermission(ADMIN_PERMISSIONS.walletCodes), (req, res) => {
  const code = normalizeWalletCode(req.body.code);
  const amount = Number(req.body.amount);
  const expiresAt = parseOptionalIsoDate(req.body.expiresAt);
  const usageLimit = req.body.usageLimit ? Number(req.body.usageLimit) : null;

  const walletCodes = db.walletCodes();

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
    occasion: occasion || null,
    durationDays,
    expiresAt,
    createdAt: new Date().toISOString(),
    lastUsedAt: null
  });

  db.saveWalletCodes(walletCodes);
  return res.redirect('/admin/wallet-codes');
});

app.post('/admin/wallet-codes/:id/toggle', requireAdmin, (req, res) => {
  const walletCodes = db.walletCodes();
  const idx = walletCodes.findIndex(c => c.id === req.params.id);
  if (idx === -1) return res.status(404).send('Code not found');
  walletCodes[idx].active = !walletCodes[idx].active;
  db.saveWalletCodes(walletCodes);
  return res.redirect('/admin/wallet-codes');
});

app.post('/admin/wallet-codes/:id/delete', requireAdmin, (req, res) => {
  const walletCodes = db.walletCodes();
  db.saveWalletCodes(walletCodes.filter(c => c.id !== req.params.id));
  return res.redirect('/admin/wallet-codes');
});

// Modification Request - User requests custom changes
app.get('/request-modification/:purchaseId', requireAuth, (req, res) => {
  const purchases = db.purchases();
  const purchase = purchases.find(p => p.id === req.params.purchaseId && p.userId === req.session.user.id);
  if (!purchase) return res.status(404).send('Purchase not found');
  
  res.render('request-modification', { purchase, user: req.session.user, error: null });
});

app.post('/request-modification/:purchaseId', requireAuth, (req, res) => {
  const { description } = req.body;
  const purchases = db.purchases();
  const purchase = purchases.find(p => p.id === req.params.purchaseId && p.userId === req.session.user.id);
  if (!purchase) return res.status(404).send('Purchase not found');
  
  const modifications = db.modifications();
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
  
  db.saveModifications(modifications);
  res.redirect('/my-purchases');
});

// Admin - View modification requests
app.get('/admin/modifications', requireAdminPermission(ADMIN_PERMISSIONS.modifications), (req, res) => {
  const modifications = db.modifications();
  const users = db.users();
  const purchases = db.purchases();
  res.render('admin/modifications', { modifications, users, purchases, user: req.session.user });
});

// Admin - Complete modification request
app.post('/admin/modifications/:id/complete', requireAdminPermission(ADMIN_PERMISSIONS.modifications), upload.single('modifiedFile'), (req, res) => {
  const modifications = db.modifications();
  const modIndex = modifications.findIndex(m => m.id === req.params.id);
  if (modIndex === -1) return res.status(404).send('Modification request not found');
  
  const modification = modifications[modIndex];
  
  // Update the purchase with the modified file
  const purchases = db.purchases();
  const purchaseIndex = purchases.findIndex(p => p.id === modification.purchaseId);
  
  if (purchaseIndex !== -1 && req.file) {
    purchases[purchaseIndex].filePath = `uploads/${req.file.filename}`;
    purchases[purchaseIndex].originalFileName = req.file.originalname;
    purchases[purchaseIndex].isModified = true;
    purchases[purchaseIndex].modificationNote = req.body.note || 'Project modified as requested';
    db.savePurchases(purchases);
  }
  
  modifications[modIndex].status = 'completed';
  modifications[modIndex].completedAt = new Date().toISOString();
  db.saveModifications(modifications);
  
  res.redirect('/admin/modifications');
});
// ========== MESSAGING SYSTEM ==========

// User - View chat with admin
app.get('/messages', requireAuth, (req, res) => {
  const messages = db.messages().filter(m => 
    (m.senderId === req.session.user.id && m.receiverId === 'admin') ||
    (m.senderId === 'admin' && m.receiverId === req.session.user.id)
  );
  res.render('messages', { messages, user: req.session.user, purchaseChat: null });
});

app.get('/messages/purchase/:purchaseId', requireAuth, (req, res) => {
  const purchaseChat = getPurchaseChatContextForUser(req.params.purchaseId, req.session.user.id);
  if (!purchaseChat) return res.status(404).send('Purchase not found');

  const messages = db.messages().filter((message) => (
    message && message.purchaseId === purchaseChat.purchaseId && (
      (message.senderId === req.session.user.id && message.receiverId === 'admin') ||
      (message.senderId === 'admin' && message.receiverId === req.session.user.id)
    )
  ));

  const allMessages = db.messages();
  let changed = false;
  allMessages.forEach((message) => {
    if (message && message.purchaseId === purchaseChat.purchaseId && message.senderId === 'admin' && message.receiverId === req.session.user.id && !message.read) {
      message.read = true;
      changed = true;
    }
  });
  if (changed) db.saveMessages(allMessages);

  res.render('messages', { messages, user: req.session.user, purchaseChat });
});

app.get('/subscriptions', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const plans = db.subscriptionPlans().filter(p => p && p.active);
  const activeSubscription = getActiveSubscriptionForUser({ userId: req.session.user.id });
  const activePlan = activeSubscription ? plans.find(p => p.id === activeSubscription.planId) : null;
  const activeSubscriptionCouponPromos = getActiveCouponPromoCards(db.subscriptionCoupons(), getSubscriptionCouponEligibility);

  res.render('subscriptions', {
    user: req.session.user,
    plans,
    activeSubscriptionCouponPromos,
    activeSubscription,
    activePlan,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.post('/subscriptions/subscribe', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const planId = req.body.planId;
  const plans = db.subscriptionPlans().filter(p => p && p.active);
  const plan = plans.find(p => p.id === planId);
  if (!plan) return res.redirect('/subscriptions?error=' + encodeURIComponent('الخطة غير صحيحة'));

  const existing = getActiveSubscriptionForUser({ userId: req.session.user.id });
  if (existing) return res.redirect('/subscriptions?error=' + encodeURIComponent('لديك اشتراك نشط بالفعل'));

  const users = db.users();
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
    coupons = db.subscriptionCoupons();
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
  db.saveUsers(users);

  if (appliedCoupon && coupons && couponIndex !== -1) {
    coupons[couponIndex].usedCount = Number(coupons[couponIndex].usedCount || 0) + 1;
    coupons[couponIndex].lastUsedAt = new Date().toISOString();
    db.saveSubscriptionCoupons(coupons);
  }

  const now = new Date();
  const end = new Date(now.getTime() + (Number(plan.durationDays || 30) * 24 * 60 * 60 * 1000));

  const subs = db.subscriptions();
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
  db.saveSubscriptions(subs);

  const payments = db.subscriptionPayments();
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
  db.saveSubscriptionPayments(payments);

  req.session.user.walletBalance = Number(users[idx].walletBalance || 0);
  req.session.user.subscription = {
    planId: sub.planId,
    status: sub.status,
    currentPeriodEnd: sub.currentPeriodEnd
  };

  res.redirect('/subscriptions?success=' + encodeURIComponent('تم الاشتراك بنجاح'));
});

app.post('/subscriptions/cancel', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const subs = db.subscriptions();
  const active = getActiveSubscriptionForUser({ userId: req.session.user.id });
  if (!active) return res.redirect('/subscriptions?error=' + encodeURIComponent('لا يوجد اشتراك نشط'));

  const idx = subs.findIndex(s => s.id === active.id);
  if (idx === -1) return res.redirect('/subscriptions?error=' + encodeURIComponent('لا يوجد اشتراك نشط'));

  subs[idx].status = 'canceled';
  subs[idx].canceledAt = new Date().toISOString();
  db.saveSubscriptions(subs);

  req.session.user.subscription = null;
  res.redirect('/subscriptions?success=' + encodeURIComponent('تم إلغاء الاشتراك'));
});

app.get('/codentra-presentations', (req, res) => {
  const plans = ensurePresentationPlans();
  let currentUser = null;
  let activeSubscription = null;
  let activePlan = null;
  let decks = [];
  let incomingApprovals = [];
  let walletCardNumber = '';

  if (req.session.user && req.session.user.role === 'user') {
    const users = db.users();
    const userIndex = users.findIndex((item) => item && item.id === req.session.user.id);
    if (userIndex !== -1) {
      currentUser = users[userIndex];
      if (ensureUserPaymentProfile({ user: currentUser, users })) {
        users[userIndex] = currentUser;
        db.saveUsers(users);
      }
      req.session.user = buildSessionUser(currentUser);
      activeSubscription = getActivePresentationSubscriptionForUser({ userId: currentUser.id });
      activePlan = activeSubscription ? plans.find((plan) => plan.id === activeSubscription.planId) || null : null;
      decks = getPresentationDecksForUser({ userId: currentUser.id, limit: 6 });
      incomingApprovals = getPresentationIncomingApprovals({ ownerUserId: currentUser.id });
      walletCardNumber = formatWalletCardNumber(currentUser.walletCardNumber);
    }
  }

  res.render('presentations/home', {
    user: req.session.user,
    presentationUser: currentUser,
    plans,
    activeSubscriptionCouponPromos,
    activeSubscription,
    activePlan,
    decks,
    incomingApprovals,
    walletCardNumber,
    error: req.query.error || null,
    success: req.query.success || null
  });
});

app.get('/codentra-presentations/dashboard', (req, res) => {
  res.redirect('/codentra-presentations');
});

app.post('/codentra-presentations/subscribe/:planId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  try {
    const plan = getPresentationPlanById({ planId: req.params.planId });
    if (!plan) {
      return res.redirect('/codentra-presentations?error=' + encodeURIComponent('الخطة غير موجودة'));
    }

    const users = db.users();
    const buyer = users.find((item) => item && item.id === req.session.user.id && item.role === 'user');
    if (!buyer) {
      return res.redirect('/login');
    }

    let shouldSaveUsers = false;
    if (ensureUserPaymentProfile({ user: buyer, users })) {
      shouldSaveUsers = true;
    }

    const enteredCardNumber = normalizeWalletCardNumber(req.body.walletCardNumber) || normalizeWalletCardNumber(buyer.walletCardNumber);
    const payer = findUserByWalletCardNumber({ users, walletCardNumber: enteredCardNumber });
    if (!payer) {
      return res.redirect('/codentra-presentations?error=' + encodeURIComponent('رقم بطاقة المحفظة غير صحيح'));
    }
    if (payer.walletCardFrozen) {
      return res.redirect('/codentra-presentations?error=' + encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة'));
    }

    if (ensureUserPaymentProfile({ user: payer, users })) {
      shouldSaveUsers = true;
    }

    if (shouldSaveUsers) {
      db.saveUsers(users);
    }

    if (payer.id === buyer.id) {
      const amount = Number(plan.price || 0);
      try {
        enforceWalletCardSpendingLimit({ payerUser: payer, amount });
      } catch (limitError) {
        return res.redirect('/codentra-presentations?error=' + encodeURIComponent(limitError.message || 'تم تجاوز حد البطاقة'));
      }
      if (Number(payer.walletBalance || 0) < amount) {
        return res.redirect('/codentra-presentations?error=' + encodeURIComponent('رصيد البطاقة غير كافٍ'));
      }

      payer.walletBalance = Math.round((Number(payer.walletBalance || 0) - amount) * 100) / 100;
      users[users.findIndex((item) => item && item.id === payer.id)] = payer;
      db.saveUsers(users);

      upsertPresentationSubscription({ userId: buyer.id, plan, payerUserId: payer.id });
      finalizeWalletCardOwnerActivity({
        payerUserId: payer.id,
        buyerUser: buyer,
        amount,
        kind: 'presentations-subscription',
        payload: { planName: plan.name, planId: plan.id },
        status: 'completed',
        note: 'تم تفعيل الاشتراك مباشرة باستخدام بطاقتك الشخصية',
        notifyTitle: 'تم استخدام بطاقتك',
        notifyMessage: `استخدمت بطاقتك في الاشتراك بخطة ${plan.name}.`
      });

      req.session.user = buildSessionUser(buyer);
      return res.redirect('/codentra-presentations?success=' + encodeURIComponent('تم تفعيل الاشتراك مباشرة باستخدام بطاقتك'));
    }

    const attempt = createPresentationPaymentAttempt({ buyerUser: buyer, payerUser: payer, plan });
    return res.redirect(`/codentra-presentations/payment/verify/${attempt.id}`);
  } catch (error) {
    const message = error && error.message ? error.message : 'تعذر بدء الاشتراك الآن';
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent(message));
  }
});

app.get('/codentra-presentations/payment/verify/:attemptId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const attempt = getPresentationPaymentAttemptById({ attemptId: req.params.attemptId });
  if (!attempt || attempt.buyerUserId !== req.session.user.id) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('طلب التحقق غير موجود أو انتهت صلاحيته'));
  }

  const users = db.users();
  const payer = users.find((item) => item && item.id === attempt.payerUserId) || null;
  const plan = getPresentationPlanById({ planId: attempt.planId });

  return res.render('presentations/payment-verify', {
    user: req.session.user,
    attempt,
    plan,
    payer,
    walletCardNumber: payer ? formatWalletCardNumber(payer.walletCardNumber) : '',
    ownerVisibleCode: payer && payer.id === req.session.user.id ? attempt.ownerVisibleCode : null,
    error: req.query.error || null
  });
});

app.post('/codentra-presentations/payment/verify/:attemptId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const attempt = getPresentationPaymentAttemptById({ attemptId: req.params.attemptId });
  if (!attempt || attempt.buyerUserId !== req.session.user.id) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('طلب التحقق غير موجود أو انتهت صلاحيته'));
  }

  const verificationCode = String(req.body.verificationCode || '').trim();
  if (!verificationCode || hashPaymentVerificationCode(verificationCode) !== attempt.codeHash) {
    return res.redirect(`/codentra-presentations/payment/verify/${attempt.id}?error=${encodeURIComponent('كود التحقق غير صحيح')}`);
  }

  const users = db.users();
  const buyerIndex = users.findIndex((item) => item && item.id === req.session.user.id && item.role === 'user');
  const payerIndex = users.findIndex((item) => item && item.id === attempt.payerUserId && item.role === 'user');
  if (buyerIndex === -1 || payerIndex === -1) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('تعذر العثور على المستخدم'));
  }

  const buyer = users[buyerIndex];
  const payer = users[payerIndex];
  if (ensureUserPaymentProfile({ user: buyer, users })) {
    users[buyerIndex] = buyer;
  }
  if (ensureUserPaymentProfile({ user: payer, users })) {
    users[payerIndex] = payer;
  }
  if (payer.walletCardFrozen) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('البطاقة مجمدة. يمكن لصاحبها فقط فك التجميد من إعدادات البطاقة'));
  }

  const plan = getPresentationPlanById({ planId: attempt.planId });
  if (!plan) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('الخطة غير موجودة'));
  }

  if (attempt.requiresPassword) {
    const walletPassword = String(req.body.walletPassword || '');
    if (!payer.walletPaymentPasswordHash || !bcrypt.compareSync(walletPassword, payer.walletPaymentPasswordHash)) {
      return res.redirect(`/codentra-presentations/payment/verify/${attempt.id}?error=${encodeURIComponent('كلمة مرور البطاقة غير صحيحة')}`);
    }
  }

  const amount = Number(plan.price || 0);
  if (Number(payer.walletBalance || 0) < amount) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('رصيد البطاقة غير كافٍ'));
  }

  payer.walletBalance = Math.round((Number(payer.walletBalance || 0) - amount) * 100) / 100;
  users[buyerIndex] = buyer;
  users[payerIndex] = payer;
  db.saveUsers(users);

  upsertPresentationSubscription({ userId: buyer.id, plan, payerUserId: payer.id });
  finalizeWalletCardOwnerActivity({
    payerUserId: payer.id,
    buyerUser: buyer,
    amount,
    kind: 'presentations-subscription',
    payload: { planName: plan.name, planId: plan.id },
    attemptId: attempt.id,
    usageLogEntryId: attempt.usageLogEntryId || null,
    status: 'completed',
    note: 'تم إدخال كود التحقق وتفعيل الاشتراك بنجاح',
    notifyTitle: 'تم استخدام بطاقتك بنجاح',
    notifyMessage: `اكتمل الدفع الخاص بخطة ${plan.name} بنجاح.`
  });
  markPresentationPaymentAttemptUsed({ attemptId: attempt.id });

  req.session.user = buildSessionUser(buyer);
  return res.redirect('/codentra-presentations?success=' + encodeURIComponent('تم تفعيل اشتراك Codentra Presentations بنجاح'));
});

app.get('/codentra-presentations/generate', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const activeSubscription = getActivePresentationSubscriptionForUser({ userId: req.session.user.id });
  if (!activeSubscription) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('تحتاج اشتراكًا نشطًا للبدء'));
  }

  res.render('presentations/generate', {
    user: req.session.user,
    activeSubscription,
    error: req.query.error || null
  });
});

app.post('/codentra-presentations/generate', requireAuth, async (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const activeSubscription = getActivePresentationSubscriptionForUser({ userId: req.session.user.id });
  if (!activeSubscription) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('تحتاج اشتراكًا نشطًا للبدء'));
  }

  if (Number(activeSubscription.remainingCredits || 0) <= 0) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('لا توجد عمليات إنشاء متبقية في اشتراكك'));
  }

  const topic = String(req.body.topic || '').trim();
  const audience = String(req.body.audience || '').trim();
  const tone = String(req.body.tone || '').trim();
  const purpose = String(req.body.purpose || '').trim();
  const language = req.body.language === 'en' ? 'en' : 'ar';
  const slideCount = countPresentationSlides(req.body.slideCount);

  if (!topic) {
    return res.redirect('/codentra-presentations/generate?error=' + encodeURIComponent('اكتب موضوع العرض أولًا'));
  }

  const generated = await generatePresentationDeck({
    topic,
    audience,
    tone,
    purpose,
    language,
    slideCount
  });

  const decks = db.presentationDecks();
  const deck = {
    id: uuidv4(),
    userId: req.session.user.id,
    subscriptionId: activeSubscription.id,
    topic,
    audience,
    tone,
    purpose,
    language,
    title: generated.title,
    subtitle: generated.subtitle,
    theme: generated.theme,
    slides: generated.slides,
    createdAt: new Date().toISOString(),
    modelUsed: process.env.OPENAI_API_KEY ? PRESENTATION_AI_MODEL : 'local-fallback'
  };
  decks.push(deck);
  db.savePresentationDecks(decks);
  consumePresentationCredit({ subscriptionId: activeSubscription.id });

  return res.redirect(`/codentra-presentations/decks/${deck.id}?success=${encodeURIComponent('تم إنشاء العرض بنجاح')}`);
});

app.get('/codentra-presentations/decks/:deckId', requireAuth, (req, res) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  const decks = db.presentationDecks();
  const deck = decks.find((item) => item && item.id === req.params.deckId && item.userId === req.session.user.id);
  if (!deck) {
    return res.redirect('/codentra-presentations?error=' + encodeURIComponent('العرض غير موجود'));
  }

  res.render('presentations/deck', {
    user: req.session.user,
    deck,
    success: req.query.success || null
  });
});

app.get('/codentra-presentations/decks/:deckId/download.pptx', requireAuth, async (req, res, next) => {
  if (!req.session.user || req.session.user.role !== 'user') return res.redirect('/');

  try {
    const decks = db.presentationDecks();
    const deck = decks.find((item) => item && item.id === req.params.deckId && item.userId === req.session.user.id);
    if (!deck) {
      return res.redirect('/codentra-presentations?error=' + encodeURIComponent('العرض غير موجود'));
    }

    const { fileName, filePath } = await buildPresentationPptxFile(deck);
    return res.download(filePath, fileName, () => {
      fs.unlink(filePath, () => {});
    });
  } catch (error) {
    return next(error);
  }
});

// User - Send message to admin
app.post('/messages', requireAuth, (req, res) => {
  const { content, purchaseId } = req.body;
  
  const messages = db.messages();
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
  
  db.saveMessages(messages);
  res.redirect('/messages');
});

app.post('/messages/purchase/:purchaseId', requireAuth, (req, res) => {
  const purchaseChat = getPurchaseChatContextForUser(req.params.purchaseId, req.session.user.id);
  if (!purchaseChat) return res.status(404).send('Purchase not found');

  const content = String(req.body.content || '').trim();
  if (!content) return res.redirect(`/messages/purchase/${purchaseChat.purchaseId}`);

  const messages = db.messages();
  messages.push({
    id: uuidv4(),
    senderId: req.session.user.id,
    senderName: req.session.user.name,
    receiverId: 'admin',
    content,
    read: false,
    purchaseId: purchaseChat.purchaseId,
    orderId: purchaseChat.orderId,
    projectTitle: purchaseChat.projectTitle,
    createdAt: new Date().toISOString()
  });

  db.saveMessages(messages);
  res.redirect(`/messages/purchase/${purchaseChat.purchaseId}`);
});

// Admin - View all conversations
app.get('/admin/messages', requireAdminPermission(ADMIN_PERMISSIONS.messages), (req, res) => {
  const messages = db.messages();
  const users = db.users();
  
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
app.get('/admin/messages/:userId', requireAdmin, (req, res) => {
  const messages = db.messages().filter(m => 
    (m.senderId === req.params.userId && m.receiverId === 'admin') ||
    (m.senderId === 'admin' && m.receiverId === req.params.userId)
  );
  
  const users = db.users();
  const chatUser = users.find(u => u.id === req.params.userId);
  
  // Mark messages as read
  const allMessages = db.messages();
  allMessages.forEach(m => {
    if (m.senderId === req.params.userId && m.receiverId === 'admin') {
      m.read = true;
    }
  });
  db.saveMessages(allMessages);
  
  res.render('admin/conversation', { messages, chatUser, user: req.session.user, purchaseChat: null });
});

app.get('/admin/purchases/:purchaseId/messages', requireAdminPermission(ADMIN_PERMISSIONS.messages), (req, res) => {
  const purchaseChat = getPurchaseChatContextForAdmin(req.params.purchaseId);
  if (!purchaseChat) return res.status(404).send('Purchase not found');

  const messages = db.messages().filter((message) => (
    message && message.purchaseId === purchaseChat.purchaseId && (
      (message.senderId === purchaseChat.buyerId && message.receiverId === 'admin') ||
      (message.senderId === 'admin' && message.receiverId === purchaseChat.buyerId)
    )
  ));

  const users = db.users();
  const chatUser = users.find((item) => item && item.id === purchaseChat.buyerId) || null;

  const allMessages = db.messages();
  let changed = false;
  allMessages.forEach((message) => {
    if (message && message.purchaseId === purchaseChat.purchaseId && message.senderId === purchaseChat.buyerId && message.receiverId === 'admin' && !message.read) {
      message.read = true;
      changed = true;
    }
  });
  if (changed) db.saveMessages(allMessages);

  res.render('admin/conversation', { messages, chatUser, user: req.session.user, purchaseChat });
});

// Admin - Reply to user
app.post('/admin/messages/:userId', requireAdmin, (req, res) => {
  const { content } = req.body;
  
  const messages = db.messages();
  messages.push({
    id: uuidv4(),
    senderId: 'admin',
    senderName: 'Admin',
    receiverId: req.params.userId,
    content,
    read: false,
    createdAt: new Date().toISOString()
  });
  
  db.saveMessages(messages);

  try {
    const users = db.users();
    const targetIndex = users.findIndex((item) => item && item.id === req.params.userId && item.role === 'user');
    if (targetIndex !== -1) {
      addUserNotificationEntry({
        targetUser: users[targetIndex],
        type: 'message-received',
        title: 'رسالة جديدة من الدعم',
        message: 'لديك رسالة جديدة من فريق Codentra.',
        metadata: {}
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore notification errors
  }

  res.redirect(`/admin/messages/${req.params.userId}`);
});

app.post('/admin/purchases/:purchaseId/messages', requireAdminPermission(ADMIN_PERMISSIONS.messages), (req, res) => {
  const purchaseChat = getPurchaseChatContextForAdmin(req.params.purchaseId);
  if (!purchaseChat) return res.status(404).send('Purchase not found');

  const content = String(req.body.content || '').trim();
  if (!content) return res.redirect(`/admin/purchases/${purchaseChat.purchaseId}/messages`);

  const messages = db.messages();
  messages.push({
    id: uuidv4(),
    senderId: 'admin',
    senderName: 'Admin',
    receiverId: purchaseChat.buyerId,
    content,
    read: false,
    purchaseId: purchaseChat.purchaseId,
    orderId: purchaseChat.orderId,
    projectTitle: purchaseChat.projectTitle,
    createdAt: new Date().toISOString()
  });

  db.saveMessages(messages);

  try {
    const users = db.users();
    const buyerIndex = users.findIndex((item) => item && item.id === purchaseChat.buyerId && item.role === 'user');
    if (buyerIndex !== -1) {
      addUserNotificationEntry({
        targetUser: users[buyerIndex],
        type: 'purchase-message',
        title: 'رسالة جديدة بخصوص طلبك',
        message: `لديك رسالة جديدة بخصوص ${purchaseChat.projectTitle || 'طلبك'}.`,
        metadata: {
          purchaseId: purchaseChat.purchaseId,
          orderId: purchaseChat.orderId,
          projectTitle: purchaseChat.projectTitle || null
        }
      });
      db.saveUsers(users);
    }
  } catch (error) {
    // ignore notification errors
  }

  res.redirect(`/admin/purchases/${purchaseChat.purchaseId}/messages`);
});

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: ["http://localhost:3000", "http://192.168.8.110:3000", "*"],
    methods: ["GET", "POST"],
    credentials: true
  }
});

// Used by Live Admin Feed + Audit Log broadcasting.
liveIo = io;

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

// Vercel auto-detects Express apps from supported entry files like server.js.
// Keep the local port listener for normal development, but export the app for Vercel.
if (!process.env.VERCEL) {
  startAbandonedCartRecoveryJob();
  startFileHealthMonitorJob();
  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`Codentra running on http://localhost:${PORT}`);
    console.log(`Network access: http://192.168.8.110:${PORT}`);
    console.log(`Admin: admin@codentra.com / admin123`);
  });
}

module.exports = app;
