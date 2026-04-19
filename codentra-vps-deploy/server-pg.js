const express = require('express');
const session = require('express-session');
const multer = require('multer');
const bcrypt = require('bcryptjs');
const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { Server } = require('socket.io');
const PDFDocument = require('pdfkit');
const jwt = require('jsonwebtoken');
const WatermarkProcessor = require('./utils/watermark');

require('dotenv').config();
const db = require('./db/database');

const app = express();
const PORT = process.env.PORT || 3000;

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

const normalizeStoredPath = (storedPath) => {
  if (!storedPath) return null;
  if (typeof storedPath !== 'string') return null;
  return storedPath.startsWith('/') ? storedPath.slice(1) : storedPath;
};

const toAbsolutePath = (storedPath) => {
  const normalized = normalizeStoredPath(storedPath);
  if (!normalized) return null;
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

// JWT Helpers
const JWT_SECRET = process.env.JWT_SECRET || 'codentra-jwt-secret-2024';
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

// Data storage paths
const DATA_DIR = path.join(__dirname, 'data');
const UPLOADS_DIR = path.join(__dirname, 'uploads');
const MEETING_RECORDINGS_DIR = path.join(UPLOADS_DIR, 'meeting-recordings');
const ADMIN_TEAM_UPLOADS_DIR = path.join(UPLOADS_DIR, 'admin-team');

// Ensure directories exist
[DATA_DIR, UPLOADS_DIR, MEETING_RECORDINGS_DIR, ADMIN_TEAM_UPLOADS_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// Helper functions
const normalizeCouponCode = (code) => {
  if (!code || typeof code !== 'string') return '';
  return code.trim().toUpperCase();
};

const isProjectVisibleToUser = ({ project, sessionUser }) => {
  if (project.visibility === 'public') return true;
  if (!sessionUser) return false;
  if (project.visibility === 'basic' && sessionUser.role === 'user') return true;
  if (project.visibility === 'premium' && sessionUser.role === 'user') return true;
  if (sessionUser.role === 'admin') return true;
  return false;
};

const getSubscriptionPlanById = async ({ planId }) => {
  if (!planId) return null;
  const result = await db.subscriptionPlans.findAll();
  return result.rows.find(p => p && p.plan_id === planId && p.active) || null;
};

const getUserSubscriptionTier = ({ sessionUser }) => {
  if (!sessionUser || sessionUser.role !== 'user') return 'none';
  const sub = sessionUser.subscription;
  if (!sub || sub.status !== 'active') return 'none';
  if (sub.planId === 'premium') return 'premium';
  if (sub.planId === 'basic') return 'basic';
  return 'none';
};

const getSubscriberDiscountPercent = ({ sessionUser }) => {
  const tier = getUserSubscriptionTier({ sessionUser });
  if (tier === 'premium') return 20;
  if (tier === 'basic') return 10;
  return 0;
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

const ensureUniqueReferralCode = async (users) => {
  const used = new Set(users.map(u => normalizeReferralCode(u.referral_code)).filter(Boolean));
  let code = generateReferralCode();
  while (used.has(code)) code = generateReferralCode();
  return code;
};

const validateReferralCodeForUser = ({ users, code, targetUserId }) => {
  const normalized = normalizeReferralCode(code);
  if (!normalized) return { valid: true, normalized: '' };
  const referrer = users.find(u => normalizeReferralCode(u.referral_code) === normalized);
  if (!referrer) return { valid: false, normalized, reason: 'كود الإحالة غير صحيح' };
  if (targetUserId && referrer.id === targetUserId) {
    return { valid: false, normalized, reason: 'لا يمكنك استخدام كود الإحالة الخاص بك' };
  }
  return { valid: true, normalized, referrerUserId: referrer.id };
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

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));
app.use('/uploads', express.static('uploads'));

app.use(session({
  secret: process.env.SESSION_SECRET || 'codentra-session-secret-2024',
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 1000 * 60 * 60 * 24 * 7 // 7 days
  }
}));

const isBlockedExpired = (u) => {
  if (!u) return false;
  if (!u.is_blocked) return false;
  if (!u.blocked_until) return false;
  const until = new Date(u.blocked_until);
  if (Number.isNaN(until.getTime())) return false;
  return until.getTime() <= Date.now();
};

const unblockUserInPlace = (u) => {
  if (!u) return;
  u.is_blocked = false;
  u.blocked_reason = null;
  u.blocked_by = null;
  u.blocked_at = null;
  u.blocked_until = null;
};

const getBlockedMessage = (u) => {
  if (!u || !u.is_blocked) return null;
  const reason = u.blocked_reason ? `سبب الحظر: ${u.blocked_reason}` : 'تم حظر الحساب';
  if (u.blocked_until) {
    return `${reason} (حتى ${new Date(u.blocked_until).toLocaleString('ar-EG')})`;
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

    const result = await db.users.findById(req.session.user.id);
    const fullUser = result.rows[0];

    if (!fullUser) return next();

    if (isBlockedExpired(fullUser)) {
      await db.users.update(fullUser.id, {
        is_blocked: false,
        blocked_reason: null,
        blocked_by: null,
        blocked_at: null,
        blocked_until: null
      });
    }

    const updatedResult = await db.users.findById(req.session.user.id);
    const updatedUser = updatedResult.rows[0];

    if (updatedUser.is_blocked) {
      if (allowWhileBlocked) return next();
      return res.redirect('/blocked');
    }

    req.session.user = {
      ...req.session.user,
      name: updatedUser.name,
      email: updatedUser.email,
      role: updatedUser.role,
      isSuperAdmin: updatedUser.is_super_admin,
      adminPermissions: updatedUser.admin_permissions,
      walletBalance: updatedUser.wallet_balance
    };
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
const upload = multer({ storage, limits: { fileSize: 500 * 1024 * 1024, files: 6 } }); // 500MB max, 6 files

// Create default admin user if none exists
async function createDefaultAdmin() {
  try {
    const existingAdmin = await db.users.findByEmail('admin@codentra.com');
    if (existingAdmin.rows.length === 0) {
      await db.users.create({
        id: uuidv4(),
        name: 'Admin',
        email: 'admin@codentra.com',
        password: bcrypt.hashSync('admin123', 10),
        role: 'admin',
        is_super_admin: true,
        created_at: new Date()
      });
      console.log('Default admin user created');
    }
  } catch (error) {
    console.error('Error creating default admin:', error);
  }
}

// Routes
app.get('/', async (req, res) => {
  try {
    const projectsResult = await db.projects.findAll();
    const projects = projectsResult.rows.filter(p => isProjectVisibleToUser({ project: p, sessionUser: req.session.user }));
    res.render('index', { projects, user: req.session.user });
  } catch (error) {
    console.error('Error loading home:', error);
    res.status(500).send('Server error');
  }
});

// Auth routes
app.get('/login', (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('login', { error: req.query.error || null, user: null });
});

app.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const userResult = await db.users.findByEmail(email);
    const user = userResult.rows[0];
    
    if (!user || !bcrypt.compareSync(password, user.password)) {
      return res.render('login', { error: 'Invalid email or password', user: null });
    }

    if (isBlockedExpired(user)) {
      await db.users.update(user.id, {
        is_blocked: false,
        blocked_reason: null,
        blocked_by: null,
        blocked_at: null,
        blocked_until: null
      });
    }

    if (user.is_blocked) {
      req.session.user = {
        id: user.id,
        name: user.name,
        email: user.email,
        role: user.role,
        isSuperAdmin: user.is_super_admin,
        adminPermissions: user.admin_permissions,
        walletBalance: user.wallet_balance
      };
      return res.redirect('/blocked');
    }

    req.session.user = {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role,
      isSuperAdmin: user.is_super_admin,
      adminPermissions: user.admin_permissions,
      walletBalance: user.wallet_balance
    };
    res.redirect('/');
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).send('Server error');
  }
});

app.get('/blocked', async (req, res) => {
  try {
    if (!req.session || !req.session.user || !req.session.user.id) {
      return res.redirect('/login');
    }
    const result = await db.users.findById(req.session.user.id);
    const user = result.rows[0];
    if (!user) return res.redirect('/login');

    if (isBlockedExpired(user)) {
      await db.users.update(user.id, {
        is_blocked: false,
        blocked_reason: null,
        blocked_by: null,
        blocked_at: null,
        blocked_until: null
      });
      return res.redirect('/');
    }

    if (!user.is_blocked) return res.redirect('/');

    return res.render('blocked', {
      user: req.session.user,
      blockedReason: user.blocked_reason || null,
      blockedUntil: user.blocked_until || null
    });
  } catch (e) {
    return res.redirect('/login');
  }
});

app.get('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/');
});

// Admin routes
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
  subscriptionReports: 'subscriptionReports'
};

const requireAdmin = (req, res, next) => {
  if (!req.session.user || req.session.user.role !== 'admin') {
    return res.redirect('/login');
  }
  next();
};

const requireAdminPermission = (permission) => {
  return (req, res, next) => {
    if (!req.session.user || req.session.user.role !== 'admin') {
      return res.redirect('/login');
    }
    if (req.session.user.isSuperAdmin) return next();
    if (!req.session.user.adminPermissions || !req.session.user.adminPermissions[permission]) {
      return res.status(403).send('Access denied');
    }
    next();
  };
};

// Admin dashboard
app.get('/admin', requireAdmin, async (req, res) => {
  try {
    const usersResult = await db.users.findAll();
    const projectsResult = await db.projects.findAll();
    const purchasesResult = await db.purchases.findAll();
    
    const users = usersResult.rows;
    const projects = projectsResult.rows;
    const purchases = purchasesResult.rows;

    const stats = {
      totalUsers: users.filter(u => u.role === 'user').length,
      totalProjects: projects.length,
      totalPurchases: purchases.length,
      totalRevenue: purchases.reduce((sum, p) => sum + Number(p.price || 0), 0)
    };

    res.render('admin/index', { stats, user: req.session.user });
  } catch (error) {
    console.error('Admin dashboard error:', error);
    res.status(500).send('Server error');
  }
});

// Admin users
app.get('/admin/users', requireAdminPermission(ADMIN_PERMISSIONS.users), async (req, res) => {
  try {
    const usersResult = await db.users.findAll();
    res.render('admin/users', { 
      users: usersResult.rows, 
      user: req.session.user, 
      success: req.query.success || null, 
      error: req.query.error || null 
    });
  } catch (error) {
    console.error('Admin users error:', error);
    res.status(500).send('Server error');
  }
});

// Admin projects
app.get('/admin/projects', requireAdminPermission(ADMIN_PERMISSIONS.projects), async (req, res) => {
  try {
    const projectsResult = await db.projects.findAll();
    res.render('admin/projects', { projects: projectsResult.rows, user: req.session.user });
  } catch (error) {
    console.error('Admin projects error:', error);
    res.status(500).send('Server error');
  }
});

// Start server
const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: ["http://localhost:3000", "http://172.20.10.2:3000", "*"],
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

  socket.on('ice-candidate', ({ roomId, candidate }) => {
    if (!roomId || !candidate) return;
    socket.to(roomId).emit('ice-candidate', { candidate });
  });
});

// Initialize server
async function startServer() {
  try {
    await createDefaultAdmin();
    httpServer.listen(PORT, '0.0.0.0', () => {
      console.log(`Codentra running on http://localhost:${PORT}`);
      console.log(`Network access: http://172.20.10.2:${PORT}`);
      console.log(`Admin: admin@codentra.com / admin123`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
