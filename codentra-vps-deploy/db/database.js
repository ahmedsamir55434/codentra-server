const { query } = require('./models');

// Users
const users = {
  findAll: () => query('SELECT * FROM users ORDER BY created_at DESC'),
  findById: (id) => query('SELECT * FROM users WHERE id = $1', [id]),
  findByEmail: (email) => query('SELECT * FROM users WHERE email = $1', [email]),
  create: (user) => query(`
    INSERT INTO users (id, name, email, password, role, wallet_balance, referral_code, referred_by, loyalty_points, is_blocked, blocked_reason, blocked_by, blocked_at, blocked_until, is_super_admin, admin_permissions, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    RETURNING *
  `, [
    user.id,
    user.name,
    user.email,
    user.password,
    user.role || 'user',
    user.wallet_balance || 0,
    user.referral_code || null,
    user.referred_by || null,
    user.loyalty_points || 0,
    user.is_blocked || false,
    user.blocked_reason || null,
    user.blocked_by || null,
    user.blocked_at || null,
    user.blocked_until || null,
    user.is_super_admin || false,
    JSON.stringify(user.admin_permissions || {}),
    user.created_at || new Date()
  ]),
  update: (id, updates) => {
    const fields = Object.keys(updates);
    const values = Object.values(updates);
    const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
    return query(`UPDATE users SET ${setClause} WHERE id = $1 RETURNING *`, [id, ...values]);
  },
  save: (user) => {
    if (user.id) {
      return users.update(user.id, user);
    }
    return users.create(user);
  }
};

// Projects
const projects = {
  findAll: () => query('SELECT * FROM projects ORDER BY created_at DESC'),
  findById: (id) => query('SELECT * FROM projects WHERE id = $1', [id]),
  create: (project) => query(`
    INSERT INTO projects (id, title, description, price, category, technologies, visibility, file_path, original_file_name, images, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    RETURNING *
  `, [
    project.id,
    project.title,
    project.description,
    project.price,
    project.category,
    JSON.stringify(project.technologies || []),
    project.visibility || 'public',
    project.file_path || null,
    project.original_file_name || null,
    JSON.stringify(project.images || []),
    project.created_at || new Date()
  ]),
  update: (id, updates) => {
    const fields = Object.keys(updates);
    const values = Object.values(updates);
    const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
    return query(`UPDATE projects SET ${setClause} WHERE id = $1 RETURNING *`, [id, ...values]);
  },
  delete: (id) => query('DELETE FROM projects WHERE id = $1', [id]),
  save: (project) => {
    if (project.id) {
      return projects.update(project.id, project);
    }
    return projects.create(project);
  }
};

// Purchases
const purchases = {
  findAll: () => query('SELECT * FROM purchases ORDER BY created_at DESC'),
  findByUserId: (userId) => query('SELECT * FROM purchases WHERE user_id = $1 ORDER BY created_at DESC', [userId]),
  findByProjectId: (projectId) => query('SELECT * FROM purchases WHERE project_id = $1 ORDER BY created_at DESC', [projectId]),
  create: (purchase) => query(`
    INSERT INTO purchases (id, user_id, project_id, order_id, price, price_before, discount_amount, wallet_debit_amount, coupon_code, status, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    RETURNING *
  `, [
    purchase.id,
    purchase.user_id,
    purchase.project_id,
    purchase.order_id || null,
    purchase.price,
    purchase.price_before || purchase.price,
    purchase.discount_amount || 0,
    purchase.wallet_debit_amount || 0,
    purchase.coupon_code || null,
    purchase.status || 'pending',
    purchase.created_at || new Date()
  ]),
  update: (id, updates) => {
    const fields = Object.keys(updates);
    const values = Object.values(updates);
    const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
    return query(`UPDATE purchases SET ${setClause} WHERE id = $1 RETURNING *`, [id, ...values]);
  },
  save: (purchase) => {
    if (purchase.id) {
      return purchases.update(purchase.id, purchase);
    }
    return purchases.create(purchase);
  }
};

// Coupons
const coupons = {
  findAll: () => query('SELECT * FROM coupons ORDER BY created_at DESC'),
  findByCode: (code) => query('SELECT * FROM coupons WHERE code = $1', [code]),
  create: (coupon) => query(`
    INSERT INTO coupons (id, code, type, value, active, used_count, usage_limit, expires_at, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    RETURNING *
  `, [
    coupon.id,
    coupon.code,
    coupon.type,
    coupon.value,
    coupon.active,
    coupon.used_count || 0,
    coupon.usage_limit || null,
    coupon.expires_at || null,
    coupon.created_at || new Date()
  ]),
  update: (id, updates) => {
    const fields = Object.keys(updates);
    const values = Object.values(updates);
    const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
    return query(`UPDATE coupons SET ${setClause} WHERE id = $1 RETURNING *`, [id, ...values]);
  },
  save: (coupon) => {
    if (coupon.id) {
      return coupons.update(coupon.id, coupon);
    }
    return coupons.create(coupon);
  }
};

// Invoices
const invoices = {
  findAll: () => query('SELECT * FROM invoices ORDER BY created_at DESC'),
  findByUserId: (userId) => query('SELECT * FROM invoices WHERE user_id = $1 ORDER BY created_at DESC', [userId]),
  create: (invoice) => query(`
    INSERT INTO invoices (id, invoice_number, order_id, user_id, user_name, user_email, coupon_code, items, total_before, total_discount, total_after, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    RETURNING *
  `, [
    invoice.id,
    invoice.invoice_number,
    invoice.order_id,
    invoice.user_id,
    invoice.user_name,
    invoice.user_email,
    invoice.coupon_code || null,
    JSON.stringify(invoice.items || []),
    invoice.total_before,
    invoice.total_discount || 0,
    invoice.total_after,
    invoice.created_at || new Date()
  ]),
  save: (invoice) => invoices.create(invoice)
};

// Messages
const messages = {
  findAll: () => query('SELECT * FROM messages ORDER BY created_at DESC'),
  findByUserId: (userId) => query('SELECT * FROM messages WHERE user_id = $1 ORDER BY created_at DESC', [userId]),
  create: (message) => query(`
    INSERT INTO messages (id, user_id, admin_id, content, read, sender_type, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING *
  `, [
    message.id,
    message.user_id,
    message.admin_id || null,
    message.content,
    message.read || false,
    message.sender_type,
    message.created_at || new Date()
  ]),
  save: (message) => messages.create(message)
};

// Reviews
const reviews = {
  findAll: () => query('SELECT * FROM reviews ORDER BY created_at DESC'),
  findByProjectId: (projectId) => query('SELECT * FROM reviews WHERE project_id = $1 ORDER BY created_at DESC', [projectId]),
  create: (review) => query(`
    INSERT INTO reviews (id, project_id, user_id, rating, comment, created_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING *
  `, [
    review.id,
    review.project_id,
    review.user_id,
    review.rating,
    review.comment || null,
    review.created_at || new Date()
  ]),
  save: (review) => reviews.create(review)
};

// Appointments
const appointments = {
  findAll: () => query('SELECT * FROM appointments ORDER BY start_at DESC'),
  findByAdminId: (adminId) => query('SELECT * FROM appointments WHERE admin_id = $1 ORDER BY start_at DESC', [adminId]),
  create: (appointment) => query(`
    INSERT INTO appointments (id, admin_id, start_at, duration_minutes, label, created_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING *
  `, [
    appointment.id,
    appointment.admin_id,
    appointment.start_at,
    appointment.duration_minutes || 30,
    appointment.label || null,
    appointment.created_at || new Date()
  ]),
  save: (appointment) => appointments.create(appointment)
};

// Appointment Bookings
const appointmentBookings = {
  findAll: () => query('SELECT * FROM appointment_bookings ORDER BY created_at DESC'),
  findByUserId: (userId) => query('SELECT * FROM appointment_bookings WHERE user_id = $1 ORDER BY created_at DESC', [userId]),
  create: (booking) => query(`
    INSERT INTO appointment_bookings (id, appointment_id, user_id, notes, status, created_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING *
  `, [
    booking.id,
    booking.appointment_id,
    booking.user_id,
    booking.notes || null,
    booking.status || 'pending',
    booking.created_at || new Date()
  ]),
  save: (booking) => appointmentBookings.create(booking)
};

// Referrals
const referrals = {
  findAll: () => query('SELECT * FROM referrals ORDER BY created_at DESC'),
  create: (referral) => query(`
    INSERT INTO referrals (id, referrer_user_id, referred_user_id, status, reward_amount, rewarded_at, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING *
  `, [
    referral.id,
    referral.referrer_user_id,
    referral.referred_user_id,
    referral.status || 'pending',
    referral.reward_amount || 0,
    referral.rewarded_at || null,
    referral.created_at || new Date()
  ]),
  save: (referral) => referrals.create(referral)
};

// Wallet Codes
const walletCodes = {
  findAll: () => query('SELECT * FROM wallet_codes ORDER BY created_at DESC'),
  findByCode: (code) => query('SELECT * FROM wallet_codes WHERE code = $1', [code]),
  create: (code) => query(`
    INSERT INTO wallet_codes (id, code, amount, active, used_count, usage_limit, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING *
  `, [
    code.id,
    code.code,
    code.amount,
    code.active,
    code.used_count || 0,
    code.usage_limit || null,
    code.created_at || new Date()
  ]),
  save: (code) => walletCodes.create(code)
};

// Modifications
const modifications = {
  findAll: () => query('SELECT * FROM modifications ORDER BY created_at DESC'),
  create: (modification) => query(`
    INSERT INTO modifications (id, user_id, project_id, description, price, status, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING *
  `, [
    modification.id,
    modification.user_id,
    modification.project_id,
    modification.description,
    modification.price,
    modification.status || 'pending',
    modification.created_at || new Date()
  ]),
  save: (modification) => modifications.create(modification)
};

// Subscriptions
const subscriptions = {
  findAll: () => query('SELECT * FROM subscriptions ORDER BY created_at DESC'),
  findByUserId: (userId) => query('SELECT * FROM subscriptions WHERE user_id = $1 ORDER BY created_at DESC', [userId]),
  create: (subscription) => query(`
    INSERT INTO subscriptions (id, user_id, plan_id, status, current_period_start, current_period_end, canceled_at, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING *
  `, [
    subscription.id,
    subscription.user_id,
    subscription.plan_id,
    subscription.status || 'active',
    subscription.current_period_start,
    subscription.current_period_end,
    subscription.canceled_at || null,
    subscription.created_at || new Date()
  ]),
  save: (subscription) => subscriptions.create(subscription)
};

// Subscription Plans
const subscriptionPlans = {
  findAll: () => query('SELECT * FROM subscription_plans ORDER BY created_at DESC'),
  create: (plan) => query(`
    INSERT INTO subscription_plans (id, plan_id, name, price, duration_days, features, active, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING *
  `, [
    plan.id,
    plan.plan_id,
    plan.name,
    plan.price,
    plan.duration_days,
    JSON.stringify(plan.features || []),
    plan.active !== false,
    plan.created_at || new Date()
  ]),
  save: (plan) => subscriptionPlans.create(plan)
};

module.exports = {
  users,
  projects,
  purchases,
  coupons,
  invoices,
  messages,
  reviews,
  appointments,
  appointmentBookings,
  referrals,
  walletCodes,
  modifications,
  subscriptions,
  subscriptionPlans
};
