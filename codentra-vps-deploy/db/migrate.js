const fs = require('fs');
const path = require('path');
const { pool } = require('./models');

async function migrate() {
  try {
    console.log('Starting migration from JSON to PostgreSQL...');
    
    // Read existing JSON data
    const dataDir = path.join(__dirname, '../data');
    const users = JSON.parse(fs.readFileSync(path.join(dataDir, 'users.json'), 'utf8') || '[]');
    const projects = JSON.parse(fs.readFileSync(path.join(dataDir, 'projects.json'), 'utf8') || '[]');
    const purchases = JSON.parse(fs.readFileSync(path.join(dataDir, 'purchases.json'), 'utf8') || '[]');
    const coupons = JSON.parse(fs.readFileSync(path.join(dataDir, 'coupons.json'), 'utf8') || '[]');
    const invoices = JSON.parse(fs.readFileSync(path.join(dataDir, 'invoices.json'), 'utf8') || '[]');
    const messages = JSON.parse(fs.readFileSync(path.join(dataDir, 'messages.json'), 'utf8') || '[]');
    const reviews = JSON.parse(fs.readFileSync(path.join(dataDir, 'reviews.json'), 'utf8') || '[]');
    const appointments = JSON.parse(fs.readFileSync(path.join(dataDir, 'appointments.json'), 'utf8') || '{"timeSlots":[],"bookings":[]}');
    const referrals = JSON.parse(fs.readFileSync(path.join(dataDir, 'referrals.json'), 'utf8') || '[]');
    const walletCodes = JSON.parse(fs.readFileSync(path.join(dataDir, 'wallet-codes.json'), 'utf8') || '[]');
    const modifications = JSON.parse(fs.readFileSync(path.join(dataDir, 'modifications.json'), 'utf8') || '[]');
    const subscriptions = JSON.parse(fs.readFileSync(path.join(dataDir, 'subscriptions.json'), 'utf8') || '[]');
    const subscriptionPlans = JSON.parse(fs.readFileSync(path.join(dataDir, 'subscription-plans.json'), 'utf8') || '[]');

    // Clear existing data
    await pool.query('TRUNCATE TABLE users, projects, purchases, coupons, invoices, messages, reviews, appointments, appointment_bookings, referrals, wallet_codes, modifications, subscriptions, subscription_plans RESTART IDENTITY CASCADE');

    // Migrate Users
    for (const user of users) {
      await pool.query(`
        INSERT INTO users (id, name, email, password, role, wallet_balance, referral_code, referred_by, loyalty_points, is_blocked, blocked_reason, blocked_by, blocked_at, blocked_until, is_super_admin, admin_permissions, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
      `, [
        user.id,
        user.name,
        user.email,
        user.password,
        user.role || 'user',
        user.walletBalance || 0,
        user.referralCode || null,
        user.referredBy ? user.referredBy.id : null,
        user.loyaltyPoints || 0,
        user.isBlocked || false,
        user.blockedReason || null,
        user.blockedBy || null,
        user.blockedAt || null,
        user.blockedUntil || null,
        user.isSuperAdmin || false,
        JSON.stringify(user.adminPermissions || {}),
        user.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${users.length} users`);

    // Migrate Projects
    for (const project of projects) {
      await pool.query(`
        INSERT INTO projects (id, title, description, price, category, technologies, visibility, file_path, original_file_name, images, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      `, [
        project.id,
        project.title,
        project.description,
        project.price,
        project.category,
        JSON.stringify(project.technologies || []),
        project.visibility || 'public',
        project.filePath || null,
        project.originalFileName || null,
        JSON.stringify(project.images || []),
        project.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${projects.length} projects`);

    // Migrate Purchases
    for (const purchase of purchases) {
      await pool.query(`
        INSERT INTO purchases (id, user_id, project_id, order_id, price, price_before, discount_amount, wallet_debit_amount, coupon_code, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      `, [
        purchase.id,
        purchase.userId,
        purchase.projectId,
        purchase.orderId || null,
        purchase.price,
        purchase.priceBefore || purchase.price,
        purchase.discountAmount || 0,
        purchase.walletDebitAmount || 0,
        purchase.couponCode || null,
        purchase.status || 'pending',
        purchase.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${purchases.length} purchases`);

    // Migrate Coupons
    for (const coupon of coupons) {
      await pool.query(`
        INSERT INTO coupons (id, code, type, value, active, used_count, usage_limit, expires_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `, [
        coupon.id,
        coupon.code,
        coupon.type,
        coupon.value,
        coupon.active,
        coupon.usedCount || 0,
        coupon.usageLimit || null,
        coupon.expiresAt || null,
        coupon.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${coupons.length} coupons`);

    // Migrate Invoices
    for (const invoice of invoices) {
      await pool.query(`
        INSERT INTO invoices (id, invoice_number, order_id, user_id, user_name, user_email, coupon_code, items, total_before, total_discount, total_after, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      `, [
        invoice.id,
        invoice.invoiceNumber,
        invoice.orderId,
        invoice.userId,
        invoice.userName,
        invoice.userEmail,
        invoice.couponCode || null,
        JSON.stringify(invoice.items || []),
        invoice.totalBefore,
        invoice.totalDiscount || 0,
        invoice.totalAfter,
        invoice.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${invoices.length} invoices`);

    // Migrate Messages
    for (const message of messages) {
      await pool.query(`
        INSERT INTO messages (id, user_id, admin_id, content, read, sender_type, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `, [
        message.id,
        message.userId,
        message.adminId || null,
        message.content,
        message.read || false,
        message.senderType,
        message.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${messages.length} messages`);

    // Migrate Reviews
    for (const review of reviews) {
      await pool.query(`
        INSERT INTO reviews (id, project_id, user_id, rating, comment, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [
        review.id,
        review.projectId,
        review.userId,
        review.rating,
        review.comment || null,
        review.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${reviews.length} reviews`);

    // Migrate Appointments & Bookings
    for (const slot of appointments.timeSlots || []) {
      await pool.query(`
        INSERT INTO appointments (id, admin_id, start_at, duration_minutes, label, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [
        slot.id,
        slot.adminId,
        slot.startAt,
        slot.durationMinutes || 30,
        slot.label || null,
        slot.createdAt || new Date().toISOString()
      ]);
    }

    for (const booking of appointments.bookings || []) {
      await pool.query(`
        INSERT INTO appointment_bookings (id, appointment_id, user_id, notes, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [
        booking.id,
        booking.slotId,
        booking.userId,
        booking.notes || null,
        booking.status || 'pending',
        booking.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${appointments.timeSlots?.length || 0} appointments and ${appointments.bookings?.length || 0} bookings`);

    // Migrate Referrals
    for (const referral of referrals) {
      await pool.query(`
        INSERT INTO referrals (id, referrer_user_id, referred_user_id, status, reward_amount, rewarded_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `, [
        referral.id,
        referral.referrerUserId,
        referral.referredUserId,
        referral.status || 'pending',
        referral.rewardAmount || 0,
        referral.rewardedAt || null,
        referral.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${referrals.length} referrals`);

    // Migrate Wallet Codes
    for (const code of walletCodes) {
      await pool.query(`
        INSERT INTO wallet_codes (id, code, amount, active, used_count, usage_limit, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `, [
        code.id,
        code.code,
        code.amount,
        code.active,
        code.usedCount || 0,
        code.usageLimit || null,
        code.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${walletCodes.length} wallet codes`);

    // Migrate Modifications
    for (const modification of modifications) {
      await pool.query(`
        INSERT INTO modifications (id, user_id, project_id, description, price, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `, [
        modification.id,
        modification.userId,
        modification.projectId,
        modification.description,
        modification.price,
        modification.status || 'pending',
        modification.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${modifications.length} modifications`);

    // Migrate Subscriptions
    for (const subscription of subscriptions) {
      await pool.query(`
        INSERT INTO subscriptions (id, user_id, plan_id, status, current_period_start, current_period_end, canceled_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `, [
        subscription.id,
        subscription.userId,
        subscription.planId,
        subscription.status || 'active',
        subscription.currentPeriodStart,
        subscription.currentPeriodEnd,
        subscription.canceledAt || null,
        subscription.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${subscriptions.length} subscriptions`);

    // Migrate Subscription Plans
    for (const plan of subscriptionPlans) {
      await pool.query(`
        INSERT INTO subscription_plans (id, plan_id, name, price, duration_days, features, active, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `, [
        plan.id,
        plan.planId,
        plan.name,
        plan.price,
        plan.durationDays,
        JSON.stringify(plan.features || []),
        plan.active !== false,
        plan.createdAt || new Date().toISOString()
      ]);
    }
    console.log(`Migrated ${subscriptionPlans.length} subscription plans`);

    console.log('Migration completed successfully!');
    
  } catch (error) {
    console.error('Migration failed:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  migrate();
}

module.exports = { migrate };
