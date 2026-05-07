// Check email configuration on server
console.log('=== Email Configuration Check ===\n');

const config = {
  RESEND_API_KEY: process.env.RESEND_API_KEY,
  RESEND_FROM_EMAIL: process.env.RESEND_FROM_EMAIL,
  SMTP_HOST: process.env.SMTP_HOST,
  SMTP_USER: process.env.SMTP_USER,
  NODE_ENV: process.env.NODE_ENV
};

let hasResend = false;
let hasSMTP = false;

console.log('Environment:', config.NODE_ENV || 'not set');
console.log('');

// Check Resend
if (config.RESEND_API_KEY) {
  console.log('✅ Resend API Key:', config.RESEND_API_KEY.substring(0, 10) + '...');
  console.log('✅ Resend From:', config.RESEND_FROM_EMAIL);
  hasResend = true;
} else {
  console.log('❌ Resend API Key: NOT SET');
}

// Check SMTP
if (config.SMTP_HOST) {
  console.log('✅ SMTP Host:', config.SMTP_HOST);
  console.log('✅ SMTP User:', config.SMTP_USER);
  hasSMTP = true;
} else {
  console.log('❌ SMTP: NOT CONFIGURED');
}

console.log('\n=== Status ===');
if (hasResend) {
  console.log('🟢 Resend is ready');
  console.log('   - For testing: emails only to your Gmail');
  console.log('   - For production: verify domain at resend.com/domains');
} else if (hasSMTP) {
  console.log('🟡 SMTP configured (may be blocked on server)');
} else {
  console.log('🔴 No email provider configured!');
  console.log('   Add to .env or environment variables:');
  console.log('   RESEND_API_KEY=re_xxxxxxxx');
  console.log('   RESEND_FROM_EMAIL=onboarding@resend.dev');
}

process.exit(hasResend || hasSMTP ? 0 : 1);
