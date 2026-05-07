// Test Resend email configuration
try {
  require('dotenv').config({ path: '.env.resend' });
} catch (e) {
  require('dotenv').config();
}

const RESEND_API_KEY = process.env.RESEND_API_KEY;
const RESEND_FROM_EMAIL = process.env.RESEND_FROM_EMAIL;

async function sendTestEmail() {
  if (!RESEND_API_KEY) {
    console.error('❌ RESEND_API_KEY not set in .env');
    console.log('   Get one from: https://resend.com/api-keys');
    return;
  }

  if (!RESEND_FROM_EMAIL) {
    console.error('❌ RESEND_FROM_EMAIL not set in .env');
    console.log('   Example: notifications@yourdomain.com');
    return;
  }

  console.log('Testing Resend with API Key:', RESEND_API_KEY.substring(0, 10) + '...');
  console.log('From email:', RESEND_FROM_EMAIL);

  try {
    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${RESEND_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        from: RESEND_FROM_EMAIL,
        to: 'ahmedsamirsabry2100@gmail.com',
        subject: 'اختبار Resend من Codentra',
        html: `
          <div style="font-family:Arial,sans-serif;direction:rtl;text-align:right;max-width:600px;margin:0 auto;padding:20px;border:1px solid #e0e0e0;border-radius:8px;">
            <h2 style="color:#007bff">تم إعداد Resend بنجاح! 🎉</h2>
            <p>إذا رأيت هذا الإيميل، فإن إعدادات Resend تعمل بشكل صحيح.</p>
            <p>الآن كل الإشعارات ستصل للعملاء على إيميلاتهم.</p>
          </div>
        `
      })
    });

    if (!response.ok) {
      const error = await response.text();
      console.error('❌ Failed to send email:', error);
      return;
    }

    const result = await response.json();
    console.log('✅ Email sent successfully!');
    console.log('   Email ID:', result.id);
    console.log('   Check your inbox at: your-email@example.com');
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

sendTestEmail();
