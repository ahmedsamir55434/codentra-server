# Email Setup for Production Server

## Option 1: Resend API (Recommended)

### A. Quick Test (Limited)
Works immediately but only sends to your email:
```env
RESEND_API_KEY=re_ba4sE6xw_8d862R97iQwJ18h2mXPT644K
RESEND_FROM_EMAIL=onboarding@resend.dev
```

### B. Full Production (Send to any email)
1. Go to https://resend.com/domains
2. Add your domain (e.g., codentra.com)
3. Add DNS records to your hosting
4. Wait for verification (5-10 mins)
5. Update .env:
```env
RESEND_FROM_EMAIL=notifications@codentra.com
```

## Option 2: Use Email Service Provider

### For Railway/Render/Heroku:
Add environment variables in dashboard:
- RESEND_API_KEY = your_api_key
- RESEND_FROM_EMAIL = onboarding@resend.dev

### For VPS (DigitalOcean/Linode/AWS):
```bash
# Add to ~/.bashrc or ~/.profile
export RESEND_API_KEY="re_ba4sE6xw_8d862R97iQwJ18h2mXPT644K"
export RESEND_FROM_EMAIL="onboarding@resend.dev"
```

Or create ecosystem file for PM2:
```javascript
// ecosystem.config.js
module.exports = {
  apps: [{
    name: 'codentra',
    script: './server.js',
    env: {
      NODE_ENV: 'production',
      RESEND_API_KEY: 're_ba4sE6xw_8d862R97iQwJ18h2mXPT644K',
      RESEND_FROM_EMAIL: 'onboarding@resend.dev'
    }
  }]
}
```

## Troubleshooting

### Check if env vars are set:
```bash
node check-email.js
```

### Test email from server:
```bash
curl -X POST https://your-domain.com/api/test-email \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Common Issues:

1. **Gmail SMTP blocked**: Use Resend API instead
2. **Port 587 blocked**: Resend uses HTTPS (port 443) - works everywhere
3. **Env vars not loading**: Check if .env file exists on server or set vars in hosting dashboard

## Recommended for Production

Use Resend with verified domain:
- ✅ Works on all hosting providers
- ✅ High deliverability (not spam)
- ✅ No SMTP ports needed
- ✅ Simple API, no authentication issues
