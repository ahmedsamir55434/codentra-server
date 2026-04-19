═══════════════════════════════════════════════════════════════
    CODENTRA VPS DEPLOYMENT PACKAGE
═══════════════════════════════════════════════════════════════

📦 Package Contents:
  • server-pg.js          - Main application server
  • package.json          - Node.js dependencies
  • db/                   - Database schema & migrations
  • views/                - EJS templates
  • public/               - Static assets (CSS, JS, images)
  • docker-compose.yml    - Docker orchestration
  • nginx.conf            - Nginx reverse proxy config
  • deploy.sh             - VPS setup script
  • setup.sh              - Application setup script

🚀 Quick Deploy (3 steps):

1. Upload to VPS:
   scp -r codentra-vps-deploy/ root@YOUR_VPS_IP:/opt/

2. SSH into VPS:
   ssh root@YOUR_VPS_IP

3. Run deployment:
   cd /opt/codentra-vps-deploy
   sudo ./deploy.sh
   ./setup.sh

⚙️ Configuration:
   Edit .env file and change:
   - DB_PASSWORD (database password)
   - SESSION_SECRET (random string)
   - JWT_SECRET (random string)

🔒 SSL Setup (optional):
   certbot --nginx -d yourdomain.com

📖 Full Guide:
   See README-VPS.md for detailed instructions

═══════════════════════════════════════════════════════════════
