# Codentra VPS Deployment Guide

Complete guide for deploying Codentra to a VPS with Docker, PostgreSQL, and Nginx.

## 🚀 Quick Start (One-Command Deploy)

```bash
# 1. Upload project to VPS
scp -r codentra/ root@your-vps-ip:/root/

# 2. SSH into VPS
ssh root@your-vps-ip

# 3. Run deployment
cd codentra
chmod +x deploy.sh setup.sh
sudo ./deploy.sh
./setup.sh
```

## 📋 Prerequisites

- VPS with Ubuntu 20.04+ / Debian 11+
- Minimum 2GB RAM, 20GB storage
- Domain name (optional but recommended)
- Root or sudo access

## 🔧 Manual Step-by-Step Deployment

### 1. Server Setup

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y git curl ufw certbot python3-certbot-nginx
```

### 2. Install Docker

```bash
# Add Docker repository
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### 3. Upload Application

```bash
# From local machine
scp -r codentra/ root@your-vps-ip:/opt/

# Or clone from git
sudo git clone https://github.com/yourusername/codentra.git /opt/codentra
```

### 4. Configure Environment

```bash
cd /opt/codentra

# Create environment file
sudo nano .env
```

Add these settings:
```env
# Database (change password!)
DB_PASSWORD=your_secure_password_here

# Secrets (generate random strings)
SESSION_SECRET=random_string_32_chars
JWT_SECRET=random_string_32_chars

# Environment
NODE_ENV=production
```

### 5. Start Application

```bash
# Build and start
docker-compose up -d

# Check logs
docker-compose logs -f
```

### 6. Setup SSL (HTTPS)

```bash
# With domain name
sudo certbot --nginx -d yourdomain.com -d www.yourdomain.com

# Auto-renewal
sudo certbot renew --dry-run
```

## 📁 File Structure

```
codentra/
├── docker-compose.yml      # Main Docker config
├── docker-compose.prod.yml # Production overrides
├── Dockerfile              # App container
├── nginx.conf              # Nginx reverse proxy
├── deploy.sh               # One-time server setup
├── setup.sh                # Application setup
├── .env                    # Environment variables
├── .env.example            # Template
├── db/
│   ├── schema.sql          # Database schema
│   └── migrate.js          # Data migration
├── server-pg.js            # PostgreSQL server
└── uploads/                # File storage
```

## 🔐 Security Checklist

- [ ] Change default passwords in `.env`
- [ ] Configure firewall (UFW)
- [ ] Setup SSL certificates
- [ ] Disable root SSH login
- [ ] Use SSH keys instead of passwords
- [ ] Regular security updates
- [ ] Database backups

## 🛠️ Management Commands

```bash
# View logs
docker-compose logs -f app
docker-compose logs -f db
docker-compose logs -f nginx

# Restart services
docker-compose restart app
docker-compose restart db

# Update application
docker-compose down
git pull
docker-compose up -d --build

# Database backup
docker-compose exec db pg_dump -U codentra codentra > backup.sql

# Database restore
docker-compose exec -T db psql -U codentra codentra < backup.sql

# Shell access
docker-compose exec app sh
docker-compose exec db psql -U codentra

# Clean up
docker-compose down -v  # Remove volumes too
docker system prune      # Clean unused images
```

## 🌐 Domain Setup

### DNS Records
```
Type    Name        Value           TTL
A       @           your-vps-ip     3600
A       www         your-vps-ip     3600
```

### Nginx with Domain
```bash
# Update nginx.conf server_name
server_name yourdomain.com www.yourdomain.com;

# Reload nginx
docker-compose restart nginx
```

## 📊 Monitoring

```bash
# System resources
htop
docker stats

# Application health
curl http://localhost:3000/health

# Database status
docker-compose exec db pg_isready -U codentra
```

## 🚨 Troubleshooting

### Application won't start
```bash
# Check logs
docker-compose logs app

# Check database connection
docker-compose exec app node -e "require('./db/models').query('SELECT NOW()').then(console.log)"
```

### Database connection failed
```bash
# Restart database
docker-compose restart db

# Check database logs
docker-compose logs db
```

### Permission denied
```bash
# Fix permissions
sudo chown -R $USER:$USER /opt/codentra
sudo chmod -R 755 /opt/codentra/uploads
```

### Port already in use
```bash
# Check what's using port 80/443
sudo netstat -tlnp | grep :80
sudo netstat -tlnp | grep :443

# Kill process or change ports in docker-compose.yml
```

## 💾 Backup & Restore

### Automated Backup Script
```bash
#!/bin/bash
# backup.sh

BACKUP_DIR="/opt/backups/codentra"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $BACKUP_DIR

# Backup database
docker-compose exec -T db pg_dump -U codentra codentra > $BACKUP_DIR/db_$DATE.sql

# Backup uploads
tar -czf $BACKUP_DIR/uploads_$DATE.tar.gz uploads/

# Keep only last 7 backups
ls -t $BACKUP_DIR/db_*.sql | tail -n +8 | xargs -r rm
ls -t $BACKUP_DIR/uploads_*.tar.gz | tail -n +8 | xargs -r rm

echo "Backup completed: $DATE"
```

Add to crontab:
```bash
# Daily backup at 2 AM
0 2 * * * /opt/codentra/backup.sh >> /var/log/codentra-backup.log 2>&1
```

## 🔄 SSL Auto-Renewal

```bash
# Test renewal
sudo certbot renew --dry-run

# Add to crontab
0 0 1 * * /usr/bin/certbot renew --quiet
```

## 📞 Support

- Check logs: `docker-compose logs -f`
- Database issues: Check PostgreSQL connection
- File uploads: Verify `uploads/` directory permissions
- SSL issues: Verify certificates in `ssl/` directory

---

**Ready for production!** 🎉
