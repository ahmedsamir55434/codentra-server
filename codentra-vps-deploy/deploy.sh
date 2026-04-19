#!/bin/bash

# Codentra VPS Deployment Script
# Usage: ./deploy.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Codentra VPS Deployment ===${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Please run as root or with sudo${NC}"
    exit 1
fi

# Update system
echo -e "${YELLOW}Updating system...${NC}"
apt-get update && apt-get upgrade -y

# Install required packages
echo -e "${YELLOW}Installing required packages...${NC}"
apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    software-properties-common \
    git \
    ufw \
    certbot \
    python3-certbot-nginx

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}Installing Docker...${NC}"
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi

# Install Docker Compose if not present
if ! command -v docker-compose &> /dev/null; then
    echo -e "${YELLOW}Installing Docker Compose...${NC}"
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
fi

# Create app directory
APP_DIR="/opt/codentra"
echo -e "${YELLOW}Creating app directory at $APP_DIR...${NC}"
mkdir -p $APP_DIR

# Setup firewall
echo -e "${YELLOW}Configuring firewall...${NC}"
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow http
ufw allow https
ufw --force enable

# Create environment file
echo -e "${YELLOW}Setting up environment...${NC}"
if [ ! -f "$APP_DIR/.env" ]; then
    cat > $APP_DIR/.env << EOF
# Database
DB_PASSWORD=$(openssl rand -base64 32)

# Secrets (CHANGE THESE!)
SESSION_SECRET=$(openssl rand -base64 32)
JWT_SECRET=$(openssl rand -base64 32)

# Node
NODE_ENV=production
EOF
    echo -e "${GREEN}Environment file created at $APP_DIR/.env${NC}"
    echo -e "${YELLOW}IMPORTANT: Review and update $APP_DIR/.env before starting!${NC}"
fi

# Create SSL directory
mkdir -p $APP_DIR/ssl

# Generate self-signed SSL certificate (for initial setup)
if [ ! -f "$APP_DIR/ssl/cert.pem" ]; then
    echo -e "${YELLOW}Generating self-signed SSL certificate...${NC}"
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout $APP_DIR/ssl/key.pem \
        -out $APP_DIR/ssl/cert.pem \
        -subj "/C=EG/ST=Cairo/L=Cairo/O=Codentra/CN=localhost"
    echo -e "${YELLOW}Note: Replace with proper SSL certificates for production!${NC}"
fi

# Create docker-compose override for production
cat > $APP_DIR/docker-compose.prod.yml << 'EOF'
version: '3.8'

services:
  app:
    restart: always
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 256M
  
  db:
    restart: always
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G
        reservations:
          cpus: '0.25'
          memory: 256M
  
  nginx:
    restart: always
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 256M
        reservations:
          cpus: '0.1'
          memory: 128M
EOF

# Create systemd service
cat > /etc/systemd/system/codentra.service << EOF
[Unit]
Description=Codentra Application
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$APP_DIR
ExecStart=/usr/local/bin/docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
ExecStop=/usr/local/bin/docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
systemctl daemon-reload
systemctl enable codentra.service

echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Copy your application files to $APP_DIR"
echo "2. Update $APP_DIR/.env with your settings"
echo "3. Run: cd $APP_DIR && docker-compose up -d"
echo "4. For SSL: certbot --nginx -d yourdomain.com"
echo ""
echo -e "${GREEN}To start the application:${NC}"
echo "cd $APP_DIR && docker-compose up -d"
