# PostgreSQL Migration Guide

## Overview
The application has been migrated from JSON file-based storage to PostgreSQL for production deployment on VPS.

## Setup Instructions

### 1. Install PostgreSQL
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib

# macOS
brew install postgresql
brew services start postgresql

# Create database
sudo -u postgres createdb codentra
sudo -u postgres createuser --interactive
```

### 2. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit database settings
nano .env
```

Update these values in `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=codentra
DB_USER=your_postgres_user
DB_PASSWORD=your_postgres_password
```

### 3. Initialize Database
```bash
# Create schema
psql -U your_postgres_user -d codentra -f db/schema.sql

# Run migration (imports existing JSON data)
node db/migrate.js
```

### 4. Start Server
```bash
# Use PostgreSQL version
npm run start:pg

# Or run directly
node server-pg.js
```

## Database Schema

### Main Tables
- **users** - User accounts and authentication
- **projects** - Product listings and files
- **purchases** - Order history and transactions
- **invoices** - Billing records
- **coupons** - Discount codes
- **messages** - User-admin communication
- **reviews** - Product ratings
- **appointments** - Meeting scheduling
- **referrals** - Referral system
- **wallet_codes** - Wallet recharge codes
- **modifications** - Custom project requests
- **subscriptions** - User subscription plans
- **subscription_plans** - Available subscription tiers

## Key Features

### Production Ready
- ✅ Environment variable configuration
- ✅ PostgreSQL connection pooling
- ✅ SQL injection protection
- ✅ Database indexes for performance
- ✅ UUID primary keys
- ✅ JSONB for flexible data storage

### Migration Features
- ✅ Automatic data migration from JSON
- ✅ Preserves existing users and projects
- ✅ Maintains file structure
- ✅ Backward compatibility

## Deployment on VPS

### 1. Server Setup
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Node.js
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Install PostgreSQL
sudo apt install postgresql postgresql-contrib

# Install PM2 (process manager)
sudo npm install -g pm2
```

### 2. Database Setup
```bash
# Create database and user
sudo -u postgres psql
CREATE DATABASE codentra;
CREATE USER codentra_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE codentra TO codentra_user;
\q
```

### 3. Application Setup
```bash
# Clone and setup
git clone <your-repo>
cd codentra
npm install

# Configure environment
cp .env.example .env
nano .env  # Update with production settings

# Initialize database
psql -U codentra_user -d codentra -f db/schema.sql
node db/migrate.js
```

### 4. Start with PM2
```bash
# Start application
pm2 start server-pg.js --name "codentra"

# Save PM2 configuration
pm2 save
pm2 startup
```

### 5. Configure Firewall
```bash
# Allow HTTP/HTTPS and SSH
sudo ufw allow 22
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `codentra` |
| `DB_USER` | Database user | `codentra_user` |
| `DB_PASSWORD` | Database password | `secure_password` |
| `PORT` | Server port | `3000` |
| `NODE_ENV` | Environment | `production` |
| `SESSION_SECRET` | Session secret | `random-string` |
| `JWT_SECRET` | JWT secret | `random-string` |

## File Structure
```
codentra/
├── db/
│   ├── models.js          # PostgreSQL connection
│   ├── database.js        # Database functions
│   ├── schema.sql         # Database schema
│   └── migrate.js         # Migration script
├── .env.example           # Environment template
├── .env                   # Environment variables
├── server-pg.js           # PostgreSQL server
├── server.js              # Original JSON server
└── uploads/               # File uploads
```

## Testing
```bash
# Test database connection
node -e "require('./db/models').query('SELECT NOW()').then(console.log)"

# Test migration
node db/migrate.js

# Start development server
npm run start:pg
```

## Troubleshooting

### Connection Issues
- Check PostgreSQL is running: `sudo systemctl status postgresql`
- Verify database exists: `psql -l`
- Test connection: `psql -U username -d codentra`

### Migration Issues
- Backup JSON files before migration
- Check file permissions: `chmod +x db/migrate.js`
- Verify JSON data format

### Performance
- Monitor with PM2: `pm2 monit`
- Check database connections: `psql -c "SELECT * FROM pg_stat_activity"`
- Optimize indexes: `psql -d codentra -f db/schema.sql`

## Security Notes
- Use strong database passwords
- Restrict database user permissions
- Enable SSL in production
- Regular database backups
- Keep environment variables secure
