#!/bin/bash
# Export environment variables for production server
# Run this on your server or add to startup script

export RESEND_API_KEY="re_ba4sE6xw_8d862R97iQwJ18h2mXPT644K"
export RESEND_FROM_EMAIL="onboarding@resend.dev"
export ADMIN_EMAIL="ahmedsamirsabry2100@gmail.com"
export NODE_ENV="production"

# Start the server
npm start
