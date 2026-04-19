FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache curl

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

RUN mkdir -p /app/uploads /app/uploads/meeting-recordings /app/uploads/admin-team /app/uploads/project-images /app/data

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD curl -fsS http://127.0.0.1:3000/health || exit 1

CMD ["node", "server.js"]
