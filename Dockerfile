# Build stage
FROM node:20-slim AS builder
WORKDIR /app

COPY package*.json ./
RUN npm install

COPY src/ ./src/
COPY tsconfig.json ./

RUN npm run build

# Production stage
FROM node:20-slim AS production
WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force

COPY --from=builder /app/dist ./dist

EXPOSE 3001

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3001/health', r => process.exit(r.statusCode === 200 ? 0 : 1)).on('error', () => process.exit(1))"

CMD ["node", "dist/server-sdk.js"]
