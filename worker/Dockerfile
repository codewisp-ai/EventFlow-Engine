# --- STAGE 1: Builder ---
FROM node:20-alpine AS builder
WORKDIR /usr/src

COPY package*.json ./
COPY api/package*.json ./api/
COPY worker/package*.json ./worker/
COPY shared/package*.json ./shared/

RUN npm ci --omit=dev

# --- STAGE 2: Runner ---
FROM node:20-alpine AS runner
WORKDIR /usr/src

ENV NODE_ENV=production

COPY --from=builder /usr/src/node_modules ./node_modules

COPY worker/ ./worker/
COPY shared/ ./shared/

USER node

CMD ["node", "worker/index.js"]