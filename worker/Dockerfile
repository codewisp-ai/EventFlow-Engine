# --- STAGE 1: Builder ---
FROM node:20-alpine AS builder
WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --only=production

# --- STAGE 2: Runner ---
FROM node:20-alpine AS runner
WORKDIR /usr/src/app

ENV NODE_ENV=production

COPY --from=builder /usr/src/app/node_modules ./node_modules
COPY . .

USER node

# Workers are headless—no port exposure required
CMD ["node", "index.js"]