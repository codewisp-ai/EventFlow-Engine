# --- STAGE 1: Builder ---
FROM node:20-alpine AS builder
WORKDIR /usr/src/app

# Leverage layer caching for node_modules
COPY package*.json ./
RUN npm ci --only=production

# --- STAGE 2: Runner ---
FROM node:20-alpine AS runner
WORKDIR /usr/src/app

ENV NODE_ENV=production

# Copy only production dependencies from builder stage
COPY --from=builder /usr/src/app/node_modules ./node_modules
# Copy application source code
COPY . .

USER node
EXPOSE 8080

CMD ["node", "server.js"]