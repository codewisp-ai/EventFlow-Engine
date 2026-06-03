# --- STAGE 1: Builder ---
FROM node:20-alpine AS builder
WORKDIR /usr/src

# Copy all project manifests
COPY package*.json ./
COPY api/package*.json ./api/
COPY worker/package*.json ./worker/
COPY shared/package*.json ./shared/

# Run clean install globally across all registered workspaces
RUN npm ci --omit=dev

# --- STAGE 2: Runner ---
FROM node:20-alpine AS runner
WORKDIR /usr/src

ENV NODE_ENV=production

# Copy the complete, unified node_modules tree
COPY --from=builder /usr/src/node_modules ./node_modules

# Copy app source and shared business code
COPY api/ ./api/
COPY shared/ ./shared/

USER node
EXPOSE 8080

CMD ["node", "api/server.js"]