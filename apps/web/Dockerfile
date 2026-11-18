# ---- build stage ----
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* ./
COPY tsconfig.base.json ./
COPY packages ./packages
COPY apps ./apps

RUN npm ci

# Build workspace packages (root tsconfig + per-package tsconfig).
RUN npm run build --workspaces --if-present

# Drop devDependencies so the runtime image carries only production code.
RUN npm prune --omit=dev

# ---- runtime stage ----
FROM node:22-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production
ENV SERVICE_NAME=web
ENV WEB_PORT=3000

RUN addgroup -S appuser && adduser -S appuser -G appuser

# Standalone runs default OUTBOX_DB_PATH to data/outbox.db under /app; the
# directory must exist and be writable by appuser (compose overrides to /data).
RUN mkdir -p /app/data && chown appuser:appuser /app/data

COPY --from=build --chown=appuser:appuser /app/node_modules ./node_modules
COPY --from=build --chown=appuser:appuser /app/packages ./packages
COPY --from=build --chown=appuser:appuser /app/apps/web ./apps/web

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD wget -qO- http://localhost:${WEB_PORT}/api/health || exit 1

CMD ["node", "apps/web/dist/main.js"]
