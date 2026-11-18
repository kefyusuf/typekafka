# ---- build stage ----
FROM node:26-alpine AS build
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
FROM node:26-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production
ENV SERVICE_NAME=customer-view
ENV CUSTOMER_VIEW_PORT=3001

RUN addgroup -S appuser && adduser -S appuser -G appuser

COPY --from=build --chown=appuser:appuser /app/node_modules ./node_modules
COPY --from=build --chown=appuser:appuser /app/packages ./packages
COPY --from=build --chown=appuser:appuser /app/apps/customer-view ./apps/customer-view

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD wget -qO- http://localhost:${CUSTOMER_VIEW_PORT}/health || exit 1

CMD ["node", "apps/customer-view/dist/main.js"]
