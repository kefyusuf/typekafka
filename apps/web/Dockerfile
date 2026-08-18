# ---- build stage ----
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* ./
COPY tsconfig.base.json ./
COPY packages ./packages
COPY apps ./apps

RUN npm install

# Build workspace packages (root tsconfig + per-package tsconfig).
RUN npm run build --workspaces --if-present

# ---- runtime stage ----
FROM node:22-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production
ENV SERVICE_NAME=web
ENV WEB_PORT=3000

RUN addgroup -S appuser && adduser -S appuser -G appuser

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/apps/web ./apps/web

RUN chown -R appuser:appuser /app

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD wget -qO- http://localhost:${WEB_PORT}/api/health || exit 1

CMD ["node", "apps/web/dist/main.js"]
