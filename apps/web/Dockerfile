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

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/apps/web ./apps/web

CMD ["node", "apps/web/dist/main.js"]
