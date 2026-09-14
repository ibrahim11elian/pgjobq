# Multi-stage build. Runs as a non-root user, ships no build toolchain, and includes
# the migrations directory the runtime version-gate needs.

# ---------------------------------------------------------------------------
# deps: install with the lockfile only, so this layer caches across source edits
# ---------------------------------------------------------------------------
FROM node:22.22-alpine AS deps
WORKDIR /app

COPY package.json package-lock.json ./
COPY packages/core/package.json      packages/core/
COPY packages/server/package.json    packages/server/
COPY packages/worker/package.json    packages/worker/
COPY packages/dashboard/package.json packages/dashboard/

# `npm ci` requires the lockfile to match package.json exactly, which is what makes the
# build reproducible. --ignore-scripts blocks arbitrary postinstall execution.
RUN npm ci --ignore-scripts

# ---------------------------------------------------------------------------
# build: compile TypeScript, then the dashboard bundle
# ---------------------------------------------------------------------------
FROM deps AS build
WORKDIR /app

COPY tsconfig.base.json tsconfig.json ./
COPY packages/ packages/

RUN npx tsc --build
RUN npm run build --workspace @pgjobq/dashboard

# Drop dev dependencies from node_modules so they are not copied into the runtime image.
RUN npm prune --omit=dev

# ---------------------------------------------------------------------------
# runtime
# ---------------------------------------------------------------------------
FROM node:22.22-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production
# Fail fast and loudly rather than limping along in a degraded state.
ENV NODE_OPTIONS="--unhandled-rejections=strict"

# dumb-init reaps zombies and forwards SIGTERM to the Node process. Without a real init,
# PID 1 signal semantics mean SIGTERM is ignored and the container is SIGKILLed after the
# grace period — which strands every in-flight job for a full lease period on each deploy.
RUN apk add --no-cache dumb-init

COPY --from=build /app/node_modules              ./node_modules
COPY --from=build /app/package.json              ./package.json
COPY --from=build /app/packages/core/dist        ./packages/core/dist
COPY --from=build /app/packages/core/package.json ./packages/core/package.json
# Migrations are read at runtime by the startup version gate, so they must ship.
COPY --from=build /app/packages/core/migrations  ./packages/core/migrations
COPY --from=build /app/packages/server/dist      ./packages/server/dist
COPY --from=build /app/packages/server/package.json ./packages/server/package.json
COPY --from=build /app/packages/worker/dist      ./packages/worker/dist
COPY --from=build /app/packages/worker/package.json ./packages/worker/package.json
COPY --from=build /app/packages/dashboard/dist   ./packages/dashboard/dist

# The node image ships an unprivileged `node` user; use it rather than root.
USER node

EXPOSE 3001

# Liveness only — deliberately not readiness. Tying container health to the database would
# make a brief outage restart every container, turning a recoverable blip into an outage.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3001)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

ENTRYPOINT ["dumb-init", "--"]

# Override for the worker: CMD ["node", "packages/worker/dist/main.js"]
CMD ["node", "packages/server/dist/main.js"]
