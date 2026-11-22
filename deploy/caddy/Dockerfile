# syntax=docker/dockerfile:1
# Caddy with the built web client baked in:  docker build -f deploy/caddy/Dockerfile -t darkfleet/web .
FROM node:24-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM caddy:2.11.9-alpine
COPY deploy/caddy/Caddyfile /etc/caddy/Caddyfile
COPY --from=web /web/dist /srv
