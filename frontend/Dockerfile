FROM node:22.12.0-alpine3.20 AS builder

WORKDIR /app
COPY .npmrc package.json package-lock.json tsconfig.json vite.config.ts index.html ./
RUN npm ci --ignore-scripts --no-audit --no-fund

COPY src ./src
RUN npm run build

FROM nginx:1.27.3-alpine

# Run nginx as non-root user on port 8080
RUN sed -i 's|/var/run/nginx.pid|/tmp/nginx.pid|g' /etc/nginx/nginx.conf && \
    sed -i '/^user /d' /etc/nginx/nginx.conf && \
    chown -R nginx:nginx /var/cache/nginx /var/log/nginx /etc/nginx/conf.d /usr/share/nginx/html

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html

USER nginx
EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
