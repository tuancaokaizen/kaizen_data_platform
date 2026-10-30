# Kaizen Report UI — single image (React SPA + Go API)
# Ảnh duy nhất: SPA React + API Go
FROM node:20-alpine AS frontend
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
# Empty base URL → same-origin API calls from the pod / Base URL rỗng → gọi API cùng origin trong pod
ENV VITE_API_BASE_URL=
RUN npm run build

FROM golang:1.22-alpine AS backend
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/kaizen-report-api ./cmd/api

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
  && adduser -D -u 1000 app
WORKDIR /app
COPY --from=backend /out/kaizen-report-api /app/kaizen-report-api
COPY --from=frontend /ui/dist /app/static
USER 1000
ENV API_PORT=8090
ENV STATIC_DIR=/app/static
ENV CLICKHOUSE_HOST=clickhouse-service.databases.svc.cluster.local
ENV CLICKHOUSE_PORT=9000
ENV CLICKHOUSE_DATABASE=report
EXPOSE 8090
ENTRYPOINT ["/app/kaizen-report-api"]
