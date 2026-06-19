# Report Dashboard — local UI, data từ ClickHouse trên K3s

## Quick start (bạn — có kubectl)

```bash
cd projects && ./run_local.sh
```

→ http://localhost:8092

**Deploy toàn platform:** [docs/DEPLOY.md](../docs/DEPLOY.md)  
**Publish Superset + BI:** [docs/publish-superset.md](docs/publish-superset.md)  
**BI embed chi tiết:** [docs/superset-and-embed.md](docs/superset-and-embed.md)

---

## Đồng nghiệp — chỉ cần Tailscale (không kubectl, không forward tay)

### Bạn setup 1 lần (admin, máy chạy K3s)

1. **Cài Tailscale** trên node K3s và join tailnet:
   ```bash
   tailscale up
   tailscale ip -4                    # ví dụ 100.64.0.5
   tailscale status --json | head     # hoặc bật MagicDNS trong admin console
   ```

2. **ClickHouse phải lộ qua NodePort** trên IP Tailscale (mặc định K3s bind all interfaces):
   ```bash
   kubectl get svc -n databases | grep -i clickhouse
   # Cần service kiểu NodePort, ví dụ clickhouse-shared → port 30900
   ```

3. **Test từ máy khác trong tailnet** (hoặc điện thoại bật Tailscale):
   ```bash
   nc -zv 100.64.0.5 30900
   # hoặc: nc -zv k3s-node.your-tailnet.ts.net 30900
   ```

4. **Gửi team** (Slack, không cần kubeconfig):
   - `CLICKHOUSE_HOST` = `100.104.250.97` (K3s master Tailscale)
   - `CLICKHOUSE_PORT` = `30900` (`clickhouse-shared` NodePort)
   - Đã có sẵn trong `backend/.env.tailscale.example`

### Đồng nghiệp (mỗi người)

1. Cài **Tailscale** + login **cùng tailnet**
2. Cài **Go**, **Node.js** (`brew install go node`)
3. Clone repo:
   ```bash
   cd projects/backend
   cp .env.tailscale.example .env
   cd ..
   ./run_local.sh
   ```
4. Mở http://localhost:8092

`run_local.sh` **không** chạy kubectl — nối thẳng ClickHouse qua Tailscale.

### Sơ đồ

```
[Đồng nghiệp Mac]                    [Tailscale tailnet]              [Máy K3s]
  run_local.sh  ──► backend :8090  ──► 100.x.x.x:30900  ──►  NodePort  ──► ClickHouse pod
  frontend :8092
```

---

## Các cách kết nối ClickHouse (chọn 1)

| Cách | Ai dùng | `backend/.env` |
|------|---------|----------------|
| **Tailscale** | Đồng nghiệp remote | `.env.tailscale.example` → `.env` |
| **kubectl forward** | Dev có kubeconfig | mặc định `localhost:9000`, `./run_local.sh` tự forward |
| **LAN NodePort** | Cùng WiFi, không TS | `SKIP_K8S_FORWARD=1`, `HOST=<LAN-IP>`, `PORT=30900` |

---

## Ports

| Service | Port |
|---------|------|
| Frontend | 8092 |
| Backend | 8090 |
| ClickHouse (forward local) | 9000 |
| ClickHouse (Tailscale NodePort) | **30900** |
