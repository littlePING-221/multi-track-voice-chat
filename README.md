# Voice Chat MVP

单房间多人语音聊天室，前端使用 React + Vite，业务服务使用 Rust/Axum，实时媒体使用 LiveKit。录音目录采用本地磁盘持久化，PCM segment 和事件日志先落盘，最终 WAV 使用临时文件后原子 rename。

## 本地开发

```sh
cargo run -p voice-server
cd web && npm install && npm run dev
```

默认 API 在 `http://localhost:3000`，前端在 `http://localhost:5173`。浏览器麦克风通常要求 HTTPS 或 localhost。设置 `LIVEKIT_URL`、`LIVEKIT_API_KEY`、`LIVEKIT_API_SECRET` 后，前端会拿到只允许加入 `main` 房间、发布/订阅音频的 token。

## Docker 部署

1. 将 `web` 构建为 `web/dist`；复制 `.env.example` 为 `.env`、`deploy/livekit.yaml.example` 为 `deploy/livekit.yaml`、`deploy/egress.yaml.example` 为 `deploy/egress.yaml`，并在三个本地配置中使用同一组 LiveKit 密钥，同时设置高强度 `HOST_PASSWORD`。
2. 使用现有 Nginx 时，将 `deploy/nginx/voice-chat.locations.conf` 的内容 include 到 `example.com` 的 TLS `server` 块；构建后的前端通过 `/voice-chat/` 访问，API 通过 `/voice-chat/api/` 访问，LiveKit 信令继续使用根路径 `/rtc`。
3. 在服务器开放标准 HTTPS 端口 TCP 443、LiveKit TCP 7881 以及 UDP 50000-50100；启用 TURN 后还需要 UDP 3478 和 TCP 5349。
4. 执行 `docker compose -f deploy/docker-compose.yml up -d --build`。

生产环境需要将 LiveKit、Egress、webhook 与 `.env` 中的 API key/secret 保持一致，并配置 TURN/公网证书。`recordings/` 和 `data/sqlite/` 必须备份。服务把参与者、恢复凭证哈希、唯一房主、连接代次、录音 session、segment、egress ID 和已处理的 webhook ID 保存在 SQLite；进程重启后已关闭的 PCM segment 可继续用于最终封装。Docker Compose 不再运行 Caddy：现有 Nginx 通过本机 `127.0.0.1:3000` 和 `127.0.0.1:7880` 反代 Rust API 与 LiveKit 信令。

## 身份与房间生命周期

新身份加入时服务返回随机恢复令牌，浏览器刷新后用它换取新的应用会话和 LiveKit token。每次恢复都会轮换恢复令牌并创建新的连接代次；旧代次的迟到 webhook 不能覆盖新连接。普通参与者断线保留 60 秒重连状态，房主保留 5 分钟。显式点击退出会立即结束当前连接，但恢复凭证仍可用于手动以原身份再次加入。

每个房间会话代次只允许一个房主。当前代没有房主时，任意在线参与者可通过 `POST /api/host/claim` 和 `HOST_PASSWORD` 认领；当前代不支持转移房主。房主不在线时普通参与者仍可聊天，但录音控制要求当前在线房主的有效连接代次。房间在全部参与者及重连宽限期均结束后进入空置状态；持续空置 60 秒会自动停止并封装正在进行的录音，随后关闭当前代并释放房主。下一位加入者创建递增的新代，并可再次凭口令认领房主。

## 录音文件

每个 session 目录包含 `session.json`、`events.jsonl`、`segments/`、`tracks/` 和 `tmp/`；`session.json` 会记录 `room_session_id` 与 `room_generation`。最终 WAV 为 mono、48 kHz、16-bit PCM，按参与者单独生成并以公共时间轴补静音。

## 测试与限制

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

当前版本使用 LiveKit 的 Twirp JSON 端点启动/停止 Track Egress，并以签名 WebSocket 接收 `pcm_s16le`。Webhook JWT 会校验签名、issuer 与请求体 SHA-256，并按 webhook ID 去重。仍需在目标公网环境完成真实浏览器、TURN、LiveKit/Egress 的端到端验收；仓库中的单元测试不会替代该项。
