# 手工验收记录

日期：2026-07-13

## 已自动验证

- `cargo fmt --all -- --check` 通过。
- Docker Rust 1.96 环境执行 `cargo test --workspace` 通过，覆盖时间轴换算、等长 WAV、晚加入静音补齐。
- `web/npm run build` 通过。
- API 冒烟测试已覆盖健康检查、主持人凭证、主持人/普通参与者加入、录音 start/stop 幂等。

## 待部署环境验证

- A01-A06：两台浏览器和 iPhone Safari 的麦克风、远端播放、静音、重连与网络切换。
- R01-R13：至少两台真实浏览器发布麦克风，确认 Egress WebSocket 产生分段 PCM，并检查最终 WAV 的采样参数、长度与对齐。
- 公网 HTTPS、TURN 回退、UDP 50000-50100 防火墙及 Nginx 根路径 `/rtc` 反向代理。
- 使用生产 API key/secret 后的 LiveKit Webhook 签名和 Egress Twirp 调用。

真实 LiveKit/Egress 容器尚未在本工作区启动，因此上述项不标记为通过。
