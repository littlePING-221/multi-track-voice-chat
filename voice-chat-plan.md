# 单房间语音聊天室 MVP 开发实施文档

> Web + Rust + LiveKit
> 实现范围：实时语音 + 服务器分轨录音
> 适用对象：Coding Agent

---

## 1. 文档目的

本文档用于指导 Coding Agent 一次性完成语音聊天室 MVP。

本轮只实现以下两部分：

1. 固定单房间的多人实时语音
2. 服务器端按参与者分轨录音

不得在本轮扩展以下功能：

- 多房间
- 房间密码
- 文字聊天
- 举手
- 踢人或屏蔽
- 暂停和继续录音
- 浏览器本地录音缓存
- 断点补传
- AI 降噪
- 对象存储
- Android 或 iOS 原生应用
- Tauri 封装

项目应优先保证：

- 2–8 人能够稳定实时通话
- 支持桌面浏览器和手机浏览器
- 重点兼容 iPhone Safari
- 参与者退出或断线后，已经录制的内容不会丢失
- 最终得到每位参与者一个独立、时间轴对齐的 WAV 文件

---

## 2. 项目约束

| 项目 | 要求 |
|---|---|
| 并发人数 | 2–8 人 |
| 房间数量 | 固定单房间 |
| 用户身份 | 输入昵称加入 |
| 主持人 | 创建会话的用户 |
| 实时语音 | 所有人可同时说话 |
| 客户端 | 桌面浏览器、Android 浏览器、iPhone Safari |
| 部署方式 | 公网 Linux 服务器 |
| 实时媒体 | LiveKit |
| 业务后端 | Rust |
| 录音方式 | 服务器按参与者分轨录音 |
| 录音控制 | 主持人开始和结束 |
| 最终文件 | 每位参与者一个 WAV |
| 文件存储 | Linux 本地磁盘 |

---

## 3. Definition of Done

以下条件全部满足时，本轮开发才算完成：

- 2–8 名用户可通过昵称进入同一个固定房间
- 所有人可以同时说话并互相听见
- 用户可以静音和恢复自己的麦克风
- 页面可以显示参与者列表和音量指示
- Chrome、Edge、Android Chrome 和 iPhone Safari 可正常使用
- 主持人可以开始和结束录音
- 普通参与者无法调用录音控制接口
- 录音开始后，每个麦克风 Track 都由服务器独立录制
- 录音中加入的用户会自动开始录制
- 用户退出、刷新或断线时，已录内容不会删除
- 同一参与者重连后产生的新 Track 会合并到同一个最终文件
- 最终生成每人一个 WAV 文件
- 所有 WAV 参数一致
- 所有 WAV 从同一个时间轴起点开始
- 所有 WAV 总长度完全一致
- 晚加入和断线区间用数字静音补齐
- 生成 `session.json`
- 生成 `events.jsonl`
- Docker Compose 可在一台公网 Linux 主机运行全部服务

---

## 4. 技术架构

```text
Browser
React + TypeScript
  │
  ├── HTTPS / WebSocket
  │        │
  │        ▼
  │   Rust API
  │   Axum + Tokio
  │        ├── 昵称和参与者管理
  │        ├── 主持人权限
  │        ├── LiveKit Token 签发
  │        ├── 录音状态管理
  │        ├── LiveKit Webhook
  │        └── Recorder WebSocket
  │
  └── WebRTC
           │
           ▼
      LiveKit Server
      SFU 媒体转发
           │
           ▼
      LiveKit Egress
      每个 Track 单独导出
           │
           ▼
      PCM WebSocket
           │
           ▼
      Rust Recorder
           ├── PCM 分段文件
           ├── 时间轴元数据
           └── 最终 WAV
```

### 4.1 技术职责

#### LiveKit

负责：

- WebRTC 信令
- SFU 音频转发
- ICE、STUN、TURN
- 音轨发布和订阅
- 网络变化适应
- 浏览器兼容
- 参与者和 Track 生命周期
- Track Egress

#### Rust 后端

负责：

- 昵称加入
- 参与者身份
- 主持人权限
- LiveKit JWT 签发
- 录音状态
- Webhook 验证和处理
- 启动和停止 Track Egress
- PCM 数据接收
- 文件写入
- 时间轴计算
- WAV 封装
- 文件下载和索引

---

## 5. 固定技术选型

| 层 | 技术 | 说明 |
|---|---|---|
| 前端 | React + TypeScript + Vite | 使用 `livekit-client` |
| Rust Web 框架 | Axum | 提供 REST 和 WebSocket |
| 异步运行时 | Tokio | 网络和文件异步处理 |
| 数据库 | SQLite + SQLx | 保存业务状态和元数据 |
| 实时媒体 | LiveKit Server | 自托管 |
| 录音导出 | LiveKit Egress | Track Egress |
| 服务协调 | Redis | LiveKit 与 Egress 使用 |
| 反向代理 | Caddy 或 Nginx | HTTPS 和 WebSocket |
| 音频格式 | PCM S16LE → WAV | 48 kHz、mono、16-bit |
| 部署 | Docker Compose | 单机公网 Linux |
| 存储 | 本地磁盘 | Bind mount |

---

## 6. 核心数据模型

### 6.1 Participant

```rust
struct Participant {
    id: Uuid,
    livekit_identity: String,
    nickname: String,
    role: ParticipantRole,
    created_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
}

enum ParticipantRole {
    Host,
    Participant,
}
```

要求：

- `nickname` 只用于显示
- 不允许用昵称作为主键
- 昵称可以重复
- `livekit_identity` 必须使用稳定的内部 ID
- 用户刷新或重连时，应尽量恢复原有参与者身份
- 同一参与者的新 Track 必须归入同一个最终录音文件

---

### 6.2 RecordingSession

```rust
struct RecordingSession {
    id: Uuid,
    status: RecordingStatus,
    started_at_utc: DateTime<Utc>,
    stopped_at_utc: Option<DateTime<Utc>>,
    target_sample_rate: u32,
    target_channels: u16,
    target_sample_format: String,
    output_dir: PathBuf,
    version: i64,
}

enum RecordingStatus {
    Starting,
    Recording,
    Stopping,
    Completed,
    Failed,
}
```

运行时还需要保存：

```rust
struct RecordingRuntime {
    started_monotonic_ns: u64,
}
```

单调时钟只用于当前进程内的时间轴计算，不直接持久化后恢复。

---

### 6.3 TrackSegment

```rust
struct TrackSegment {
    id: Uuid,
    recording_id: Uuid,
    participant_id: Uuid,
    livekit_track_sid: String,
    segment_index: u32,
    first_frame_at_ns: u64,
    last_frame_at_ns: Option<u64>,
    timeline_start_sample: u64,
    sample_count: u64,
    pcm_path: PathBuf,
    status: TrackSegmentStatus,
}

enum TrackSegmentStatus {
    Opening,
    Writing,
    Closed,
    Failed,
}
```

---

## 7. 用户和主持人流程

### 7.1 创建主持人会话

创建会话时：

1. 前端调用 `POST /api/host/session`
2. 后端生成随机 `host_token`
3. 后端只保存 `host_token` 的 Argon2 哈希
4. 明文 token 只返回一次
5. 前端保存在 `localStorage`
6. 主持人仍作为普通 LiveKit 参与者加入房间

主持人身份不得只依赖前端变量。

---

### 7.2 普通用户加入

1. 用户输入昵称
2. 前端调用 `POST /api/join`
3. Rust 后端创建或恢复参与者
4. 后端签发 LiveKit JWT
5. 前端在用户点击后申请麦克风权限
6. 前端连接 LiveKit
7. 前端发布麦克风 Track
8. 前端订阅其他参与者的音频 Track

---

### 7.3 LiveKit Token 权限

浏览器 Token 只允许：

- 加入固定房间
- 发布麦克风
- 订阅其他 Track

浏览器 Token 不允许：

- `roomAdmin`
- `roomRecord`
- 管理其他参与者
- 直接调用 Egress

录音 Egress 只能由 Rust 后端使用服务器密钥调用。

---

## 8. 前端实现要求

### 8.1 页面

#### 加入页

需要包含：

- 昵称输入框
- 创建会话按钮
- 加入会话按钮
- 麦克风权限提示
- HTTPS 环境提示
- 浏览器兼容提示

#### 聊天室页面

需要包含：

- 参与者列表
- 主持人标记
- 在线状态
- 麦克风静音状态
- 实时音量条
- 当前连接状态
- 自己的静音按钮

#### 主持人区域

需要包含：

- 开始录音
- 结束录音
- 当前录音状态
- 已录活动时长
- 当前录制中的音轨数量
- 错误或警告提示

---

### 8.2 LiveKit 客户端

基础结构：

```ts
import { Room } from "livekit-client";

const room = new Room({
  adaptiveStream: true,
  dynacast: true,
});

await room.connect(livekitUrl, token);
await room.localParticipant.setMicrophoneEnabled(true);
```

Agent 必须以实际安装版本的 TypeScript 类型为准，不得照抄已经失效的 API。

需要监听的事件包括：

- ParticipantConnected
- ParticipantDisconnected
- TrackSubscribed
- TrackUnsubscribed
- ActiveSpeakersChanged
- Reconnecting
- Reconnected
- Disconnected

---

### 8.3 Safari 兼容要求

加入房间和启用音频必须由用户点击触发。

需要处理：

- 麦克风授权
- 自动播放限制
- `Room.canPlaybackAudio`
- `room.startAudio()`
- 页面切后台
- 手机锁屏
- 蓝牙耳机切换
- 网络切换
- 页面刷新

远端 Track 订阅后：

- 创建或复用 `<audio>` 元素
- 调用 Track 的 `attach`
- Track 离开时调用 `detach`
- 清理对应 DOM

---

### 8.4 麦克风约束

```ts
const constraints: MediaTrackConstraints = {
  echoCancellation: true,
  noiseSuppression: true,
  autoGainControl: true,
  channelCount: 1,
};
```

这些参数属于请求，不可假设所有浏览器都完全支持。

前端应读取实际 Track settings，并允许浏览器忽略不支持的参数。

---

## 9. Rust API

| Method | Path | 权限 | 作用 |
|---|---|---|---|
| POST | `/api/host/session` | 无 | 创建主持人凭证 |
| POST | `/api/join` | 无 | 昵称加入并返回 LiveKit Token |
| GET | `/api/state` | Participant | 获取参与者和录音状态 |
| POST | `/api/recordings/start` | Host | 开始录音 |
| POST | `/api/recordings/{id}/stop` | Host | 结束录音 |
| GET | `/api/recordings/{id}` | Participant | 查询录音状态和文件 |
| GET | `/api/recordings/{id}/tracks/{participant_id}` | Host | 下载单轨 WAV |
| POST | `/api/livekit/webhook` | LiveKit 签名 | 处理生命周期事件 |
| GET | `/internal/egress/{recording_id}/{track_sid}` | 临时签名 | 接收 Egress PCM |

---

### 9.1 Join 请求

```http
POST /api/join
Content-Type: application/json
```

```json
{
  "nickname": "小明"
}
```

响应：

```json
{
  "participant_id": "p_xxx",
  "nickname": "小明",
  "role": "participant",
  "livekit_url": "wss://voice.example.com",
  "livekit_token": "xxx",
  "recording_state": "recording"
}
```

---

### 9.2 开始录音

```http
POST /api/recordings/start
Authorization: Bearer <host_token>
```

响应：

```json
{
  "recording_id": "rec_xxx",
  "status": "starting"
}
```

---

### 9.3 结束录音

```http
POST /api/recordings/{recording_id}/stop
Authorization: Bearer <host_token>
```

响应：

```json
{
  "recording_id": "rec_xxx",
  "status": "stopping"
}
```

---

### 9.4 幂等性

`start` 和 `stop` 必须幂等。

要求：

- 已经处于 `starting` 或 `recording` 时重复调用 start，返回当前 session
- 不得创建第二个 RecordingSession
- 不得为同一个 Track 启动两个 Egress
- 已经处于 `stopping` 或 `completed` 时重复调用 stop，返回当前状态
- 不得重复封装 WAV
- 不得覆盖已完成文件

---

## 10. 服务器分轨录音

### 10.1 开始录音流程

1. 校验主持人 token
2. 检查录音目录可写
3. 检查磁盘剩余空间
4. 创建 RecordingSession
5. 状态设为 `starting`
6. 创建录音目录
7. 写入初始 `session.json`
8. 记录 UTC 开始时间
9. 记录单调时钟开始时间
10. 调用 LiveKit Room API 查询当前参与者和 Track
11. 过滤出 microphone audio Track
12. 为每个 Track 启动 Track Egress
13. Egress 输出到 Rust Recorder WebSocket
14. 成功启动后状态设为 `recording`

如果当前房间无人发布麦克风，也允许进入 `recording` 状态。

后续有人发布麦克风时再启动该 Track 的 Egress。

---

### 10.2 Track Egress 唯一性

必须对以下组合建立唯一约束：

```text
recording_id + track_sid
```

用于避免：

- Webhook 重试
- API 重试
- 网络超时后重复提交
- 同一 Track 启动多个 Egress

---

### 10.3 录音中新增用户

收到 LiveKit `track_published` Webhook 时：

1. 验证 Webhook 签名
2. 检查当前 RecordingSession 是否为 `recording`
3. 检查 Track 是否为 audio
4. 检查 Track source 是否为 microphone
5. 检查是否已经存在对应 Egress
6. 创建 TrackSegment
7. 启动 Track Egress
8. 接收 PCM 并落盘

---

### 10.4 用户离开或断线

发生以下情况时应关闭当前 segment：

- `track_unpublished`
- `participant_left`
- Egress WebSocket 断开
- Egress ended
- 网络错误

处理步骤：

1. Flush PCM 文件
2. 尝试 `fsync`
3. 关闭文件
4. 记录最后样本数
5. 将 segment 标记为 `closed`
6. 保留文件
7. 写入事件日志

不得删除已经写入的录音数据。

---

### 10.5 用户重连

同一参与者重连后通常会产生新的 `track_sid`。

必须：

- 使用稳定 `participant_id` 识别同一个人
- 为新 Track 创建新的 TrackSegment
- 不覆盖旧 segment
- 最终封装时把多个 segment 合并到同一个 WAV
- 中间没有音频的区间补静音

---

### 10.6 结束录音流程

1. 将 RecordingSession 原子更新为 `stopping`
2. 停止接受新的 Track Egress
3. 调用 StopEgress 停止全部活跃 Egress
4. 设置合理超时
5. 等待 Recorder WebSocket 结束
6. Flush 并关闭全部 PCM segment
7. 确定公共时间轴总长度
8. 按参与者读取所有 segment
9. 在时间轴正确位置写入音频
10. 空白区间写入数字静音
11. 为每位参与者生成 `final.wav.tmp`
12. Flush 和 fsync
13. 原子 rename 为最终 WAV
14. 补齐所有 WAV 到相同 sample count
15. 写入 `session.json`
16. 写入 `manifest.json`
17. 写入 `events.jsonl`
18. 将 RecordingSession 更新为 `completed`

单个 Track 失败时：

- 其他 Track 仍然继续完成
- 在 manifest 中记录 warning
- 不得因为一个人失败而丢弃整个 session

---

## 11. 时间轴对齐

最终输出是多个独立 WAV 文件，不是单个多轨容器。

```text
recording timeline:

0 --------------------------------------------------------- T

participant A:
[audio segment]----silence----[audio segment]---------silence

participant B:
----------silence--------[audio segment]---------------------

participant C:
----------------silence-------------[audio segment]----------

final:
A.wav length == B.wav length == C.wav length == T
```

---

### 11.1 MVP 时间轴算法

Recorder 接收到某 Track 第一批有效 PCM 时，记录单调时钟时间。

```text
timeline_start_sample =
round(
  (first_frame_monotonic_ns - recording_start_monotonic_ns)
  × 48000
  ÷ 1_000_000_000
)
```

结果表示该 segment 在公共时间轴上的开始样本位置。

同一 Track 后续 PCM 按连续样本写入。

下一个 segment 到来时，根据它自己的 `timeline_start_sample` 定位。

两个 segment 之间的空白使用零值样本补齐。

---

### 11.2 同步精度

MVP 使用：

- 服务器单调时钟
- Egress PCM 首帧到达时间
- 统一 48 kHz 样本时间轴

目标是满足语音讨论录音和后期剪辑，不承诺跨轨采样级同步。

Agent 不得在文档或 UI 中声称其为专业录音棚级采样同步。

---

### 11.3 必须记录的时间信息

需要记录：

- RecordingSession UTC 开始时间
- RecordingSession 单调时钟基准
- Egress WebSocket 建立时间
- 第一字节到达时间
- 第一完整 PCM 帧时间
- 最后一帧时间
- 累计 PCM 字节数
- 累计样本数
- Track publish 时间
- Track unpublish 时间
- Participant join 时间
- Participant leave 时间
- Egress start 时间
- Egress end 时间

---

## 12. 音频文件规范

| 属性 | 值 |
|---|---|
| 容器 | WAV / RIFF |
| 编码 | Linear PCM |
| 采样格式 | Signed 16-bit little-endian |
| 采样率 | 48,000 Hz |
| 声道 | Mono |
| 字节率 | 96,000 bytes/s |
| 最终命名 | `{participant_id}_{safe_nickname}.wav` |
| 临时分段 | `segments/{participant_id}/{index}_{track_sid}.pcm` |

最终文件必须：

- 参数一致
- 时间轴起点一致
- 总 sample count 一致
- 可被 Audacity、Reaper、Premiere、DaVinci Resolve 等软件直接读取

不得直接写最终文件名。

必须先写：

```text
final.wav.tmp
```

成功后再原子 rename：

```text
final.wav
```

---

## 13. 文件目录

```text
recordings/
└── rec_<uuid>/
    ├── session.json
    ├── events.jsonl
    ├── manifest.json
    ├── segments/
    │   ├── p_<uuid>/
    │   │   ├── 0001_<track_sid>.pcm
    │   │   ├── 0001_<track_sid>.json
    │   │   ├── 0002_<track_sid>.pcm
    │   │   └── 0002_<track_sid>.json
    │   └── ...
    ├── tracks/
    │   ├── p_<uuid>_<nickname>.wav
    │   └── ...
    └── tmp/
```

---

### 13.1 session.json

至少包含：

```json
{
  "recording_id": "rec_xxx",
  "room_name": "main",
  "status": "completed",
  "started_at": "2026-07-13T12:00:00Z",
  "stopped_at": "2026-07-13T13:00:00Z",
  "sample_rate": 48000,
  "channels": 1,
  "sample_format": "s16le",
  "timeline_samples": 172800000,
  "participants": [
    {
      "participant_id": "p_xxx",
      "nickname": "小明",
      "final_file": "tracks/p_xxx_小明.wav",
      "segments": 2,
      "warnings": []
    }
  ]
}
```

---

### 13.2 events.jsonl

每行一个 JSON 事件。

示例：

```json
{"type":"recording_started","at":"2026-07-13T12:00:00Z"}
{"type":"participant_joined","participant_id":"p_1","at":"2026-07-13T12:00:03Z"}
{"type":"track_published","participant_id":"p_1","track_sid":"TR_xxx","at":"2026-07-13T12:00:04Z"}
{"type":"segment_closed","participant_id":"p_1","samples":1440000,"at":"2026-07-13T12:00:34Z"}
```

---

## 14. 项目目录

```text
voice-room/
├── Cargo.toml
├── crates/
│   ├── server/
│   │   ├── src/
│   │   │   ├── api/
│   │   │   ├── auth/
│   │   │   ├── livekit/
│   │   │   ├── recording/
│   │   │   ├── webhook/
│   │   │   └── main.rs
│   ├── recorder/
│   │   ├── src/
│   │   │   ├── ws.rs
│   │   │   ├── pcm_writer.rs
│   │   │   ├── timeline.rs
│   │   │   └── wav.rs
│   └── domain/
├── web/
│   ├── src/
│   │   ├── pages/
│   │   ├── components/
│   │   ├── livekit/
│   │   └── api/
├── migrations/
├── deploy/
│   ├── docker-compose.yml
│   ├── livekit.yaml
│   ├── egress.yaml
│   └── Caddyfile
├── recordings/
├── .env.example
└── README.md
```

---

## 15. Rust 建议依赖

```toml
axum
tokio
tower-http
serde
serde_json
sqlx
uuid
chrono
jsonwebtoken
argon2
reqwest
tokio-tungstenite
tracing
tracing-subscriber
hound
thiserror
anyhow
```

Agent 应锁定具体版本，并确保依赖之间兼容。

---

## 16. 工程规范

必须满足：

- 使用 Rust stable
- `cargo fmt` 通过
- `cargo clippy -- -D warnings` 通过
- `cargo test` 通过
- 不在日志中输出任何密钥或 token
- 不允许使用昵称直接构造文件路径
- 所有外部 HTTP 请求必须设置超时
- 所有 WebSocket 连接必须设置最大消息和缓冲限制
- PCM 写入必须使用有界缓冲
- 磁盘写入速度不足时不得无限增长内存
- Webhook 必须验签
- Webhook 必须支持幂等
- Egress 启动和停止必须有超时
- 文件落盘后必须保留可恢复元数据
- 录音开始前必须检查磁盘空间
- 空间不足时应拒绝开始录音

---

## 17. Docker Compose

至少包含：

```text
caddy
rust-server
livekit
livekit-egress
redis
```

前端静态文件可以由 Caddy 直接托管。

录音目录：

```yaml
volumes:
  - ./data/recordings:/data/recordings
  - ./data/sqlite:/data/sqlite
```

部署要求：

- 必须配置 HTTPS
- 必须支持 WebSocket
- LiveKit 媒体 UDP 端口必须开放
- 必须部署 TURN 后备路径
- Egress 必须作为独立服务运行
- Redis 必须供 LiveKit 和 Egress 使用
- 录音目录必须持久化到宿主机
- SQLite 目录必须持久化
- 生产环境不得使用默认密钥

---

## 18. 实施顺序

### 步骤 1：工程初始化

- 创建 Rust workspace
- 创建 React + Vite 前端
- 创建 Docker Compose
- 启动 Redis
- 启动 LiveKit
- 启动 LiveKit Egress
- 配置 Caddy

完成标志：

- 前端可访问
- Rust 健康检查通过
- LiveKit 服务可连接

---

### 步骤 2：身份和 Token

- 实现主持人创建接口
- 实现昵称加入
- 实现参与者 session
- 实现 LiveKit JWT
- 实现 host token 哈希保存

完成标志：

- 两个浏览器可加入同一房间
- 两人 identity 不冲突
- 普通参与者没有录音权限

---

### 步骤 3：实时语音

- 发布本地麦克风
- 订阅远程 Track
- 实现参与者列表
- 实现静音
- 实现音量指示
- 实现连接状态
- 处理 Safari 播放限制

完成标志：

- 2–8 人可以实时通话
- 桌面和手机浏览器可用

---

### 步骤 4：Webhook

- 实现 LiveKit Webhook endpoint
- 验证签名
- 记录 participant 和 track 事件
- 实现幂等去重

完成标志：

- 加入、离开、发布和取消发布事件准确写入数据库及日志

---

### 步骤 5：录音状态

- 实现 RecordingSession
- 实现 start
- 实现 stop
- 实现主持人权限
- 实现状态幂等
- 实现目录创建和磁盘检查

完成标志：

- 只能由主持人控制录音
- 重复请求不会产生重复 session

---

### 步骤 6：Egress 和 Recorder

- 实现 Track Egress 调用
- 实现临时签名 URL
- 实现 Recorder WebSocket
- 实现 PCM 文件写入
- 实现有界缓冲
- 实现 segment 元数据

完成标志：

- 单个麦克风 Track 可生成连续 PCM 文件

---

### 步骤 7：动态加入和断线

- 录音中发布新 Track 时自动启动 Egress
- 用户退出时关闭 segment
- 用户重连时创建新 segment
- 保留旧 segment
- 同一参与者归并

完成标志：

- 每个参与者拥有独立 PCM segment
- 刷新或断线不会删除已有内容

---

### 步骤 8：WAV 封装

- 计算公共时间轴
- 计算每个 segment 的起始样本
- 插入数字静音
- 合并同一参与者的多个 segment
- 生成 WAV
- 补齐总长度
- 写 manifest

完成标志：

- 每人一个 WAV
- 所有文件等长
- 晚加入和断线位置正确补静音

---

### 步骤 9：异常处理

- Egress 启动失败
- Egress 停止超时
- Recorder WS 中断
- 磁盘空间不足
- Rust 进程退出
- 部分 Track 失败
- 文件 finalize 失败

完成标志：

- 已落盘文件不会被删除
- 其他参与者录音仍能完成
- 错误被记录在 manifest 和日志中

---

### 步骤 10：验收

执行全部自动化测试和人工测试。

---

## 19. 测试用例

| 编号 | 场景 | 预期结果 |
|---|---|---|
| A01 | 两人 Chrome 加入 | 互相听见 |
| A02 | 8 人同时在线 | 通话可用，服务无明显异常 |
| A03 | iPhone Safari 加入 | 可授权麦克风并播放远端音频 |
| A04 | 用户拒绝麦克风 | 页面显示明确错误 |
| A05 | 用户静音和恢复 | 远端声音停止和恢复，连接不断开 |
| A06 | Wi-Fi 切换移动网络 | 尝试自动重连并恢复 |
| R01 | 两人开始录音 | 生成两个独立 WAV |
| R02 | 普通用户调用 start | 返回 403 |
| R03 | 录音 10 秒后第三人加入 | 第三人文件前 10 秒静音 |
| R04 | 用户说话后关闭页面 | 已录内容保留 |
| R05 | 同一用户刷新重连 | 新旧 segment 合并到同一 WAV |
| R06 | 用户录音中静音 | 时间轴不断开，该区间为静音或近静音 |
| R07 | 重复点击开始 | 只有一个 session |
| R08 | 重复点击结束 | 只 finalize 一次 |
| R09 | 一个 Egress 失败 | 其他轨道仍完成 |
| R10 | Recorder WebSocket 异常中断 | PCM 文件被安全关闭 |
| R11 | Rust 进程录音中重启 | 已落盘 PCM 不损坏 |
| R12 | 检查所有 WAV | 参数一致且 sample count 完全相等 |
| R13 | 两人同时拍手 | 导入剪辑软件后时间差在 MVP 目标范围内 |

---

## 20. Agent 最终交付物

Agent 必须交付：

- 完整源码
- 可运行的前端
- 可运行的 Rust 服务
- 数据库 migration
- Docker Compose
- LiveKit 配置
- Egress 配置
- Caddy 或 Nginx 配置
- `.env.example`
- README
- 自动化测试
- 手工验收记录
- 已知限制说明

README 至少包含：

- 本地开发步骤
- Linux 部署步骤
- 域名配置
- HTTPS 配置
- 防火墙端口
- TURN 配置
- LiveKit API key 和 secret 配置
- Egress 配置
- 数据目录
- 录音文件说明
- 常见故障排查

---

## 21. 不得擅自改变的决策

- 不自行实现 WebRTC SFU
- 使用 LiveKit
- Rust 负责业务和录音编排
- 不使用浏览器 MediaRecorder 作为主录音来源
- 不先混音再做说话人分离
- 最终产物是每人一个 WAV
- 不使用单文件多轨容器作为主要交付格式
- 晚加入、退出和重连通过 segment 和静音补齐处理
- 录音状态必须存放在 Rust 后端
- 本轮不实现暂停和继续
- 本轮不实现本地录音缓存
- 本轮不实现复杂音频后处理
- 本轮不实现多个房间

---

## 22. 开发前必须核对的官方资料

Agent 开发前应核对当前版本的 LiveKit 官方文档：

- Self-hosting
- LiveKit Server 部署
- 防火墙和 UDP 端口
- TURN 配置
- JavaScript Client SDK
- Room 和 Participant
- Track 发布与订阅
- Safari 音频播放限制
- Webhooks
- Webhook 验签
- Room Service API
- Egress API
- Track Egress
- WebSocket PCM 输出
- LiveKit Server LICENSE
- LiveKit Egress LICENSE

所有 API 名称和参数应以项目锁定版本的官方文档和类型定义为准。
