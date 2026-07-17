use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{ws::Message, Path, Query, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::{distributions::Alphanumeric, Rng};
use reqwest::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    env,
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info, warn};
use uuid::Uuid;
use voice_domain::{
    Participant, ParticipantRole, RecordingSession, RecordingStatus, TrackSegment,
    TrackSegmentStatus,
};
use voice_recorder::{
    append_event, check_disk, downmix_stereo_s16le, finalize_wav_from_files, timeline_start_sample,
    SAMPLE_RATE,
};

const ROOM: &str = "main";
const MIN_FREE_BYTES: u64 = 256 * 1024 * 1024;
type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct AppState {
    store: Store,
    recordings: PathBuf,
    livekit_url: String,
    egress_url: String,
    callback_base_url: String,
    api_key: String,
    api_secret: String,
    host_password: Option<String>,
    http: Client,
    active_streams: Arc<AsyncMutex<HashSet<(Uuid, String)>>>,
}

#[derive(Clone)]
struct Store(Arc<Mutex<Connection>>);

#[derive(Clone, Debug)]
struct SegmentRow {
    segment: TrackSegment,
    egress_id: Option<String>,
}

#[derive(Serialize)]
struct JoinResponse {
    participant_id: Uuid,
    nickname: String,
    role: ParticipantRole,
    livekit_url: String,
    livekit_token: String,
    session_token: String,
    resume_token: String,
    connection_generation: Uuid,
    recording_state: Option<RecordingStatus>,
}

#[derive(Deserialize)]
struct JoinRequest {
    nickname: String,
}

#[derive(Deserialize)]
struct ResumeRequest {
    resume_token: String,
}

#[derive(Deserialize)]
struct HostClaimRequest {
    password: String,
}

#[derive(Clone, Debug)]
struct RoomSession {
    id: Uuid,
    generation: i64,
    host_participant_id: Option<Uuid>,
}

#[derive(Serialize)]
struct ParticipantView {
    #[serde(flatten)]
    participant: Participant,
    connection_state: String,
}

#[derive(Serialize)]
struct StateResponse {
    participants: Vec<ParticipantView>,
    recording: Option<RecordingSession>,
    room_generation: i64,
    has_host: bool,
}

#[derive(Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    exp: usize,
    video: VideoGrant,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct SessionClaims {
    iss: String,
    sub: String,
    exp: usize,
    generation: Uuid,
    purpose: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VideoGrant {
    room: String,
    room_join: bool,
    can_publish: bool,
    can_subscribe: bool,
    can_publish_data: bool,
    room_admin: bool,
    room_record: bool,
}

#[derive(Deserialize)]
struct WebhookClaims {
    sha256: String,
}

#[derive(Deserialize)]
struct EgressQuery {
    participant_id: Uuid,
    exp: u64,
    sig: String,
}

impl Store {
    fn open(path: &FsPath) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(include_str!("../../../migrations/001_initial.sql"))?;
        conn.execute_batch(include_str!(
            "../../../migrations/002_participant_presence.sql"
        ))?;
        conn.execute_batch(include_str!(
            "../../../migrations/003_identity_lifecycle.sql"
        ))?;
        conn.execute_batch(include_str!("../../../migrations/004_room_sessions.sql"))?;
        Ok(Self(Arc::new(Mutex::new(conn))))
    }

    fn room_host(&self) -> anyhow::Result<Option<Uuid>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT host_participant_id FROM room_sessions WHERE room_name=?1 AND state='active'",
            [ROOM],
            |row| row.get::<_, Option<String>>(0)?.map(parse_uuid).transpose(),
        )
        .optional()
        .map(|value| value.flatten())
        .map_err(Into::into)
    }

    fn active_room_session(&self) -> anyhow::Result<Option<RoomSession>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT id,generation,host_participant_id FROM room_sessions WHERE room_name=?1 AND state='active'",
            [ROOM],
            |row| Ok(RoomSession { id: parse_uuid(row.get(0)?)?, generation: row.get(1)?, host_participant_id: row.get::<_, Option<String>>(2)?.map(parse_uuid).transpose()? }),
        ).optional().map_err(Into::into)
    }

    fn ensure_active_room_session(&self) -> anyhow::Result<RoomSession> {
        if let Some(session) = self.active_room_session()? {
            return Ok(session);
        }
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let generation: i64 = conn.query_row(
            "SELECT COALESCE(MAX(generation),0)+1 FROM room_sessions WHERE room_name=?1",
            [ROOM],
            |row| row.get(0),
        )?;
        let session = RoomSession {
            id: Uuid::new_v4(),
            generation,
            host_participant_id: None,
        };
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO room_sessions(id,room_name,generation,state,host_participant_id,opened_at,closed_at) VALUES(?1,?2,?3,'active',NULL,?4,NULL)",
            params![session.id.to_string(), ROOM, generation, Utc::now().to_rfc3339()],
        )?;
        if inserted == 1 {
            return Ok(session);
        }
        conn.query_row(
            "SELECT id,generation,host_participant_id FROM room_sessions WHERE room_name=?1 AND state='active'",
            [ROOM],
            |row| Ok(RoomSession { id: parse_uuid(row.get(0)?)?, generation: row.get(1)?, host_participant_id: row.get::<_, Option<String>>(2)?.map(parse_uuid).transpose()? }),
        ).map_err(Into::into)
    }

    fn claim_session_host(&self, participant_id: Uuid) -> anyhow::Result<bool> {
        let session = self.ensure_active_room_session()?;
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let changed = conn.execute(
            "UPDATE room_sessions SET host_participant_id=?2 WHERE id=?1 AND state='active' AND host_participant_id IS NULL",
            params![session.id.to_string(), participant_id.to_string()],
        )? == 1;
        if changed {
            conn.execute(
                "UPDATE participants SET role='host' WHERE id=?1",
                [participant_id.to_string()],
            )?;
        }
        Ok(changed)
    }

    fn close_active_room_session_if_empty(&self) -> anyhow::Result<Option<RoomSession>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let present: u64 = conn.query_row(
            "SELECT COUNT(*) FROM participant_connections WHERE state IN ('connecting','online','reconnecting')",
            [],
            |row| row.get(0),
        )?;
        if present > 0 {
            return Ok(None);
        }
        let Some(session) = conn.query_row(
            "SELECT id,generation,host_participant_id FROM room_sessions WHERE room_name=?1 AND state='active'",
            [ROOM],
            |row| Ok(RoomSession { id: parse_uuid(row.get(0)?)?, generation: row.get(1)?, host_participant_id: row.get::<_, Option<String>>(2)?.map(parse_uuid).transpose()? }),
        ).optional()? else {
            return Ok(None);
        };
        let changed = conn.execute(
            "UPDATE room_sessions SET state='closed',closed_at=?2 WHERE id=?1 AND state='active'",
            params![session.id.to_string(), Utc::now().to_rfc3339()],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        if let Some(host_id) = session.host_participant_id {
            conn.execute(
                "UPDATE participants SET role='participant' WHERE id=?1",
                [host_id.to_string()],
            )?;
        }
        Ok(Some(session))
    }

    fn host(&self) -> anyhow::Result<Option<(Uuid, String)>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT participant_id, token_hash FROM host_credentials WHERE room_name = ?1",
            [ROOM],
            |row| Ok((parse_uuid(row.get::<_, String>(0)?)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
    }

    fn participant(&self, id: Uuid) -> anyhow::Result<Option<Participant>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT id, livekit_identity, nickname, role, created_at, last_seen_at FROM participants WHERE id=?1",
            [id.to_string()],
            participant_from_row,
        ).optional().map_err(Into::into)
    }

    fn upsert_participant(&self, participant: &Participant) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.execute(
            "INSERT INTO participants(id, livekit_identity, nickname, role, created_at, last_seen_at) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(id) DO UPDATE SET nickname=excluded.nickname,role=excluded.role,last_seen_at=excluded.last_seen_at",
            params![participant.id.to_string(), participant.livekit_identity, participant.nickname, role_name(&participant.role), participant.created_at.to_rfc3339(), participant.last_seen_at.to_rfc3339()],
        )?;
        Ok(())
    }

    fn set_resume_credential(&self, participant_id: Uuid, hash: &str) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO identity_credentials(participant_id,resume_token_hash,created_at,rotated_at) VALUES(?1,?2,?3,?3) ON CONFLICT(participant_id) DO UPDATE SET resume_token_hash=excluded.resume_token_hash,rotated_at=excluded.rotated_at",
            params![participant_id.to_string(), hash, now],
        )?;
        Ok(())
    }

    fn resume_credential(&self, participant_id: Uuid) -> anyhow::Result<Option<String>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT resume_token_hash FROM identity_credentials WHERE participant_id=?1",
            [participant_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    fn begin_connection(&self, participant_id: Uuid, generation: Uuid) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let deadline = (Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        conn.execute(
            "INSERT INTO participant_connections(participant_id,generation,livekit_participant_sid,state,reconnect_deadline,changed_at) VALUES(?1,?2,NULL,'connecting',?3,?4) ON CONFLICT(participant_id) DO UPDATE SET generation=excluded.generation,livekit_participant_sid=NULL,state='connecting',reconnect_deadline=excluded.reconnect_deadline,changed_at=excluded.changed_at",
            params![participant_id.to_string(), generation.to_string(), deadline, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    fn mark_connection_online(
        &self,
        participant_id: Uuid,
        generation: Uuid,
        sid: Option<&str>,
    ) -> anyhow::Result<bool> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        Ok(conn.execute(
            "UPDATE participant_connections SET livekit_participant_sid=?3,state='online',reconnect_deadline=NULL,changed_at=?4 WHERE participant_id=?1 AND generation=?2",
            params![participant_id.to_string(), generation.to_string(), sid, Utc::now().to_rfc3339()],
        )? == 1)
    }

    fn mark_connection_reconnecting(
        &self,
        participant_id: Uuid,
        generation: Uuid,
        deadline: DateTime<Utc>,
    ) -> anyhow::Result<bool> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        Ok(conn.execute(
            "UPDATE participant_connections SET state='reconnecting',reconnect_deadline=?3,changed_at=?4 WHERE participant_id=?1 AND generation=?2 AND state!='left'",
            params![participant_id.to_string(), generation.to_string(), deadline.to_rfc3339(), Utc::now().to_rfc3339()],
        )? == 1)
    }

    fn mark_connection_left(&self, participant_id: Uuid, generation: Uuid) -> anyhow::Result<bool> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        Ok(conn.execute(
            "UPDATE participant_connections SET state='left',reconnect_deadline=NULL,changed_at=?3 WHERE participant_id=?1 AND generation=?2",
            params![participant_id.to_string(), generation.to_string(), Utc::now().to_rfc3339()],
        )? == 1)
    }

    fn connection_state(
        &self,
        participant_id: Uuid,
        generation: Uuid,
    ) -> anyhow::Result<Option<String>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT state FROM participant_connections WHERE participant_id=?1 AND generation=?2",
            params![participant_id.to_string(), generation.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    fn expire_connections(&self) -> anyhow::Result<usize> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        Ok(conn.execute(
            "UPDATE participant_connections SET state='offline',changed_at=?1 WHERE state IN ('connecting','reconnecting') AND reconnect_deadline<=?1",
            [Utc::now().to_rfc3339()],
        )?)
    }

    fn all_participants(&self) -> anyhow::Result<Vec<Participant>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let mut stmt = conn.prepare("SELECT id, livekit_identity, nickname, role, created_at, last_seen_at FROM participants ORDER BY created_at")?;
        let result = stmt
            .query_map([], participant_from_row)?
            .collect::<Result<_, _>>()
            .map_err(Into::into);
        result
    }

    fn present_participants(&self) -> anyhow::Result<Vec<ParticipantView>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT p.id,p.livekit_identity,p.nickname,p.role,p.created_at,p.last_seen_at,c.state FROM participants p JOIN participant_connections c ON c.participant_id=p.id WHERE c.state IN ('connecting','online','reconnecting') ORDER BY p.created_at",
        )?;
        let participants = stmt
            .query_map([], |row| {
                Ok(ParticipantView {
                    participant: participant_from_row(row)?,
                    connection_state: row.get(6)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(participants)
    }

    fn present_count(&self) -> anyhow::Result<u64> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        Ok(conn.query_row("SELECT COUNT(*) FROM participant_connections WHERE state IN ('connecting','online','reconnecting')", [], |row| row.get(0))?)
    }

    fn room_empty_since(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let value: Option<String> = conn.query_row(
            "SELECT empty_since FROM room_runtime WHERE room_name=?1",
            [ROOM],
            |row| row.get(0),
        )?;
        value.map(parse_time).transpose().map_err(Into::into)
    }

    fn set_room_empty_since(&self, value: Option<DateTime<Utc>>) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.execute(
            "UPDATE room_runtime SET empty_since=?2 WHERE room_name=?1",
            params![ROOM, value.map(|v| v.to_rfc3339())],
        )?;
        Ok(())
    }

    fn active_recording(&self) -> anyhow::Result<Option<RecordingSession>> {
        self.recording_by_status(&["starting", "recording", "stopping"])
    }

    fn recording(&self, id: Uuid) -> anyhow::Result<Option<RecordingSession>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT id,status,started_at_utc,stopped_at_utc,sample_rate,channels,sample_format,output_dir,version FROM recording_sessions WHERE id=?1",
            [id.to_string()], recording_from_row,
        ).optional().map_err(Into::into)
    }

    fn recording_by_status(&self, statuses: &[&str]) -> anyhow::Result<Option<RecordingSession>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        for status in statuses {
            if let Some(recording) = conn.query_row(
                "SELECT id,status,started_at_utc,stopped_at_utc,sample_rate,channels,sample_format,output_dir,version FROM recording_sessions WHERE status=?1 ORDER BY started_at_utc DESC LIMIT 1",
                [*status], recording_from_row,
            ).optional()? { return Ok(Some(recording)); }
        }
        Ok(None)
    }

    fn create_recording(
        &self,
        recording: &RecordingSession,
        room_session: &RoomSession,
    ) -> anyhow::Result<()> {
        let mut conn = self.0.lock().expect("sqlite mutex poisoned");
        let transaction = conn.transaction()?;
        transaction.execute("INSERT INTO recording_sessions(id,status,started_at_utc,stopped_at_utc,sample_rate,channels,sample_format,output_dir,version) VALUES(?1,?2,?3,NULL,?4,?5,?6,?7,?8)", params![recording.id.to_string(), status_name(&recording.status), recording.started_at_utc.to_rfc3339(), recording.target_sample_rate, recording.target_channels, recording.target_sample_format, recording.output_dir.to_string_lossy(), recording.version])?;
        transaction.execute(
            "INSERT INTO recording_room_sessions(recording_id,room_session_id,room_generation) VALUES(?1,?2,?3)",
            params![recording.id.to_string(), room_session.id.to_string(), room_session.generation],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn recording_room_session(&self, recording_id: Uuid) -> anyhow::Result<RoomSession> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row(
            "SELECT s.id,m.room_generation,s.host_participant_id FROM recording_room_sessions m JOIN room_sessions s ON s.id=m.room_session_id WHERE m.recording_id=?1",
            [recording_id.to_string()],
            |row| Ok(RoomSession { id: parse_uuid(row.get(0)?)?, generation: row.get(1)?, host_participant_id: row.get::<_, Option<String>>(2)?.map(parse_uuid).transpose()? }),
        ).map_err(Into::into)
    }

    fn update_recording(&self, recording: &RecordingSession) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.execute(
            "UPDATE recording_sessions SET status=?2, stopped_at_utc=?3, version=?4 WHERE id=?1",
            params![
                recording.id.to_string(),
                status_name(&recording.status),
                recording.stopped_at_utc.map(|v| v.to_rfc3339()),
                recording.version
            ],
        )?;
        Ok(())
    }

    fn insert_segment(&self, segment: &TrackSegment) -> anyhow::Result<bool> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let changed = conn.execute("INSERT OR IGNORE INTO track_segments(id,recording_id,participant_id,track_sid,segment_index,first_frame_at_ns,last_frame_at_ns,timeline_start_sample,sample_count,pcm_path,status,egress_id) VALUES(?1,?2,?3,?4,?5,?6,NULL,?7,0,?8,?9,NULL)", params![segment.id.to_string(), segment.recording_id.to_string(), segment.participant_id.to_string(), segment.livekit_track_sid, segment.segment_index, segment.first_frame_at_ns, segment.timeline_start_sample, segment.pcm_path.to_string_lossy(), segment_status_name(&segment.status)])?;
        Ok(changed == 1)
    }

    fn set_egress(
        &self,
        recording_id: Uuid,
        track_sid: &str,
        egress_id: Option<&str>,
        status: TrackSegmentStatus,
    ) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.execute("UPDATE track_segments SET egress_id=?3,status=?4 WHERE recording_id=?1 AND track_sid=?2", params![recording_id.to_string(), track_sid, egress_id, segment_status_name(&status)])?;
        Ok(())
    }

    fn segment(&self, recording_id: Uuid, track_sid: &str) -> anyhow::Result<Option<SegmentRow>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.query_row("SELECT id,recording_id,participant_id,track_sid,segment_index,first_frame_at_ns,last_frame_at_ns,timeline_start_sample,sample_count,pcm_path,status,egress_id FROM track_segments WHERE recording_id=?1 AND track_sid=?2", params![recording_id.to_string(), track_sid], segment_from_row).optional().map_err(Into::into)
    }

    fn update_segment_progress(
        &self,
        recording_id: Uuid,
        track_sid: &str,
        first_ns: u64,
        timeline_start: u64,
        samples: u64,
        status: TrackSegmentStatus,
    ) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.execute("UPDATE track_segments SET first_frame_at_ns=?3,timeline_start_sample=?4,sample_count=?5,status=?6 WHERE recording_id=?1 AND track_sid=?2", params![recording_id.to_string(), track_sid, first_ns as i64, timeline_start as i64, samples as i64, segment_status_name(&status)])?;
        Ok(())
    }

    fn close_segment(
        &self,
        recording_id: Uuid,
        track_sid: &str,
        last_ns: u64,
        samples: u64,
    ) -> anyhow::Result<()> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        conn.execute("UPDATE track_segments SET last_frame_at_ns=?3,sample_count=?4,status='closed' WHERE recording_id=?1 AND track_sid=?2", params![recording_id.to_string(), track_sid, last_ns as i64, samples as i64])?;
        Ok(())
    }

    fn segments(&self, recording_id: Uuid) -> anyhow::Result<Vec<SegmentRow>> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        let mut stmt = conn.prepare("SELECT id,recording_id,participant_id,track_sid,segment_index,first_frame_at_ns,last_frame_at_ns,timeline_start_sample,sample_count,pcm_path,status,egress_id FROM track_segments WHERE recording_id=?1 ORDER BY participant_id,timeline_start_sample")?;
        let result = stmt
            .query_map([recording_id.to_string()], segment_from_row)?
            .collect::<Result<_, _>>()
            .map_err(Into::into);
        result
    }

    fn record_webhook(&self, event_id: &str, event: &str, payload: &str) -> anyhow::Result<bool> {
        let conn = self.0.lock().expect("sqlite mutex poisoned");
        Ok(conn.execute("INSERT OR IGNORE INTO webhook_events(id,event,received_at,payload) VALUES(?1,?2,?3,?4)", params![event_id, event, Utc::now().to_rfc3339(), payload])? == 1)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    let recordings =
        PathBuf::from(env::var("RECORDINGS_DIR").unwrap_or_else(|_| "./recordings".into()));
    fs::create_dir_all(&recordings)?;
    let sqlite_path = PathBuf::from(
        env::var("SQLITE_PATH").unwrap_or_else(|_| "./data/sqlite/voice-chat.db".into()),
    );
    let state = AppState {
        store: Store::open(&sqlite_path)?,
        recordings,
        livekit_url: env::var("LIVEKIT_URL").unwrap_or_else(|_| "ws://localhost:7880".into()),
        egress_url: env::var("LIVEKIT_EGRESS_URL").unwrap_or_else(|_| "http://livekit:7880".into()),
        callback_base_url: env::var("EGRESS_CALLBACK_BASE_URL")
            .unwrap_or_else(|_| "ws://rust-server:3000".into())
            .trim_end_matches('/')
            .to_owned(),
        api_key: required_env("LIVEKIT_API_KEY")?,
        api_secret: required_env("LIVEKIT_API_SECRET")?,
        host_password: env::var("HOST_PASSWORD")
            .or_else(|_| env::var("HOST_BOOTSTRAP_KEY"))
            .ok()
            .filter(|value| !value.is_empty()),
        http: Client::builder().timeout(Duration::from_secs(10)).build()?,
        active_streams: Arc::new(AsyncMutex::new(HashSet::new())),
    };
    tokio::spawn(lifecycle_monitor(state.clone()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/host/claim", post(claim_host))
        .route("/api/join", post(join))
        .route("/api/resume", post(resume))
        .route("/api/leave", post(leave))
        .route("/api/state", get(state_view))
        .route("/api/recordings/start", post(start_recording))
        .route("/api/recordings/{id}/stop", post(stop_recording))
        .route("/api/recordings/{id}", get(recording_view))
        .route(
            "/api/recordings/{id}/tracks/{participant_id}",
            get(download_track),
        )
        .route("/api/livekit/webhook", post(webhook))
        .route(
            "/internal/egress/{recording_id}/{track_sid}",
            get(egress_ws),
        )
        .with_state(state);
    let addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    info!(%addr, "voice server listening");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn join(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<JoinRequest>,
) -> impl IntoResponse {
    let nickname = input.nickname.trim();
    if nickname.is_empty() || nickname.chars().count() > 80 {
        return bad_request("nickname must be 1-80 characters");
    }
    let legacy_host = s
        .store
        .host()
        .ok()
        .flatten()
        .filter(|(_, hash)| host_ok(&headers, hash));
    let participant = if let Some((id, _)) = legacy_host {
        match s.store.participant(id) {
            Ok(Some(mut participant)) => {
                participant.nickname = nickname.into();
                participant.last_seen_at = Utc::now();
                participant
            }
            Ok(None) => Participant {
                id,
                livekit_identity: format!("p_{}", id.simple()),
                nickname: nickname.into(),
                role: ParticipantRole::Host,
                created_at: Utc::now(),
                last_seen_at: Utc::now(),
            },
            Err(e) => return internal(e),
        }
    } else {
        new_participant(nickname, ParticipantRole::Participant)
    };
    if let Err(e) = s.store.upsert_participant(&participant) {
        return internal(e);
    }
    match issue_connection(&s, participant) {
        Ok(response) => Json(response).into_response(),
        Err(e) => internal(e),
    }
}

async fn claim_host(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<HostClaimRequest>,
) -> impl IntoResponse {
    let Some((participant_id, generation)) = session_identity(&s, &headers) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"participant authorization required"})),
        )
            .into_response();
    };
    if !s
        .store
        .connection_state(participant_id, generation)
        .ok()
        .flatten()
        .is_some_and(|state| matches!(state.as_str(), "connecting" | "online"))
    {
        return forbidden();
    }
    let Some(expected) = s.host_password.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"host password is not configured"})),
        )
            .into_response();
    };
    if Sha256::digest(input.password.as_bytes()) != Sha256::digest(expected.as_bytes()) {
        return forbidden();
    }
    match s.store.claim_session_host(participant_id) {
        Ok(true) => Json(json!({"role":"host"})).into_response(),
        Ok(false) if s.store.room_host().ok().flatten() == Some(participant_id) => {
            Json(json!({"role":"host"})).into_response()
        }
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(json!({"error":"this room session already has a host"})),
        )
            .into_response(),
        Err(e) => internal(e),
    }
}

async fn resume(State(s): State<AppState>, Json(input): Json<ResumeRequest>) -> impl IntoResponse {
    let Some(participant_id) = resume_token_participant_id(&input.resume_token) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid resume credential"})),
        )
            .into_response();
    };
    let Some(hash) = s.store.resume_credential(participant_id).ok().flatten() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid resume credential"})),
        )
            .into_response();
    };
    if !verify_secret(&input.resume_token, &hash) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid resume credential"})),
        )
            .into_response();
    }
    let Some(participant) = s.store.participant(participant_id).ok().flatten() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"identity no longer exists"})),
        )
            .into_response();
    };
    match issue_connection(&s, participant) {
        Ok(response) => Json(response).into_response(),
        Err(e) => internal(e),
    }
}

async fn state_view(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_participant(&s, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"participant authorization required"})),
        )
            .into_response();
    }
    let _ = s.store.expire_connections();
    let room_session = match s.store.ensure_active_room_session() {
        Ok(value) => value,
        Err(e) => return internal(e),
    };
    match (s.store.present_participants(), s.store.active_recording()) {
        (Ok(participants), Ok(recording)) => Json(StateResponse {
            participants,
            recording,
            room_generation: room_session.generation,
            has_host: room_session.host_participant_id.is_some(),
        })
        .into_response(),
        (Err(e), _) | (_, Err(e)) => internal(e),
    }
}

async fn leave(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let Some((participant_id, generation)) = session_identity(&s, &headers) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"participant authorization required"})),
        )
            .into_response();
    };
    match s.store.mark_connection_left(participant_id, generation) {
        Ok(true) => {}
        Ok(false) => return StatusCode::NO_CONTENT.into_response(),
        Err(e) => return internal(e),
    }
    if let Some(recording) = s.store.active_recording().ok().flatten() {
        let _ = append_event(
            &recording.output_dir.join("events.jsonl"),
            &json!({"type":"participant_left","participant_id":participant_id,"reason":"graceful","at":Utc::now()}),
        );
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn start_recording(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_online_host(&s, &headers) {
        return forbidden();
    }
    if let Ok(Some(recording)) = s.store.active_recording() {
        return Json(json!({"recording_id":recording.id,"status":recording.status}))
            .into_response();
    }
    let id = Uuid::new_v4();
    let dir = s.recordings.join(format!("rec_{id}"));
    if let Err(e) = check_disk(&dir, MIN_FREE_BYTES) {
        return (
            StatusCode::INSUFFICIENT_STORAGE,
            Json(json!({"error":e.to_string()})),
        )
            .into_response();
    }
    for subdir in ["segments", "tracks", "tmp"] {
        if let Err(e) = fs::create_dir_all(dir.join(subdir)) {
            return internal(e.into());
        }
    }
    let recording = RecordingSession {
        id,
        status: RecordingStatus::Starting,
        started_at_utc: Utc::now(),
        stopped_at_utc: None,
        target_sample_rate: SAMPLE_RATE,
        target_channels: 1,
        target_sample_format: "s16le".into(),
        output_dir: dir.clone(),
        version: 1,
    };
    let room_session = match s.store.active_room_session() {
        Ok(Some(value)) => value,
        Ok(None) => return internal(anyhow::anyhow!("active room session disappeared")),
        Err(e) => return internal(e),
    };
    if let Err(e) = s.store.create_recording(&recording, &room_session) {
        return internal(e);
    }
    let _ = append_event(
        &dir.join("events.jsonl"),
        &json!({"type":"recording_started","at":recording.started_at_utc}),
    );
    if let Err(e) = write_session_snapshot(&s, &recording) {
        return internal(e);
    }
    let tracks = match list_microphone_tracks(&s).await {
        Ok(tracks) => tracks,
        Err(e) => {
            warn!(error=%e, "could not list LiveKit tracks; webhook will start later tracks");
            Vec::new()
        }
    };
    for (participant_id, track_sid) in tracks {
        if let Err(e) = start_track_egress(&s, &recording, participant_id, &track_sid).await {
            warn!(%track_sid, error=%e, "could not start track egress");
        }
    }
    let mut recording = recording;
    recording.status = RecordingStatus::Recording;
    recording.version += 1;
    if let Err(e) = s
        .store
        .update_recording(&recording)
        .and_then(|_| write_session_snapshot(&s, &recording))
    {
        return internal(e);
    }
    Json(json!({"recording_id":id,"status":recording.status})).into_response()
}

async fn stop_recording(
    Path(id): Path<Uuid>,
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !is_online_host(&s, &headers) {
        return forbidden();
    }
    match stop_recording_session(&s, id).await {
        Ok(recording) => Json(json!({"recording_id":id,"status":recording.status})).into_response(),
        Err(e) if e.to_string() == "recording not found" => not_found("recording not found"),
        Err(e) => internal(e),
    }
}

async fn stop_recording_session(s: &AppState, id: Uuid) -> anyhow::Result<RecordingSession> {
    let Some(mut recording) = s.store.recording(id)? else {
        anyhow::bail!("recording not found");
    };
    if matches!(
        recording.status,
        RecordingStatus::Stopping | RecordingStatus::Completed
    ) {
        return Ok(recording);
    }
    recording.status = RecordingStatus::Stopping;
    recording.version += 1;
    s.store.update_recording(&recording)?;
    let segments = s.store.segments(id)?;
    for segment in &segments {
        if let Some(egress_id) = &segment.egress_id {
            if let Err(e) = stop_egress(s, egress_id).await {
                warn!(%egress_id, error=%e, "egress stop failed");
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if !s
            .active_streams
            .lock()
            .await
            .iter()
            .any(|(recording_id, _)| *recording_id == id)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    match finalize_recording(s, &mut recording) {
        Ok(()) => Ok(recording),
        Err(e) => {
            recording.status = RecordingStatus::Failed;
            recording.version += 1;
            let _ = s.store.update_recording(&recording);
            let _ = write_session_snapshot(s, &recording);
            Err(e)
        }
    }
}

async fn lifecycle_monitor(state: AppState) {
    let mut timer = tokio::time::interval(Duration::from_secs(5));
    loop {
        timer.tick().await;
        if let Err(e) = state.store.expire_connections() {
            warn!(error=%e, "could not expire reconnecting participants");
            continue;
        }
        let present = match state.store.present_count() {
            Ok(value) => value,
            Err(e) => {
                warn!(error=%e, "could not count room participants");
                continue;
            }
        };
        if present > 0 {
            let _ = state.store.set_room_empty_since(None);
            continue;
        }
        let empty_since = match state.store.room_empty_since() {
            Ok(Some(value)) => value,
            Ok(None) => {
                let _ = state.store.set_room_empty_since(Some(Utc::now()));
                continue;
            }
            Err(e) => {
                warn!(error=%e, "could not read room lifecycle");
                continue;
            }
        };
        if Utc::now().signed_duration_since(empty_since) < chrono::Duration::seconds(60) {
            continue;
        }
        if let Ok(Some(recording)) = state.store.active_recording() {
            if matches!(
                recording.status,
                RecordingStatus::Starting | RecordingStatus::Recording
            ) {
                info!(recording_id=%recording.id, "stopping recording after room remained empty");
                if let Err(e) = stop_recording_session(&state, recording.id).await {
                    error!(recording_id=%recording.id, error=%e, "automatic recording stop failed");
                }
            }
        }
        match state.store.close_active_room_session_if_empty() {
            Ok(Some(session)) => info!(
                room_generation = session.generation,
                "closed empty room session and released host"
            ),
            Ok(None) => {}
            Err(e) => error!(error=%e, "could not close empty room session"),
        }
        let _ = state.store.set_room_empty_since(None);
    }
}

async fn recording_view(
    Path(id): Path<Uuid>,
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !is_participant(&s, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"participant authorization required"})),
        )
            .into_response();
    }
    match s.store.recording(id) {
        Ok(Some(recording)) => Json(recording).into_response(),
        Ok(None) => not_found("recording not found"),
        Err(e) => internal(e),
    }
}

async fn download_track(
    Path((recording_id, participant_id)): Path<(Uuid, Uuid)>,
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !is_online_host(&s, &headers) {
        return forbidden();
    }
    let Some(recording) = (match s.store.recording(recording_id) {
        Ok(v) => v,
        Err(e) => return internal(e),
    }) else {
        return not_found("recording not found");
    };
    let participant = match s.store.participant(participant_id) {
        Ok(Some(v)) => v,
        Ok(None) => return not_found("participant not found"),
        Err(e) => return internal(e),
    };
    let path = recording.output_dir.join("tracks").join(format!(
        "p_{}_{}.wav",
        participant.id,
        safe_filename(&participant.nickname)
    ));
    match fs::read(path) {
        Ok(bytes) => ([("content-type", "audio/wav")], bytes).into_response(),
        Err(_) => not_found("track file not found"),
    }
}

async fn webhook(State(s): State<AppState>, headers: HeaderMap, body: String) -> impl IntoResponse {
    let event = match verify_webhook(&s, &headers, &body) {
        Ok(v) => v,
        Err(e) => {
            warn!(error=%e, "rejected LiveKit webhook");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"invalid webhook"})),
            )
                .into_response();
        }
    };
    let event_name = event
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let event_id = event.get("id").and_then(Value::as_str).unwrap_or_default();
    if event_id.is_empty() || event_name.is_empty() {
        return bad_request("invalid webhook event");
    }
    match s.store.record_webhook(event_id, event_name, &body) {
        Ok(true) => {}
        Ok(false) => return StatusCode::OK.into_response(),
        Err(e) => return internal(e),
    }
    let now = Utc::now();
    if let Some(recording) = s.store.active_recording().ok().flatten() {
        let log = recording.output_dir.join("events.jsonl");
        let _ = append_event(
            &log,
            &json!({"type":event_name,"at":now,"livekit_event":event_id}),
        );
    }
    match event_name {
        "participant_joined" => {
            if let (Some(participant_id), Some(generation)) = (
                participant_id_from_event(&event),
                connection_generation_from_event(&event),
            ) {
                let sid = event
                    .get("participant")
                    .and_then(|value| value.get("sid"))
                    .and_then(Value::as_str);
                if let Err(e) = s
                    .store
                    .mark_connection_online(participant_id, generation, sid)
                {
                    warn!(%participant_id, %generation, error=%e, "could not mark connection online");
                }
            }
        }
        "participant_left" => {
            if let (Some(participant_id), Some(generation)) = (
                participant_id_from_event(&event),
                connection_generation_from_event(&event),
            ) {
                let grace = if s.store.room_host().ok().flatten() == Some(participant_id) {
                    300
                } else {
                    60
                };
                let deadline = Utc::now() + chrono::Duration::seconds(grace);
                if let Err(e) =
                    s.store
                        .mark_connection_reconnecting(participant_id, generation, deadline)
                {
                    warn!(%participant_id, %generation, error=%e, "could not mark connection reconnecting");
                }
            }
        }
        _ => {}
    }
    if let Some(recording) = s.store.active_recording().ok().flatten() {
        match event_name {
            "track_published" => {
                if let Some((participant_id, track_sid)) = microphone_track_from_event(&s, &event) {
                    if let Err(e) =
                        start_track_egress(&s, &recording, participant_id, &track_sid).await
                    {
                        warn!(%track_sid, error=%e, "could not start egress from webhook");
                    }
                }
            }
            "track_unpublished" => {
                if let Some((_, track_sid)) = microphone_track_from_event(&s, &event) {
                    if let Err(e) = stop_track_egress(&s, &recording, &track_sid).await {
                        warn!(%track_sid, error=%e, "could not stop egress for unpublished track");
                    }
                }
            }
            "participant_left" => {
                if let Some(participant_id) = participant_id_from_event(&event) {
                    for segment in s
                        .store
                        .segments(recording.id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|row| row.segment.participant_id == participant_id)
                    {
                        if let Err(e) =
                            stop_track_egress(&s, &recording, &segment.segment.livekit_track_sid)
                                .await
                        {
                            warn!(track_sid=%segment.segment.livekit_track_sid, error=%e, "could not stop egress for departed participant");
                        }
                    }
                }
            }
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn egress_ws(
    Path((recording_id, track_sid)): Path<(Uuid, String)>,
    Query(query): Query<EgressQuery>,
    State(s): State<AppState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if !verify_egress_signature(&s, recording_id, &track_sid, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let segment = match s.store.segment(recording_id, &track_sid) {
        Ok(Some(v)) if v.segment.participant_id == query.participant_id => v,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return internal(e),
    };
    ws.max_frame_size(256 * 1024)
        .max_message_size(256 * 1024)
        .on_upgrade(move |socket| receive_pcm(s, segment, socket))
        .into_response()
}

async fn receive_pcm(
    state: AppState,
    segment_row: SegmentRow,
    mut socket: axum::extract::ws::WebSocket,
) {
    let recording_id = segment_row.segment.recording_id;
    let track_sid = segment_row.segment.livekit_track_sid.clone();
    let key = (recording_id, track_sid.clone());
    {
        let mut active = state.active_streams.lock().await;
        if !active.insert(key.clone()) {
            warn!(%recording_id, %track_sid, "duplicate egress websocket rejected");
            let _ = socket.close().await;
            return;
        }
    }
    let result = receive_pcm_inner(&state, &segment_row, &mut socket).await;
    state.active_streams.lock().await.remove(&key);
    if let Err(e) = result {
        warn!(%recording_id, %track_sid, error=%e, "PCM receiver failed");
        let _ = state.store.set_egress(
            recording_id,
            &track_sid,
            segment_row.egress_id.as_deref(),
            TrackSegmentStatus::Failed,
        );
    }
}

async fn receive_pcm_inner(
    state: &AppState,
    row: &SegmentRow,
    socket: &mut axum::extract::ws::WebSocket,
) -> anyhow::Result<()> {
    let recording_id = row.segment.recording_id;
    let track_sid = &row.segment.livekit_track_sid;
    let path = &row.segment.pcm_path;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::with_capacity(128 * 1024, file);
    let mut first_ns = row.segment.first_frame_at_ns;
    let mut timeline_start = row.segment.timeline_start_sample;
    let mut samples = row.segment.sample_count;
    let mut stereo_pending = Vec::with_capacity(256 * 1024 + 3);
    let mut mono = Vec::with_capacity(128 * 1024);
    let recording_started_at = state
        .store
        .recording(recording_id)?
        .ok_or_else(|| anyhow::anyhow!("recording disappeared while receiving PCM"))?
        .started_at_utc;
    while let Some(message) = socket.recv().await {
        match message? {
            Message::Binary(bytes) => {
                stereo_pending.extend_from_slice(&bytes);
                let stereo_bytes = stereo_pending.len() / 4 * 4;
                if stereo_bytes == 0 {
                    continue;
                }
                mono.clear();
                let frames = downmix_stereo_s16le(&stereo_pending[..stereo_bytes], &mut mono);
                let elapsed_ns = elapsed_ns_since(recording_started_at);
                if first_ns == 0 {
                    first_ns = elapsed_ns;
                    timeline_start = timeline_start_sample(elapsed_ns, 0);
                    state.store.update_segment_progress(
                        recording_id,
                        track_sid,
                        first_ns,
                        timeline_start,
                        samples,
                        TrackSegmentStatus::Writing,
                    )?;
                }
                writer.write_all(&mono)?;
                samples += frames as u64;
                stereo_pending.drain(..stereo_bytes);
                if samples % (SAMPLE_RATE as u64 * 5) < frames as u64 {
                    writer.flush()?;
                    state.store.update_segment_progress(
                        recording_id,
                        track_sid,
                        first_ns,
                        timeline_start,
                        samples,
                        TrackSegmentStatus::Writing,
                    )?;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    if !stereo_pending.is_empty() {
        warn!(%recording_id, %track_sid, bytes=stereo_pending.len(), "discarding incomplete stereo PCM frame");
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let last_ns = elapsed_ns_since(recording_started_at).max(first_ns);
    state
        .store
        .close_segment(recording_id, track_sid, last_ns, samples)?;
    if let Some(recording) = state.store.recording(recording_id)? {
        let _ = append_event(
            &recording.output_dir.join("events.jsonl"),
            &json!({"type":"segment_closed","participant_id":row.segment.participant_id,"track_sid":track_sid,"samples":samples,"at":Utc::now()}),
        );
    }
    let metadata = json!({"track_sid":track_sid,"first_frame_at_ns":first_ns,"last_frame_at_ns":last_ns,"timeline_start_sample":timeline_start,"sample_count":samples,"status":"closed"});
    fs::write(
        path.with_extension("json"),
        serde_json::to_vec_pretty(&metadata)?,
    )?;
    Ok(())
}

async fn start_track_egress(
    state: &AppState,
    recording: &RecordingSession,
    participant_id: Uuid,
    track_sid: &str,
) -> anyhow::Result<()> {
    if !matches!(
        recording.status,
        RecordingStatus::Starting | RecordingStatus::Recording
    ) {
        anyhow::bail!("recording is not accepting tracks");
    }
    let index = state
        .segments_for_participant(recording.id, participant_id)?
        .len() as u32
        + 1;
    let dir = recording
        .output_dir
        .join("segments")
        .join(format!("p_{participant_id}"));
    fs::create_dir_all(&dir)?;
    let segment = TrackSegment {
        id: Uuid::new_v4(),
        recording_id: recording.id,
        participant_id,
        livekit_track_sid: track_sid.into(),
        segment_index: index,
        first_frame_at_ns: 0,
        last_frame_at_ns: None,
        timeline_start_sample: 0,
        sample_count: 0,
        pcm_path: dir.join(format!("{:04}_{}.pcm", index, safe_track_id(track_sid))),
        status: TrackSegmentStatus::Opening,
    };
    if !state.store.insert_segment(&segment)? {
        return Ok(());
    }
    let callback = signed_callback_url(state, recording.id, track_sid, participant_id)?;
    let response = state
        .http
        .post(format!(
            "{}/twirp/livekit.Egress/StartTrackEgress",
            state.egress_url.trim_end_matches('/')
        ))
        .bearer_auth(server_token(state)?)
        .json(&json!({"room_name":ROOM,"track_id":track_sid,"websocket_url":callback}))
        .send()
        .await?;
    if !response.status().is_success() {
        state
            .store
            .set_egress(recording.id, track_sid, None, TrackSegmentStatus::Failed)?;
        anyhow::bail!("StartTrackEgress returned {}", response.status());
    }
    let payload: Value = response.json().await?;
    let egress_id = payload
        .get("egress_id")
        .or_else(|| payload.get("egressId"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("egress response has no egress_id"))?;
    state.store.set_egress(
        recording.id,
        track_sid,
        Some(egress_id),
        TrackSegmentStatus::Opening,
    )?;
    append_event(
        &recording.output_dir.join("events.jsonl"),
        &json!({"type":"egress_started","participant_id":participant_id,"track_sid":track_sid,"egress_id":egress_id,"at":Utc::now()}),
    )?;
    Ok(())
}

impl AppState {
    fn segments_for_participant(
        &self,
        recording_id: Uuid,
        participant_id: Uuid,
    ) -> anyhow::Result<Vec<SegmentRow>> {
        Ok(self
            .store
            .segments(recording_id)?
            .into_iter()
            .filter(|v| v.segment.participant_id == participant_id)
            .collect())
    }
}

async fn stop_egress(state: &AppState, egress_id: &str) -> anyhow::Result<()> {
    let response = state
        .http
        .post(format!(
            "{}/twirp/livekit.Egress/StopEgress",
            state.egress_url.trim_end_matches('/')
        ))
        .bearer_auth(server_token(state)?)
        .json(&json!({"egress_id":egress_id}))
        .send()
        .await?;
    if response.status().is_success() {
        Ok(())
    } else {
        anyhow::bail!("StopEgress returned {}", response.status())
    }
}

async fn stop_track_egress(
    state: &AppState,
    recording: &RecordingSession,
    track_sid: &str,
) -> anyhow::Result<()> {
    let Some(row) = state.store.segment(recording.id, track_sid)? else {
        return Ok(());
    };
    if matches!(
        row.segment.status,
        TrackSegmentStatus::Closed | TrackSegmentStatus::Failed
    ) {
        return Ok(());
    }
    if let Some(egress_id) = row.egress_id.as_deref() {
        stop_egress(state, egress_id).await?;
    } else {
        let elapsed_ns =
            elapsed_ns_since(recording.started_at_utc).max(row.segment.first_frame_at_ns);
        state.store.close_segment(
            recording.id,
            track_sid,
            elapsed_ns,
            row.segment.sample_count,
        )?;
    }
    Ok(())
}

async fn list_microphone_tracks(state: &AppState) -> anyhow::Result<Vec<(Uuid, String)>> {
    let response = state
        .http
        .post(format!(
            "{}/twirp/livekit.RoomService/ListParticipants",
            state.egress_url.trim_end_matches('/')
        ))
        .bearer_auth(server_token(state)?)
        .json(&json!({"room":ROOM}))
        .send()
        .await?;
    if !response.status().is_success() {
        anyhow::bail!("ListParticipants returned {}", response.status())
    }
    let body: Value = response.json().await?;
    let mut tracks = Vec::new();
    for participant in body
        .get("participants")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let identity = participant
            .get("identity")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let participant_id = identity
            .strip_prefix("p_")
            .and_then(|v| Uuid::parse_str(v).ok());
        for track in participant
            .get("tracks")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if is_microphone_track(track) {
                if let (Some(participant_id), Some(sid)) =
                    (participant_id, track.get("sid").and_then(Value::as_str))
                {
                    tracks.push((participant_id, sid.into()));
                }
            }
        }
    }
    Ok(tracks)
}

fn finalize_recording(state: &AppState, recording: &mut RecordingSession) -> anyhow::Result<()> {
    let segments = state.store.segments(recording.id)?;
    let participants = state.store.all_participants()?;
    let runtime_samples = timeline_start_sample(elapsed_ns_since(recording.started_at_utc), 0);
    let timeline_samples = segments
        .iter()
        .map(|s| {
            s.segment
                .timeline_start_sample
                .saturating_add(s.segment.sample_count)
        })
        .max()
        .unwrap_or(0)
        .max(runtime_samples);
    let mut manifest = Vec::new();
    for participant in participants {
        let mut participant_segments: Vec<_> = segments
            .iter()
            .filter(|s| {
                s.segment.participant_id == participant.id
                    && !matches!(s.segment.status, TrackSegmentStatus::Failed)
            })
            .map(|s| (s.segment.timeline_start_sample, s.segment.pcm_path.clone()))
            .collect();
        participant_segments.sort_by_key(|v| v.0);
        if participant_segments.is_empty() {
            continue;
        }
        let name = format!(
            "p_{}_{}.wav",
            participant.id,
            safe_filename(&participant.nickname)
        );
        let output = recording.output_dir.join("tracks").join(&name);
        let tmp = recording.output_dir.join("tmp").join(format!("{name}.tmp"));
        finalize_wav_from_files(&tmp, &output, &participant_segments, timeline_samples)?;
        manifest.push(json!({"participant_id":participant.id,"nickname":participant.nickname,"final_file":format!("tracks/{name}"),"segments":participant_segments.len(),"warnings":[]}));
    }
    recording.status = RecordingStatus::Completed;
    recording.stopped_at_utc = Some(Utc::now());
    recording.version += 1;
    state.store.update_recording(recording)?;
    write_session_snapshot(state, recording)?;
    fs::write(
        recording.output_dir.join("manifest.json"),
        serde_json::to_vec_pretty(
            &json!({"recording_id":recording.id,"timeline_samples":timeline_samples,"participants":manifest}),
        )?,
    )?;
    append_event(
        &recording.output_dir.join("events.jsonl"),
        &json!({"type":"recording_completed","at":recording.stopped_at_utc}),
    )?;
    Ok(())
}

fn write_session_snapshot(state: &AppState, recording: &RecordingSession) -> anyhow::Result<()> {
    let participants = state.store.all_participants()?;
    let room_session = state.store.recording_room_session(recording.id)?;
    fs::write(
        recording.output_dir.join("session.json"),
        serde_json::to_vec_pretty(
            &json!({"recording_id":recording.id,"room_name":ROOM,"room_session_id":room_session.id,"room_generation":room_session.generation,"status":recording.status,"started_at":recording.started_at_utc,"stopped_at":recording.stopped_at_utc,"sample_rate":SAMPLE_RATE,"channels":1,"sample_format":"s16le","participants":participants}),
        )?,
    )?;
    Ok(())
}

fn verify_webhook(state: &AppState, headers: &HeaderMap, body: &str) -> anyhow::Result<Value> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("missing authorization"))?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[state.api_key.as_str()]);
    let token = decode::<WebhookClaims>(
        auth.strip_prefix("Bearer ").unwrap_or(auth),
        &DecodingKey::from_secret(state.api_secret.as_bytes()),
        &validation,
    )?;
    let expected = Sha256::digest(body.as_bytes());
    let supplied = STANDARD
        .decode(&token.claims.sha256)
        .or_else(|_| URL_SAFE_NO_PAD.decode(&token.claims.sha256))?;
    if supplied != expected.as_slice() {
        anyhow::bail!("webhook payload hash does not match")
    }
    Ok(serde_json::from_str(body)?)
}

fn microphone_track_from_event(state: &AppState, event: &Value) -> Option<(Uuid, String)> {
    let track = event.get("track")?;
    if !is_microphone_track(track) {
        return None;
    }
    let identity = event.get("participant")?.get("identity")?.as_str()?;
    let participant_id = identity
        .strip_prefix("p_")
        .and_then(|v| Uuid::parse_str(v).ok())?;
    let generation = connection_generation_from_event(event)?;
    if state
        .store
        .connection_state(participant_id, generation)
        .ok()
        .flatten()
        .is_none()
    {
        return None;
    }
    Some((participant_id, track.get("sid")?.as_str()?.to_owned()))
}

fn connection_generation_from_event(event: &Value) -> Option<Uuid> {
    let metadata = event.get("participant")?.get("metadata")?.as_str()?;
    serde_json::from_str::<Value>(metadata)
        .ok()?
        .get("connection_generation")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn participant_id_from_event(event: &Value) -> Option<Uuid> {
    event
        .get("participant")?
        .get("identity")?
        .as_str()?
        .strip_prefix("p_")
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn elapsed_ns_since(started_at: DateTime<Utc>) -> u64 {
    Utc::now()
        .signed_duration_since(started_at)
        .num_nanoseconds()
        .unwrap_or(0)
        .max(0) as u64
}
fn is_microphone_track(track: &Value) -> bool {
    let kind = track
        .get("type")
        .or_else(|| track.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let source = track.get("source");
    (kind.eq_ignore_ascii_case("audio") || kind == "KIND_AUDIO" || kind == "1")
        && source.is_some_and(|v| {
            v.as_str().is_some_and(|s| {
                s.eq_ignore_ascii_case("microphone") || s == "MICROPHONE" || s == "1"
            }) || v.as_i64() == Some(1)
        })
}
fn signed_callback_url(
    state: &AppState,
    recording_id: Uuid,
    track_sid: &str,
    participant_id: Uuid,
) -> anyhow::Result<String> {
    let exp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 300;
    let sig = egress_signature(
        &state.api_secret,
        recording_id,
        track_sid,
        participant_id,
        exp,
    )?;
    Ok(format!(
        "{}/internal/egress/{}/{}?participant_id={}&exp={}&sig={}",
        state.callback_base_url, recording_id, track_sid, participant_id, exp, sig
    ))
}
fn egress_signature(
    secret: &str,
    recording_id: Uuid,
    track_sid: &str,
    participant_id: Uuid,
    exp: u64,
) -> anyhow::Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
    mac.update(format!("{recording_id}:{track_sid}:{participant_id}:{exp}").as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}
fn verify_egress_signature(
    state: &AppState,
    recording_id: Uuid,
    track_sid: &str,
    query: &EgressQuery,
) -> bool {
    if query.exp
        < SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|v| v.as_secs())
            .unwrap_or(u64::MAX)
    {
        return false;
    }
    egress_signature(
        &state.api_secret,
        recording_id,
        track_sid,
        query.participant_id,
        query.exp,
    )
    .is_ok_and(|expected| expected == query.sig)
}
fn participant_token(
    state: &AppState,
    participant: &Participant,
    generation: Uuid,
) -> anyhow::Result<String> {
    jwt(
        state,
        &participant.livekit_identity,
        VideoGrant {
            room: ROOM.into(),
            room_join: true,
            can_publish: true,
            can_subscribe: true,
            can_publish_data: false,
            room_admin: false,
            room_record: false,
        },
        Some(json!({"connection_generation":generation}).to_string()),
    )
}
fn server_token(state: &AppState) -> anyhow::Result<String> {
    jwt(
        state,
        "voice-recorder",
        VideoGrant {
            room: ROOM.into(),
            room_join: false,
            can_publish: false,
            can_subscribe: false,
            can_publish_data: false,
            room_admin: true,
            room_record: true,
        },
        None,
    )
}
fn jwt(
    state: &AppState,
    sub: &str,
    video: VideoGrant,
    metadata: Option<String>,
) -> anyhow::Result<String> {
    let exp = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 300) as usize;
    Ok(encode(
        &Header::default(),
        &Claims {
            iss: state.api_key.clone(),
            sub: sub.into(),
            exp,
            video,
            metadata,
        },
        &EncodingKey::from_secret(state.api_secret.as_bytes()),
    )?)
}

fn new_participant(nickname: &str, role: ParticipantRole) -> Participant {
    let id = Uuid::new_v4();
    let now = Utc::now();
    Participant {
        id,
        livekit_identity: format!("p_{}", id.simple()),
        nickname: nickname.into(),
        role,
        created_at: now,
        last_seen_at: now,
    }
}

fn issue_connection(
    state: &AppState,
    mut participant: Participant,
) -> anyhow::Result<JoinResponse> {
    let room_session = state.store.ensure_active_room_session()?;
    participant.role = if room_session.host_participant_id == Some(participant.id) {
        ParticipantRole::Host
    } else {
        ParticipantRole::Participant
    };
    participant.last_seen_at = Utc::now();
    state.store.upsert_participant(&participant)?;
    let resume_token = format!("{}.{}", participant.id, random_token(48));
    let resume_hash = hash_secret(&resume_token)?;
    state
        .store
        .set_resume_credential(participant.id, &resume_hash)?;
    let generation = Uuid::new_v4();
    state.store.begin_connection(participant.id, generation)?;
    Ok(JoinResponse {
        participant_id: participant.id,
        nickname: participant.nickname.clone(),
        role: participant.role.clone(),
        livekit_url: state.livekit_url.clone(),
        livekit_token: participant_token(state, &participant, generation)?,
        session_token: session_token(state, participant.id, generation)?,
        resume_token,
        connection_generation: generation,
        recording_state: state
            .store
            .active_recording()?
            .map(|recording| recording.status),
    })
}

fn session_token(
    state: &AppState,
    participant_id: Uuid,
    generation: Uuid,
) -> anyhow::Result<String> {
    let exp = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 12 * 60 * 60) as usize;
    Ok(encode(
        &Header::default(),
        &SessionClaims {
            iss: state.api_key.clone(),
            sub: participant_id.to_string(),
            exp,
            generation,
            purpose: "voice-chat-session".into(),
        },
        &EncodingKey::from_secret(state.api_secret.as_bytes()),
    )?)
}

fn session_identity(state: &AppState, headers: &HeaderMap) -> Option<(Uuid, Uuid)> {
    let Some(value) = headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
    else {
        return None;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return None;
    };
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[state.api_key.as_str()]);
    let Ok(claims) = decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(state.api_secret.as_bytes()),
        &validation,
    ) else {
        return None;
    };
    if claims.claims.purpose != "voice-chat-session" {
        return None;
    }
    Some((
        Uuid::parse_str(&claims.claims.sub).ok()?,
        claims.claims.generation,
    ))
}

fn is_participant(state: &AppState, headers: &HeaderMap) -> bool {
    let Some((participant_id, generation)) = session_identity(state, headers) else {
        return false;
    };
    state
        .store
        .connection_state(participant_id, generation)
        .ok()
        .flatten()
        .is_some_and(|value| matches!(value.as_str(), "connecting" | "online" | "reconnecting"))
}

fn is_online_host(state: &AppState, headers: &HeaderMap) -> bool {
    let Some((participant_id, generation)) = session_identity(state, headers) else {
        return false;
    };
    state.store.room_host().ok().flatten() == Some(participant_id)
        && state
            .store
            .connection_state(participant_id, generation)
            .ok()
            .flatten()
            .is_some_and(|value| matches!(value.as_str(), "connecting" | "online"))
}

fn resume_token_participant_id(token: &str) -> Option<Uuid> {
    Uuid::parse_str(token.split_once('.')?.0).ok()
}

fn hash_secret(secret: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    Ok(Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!("could not hash resume credential: {error}"))?
        .to_string())
}

fn verify_secret(secret: &str, hash: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(secret.as_bytes(), &parsed)
            .is_ok()
    })
}
fn host_ok(headers: &HeaderMap, hash: &str) -> bool {
    let Some(value) = headers.get("authorization").and_then(|x| x.to_str().ok()) else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(token.as_bytes(), &parsed)
            .is_ok()
    })
}
fn random_token(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
fn safe_filename(name: &str) -> String {
    let value: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let value = value.trim_matches('_');
    if value.is_empty() {
        "participant".into()
    } else {
        value.chars().take(48).collect()
    }
}
fn safe_track_id(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .collect()
}
fn role_name(role: &ParticipantRole) -> &'static str {
    match role {
        ParticipantRole::Host => "host",
        ParticipantRole::Participant => "participant",
    }
}
fn status_name(status: &RecordingStatus) -> &'static str {
    match status {
        RecordingStatus::Starting => "starting",
        RecordingStatus::Recording => "recording",
        RecordingStatus::Stopping => "stopping",
        RecordingStatus::Completed => "completed",
        RecordingStatus::Failed => "failed",
    }
}
fn segment_status_name(status: &TrackSegmentStatus) -> &'static str {
    match status {
        TrackSegmentStatus::Opening => "opening",
        TrackSegmentStatus::Writing => "writing",
        TrackSegmentStatus::Closed => "closed",
        TrackSegmentStatus::Failed => "failed",
    }
}
fn parse_uuid(value: String) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(&value).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}
fn participant_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Participant> {
    Ok(Participant {
        id: parse_uuid(row.get(0)?)?,
        livekit_identity: row.get(1)?,
        nickname: row.get(2)?,
        role: match row.get::<_, String>(3)?.as_str() {
            "host" => ParticipantRole::Host,
            _ => ParticipantRole::Participant,
        },
        created_at: parse_time(row.get(4)?)?,
        last_seen_at: parse_time(row.get(5)?)?,
    })
}
fn recording_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordingSession> {
    let status = match row.get::<_, String>(1)?.as_str() {
        "starting" => RecordingStatus::Starting,
        "recording" => RecordingStatus::Recording,
        "stopping" => RecordingStatus::Stopping,
        "completed" => RecordingStatus::Completed,
        _ => RecordingStatus::Failed,
    };
    let stopped: Option<String> = row.get(3)?;
    Ok(RecordingSession {
        id: parse_uuid(row.get(0)?)?,
        status,
        started_at_utc: parse_time(row.get(2)?)?,
        stopped_at_utc: stopped.map(parse_time).transpose()?,
        target_sample_rate: row.get(4)?,
        target_channels: row.get(5)?,
        target_sample_format: row.get(6)?,
        output_dir: PathBuf::from(row.get::<_, String>(7)?),
        version: row.get(8)?,
    })
}
fn segment_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SegmentRow> {
    let status = match row.get::<_, String>(10)?.as_str() {
        "opening" => TrackSegmentStatus::Opening,
        "writing" => TrackSegmentStatus::Writing,
        "closed" => TrackSegmentStatus::Closed,
        _ => TrackSegmentStatus::Failed,
    };
    let last: Option<i64> = row.get(6)?;
    Ok(SegmentRow {
        segment: TrackSegment {
            id: parse_uuid(row.get(0)?)?,
            recording_id: parse_uuid(row.get(1)?)?,
            participant_id: parse_uuid(row.get(2)?)?,
            livekit_track_sid: row.get(3)?,
            segment_index: row.get(4)?,
            first_frame_at_ns: row.get::<_, i64>(5)? as u64,
            last_frame_at_ns: last.map(|v| v as u64),
            timeline_start_sample: row.get::<_, i64>(7)? as u64,
            sample_count: row.get::<_, i64>(8)? as u64,
            pcm_path: PathBuf::from(row.get::<_, String>(9)?),
            status,
        },
        egress_id: row.get(11)?,
    })
}
fn parse_time(value: String) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|v| v.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })
}
fn required_env(key: &str) -> anyhow::Result<String> {
    env::var(key).map_err(|_| anyhow::anyhow!("{key} must be configured"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> (Store, PathBuf) {
        let path =
            std::env::temp_dir().join(format!("voice-chat-lifecycle-{}.sqlite", Uuid::new_v4()));
        (Store::open(&path).expect("open test store"), path)
    }

    #[test]
    fn stale_connection_cannot_override_new_generation() {
        let (store, path) = test_store();
        let participant = new_participant("refresh-test", ParticipantRole::Participant);
        store.upsert_participant(&participant).unwrap();
        let old_generation = Uuid::new_v4();
        let new_generation = Uuid::new_v4();
        store
            .begin_connection(participant.id, old_generation)
            .unwrap();
        store
            .begin_connection(participant.id, new_generation)
            .unwrap();

        assert!(!store
            .mark_connection_reconnecting(
                participant.id,
                old_generation,
                Utc::now() + chrono::Duration::seconds(60),
            )
            .unwrap());
        assert_eq!(
            store
                .connection_state(participant.id, new_generation)
                .unwrap()
                .as_deref(),
            Some("connecting")
        );
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn explicit_leave_is_not_downgraded_to_reconnecting() {
        let (store, path) = test_store();
        let participant = new_participant("leave-test", ParticipantRole::Participant);
        store.upsert_participant(&participant).unwrap();
        let generation = Uuid::new_v4();
        store.begin_connection(participant.id, generation).unwrap();
        assert!(store
            .mark_connection_left(participant.id, generation)
            .unwrap());
        assert!(!store
            .mark_connection_reconnecting(
                participant.id,
                generation,
                Utc::now() + chrono::Duration::seconds(60),
            )
            .unwrap());
        assert_eq!(
            store
                .connection_state(participant.id, generation)
                .unwrap()
                .as_deref(),
            Some("left")
        );
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn room_session_releases_host_before_next_generation() {
        let (store, path) = test_store();
        let first = new_participant("host", ParticipantRole::Host);
        let second = new_participant("other", ParticipantRole::Host);
        store.upsert_participant(&first).unwrap();
        store.upsert_participant(&second).unwrap();
        let first_session = store.ensure_active_room_session().unwrap();
        assert!(store.claim_session_host(first.id).unwrap());
        assert!(!store.claim_session_host(second.id).unwrap());
        assert_eq!(store.room_host().unwrap(), Some(first.id));
        store.close_active_room_session_if_empty().unwrap();
        let second_session = store.ensure_active_room_session().unwrap();
        assert_eq!(second_session.generation, first_session.generation + 1);
        assert!(store.claim_session_host(second.id).unwrap());
        assert_eq!(store.room_host().unwrap(), Some(second.id));
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn room_session_is_not_closed_while_a_participant_is_present() {
        let (store, path) = test_store();
        let participant = new_participant("present", ParticipantRole::Participant);
        store.upsert_participant(&participant).unwrap();
        let session = store.ensure_active_room_session().unwrap();
        store
            .begin_connection(participant.id, Uuid::new_v4())
            .unwrap();

        assert!(store
            .close_active_room_session_if_empty()
            .unwrap()
            .is_none());
        assert_eq!(store.active_room_session().unwrap().unwrap().id, session.id);

        drop(store);
        let _ = fs::remove_file(path);
    }
}
fn internal(error: anyhow::Error) -> axum::response::Response {
    error!(error=%error, "request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":"internal server error"})),
    )
        .into_response()
}
fn bad_request(message: &str) -> axum::response::Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error":message}))).into_response()
}
fn forbidden() -> axum::response::Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error":"host authorization required"})),
    )
        .into_response()
}
fn not_found(message: &str) -> axum::response::Response {
    (StatusCode::NOT_FOUND, Json(json!({"error":message}))).into_response()
}
