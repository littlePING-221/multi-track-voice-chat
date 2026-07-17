use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Participant {
    pub id: Uuid,
    pub livekit_identity: String,
    pub nickname: String,
    pub role: ParticipantRole,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ParticipantRole {
    Host,
    Participant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecordingStatus {
    Starting,
    Recording,
    Stopping,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordingSession {
    pub id: Uuid,
    pub status: RecordingStatus,
    pub started_at_utc: DateTime<Utc>,
    pub stopped_at_utc: Option<DateTime<Utc>>,
    pub target_sample_rate: u32,
    pub target_channels: u16,
    pub target_sample_format: String,
    pub output_dir: PathBuf,
    pub version: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrackSegmentStatus {
    Opening,
    Writing,
    Closed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackSegment {
    pub id: Uuid,
    pub recording_id: Uuid,
    pub participant_id: Uuid,
    pub livekit_track_sid: String,
    pub segment_index: u32,
    pub first_frame_at_ns: u64,
    pub last_frame_at_ns: Option<u64>,
    pub timeline_start_sample: u64,
    pub sample_count: u64,
    pub pcm_path: PathBuf,
    pub status: TrackSegmentStatus,
}
