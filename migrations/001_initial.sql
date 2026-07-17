-- SQLite schema for the durable production implementation.
CREATE TABLE IF NOT EXISTS participants (
  id TEXT PRIMARY KEY, livekit_identity TEXT NOT NULL UNIQUE, nickname TEXT NOT NULL,
  role TEXT NOT NULL CHECK(role IN ('host','participant')), created_at TEXT NOT NULL, last_seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recording_sessions (
  id TEXT PRIMARY KEY, status TEXT NOT NULL, started_at_utc TEXT NOT NULL, stopped_at_utc TEXT,
  sample_rate INTEGER NOT NULL, channels INTEGER NOT NULL, sample_format TEXT NOT NULL,
  output_dir TEXT NOT NULL, version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS track_segments (
  id TEXT PRIMARY KEY, recording_id TEXT NOT NULL REFERENCES recording_sessions(id), participant_id TEXT NOT NULL REFERENCES participants(id),
  track_sid TEXT NOT NULL, segment_index INTEGER NOT NULL, first_frame_at_ns INTEGER NOT NULL, last_frame_at_ns INTEGER,
  timeline_start_sample INTEGER NOT NULL, sample_count INTEGER NOT NULL, pcm_path TEXT NOT NULL, status TEXT NOT NULL, egress_id TEXT,
  UNIQUE(recording_id, track_sid)
);
CREATE TABLE IF NOT EXISTS webhook_events (id TEXT PRIMARY KEY, event TEXT NOT NULL, received_at TEXT NOT NULL, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS host_credentials (room_name TEXT PRIMARY KEY, participant_id TEXT NOT NULL, token_hash TEXT NOT NULL, created_at TEXT NOT NULL);
