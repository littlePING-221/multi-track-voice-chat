CREATE TABLE IF NOT EXISTS identity_credentials (
  participant_id TEXT PRIMARY KEY REFERENCES participants(id),
  resume_token_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  rotated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS participant_connections (
  participant_id TEXT PRIMARY KEY REFERENCES participants(id),
  generation TEXT NOT NULL,
  livekit_participant_sid TEXT,
  state TEXT NOT NULL CHECK(state IN ('connecting','online','reconnecting','offline','left')),
  reconnect_deadline TEXT,
  changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS room_hosts (
  room_name TEXT PRIMARY KEY,
  participant_id TEXT NOT NULL REFERENCES participants(id),
  created_at TEXT NOT NULL
);

INSERT OR IGNORE INTO room_hosts(room_name, participant_id, created_at)
SELECT room_name, participant_id, created_at FROM host_credentials;

CREATE TABLE IF NOT EXISTS room_runtime (
  room_name TEXT PRIMARY KEY,
  empty_since TEXT
);

INSERT OR IGNORE INTO room_runtime(room_name, empty_since) VALUES('main', NULL);
