CREATE TABLE IF NOT EXISTS room_sessions (
  id TEXT PRIMARY KEY,
  room_name TEXT NOT NULL,
  generation INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('active','closed')),
  host_participant_id TEXT REFERENCES participants(id),
  opened_at TEXT NOT NULL,
  closed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_room_session
ON room_sessions(room_name) WHERE state='active';

INSERT OR IGNORE INTO room_sessions(id,room_name,generation,state,host_participant_id,opened_at,closed_at)
SELECT lower(hex(randomblob(16))), h.room_name, 1, 'active', h.participant_id, h.created_at, NULL
FROM room_hosts h
WHERE h.room_name='main'
  AND NOT EXISTS (SELECT 1 FROM room_sessions s WHERE s.room_name=h.room_name);

CREATE TABLE IF NOT EXISTS recording_room_sessions (
  recording_id TEXT PRIMARY KEY REFERENCES recording_sessions(id),
  room_session_id TEXT NOT NULL REFERENCES room_sessions(id),
  room_generation INTEGER NOT NULL
);

INSERT OR IGNORE INTO recording_room_sessions(recording_id,room_session_id,room_generation)
SELECT r.id,s.id,s.generation
FROM recording_sessions r
JOIN room_sessions s ON s.room_name='main' AND s.generation=1;
