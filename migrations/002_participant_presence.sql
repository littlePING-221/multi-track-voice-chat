CREATE TABLE IF NOT EXISTS participant_presence (
  participant_id TEXT PRIMARY KEY REFERENCES participants(id),
  state TEXT NOT NULL CHECK(state IN ('online','left','disconnected')),
  leave_reason TEXT,
  changed_at TEXT NOT NULL
);
