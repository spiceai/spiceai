CREATE TABLE append_events (
    id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    message TEXT NOT NULL,
    score REAL NOT NULL
);

INSERT INTO append_events (event_timestamp, message, score) VALUES
    ('2026-01-01 00:00:00+00', 'initial UTC event', 1.5),
    ('2026-01-01 00:00:01+00', 'second UTC event', 2.5);
