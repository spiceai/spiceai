CREATE TABLE append_events (
    id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    message TEXT NOT NULL,
    score REAL NOT NULL
);

INSERT INTO append_events (id, event_timestamp, message, score) VALUES
    (1, '2026-01-01 00:00:00.123456+00', 'first UTC event', 1.5),
    (2, '2026-01-01 00:00:01.234567+00', 'second UTC event', 2.5),
    (3, '2026-01-01 00:00:02.345678+00', 'third UTC event', 3.5);

SELECT setval('append_events_id_seq', 3, true);
