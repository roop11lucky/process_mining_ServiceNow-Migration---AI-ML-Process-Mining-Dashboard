CREATE TABLE event_log (
    ticket_id TEXT,
    queue_name TEXT,
    activity TEXT,
    timestamp TIMESTAMP,
    user_name TEXT,
    team TEXT,
    priority TEXT,
    category TEXT,
    status TEXT,
    description TEXT,
    event_order INTEGER,
    duration_sec INTEGER
);
