CREATE KEYSPACE IF NOT EXISTS eventforge WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS eventforge.events (
    event_id UUID,
    event_time TIMESTAMP,
    user_id TEXT,
    event_type TEXT,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY ((event_time), event_id)
) WITH CLUSTERING ORDER BY (event_id DESC) AND default_time_to_live=86400;