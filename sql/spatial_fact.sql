CREATE TABLE IF NOT EXISTS analytics.spatial_fact (
    id VARCHAR(255) PRIMARY KEY,
    match_id BIGINT NOT NULL,
    timestamp FLOAT NOT NULL,
    trackable_object BIGINT NOT NULL,
    period INT NOT NULL,
    x FLOAT NULL,
    y FLOAT NULL,
    z FLOAT NULL
);