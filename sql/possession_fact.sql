CREATE TABLE IF NOT EXISTS analytics.possession_fact (
    id VARCHAR(255) PRIMARY KEY,
    match_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    trackable_object BIGINT NOT NULL,
    timestamp FLOAT NULL
);