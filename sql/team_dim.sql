CREATE TABLE IF NOT EXISTS analytics.team_dim (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    short_name VARCHAR(255) NOT NULL,
    acronym VARCHAR(10)
);