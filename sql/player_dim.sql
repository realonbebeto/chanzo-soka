CREATE TABLE IF NOT EXISTS analytics.player_dim (
    id BIGINT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    short_name VARCHAR(255),
    birthday DATE,
    gender VARCHAR(255) NOT NULL,
    trackable_object BIGINT NOT NULL
);