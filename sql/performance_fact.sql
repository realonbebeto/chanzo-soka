CREATE TABLE IF NOT EXISTS analytics.player_performance_fact (
    id VARCHAR(255) PRIMARY KEY,
    match_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    player_role VARCHAR(255) NOT NULL,
    seconds_played BIGINT NOT NULL,
    goal INT NOT NULL,
    yellow_card INT NOT NULL,
    red_card INT NOT NULL,
    injured BOOL NOT NULL,
    own_goal INT NOT NULL
);