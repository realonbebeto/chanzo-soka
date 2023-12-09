CREATE TABLE IF NOT EXISTS analytics.match_dim (
    id BIGINT NOT NULL PRIMARY KEY,
    stadium VARCHAR(255) NOT NULL,
    competition VARCHAR(255) NOT NULL,
    round VARCHAR(255) NOT NULL,
    date_played TIMESTAMP NOT NULL,
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,
    home_team_coach VARCHAR(255) NOT NULL,
    away_team_coach VARCHAR(255) NOT NULL,
    home_team_score INT NOT NULL,
    away_team_score INT NOT NULL
);