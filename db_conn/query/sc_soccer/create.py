

create_tables = (
    """
    CREATE TABLE tournaments (
        tournament_id INTEGER PRIMARY KEY,
        tournament_name VARCHAR(100),
        tournament_slug VARCHAR(50)
    );
    """,
    """
    CREATE TABLE seasons(
        season_id INTEGER PRIMARY KEY,
        season_year VARCHAR(20),
        season_name VARCHAR(50),
        season_slug VARCHAR(50)
    );
    """,
    """
    CREATE TABLE teams (
        team_id INTEGER PRIMARY KEY,
        team_name VARCHAR(100),
        team_slug VARCHAR(50),
        team_short VARCHAR(50)
    );
    """,
    """
    CREATE TABLE players (
        sc_player_id INTEGER PRIMARY KEY,
        fifa_player_id INTEGER,
        full_name VARCHAR(100),
        slug VARCHAR(50),
        short_name VARCHAR(50),
        birth_date DATE,
        height FLOAT
    );
    """,
    """
    CREATE TABLE referees (
        referee_id INTEGER PRIMARY KEY,
        referee_name VARCHAR(50),
        yellow_card_per_game FLOAT,
        red_card_per_game FLOAT
    );
    """,
    """
    CREATE TABLE managers (
        manager_id INTEGER PRIMARY KEY,
        manager_name VARCHAR(50)
    );
    """,
    """
    CREATE TABLE stadiums (
        stadium_id INTEGER PRIMARY KEY,
        country VARCHAR(50),
        city VARCHAR(50),
        name VARCHAR(50),
        capacity INTEGER
    );
    """,
    """
    CREATE TABLE matches (
        match_id INTEGER PRIMARY KEY,
        tournament_id INTEGER,
        season_id INTEGER,
        match_date DATE,
        full_date TIMESTAMP,
        match_status VARCHAR(50),
        home_team_id INTEGER,
        away_team_id INTEGER,
        referee_id INTEGER,
        stadium_id INTEGER,
        FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id),
        FOREIGN KEY (season_id) REFERENCES seasons (season_id),
        FOREIGN KEY (home_team_id) REFERENCES teams (team_id),
        FOREIGN KEY (away_team_id) REFERENCES teams (team_id),
        FOREIGN KEY (referee_id) REFERENCES referees (referee_id),
        FOREIGN KEY (stadium_id) REFERENCES stadiums (stadium_id)
    );
    """,
    """
    CREATE TABLE match_statistics (
        match_id INTEGER,
        sc_statistics JSON,
        fd_statistics JSON,
        sc_forms JSON,
        sc_votes JSON,
        sc_manager_duels JSON,
        sc_h2h JSON, 
        home_score FLOAT,
        away_score FLOAT,
        FOREIGN KEY (match_id) REFERENCES matches (match_id)
    );
    """,
    """
    CREATE TABLE odds (
        match_id INTEGER,
        sc_odds JSON,
        fd_odds JSON,
        FOREIGN KEY (match_id) REFERENCES matches (match_id)
    );
    """,
    """
    CREATE TABLE lineups (
        match_id INTEGER,
        team_id INTEGER,
        formation TEXT [],
        manager_id INTEGER,
        PRIMARY KEY (match_id, team_id),
        FOREIGN KEY (match_id) REFERENCES matches (match_id),
        FOREIGN KEY (team_id) REFERENCES teams (team_id),
        FOREIGN KEY (manager_id) REFERENCES managers (manager_id)
    );
    """,
    """
    CREATE TABLE player_lineups (
        match_id INTEGER,
        team_id INTEGER,
        sc_player_id INTEGER,
        player_position_long VARCHAR(50),
        player_position_short VARCHAR(20),
        sc_rating FLOAT,
        substitute BOOLEAN,
        PRIMARY KEY (match_id, team_id, sc_player_id),
        FOREIGN KEY (match_id) REFERENCES matches (match_id),
        FOREIGN KEY (team_id) REFERENCES teams (team_id),
        FOREIGN KEY (sc_player_id) REFERENCES players (sc_player_id)
    );
    """,
    """
    CREATE TABLE player_stats (
        sc_player_id INTEGER,
        match_id INTEGER,
        sc_stat JSON,
        fifa_stat JSON,
        has_sc_stat BOOLEAN,
        has_fifa_stat BOOLEAN,
        PRIMARY KEY (sc_player_id, match_id),
        FOREIGN KEY (match_id) REFERENCES matches (match_id),
        FOREIGN KEY (sc_player_id) REFERENCES players (sc_player_id)
    );
    """)