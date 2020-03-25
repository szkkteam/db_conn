from pypika import Query, Table, Field, enums

from db_conn.query.sc_soccer import tables


def get_matches_with_teams_odds():
    """
    SELECT
      m.match_date as 'Date'
    , home.team_name AS 'home_team'
    , home.team_short_name as 'home_short'
    , away.team_name AS 'away_team'
    , away.team_short_name as 'away_short'
    , odds.*
    FROM Matches m
    INNER JOIN Teams_ref home ON m.home_team_id = home.team_id
    INNER JOIN Teams_ref away ON m.away_team_id = away.team_id
    INNER JOIN Match_Odds odds ON odds.match_id = m.match_id
    """

    home = tables.teams.as_('home')
    away = tables.teams.as_('away')
    m = tables.matches.as_('m')
    odds = tables.odds.as_('odds')

    q = Query.from_(m).join(home, enums.JoinType.inner).on(m.home_team_id == home.team_id
    ).join(away, enums.JoinType.inner).on(m.away_team_id == away.team_id
    ).join(odds, enums.JoinType.inner).on(odds.match_id == m.match_id
    ).select(
        m.match_date.as_('date'),
        home.team_name.as_('home_team'),
        away.team_name.as_('away_team'),
        home.team_short_name.as_('home_team_short'),
        away.team_short_name.as_('away_Team_short'),
        odds.sc_odds.as_('sofa_odds'),
        odds.fd_odds.as_('fd_odds')
    )
    return q

def get_matches_with_teams_scores(season_filter):
    """
    SELECT
      m.match_id as "Id"
      , m.match_date AS "Date"
      , home.team_short_name as "HomeTeam"
      , away.team_short_name as "AwayTeam"
      , stat.home_score as "FTHG"
      , stat.away_score as "FTAG"
      , odds.*
    FROM
     Matches m
     LEFT JOIN Teams_ref as home ON home.team_id = m.home_team_id
     LEFT JOIN Teams_ref as away ON away.team_id = m.away_team_id
     LEFT JOIN Match_Odds as odds ON odds.match_id = m.match_id
     LEFT JOIN Match_Statistics stat on stat.match_id = m.match_id
     LEFT JOIN Seasons_ref season on season.season_id = m.season_id
    WHERE season.season_year = "18/19";
    """
    home = tables.teams.as_('home')
    away = tables.teams.as_('away')
    m = tables.matches.as_('m')
    odds = tables.odds.as_('odds')
    stat = tables.statistics.as_('stat')
    season = tables.seasons.as_('season')

    q = Query.from_(m
    ).join(home, enums.JoinType.left).on(m.home_team_id == home.team_id
    ).join(away, enums.JoinType.left).on(m.away_team_id == away.team_id
    ).join(odds, enums.JoinType.left).on(odds.match_id == m.match_id
    ).join(stat, enums.JoinType.left).on(stat.match_id == m.match_id
    ).join(season, enums.JoinType.left).on(season.season_id == m.season_id
    ).select(
        m.match_id.as_('id'),
        m.match_date.as_('date'),
        home.team_name.as_('home_team'),
        away.team_name.as_('away_team'),
        stat.home_score.as_('home_goal'),
        stat.away_score.as_('away_goal'),
        odds.sc_odds.as_('sofa_odds'),
        odds.fd_odds.as_('fd_odds')
    ).where(season.season_year == season_filter)
    return q

def get_matches_where_odds_are_null(start_date, end_date):
    """
    SELECT
      m.match_id as "Id"
      , season.season_year AS "Season"
      , m.match_date AS "Date"
      , home.team_short_name as "HomeTeam"
      , away.team_short_name as "AwayTeam"
      , stat.home_score as "FTHG"
      , stat.away_score as "FTAG"
      , odds.*
    FROM
     matches m
     LEFT JOIN teams_ref as home ON home.team_id = m.home_team_id
     LEFT JOIN teams_ref as away ON away.team_id = m.away_team_id
     LEFT JOIN match_Odds as odds ON odds.match_id = m.match_id
     LEFT JOIN match_Statistics stat on stat.match_id = m.match_id
     LEFT JOIN seasons_ref season on season.season_id = m.season_id
    WHERE m.match_date BETWEEN "2019-02-20" and "2019-02-23" and (
      odds.pi_away_odds is null or odds.pi_home_odds is null
      or odds.pi_draw_odds is null or odds.bet365_home_odds is null
      or odds.bet365_away_odds is null or odds.bet365_draw_odds is null
      or odds.bb_over_2_5 is null or odds.bb_under_2_5 is null) ORDER BY m.match_date DESC;
    """
    home = tables.teams.as_('home')
    away = tables.teams.as_('away')
    m = tables.matches.as_('m')
    odds = tables.odds.as_('odds')
    stat = tables.statistics.as_('stat')
    season = tables.seasons.as_('season')
    tr = tables.tournaments.as_('tr')

    q = Query.from_(m
    ).join(home, enums.JoinType.left).on(m.home_team_id == home.team_id
    ).join(away, enums.JoinType.left).on(m.away_team_id == away.team_id
    ).join(odds, enums.JoinType.left).on(odds.match_id == m.match_id
    ).join(stat, enums.JoinType.left).on(stat.match_id == m.match_id
    ).join(season, enums.JoinType.left).on(season.season_id == m.season_id
    ).join(tr, enums.JoinType.left).on(tr.tournament_id == m.tournament_id
    ).select(
        tr.tournament_slug.as_('tournament'),
        season.season_year.as_('season'),
        m.match_id.as_('id'),
        m.match_date.as_('date'),
        home.team_name.as_('home_team'),
        away.team_name.as_('away_team'),
        home.team_short.as_('home_team_short'),
        away.team_short.as_('away_team_short'),
        stat.home_score.as_('home_goal'),
        stat.away_score.as_('away_goal')
    ).where(
        (m.match_date[start_date:end_date]) &
        (odds.fd_odds.isnull())
    )
    return q

def get_null_fifa_stats(limit=None):
    """
    SELECT
      pla.sc_player_id AS "player_id"
    , p.full_name AS "name"
    , p.fifa_player_id AS "fifa_id"
    , p.birth_date AS "birth"
    , p.short_name AS "short"
    , p.slug AS "slug"
    FROM (
        SELECT
          ps.sc_player_id
        FROM player_stats ps
        WHERE ps.fifa_stat IS NULL and ps.has_fifa_stat is true
        GROUP BY ps.sc_player_id) as pla
    INNER JOIN players p ON p.sc_player_id = pla.sc_player_id;
    """
    p = tables.players.as_('p')
    ps = tables.players_stats.as_('ps')

    pla = Query.from_(ps
                       ).select(
                            ps.sc_player_id
                       ).as_('pla'
                       ).where((ps.fifa_stat.isnull()) & (ps.has_fifa_stat == True))

    if limit is not None:
        pla = pla.limit(limit)

    q = Query.from_(pla
                    ).join(p, enums.JoinType.inner).on(p.sc_player_id == pla.sc_player_id
                    ).select(
                        p.full_name.as_('name'),
                        p.short_name.as_('short'),
                        p.slug.as_('slug'),
                        p.fifa_player_id.as_('fifa_id'),
                        p.sc_player_id.as_('player_id'),
                        p.birth_date.as_('birth')
                    )

    return q

def get_all_match_for_player_id(player_id):
    """
    SELECT
    ps.sc_player_id as "player_id"
    , m.match_date as "date"
    FROM player_stats ps
    INNER JOIN Matches m on m.match_id = ps.match_id
    WHERE ps.fifa_stat IS NULL and ps.sc_player_id = 142037 ORDER BY m.match_date DESC;
    """
    ps = tables.players_stats.as_('ps')
    m = tables.matches.as_('m')

    q = Query.from_(ps
        ).join(m, enums.JoinType.inner).on(m.match_id == ps.match_id
        ).select(
            m.match_id.as_('match_id'),
            ps.sc_player_id.as_('player_id'),
            m.match_date.as_('date')
        ).where((ps.sc_player_id == player_id) & (ps.fifa_stat.isnull())
        ).orderby(m.match_date, order=enums.Order.desc)

    return q


def get_last_match_date():
    """
    SELECT
    m.match_date as "date"
    FROM Matches m
    ORDER BY DESC LIMIT 1
    """
    m = tables.matches.as_('m')

    q = Query.from_(m
        ).select(
            m.match_date.as_('date')
        ).orderby(m.match_date, order=enums.Order.desc
        ).limit(1)
    return q