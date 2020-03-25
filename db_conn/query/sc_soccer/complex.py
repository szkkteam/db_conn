from pypika import Query, Table, Field, enums, JoinType

from db_conn.query.sc_soccer import tables
from db_conn.utils import listify


def get_player_lineups(matches=None, **kwargs):
    substitute = kwargs.get('substitute', None)
    pl = tables.player_lineups.as_('pl')
    m = tables.matches.as_('m')

    q = Query.from_(pl
        ).join(m).on(m.match_id == pl.match_id
        ).select(pl.star
        ).orderby(m.full_date, order=enums.Order.desc)
    if matches is not None:
        q = q.where(pl.match_id.isin(listify(matches)))
    if substitute is not None:
        q = q.where(pl.substitute == substitute)

    return q


def get_all_match_data(**kwargs):
    season_filter = kwargs.get('season', None)
    tournament_filter = kwargs.get('tournament', None)

    match_filter = kwargs.get('match_ids', None)

    date_filter = kwargs.get('date', None)
    home_team_filter = kwargs.get('home_team', None)
    away_team_filter = kwargs.get('away_team', None)

    include_odds = kwargs.get('odds', False)
    include_match_stat = kwargs.get('match_stat', False)
    include_lineups = kwargs.get('lineups', False)

    s = tables.seasons.as_('s')
    m = tables.matches.as_('m')
    t = tables.tournaments.as_('t')
    r = tables.referees.as_('r')
    st = tables.stadiums.as_('st')

    home = tables.teams.as_('home')
    away = tables.teams.as_('away')

    odds = tables.odds.as_('odds')
    stat = tables.statistics.as_('stat')
    home_l = tables.lineups.as_('home_l')
    away_l = tables.lineups.as_('away_l')
    home_m = tables.managers.as_('home_m')
    away_m = tables.managers.as_('away_m')

    q = Query.from_(m
            ).join(home).on(m.home_team_id == home.team_id
            ).join(away).on(m.home_team_id == away.team_id
            ).join(s).on(m.season_id == s.season_id
            ).join(t).on(m.tournament_id == t.tournament_id
            ).join(r, how=JoinType.left).on(m.referee_id == r.referee_id
            ).join(st, how=JoinType.left).on(m.stadium_id == st.stadium_id
            ).select(
                m.star,
                s.star,
                t.star,
                r.star,
                st.star,
                home.team_name.as_('home_team_name'),
                home.team_slug.as_('home_team_slug'),
                home.team_short.as_('home_team_short'),
                away.team_name.as_('away_team_name'),
                away.team_slug.as_('away_team_slug'),
                away.team_short.as_('away_team_short')
            ).orderby(m.full_date, order=enums.Order.desc)

    if season_filter is not None:
        q = q.where(s.season_year == season_filter)
    if tournament_filter is not None:
        q = q.where(t.tournament_slug == tournament_filter.replace(' ', '-').lower())
    if date_filter is not None:
        from datetime import datetime, date
        if isinstance(date_filter, datetime):
            q = q.where(m.full_date == date_filter)
        if isinstance(date_filter, date):
            q = q.where(m.match_date == date_filter)
    if home_team_filter is not None:
        q = q.where(home.team_slug == home_team_filter.replace(' ', '-').lower())
    if away_team_filter is not None:
        q = q.where(home.team_slug == away_team_filter.replace(' ', '-').lower())

    if match_filter is not None:
        q = q.where(m.match_id.isin(listify(match_filter)))

    if include_odds:
        q = q.join(odds).on(m.match_id == odds.match_id
            ).select(
                odds.star
            )
    if include_match_stat:
        q = q.join(stat, how=JoinType.left).on(m.match_id == stat.match_id
            ).select(
                stat.star
            )
    else:
        q = q.join(stat, how=JoinType.left).on(m.match_id == stat.match_id
            ).select(
                stat.home_score,
                stat.away_score,
            )
    if include_lineups:
        q = q.join(home_l, how=JoinType.left).on((m.match_id == home_l.match_id) & (home.team_id == home_l.team_id)
            ).join(away_l, how=JoinType.left).on((m.match_id == away_l.match_id) & (away.team_id == away_l.team_id)
            ).join(home_m, how=JoinType.left).on(home_l.manager_id == home_m.manager_id
            ).join(away_m, how=JoinType.left).on(away_l.manager_id == away_m.manager_id
            ).select(
                home_l.formation.as_('home_formation'),
                home_m.manager_id.as_('home_manager_id'),
                home_m.manager_name.as_('home_manager_name'),
                away_l.formation.as_('away_formation'),
                away_m.manager_id.as_('away_manager_id'),
                away_m.manager_name.as_('away_manager_name')
            )

    return q

def get_combined_data(conn, **kwargs):
    import pandas as pd
    from db_conn.connection.postgresql import Connection
    assert isinstance(conn, Connection), "Connection input parameters must be a type of Connection"

    result_df = conn.sql_query(str(get_all_match_data(**kwargs)))
    match_ids = result_df['match_id'].to_list()
    lineups_df = conn.sql_query(str(get_player_lineups(match_ids)))

    def join_player_lineup(lineups, matches):
        df_list = []

        match_grp = lineups.groupby('match_id')
        for match_id, e_match_grp in match_grp:
            # TODO: Remove the grouped column
            fixed_e_match_grp = e_match_grp.drop('match_id', axis=1)
            new_cols = []
            # Group by teams also
            team_grp = fixed_e_match_grp.groupby('team_id')
            for team_id, e_team_grp in team_grp:
                # TODO: Remove the grouped column
                fixed_e_team_grp = e_team_grp.drop('team_id', axis=1)
                matches_filtered = matches[matches['match_id'] == match_id]
                if matches_filtered['home_team_id'].iloc[0] == team_id:
                    prefix = 'home'
                else:
                    prefix = 'away'
                rows = len(fixed_e_team_grp.index)
                cols = fixed_e_team_grp.keys()
                for i in range(0, rows):
                    for col in cols:
                        new_cols.append(prefix + '_' + col + '_' + str(i))

            values_df = fixed_e_match_grp.drop('team_id', axis=1)
            values_list = values_df.values.flatten()
            new_df = pd.DataFrame([values_list], columns=new_cols)
            new_df['match_id'] = match_id
            df_list.append(new_df)

        return pd.concat(df_list)

    flattened_lineups = join_player_lineup(lineups_df, result_df)

    result_df = pd.merge(result_df, flattened_lineups, how='left', left_on='match_id',
                         right_on='match_id', copy=False)

    return result_df