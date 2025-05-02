from dags.get_database_conn import get_cursor, close_connection

def get_seasons_db():
    
    cursor = get_cursor()
    cursor.execute('SELECT SEASON_YEAR FROM SEASONS')
    seasons = cursor.fetchall()
    result = [season[0] for season in seasons]
    close_connection()
    return result


def get_teams_season_league_db():

    cursor = get_cursor()

    season_league_teams_query = """
        SELECT TSL.TEAM_ID, TSL.LEAGUE_ID, S.SEASON_YEAR FROM TEAMS_SEASON_LEAGUE TSL
            INNER JOIN SEASONS S ON TSL.SEASON_ID = S.SEASON_ID
        """
    cursor.execute(season_league_teams_query)
    data = cursor.fetchall()
    result = [{'team_id': row[0], 'league_id': row[1], 'season_year': row[2]} for row in data]
    close_connection()
    return result

def get_season_leagues_db():
    
    cursor = get_cursor()

    season_leagues_query = """
    SELECT L.LEAGUE_ID, S.SEASON_YEAR FROM SEASON_LEAGUES
        INNER JOIN LEAGUES L on SEASON_LEAGUES.LEAGUE_ID = L.LEAGUE_ID
        INNER JOIN SEASONS S on SEASON_LEAGUES.SEASON_ID = S.SEASON_ID
    """
    cursor.execute(season_leagues_query)
    leagues = cursor.fetchall()
    result = [{'league_id': league[0], 'season_year': league[1]} for league in leagues]

    close_connection()
    return result 

def get_match_and_team_ids_db():
    cursor = get_cursor()
    cursor.execute('''
        SELECT M.MATCH_ID, M.HOME_TEAM_ID
        FROM MATCHES M
    ''')
    data = cursor.fetchall()
    result = [{'match_id': row[0], 'team_id': row[1]} for row in data]
    close_connection()
    return result
