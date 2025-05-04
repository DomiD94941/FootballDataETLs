from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from dags.get_database_conn import get_cursor, close_connection, insert_data_into_data_base
from airflow.sdk import Variable
from pathlib import Path
from airflow.exceptions import AirflowSkipException
from dags.get_database_data import get_teams_season_league_db

def get_next_team_from_variable(ti):
    available_teams = get_teams_season_league_db()
    last_team_processed = int(Variable.get("last_processed_team_for_matches", default=0))

    remaining_teams = [t for t in available_teams if t['team_id'] > last_team_processed]
    if not remaining_teams:
        league_id = ti.xcom_pull(task_ids='get_next_team', key='league_id_for_team_matches')
        season_year = ti.xcom_pull(task_ids='get_next_team', key='season_year_for_team_matches')
        Variable.set("last_processed_league_for_team_matches", str(league_id))
        Variable.set("last_processed_season_for_team_matches", str(season_year))
        raise AirflowSkipException(f"All teams already processed, for league: {league_id} and season: {season_year}")

    next_team = min(remaining_teams, key=lambda x: (x['team_id'], x['league_id'], x['season_year']))
    print(f"Next team to process: {next_team['team_id']}, for league {next_team['league_id']} and season {next_team['season_year']}")
    season_to_push = ti.xcom_push(key='season_year_for_team_matches', value=next_team['season_year'])
    league_to_push = ti.xcom_push(key='league_id_for_team_matches', value=next_team['league_id'])
    team_to_push = ti.xcom_push(key='team_id', value=next_team['team_id'])
    return team_to_push, league_to_push, season_to_push

def update_last_processed_team(ti):
    team_id = ti.xcom_pull(task_ids='get_next_team', key='team_id')
    Variable.set("last_processed_team_for_matches", str(team_id))
    print(f"Updated last processed team to {team_id}")

def create_matches_and_results_tables():
    
    cursor = get_cursor()

    matches_sql = """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE MATCHES (
                    MATCH_ID INTEGER PRIMARY KEY,
                    SEASON_ID INTEGER,
                    LEAGUE_ID INTEGER,
                    HOME_TEAM_ID INTEGER,
                    AWAY_TEAM_ID INTEGER,
                    REFEREE VARCHAR2(255),
                    MATCH_DATE TIMESTAMP WITH TIME ZONE,
                    STADIUM_ID INTEGER
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """

    results_sql = """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE MATCH_RESULTS (
                    MATCH_ID INTEGER PRIMARY KEY,
                    HOME_TEAM_FULL_TIME_GOALS INTEGER,
                    AWAY_TEAM_FULL_TIME_GOALS INTEGER,
                    HOME_TEAM_HALF_TIME_GOALS INTEGER,
                    AWAY_TEAM_HALF_TIME_GOALS INTEGER
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """

    cursor.execute(matches_sql)
    cursor.execute(results_sql)
    close_connection()

def fetch_team_matches(ti):

    api_key = Variable.get('API_KEY')
    base_url = Variable.get('BASE_URL')
    
    headers = {
    'x-rapidapi-host': 'v3.football.api-sports.io',
    'x-rapidapi-key': api_key
    }

    team_id = ti.xcom_pull(task_ids='get_next_team', key='team_id')
    league_id = ti.xcom_pull(task_ids='get_next_team', key='league_id_for_team_matches')
    season_year = ti.xcom_pull(task_ids='get_next_team', key='season_year_for_team_matches')

    response = requests.get(
        f"{base_url}/fixtures?team={team_id}&league={league_id}&season={season_year}", headers=headers)
   
    if response.status_code == 200:
        matches = response.json()['response']

        json_path = Path("/tmp") / 'team_matches.json'
        with open(json_path, 'w') as file:
            json.dump(matches, file)
        return matches
    else:
        raise Exception(f"API Error: {response.status_code}, {response.text}")

def insert_team_matches(ti):

    cursor = get_cursor()

    json_path = Path("/tmp") / 'team_matches.json'

    
    team_id = ti.xcom_pull(task_ids='get_next_team', key='team_id')
    league_id = ti.xcom_pull(task_ids='get_next_team', key='league_id_for_team_matches')
    season_year = ti.xcom_pull(task_ids='get_next_team', key='season_year_for_team_matches')


    if not json_path.exists() or json_path.stat().st_size == 0:
        raise ValueError(f"JSON file is empty for team matches for team id: {team_id}")

    with open(json_path, 'r') as file:
        matches = json.load(file)

    if (
        not isinstance(matches, list) or
        len(matches) == 0 or
        'fixture' not in matches[0] or
        not matches[0]['fixture']
    ):
        raise ValueError(f"Invaild JSON structure for team matches for team id: {team_id}")
    
    cursor = get_cursor()
    cursor.execute('SELECT SEASON_ID FROM SEASONS WHERE SEASON_YEAR = :season_year', {'season_year': season_year})
    season_id_result = cursor.fetchone()
    if season_id_result is None:
        print(f"Season ID not found for year {season_year}")
        close_connection()
        return
    season_id = season_id_result[0]

    insert_matches = []
    insert_results = []

    check_match_query = 'SELECT COUNT(*) FROM MATCHES WHERE MATCH_ID = :match_id'
    check_result_query = 'SELECT COUNT(*) FROM MATCH_RESULTS WHERE MATCH_ID = :match_id'
    check_stadium_query = 'SELECT COUNT(*) FROM STADIUMS WHERE STADIUM_ID = :stadium_id'

    for match_entry in matches:
        match = match_entry['fixture']
        match_id = match['id']
        match_date = match['date']
        referee = match.get('referee')
        stadium_id = match['venue'].get('id')

        if referee is None:
            continue  # Skip this match if referee is missing

        if not stadium_id:
            continue
            
        cursor.execute(check_stadium_query, {'stadium_id': stadium_id})
        if cursor.fetchone()[0] == 0:
            continue

        home_team_id = match_entry['teams']['home']['id']
        away_team_id = match_entry['teams']['away']['id']
        score = match_entry['score']
        home_ft = score['fulltime'].get('home', 0)
        away_ft = score['fulltime'].get('away', 0)
        home_ht = score['halftime'].get('home', 0)
        away_ht = score['halftime'].get('away', 0)

        cursor.execute(check_match_query, {'match_id': match_id})
        if cursor.fetchone()[0] == 0:
            insert_matches.append({
                'match_id': match_id,
                'season_id': season_id,
                'league_id': league_id,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'referee': referee,
                'match_date': match_date,
                'stadium_id': stadium_id
            })

        cursor.execute(check_result_query, {'match_id': match_id})
        if cursor.fetchone()[0] == 0:
            insert_results.append({
                'match_id': match_id,
                'home_ft': home_ft,
                'away_ft': away_ft,
                'home_ht': home_ht,
                'away_ht': away_ht
            })

    match_query = """
        INSERT INTO MATCHES (MATCH_ID, SEASON_ID, LEAGUE_ID, HOME_TEAM_ID, AWAY_TEAM_ID, REFEREE, MATCH_DATE, STADIUM_ID)
        VALUES (:match_id, :season_id, :league_id, :home_team_id, :away_team_id, :referee,
        TO_TIMESTAMP_TZ(:match_date,'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM'), :stadium_id)
    """
    result_query = """
        INSERT INTO MATCH_RESULTS (MATCH_ID, HOME_TEAM_FULL_TIME_GOALS, AWAY_TEAM_FULL_TIME_GOALS, HOME_TEAM_HALF_TIME_GOALS, AWAY_TEAM_HALF_TIME_GOALS)
        VALUES (:match_id, :home_ft, :away_ft, :home_ht, :away_ht)
    """

    if insert_matches:
        insert_data_into_data_base(match_query, insert_matches)
        print(f"Inserted {len(insert_matches)} new matches.")

    if insert_results:
        insert_data_into_data_base(result_query, insert_results)
        print(f"Inserted {len(insert_results)} new results.")

    close_connection()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='team_matches_etl_dag',
    default_args=default_args,
    schedule='0 */2 * * *',
    description='Create MATCHES and MATCH_RESULTS and load data from API for a team',
    tags=['oracle', 'football', 'etl']
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_matches_and_results_tables',
        python_callable=create_matches_and_results_tables
    )

    get_next_team_task = PythonOperator(
        task_id='get_next_team',
        python_callable=get_next_team_from_variable,
    )

    fetch_matches_task = PythonOperator(
        task_id='fetch_team_matches',
        python_callable=fetch_team_matches,
    )

    insert_matches_task = PythonOperator(
        task_id='insert_team_matches_and_results',
        python_callable=insert_team_matches,
    )

    update_last_processed_match_for_team = PythonOperator(
        task_id='update_last_processed_team',
        python_callable=update_last_processed_team,
    )

    create_tables_task >>  get_next_team_task >> fetch_matches_task >> insert_matches_task >> update_last_processed_match_for_team
