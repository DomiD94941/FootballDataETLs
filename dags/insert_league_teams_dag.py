from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from dags.get_database_conn import get_cursor, close_connection, insert_data_into_data_base
from airflow.sdk import Variable
from pathlib import Path
from airflow.exceptions import AirflowSkipException
from dags.get_database_data import get_season_leagues_db

def get_next_league_from_variable(ti):
    available_leagues = get_season_leagues_db()
    last_league_processed = int(Variable.get("last_processed_league_for_league_teams", default=0))
    
    remaining_leagues = [league for league in available_leagues if league['league_id'] > last_league_processed]
    if not remaining_leagues:
        season_year = ti.xcom_pull(task_ids='get_next_league', key='season_year_for_leagues')
        Variable.set("last_processed_season_for_leagues_teams", str(season_year))
        raise AirflowSkipException(f"All leagues already processed, for season {season_year}.")
    
    next_league = min(remaining_leagues, key=lambda x: x['league_id'])
    season_year_to_push = ti.xcom_push(key='season_year_for_leagues', value=next_league['season_year'])
    league_to_push = ti.xcom_push(key='league_id', value=next_league['league_id'])
    print(f'Next league to process: {next_league}, for season {season_year_to_push}')
    return league_to_push, season_year_to_push

def update_last_processed_league(ti):
    league_id = ti.xcom_pull(task_ids='get_next_league', key='league_id')
    Variable.set("last_processed_league_for_league_teams", str(league_id))
    print(f"Updated last processed league to {league_id}")

def create_teams_tables():

    cursor = get_cursor()

    table_sql = """
    BEGIN
        EXECUTE IMMEDIATE '
            CREATE TABLE STADIUMS (
                STADIUM_ID INTEGER PRIMARY KEY,
                STADIUM_NAME VARCHAR2(255),
                STADIUM_ADDRESS VARCHAR2(255),
                STADIUM_CITY VARCHAR2(255),
                CAPACITY INTEGER,
                SURFACE VARCHAR2(100)
            )
        ';
    EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
    END;
    """

    cursor.execute(table_sql)

    table_sql = """
    BEGIN
        EXECUTE IMMEDIATE '
            CREATE TABLE TEAMS (
                TEAM_ID INTEGER PRIMARY KEY,
                TEAM_NAME VARCHAR2(100) NOT NULL,
                SHORT_TEAM_NAME VARCHAR2(5),
                TEAM_COUNTRY VARCHAR2(255),
                FOUNDED INTEGER,
                IS_NATIONAL CHAR(1) CHECK (IS_NATIONAL IN (''Y'', ''N'')),
                STADIUM_ID INTEGER,
                CONSTRAINT teams_stadium_id_fk FOREIGN KEY (STADIUM_ID) REFERENCES STADIUMS(STADIUM_ID)
            )
        ';
    EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
    END;
    """

    cursor.execute(table_sql)

    table_sql = """
    BEGIN
        EXECUTE IMMEDIATE '
            CREATE TABLE TEAMS_SEASON_LEAGUE (
                TEAM_ID INTEGER,
                SEASON_ID INTEGER,
                LEAGUE_ID INTEGER,
                CONSTRAINT teams_season_league_season_id_league_id_fk FOREIGN KEY (SEASON_ID, LEAGUE_ID) REFERENCES SEASON_LEAGUES(SEASON_ID, LEAGUE_ID),
                CONSTRAINT teams_season_league_team_id_fk FOREIGN KEY (TEAM_ID) REFERENCES TEAMS(TEAM_ID),
                PRIMARY KEY (TEAM_ID, LEAGUE_ID, SEASON_ID)
            )
        ';
    EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
    END;
    """

    cursor.execute(table_sql)
    close_connection()

def fetch_teams_from_api(ti):

    api_key = Variable.get('API_KEY')
    base_url = Variable.get('BASE_URL')
    
    headers = {
    'x-rapidapi-host': 'v3.football.api-sports.io',
    'x-rapidapi-key': api_key
    }

    season_year = ti.xcom_pull(task_ids='get_next_league', key='season_year_for_leagues')
    league_id = ti.xcom_pull(task_ids='get_next_league', key='league_id')

    response = requests.get(f"{base_url}/teams?league={league_id}&season={season_year}", headers=headers)
    if response.status_code == 200:
        teams = response.json()['response']

        json_path = Path("/tmp") / 'league_teams.json'
        
        with open(json_path, 'w') as file:
            json.dump(teams, file)
        return teams
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

def insert_teams(ti):

    cursor = get_cursor()

    season_year = ti.xcom_pull(task_ids='get_next_league', key='season_year_for_leagues')
    league_id = ti.xcom_pull(task_ids='get_next_league', key='league_id')

    json_path = Path("/tmp") / 'league_teams.json'

   #  1. Sprawdzenie, czy plik jest pusty
    if not json_path.exists() or json_path.stat().st_size == 0:
        raise ValueError(f"JSON file is empty for league {league_id} and season {season_year}")

    with open(json_path, 'r') as f:
        league_teams = json.load(f)

    #  2. Walidacja zawartości JSON-a
    if (
        not isinstance(league_teams, list) or
        len(league_teams) == 0 or 
        'team' not in league_teams[0] or
        not league_teams[0]['team']
    ):
        raise ValueError(f"Invaiild JSON structure for league {league_id} and season {season_year}")

    cursor.execute('SELECT SEASON_ID FROM SEASONS WHERE SEASON_YEAR = :season_year', {'season_year': season_year})
    season_id_result = cursor.fetchone()
    if season_id_result is None:
        print(f"Season {season_year} not found.")
        return
    season_id = season_id_result[0]

    insert_team_stadium_query = """
        INSERT INTO STADIUMS (STADIUM_ID, STADIUM_NAME, STADIUM_ADDRESS, STADIUM_CITY, CAPACITY, SURFACE) 
        VALUES (:stadium_id, :stadium_name, :address, :city, :capacity, :surface)
    """
    check_stadium_query = 'SELECT COUNT(*) FROM STADIUMS WHERE STADIUM_ID = :stadium_id'

    insert_teams_query = """
        INSERT INTO TEAMS (TEAM_ID, TEAM_NAME, SHORT_TEAM_NAME, TEAM_COUNTRY, FOUNDED, IS_NATIONAL, STADIUM_ID)
        VALUES (:team_id, :team_name, :short_team_name, :team_country, :founded, :is_national, :stadium_id)
    """
    check_teams_query = 'SELECT COUNT(*) FROM TEAMS WHERE TEAM_ID = :team_id'

    insert_teams_league_season_query = """
        INSERT INTO TEAMS_SEASON_LEAGUE (TEAM_ID, SEASON_ID, LEAGUE_ID)
        VALUES (:team_id, :season_id, :league_id)
    """
    check_teams_league_season_query = """
        SELECT COUNT(*) FROM TEAMS_SEASON_LEAGUE 
        WHERE TEAM_ID = :team_id AND SEASON_ID = :season_id AND LEAGUE_ID = :league_id
    """

    for entry in league_teams:
        team = entry['team']
        venue = entry['venue']

        team_id = team['id']
        team_name = team['name']
        short_name = team['code']
        country = team['country']
        founded = team['founded']
        is_national = 'Y' if str(team['national']).lower() == 'true' else 'N'
        stadium_id = venue['id']

        stadium_data = {
            'stadium_id': stadium_id,
            'stadium_name': venue['name'],
            'address': venue['address'],
            'city': venue['city'],
            'capacity': venue['capacity'],
            'surface': venue['surface']
        }

        cursor.execute(check_stadium_query, {'stadium_id': stadium_id})
        if cursor.fetchone()[0] == 0:
            insert_data_into_data_base(insert_team_stadium_query, [stadium_data])

        cursor.execute(check_teams_query, {'team_id': team_id})
        if cursor.fetchone()[0] == 0:
            team_data = {
                'team_id': team_id,
                'team_name': team_name,
                'short_team_name': short_name,
                'team_country': country,
                'founded': founded,
                'is_national': is_national,
                'stadium_id': stadium_id
            }
            insert_data_into_data_base(insert_teams_query, [team_data])

        cursor.execute(check_teams_league_season_query, {
            'team_id': team_id, 'season_id': season_id, 'league_id': league_id
        })
        if cursor.fetchone()[0] == 0:
            insert_data_into_data_base(insert_teams_league_season_query, [{
                'team_id': team_id,
                'season_id': season_id,
                'league_id': league_id
            }])
        
    close_connection()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='league_teams_etl_dag',
    default_args=default_args,
    schedule='0 */3 * * *',
    description='Create teams tables and insert data from API',
    tags=['oracle', 'football', 'etl']
) as dag:

    create_teams_tables_task = PythonOperator(
        task_id='create_teams_tables',
        python_callable=create_teams_tables
    )

    get_next_league_task = PythonOperator(
        task_id='get_next_league',
        python_callable=get_next_league_from_variable,
    )

    fetch_teams_task = PythonOperator(
        task_id='fetch_teams_from_api',
        python_callable=fetch_teams_from_api,
    )

    insert_teams_task = PythonOperator(
        task_id='insert_teams',
        python_callable=insert_teams,
    )

    update_last_processed_league_teams_for_season_task = PythonOperator(
        task_id='update_last_processed_league',
        python_callable=update_last_processed_league,
    )

    create_teams_tables_task  >> get_next_league_task >> fetch_teams_task >> insert_teams_task >> update_last_processed_league_teams_for_season_task
