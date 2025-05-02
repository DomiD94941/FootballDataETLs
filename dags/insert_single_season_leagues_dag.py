from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from airflow.sdk import Variable
from pathlib import Path
from airflow.exceptions import AirflowSkipException
from dags.get_database_conn import get_cursor, close_connection, insert_data_into_data_base
from dags.get_database_data import get_seasons_db


# its for free plan, so we can only get 3 seasons
def get_available_seasons():
    available_seasons = [season for season in get_seasons_db() if season in (2021, 2022, 2023)]
    if not available_seasons:
        raise AirflowSkipException("No available seasons to process.")
    return available_seasons

def get_next_season_from_variable(ti):
    available_seasons = get_available_seasons()
    last_processed = int(Variable.get("last_processed_season_for_season_leagues", default=2020))

    remaining_seasons = [s for s in available_seasons if s > last_processed]
    if not remaining_seasons:
        raise AirflowSkipException("All seasons already processed.")

    next_season = min(remaining_seasons)
    print(f"Next season to process: {next_season}")
    
    # Wykonujemy xcom_push tylko wtedy, gdy sezon jest prawidłowy
    season_to_push = ti.xcom_push(key='season_year_for_season_leagues', value=next_season)
    return season_to_push

def update_last_processed_season(ti):
    season_year = ti.xcom_pull(task_ids='get_next_season', key='season_year_for_season_leagues')
    Variable.set("last_processed_season_for_season_leagues", str(season_year))
    print(f"Updated last processed season to {season_year}")

def create_leagues_and_season_leagues_tables():

    cursor = get_cursor()

    league_sql = """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE LEAGUES (
                    LEAGUE_ID INTEGER PRIMARY KEY,
                    LEAGUE_NAME VARCHAR2(100),
                    LEAGUE_COUNTRY VARCHAR2(100)
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """

    season_league_sql = """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE SEASON_LEAGUES (
                    SEASON_ID INTEGER,
                    SEASON_START DATE,
                    SEASON_END DATE,
                    LEAGUE_ID INTEGER,
                    CONSTRAINT season_leagues_season_id_fk FOREIGN KEY (SEASON_ID) REFERENCES SEASONS(SEASON_ID),
                    CONSTRAINT season_leagues_league_id_fk FOREIGN KEY (LEAGUE_ID) REFERENCES LEAGUES(LEAGUE_ID),
                    PRIMARY KEY (SEASON_ID, LEAGUE_ID)
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """

    cursor.execute(league_sql)
    cursor.execute(season_league_sql)
    close_connection()

def fetch_leagues_from_api(ti):

    api_key = Variable.get('API_KEY')
    base_url = Variable.get('BASE_URL')
    
    headers = {
    'x-rapidapi-host': 'v3.football.api-sports.io',
    'x-rapidapi-key': api_key
    }

    season_year = ti.xcom_pull(task_ids='get_next_season', key='season_year_for_season_leagues')

    # Step 2: Get leagues from API
    response = requests.get(f'{base_url}/leagues?season={season_year}', headers=headers)
    if response.status_code == 200: 
        leagues = response.json()['response']
        # Save leagues to a file
        json_path = Path("/tmp") / 'single_season_leagues.json'
        with open(json_path, 'w') as file:
            json.dump(leagues, file)
            return leagues
    else:
        return f"Error: {response.status_code}, {response.text}"

def insert_single_season_leagues(ti):

    cursor = get_cursor()

    season_year = ti.xcom_pull(task_ids='get_next_season', key='season_year_for_season_leagues')

    json_path = Path("/tmp") / 'single_season_leagues.json'

    #  1. Sprawdzenie, czy plik jest pusty
    if not json_path.exists() or json_path.stat().st_size == 0:
        raise ValueError(f"JSON file is empty for season leagues in season year: {season_year}")

    with open(json_path, 'r') as file:
        leagues = json.load(file)

    #  2. Walidacja zawartości JSON-a
    if (
        not isinstance(leagues, list) or
        len(leagues) == 0 or
        'league' not in leagues[0] or
        not leagues[0]['league']
    ):
        raise ValueError(f"Invaild JSON structure for season leagues in season year: {season_year}")

    # Step 1: Get season ID from database
    cursor.execute('SELECT SEASON_ID FROM SEASONS WHERE SEASON_YEAR = :season_year', {'season_year': season_year})
    season_id_result = cursor.fetchone()
    if season_id_result is None:
        print(f"Season id for season: {season_year} is not exist.")
        return
    season_id = season_id_result[0]

    insert_leagues_query = '''
        INSERT INTO LEAGUES (LEAGUE_ID, LEAGUE_NAME, LEAGUE_COUNTRY)
        VALUES (:league_id, :league_name, :league_country)
    '''
    check_leagues_query = 'SELECT COUNT(*) FROM LEAGUES WHERE LEAGUE_ID = :league_id'

    insert_season_leagues = '''
        INSERT INTO SEASON_LEAGUES (SEASON_ID, SEASON_START, SEASON_END, LEAGUE_ID)
        VALUES (:season_id, TO_DATE(:season_start, 'YYYY-MM-DD'), TO_DATE(:season_end, 'YYYY-MM-DD'), :league_id)
    '''
    check_season_leagues = '''
        SELECT COUNT(*) FROM SEASON_LEAGUES WHERE SEASON_ID = :season_id AND LEAGUE_ID = :league_id
    '''

    for league_entry in leagues:
        league = league_entry['league']
        country = league_entry['country']
        season_info = league_entry['seasons'][0]

        league_id = league['id']
        league_name = league['name']
        league_country = country['name']

        season_start = season_info['start']
        season_end = season_info['end']

        cursor.execute(check_leagues_query, {'league_id': league_id})
        if cursor.fetchone()[0] == 0:
            insert_data_into_data_base(insert_leagues_query, [{
                'league_id': league_id,
                'league_name': league_name,
                'league_country': league_country
            }])
            print(f"Inserted league: {league_name}")
        else:
            print(f"League {league_name} already exists, skipping insertion.")

        cursor.execute(check_season_leagues, {'season_id': season_id, 'league_id': league_id})
        if cursor.fetchone()[0] == 0:
            insert_data_into_data_base(insert_season_leagues, [{
                'season_id': season_id,
                'season_start': season_start,
                'season_end': season_end,
                'league_id': league_id
            }])
            print(f"Inserted season_league: {league_name} for {season_year}")
        else:
            print(f"Season {season_year} and league {league_name} already exists, skipping.")

    close_connection()

# DAG configuration
default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='single_season_leagues_etl_dag',
    default_args=default_args,
    schedule='@yearly',
    description='Insert leagues and season_leagues for a single season',
    tags=['oracle', 'football', 'etl']
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_league_and_season_league_tables',
        python_callable=create_leagues_and_season_leagues_tables
    )

    get_next_season_task = PythonOperator(
        task_id='get_next_season',
        python_callable=get_next_season_from_variable,
    )

    fetch_leagues_task = PythonOperator(
        task_id='fetch_leagues_from_api',
        python_callable=fetch_leagues_from_api,
    )

    insert_leagues_task = PythonOperator(
        task_id='insert_leagues_for_season',
        python_callable=insert_single_season_leagues,
    )

    update_variable_task = PythonOperator(
        task_id='update_last_processed_season',
        python_callable=update_last_processed_season,
    )

    create_tables_task >> get_next_season_task >> fetch_leagues_task >> insert_leagues_task >> update_variable_task
