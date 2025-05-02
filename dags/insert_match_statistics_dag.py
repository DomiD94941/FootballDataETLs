from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from dags.get_database_conn import get_cursor, close_connection, insert_data_into_data_base
from airflow.sdk import Variable
from pathlib import Path
from airflow.exceptions import AirflowSkipException
from dags.get_database_data import get_match_and_team_ids_db


def get_next_match_id(ti):
    match_data = get_match_and_team_ids_db()
    last_processed_match = int(Variable.get("last_processed_match_for_match_stats", default=0))
    remaining_matches = [m for m in match_data if m['match_id'] > last_processed_match]
    if not remaining_matches:
        raise AirflowSkipException("All matches already processed.")
    
    next_match = min(remaining_matches, key=lambda x: (x['match_id'], x['team_id']))
    print(f"Next match to process: {next_match['match_id']}")

    Variable.set("last_processed_team_for_match_stats", str(next_match['team_id']))
    match_id_to_push = ti.xcom_push(key='match_for_match_stats', value=next_match['match_id'])
    team_id_to_push = ti.xcom_push(key='team_for_match_stats', value=next_match['team_id'])
    return match_id_to_push, team_id_to_push

def update_last_processed_match(ti):
    match_id = ti.xcom_pull(task_ids='get_next_match', key='match_for_match_stats')
    Variable.set("last_processed_match_for_match_stats", str(match_id))
    team_id = ti.xcom_pull(task_ids='get_next_match', key='team_for_match_stats')
    Variable.set("last_processed_team_for_match_stats", str(team_id))
    print(f"Updated last processed match to {match_id}")

def create_statistics_tables():
    
    cursor = get_cursor()

    team_match_stats_sql = """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE TEAM_MATCH_STATISTICS (
                    MATCH_ID INTEGER,
                    TEAM_ID INTEGER,
                    BALL_POSSESSION VARCHAR2(20),
                    TOTAL_PASSES INTEGER,
                    PASSES_ACCURATE INTEGER,
                    PASSES_PERCENTAGE VARCHAR2(10),
                    FOULS INTEGER,
                    CORNER_KICKS INTEGER,
                    OFFSIDES INTEGER,
                    YELLOW_CARDS INTEGER,
                    RED_CARDS INTEGER,
                    GOALKEEPER_SAVES INTEGER,
                    PRIMARY KEY (MATCH_ID, TEAM_ID)
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """

    team_shots_stats_sql = """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE TEAM_SHOTS_STATISTICS (
                    MATCH_ID INTEGER,
                    TEAM_ID INTEGER,
                    SHOTS_ON_GOAL INTEGER,
                    SHOTS_OFF_GOAL INTEGER,
                    TOTAL_SHOTS INTEGER,
                    BLOCKED_SHOTS INTEGER,
                    SHOTS_INSIDEBOX INTEGER,
                    SHOTS_OUTSIDEBOX INTEGER,
                    PRIMARY KEY (MATCH_ID, TEAM_ID)
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """

    cursor.execute(team_match_stats_sql)
    cursor.execute(team_shots_stats_sql)
    close_connection()

def fetch_match_statistics(ti):
    
    api_key = Variable.get('API_KEY')   
    base_url = Variable.get('BASE_URL')
    
    headers = {
    'x-rapidapi-host': 'v3.football.api-sports.io',
    'x-rapidapi-key': api_key
    }

    team_id = ti.xcom_pull(task_ids='get_next_match', key='team_for_match_stats')
    match_id = ti.xcom_pull(task_ids='get_next_match', key='match_for_match_stats')

    response = requests.get(f'{base_url}/fixtures/statistics?fixture={match_id}&team={team_id}', headers=headers)
    if response.status_code == 200:
        stats = response.json()['response']
        
        # Use /tmp or include/ with dynamic filename
        json_path = Path("/tmp") / 'match_stats.json'
        
        with open(json_path, 'w') as f:
            json.dump(stats, f)
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

def insert_match_statistics(ti):

    cursor = get_cursor()

    json_path = Path("/tmp") / 'match_stats.json'

    team_id = ti.xcom_pull(task_ids='get_next_match', key='team_for_match_stats')
    match_id = ti.xcom_pull(task_ids='get_next_match', key='match_for_match_stats')

    #  1. Sprawdzenie, czy plik jest pusty
    if not json_path.exists() or json_path.stat().st_size == 0:
        raise ValueError(f"JSON file is empty for match {match_id} and team {team_id}")

    with open(json_path, 'r') as f:
        match_stats = json.load(f)

    #  2. Walidacja zawartości JSON-a
    if (
        not isinstance(match_stats, list) or
        len(match_stats) == 0 or
        'statistics' not in match_stats[0] or
        not match_stats[0]['statistics']
    ):
        raise ValueError(f"Invaild JSON structure for match {match_id} and team {team_id}")


    insert_match_stats_query = """INSERT INTO TEAM_MATCH_STATISTICS (MATCH_ID, TEAM_ID, BALL_POSSESSION, TOTAL_PASSES, PASSES_ACCURATE, 
        PASSES_PERCENTAGE, FOULS, CORNER_KICKS, OFFSIDES, YELLOW_CARDS, RED_CARDS, GOALKEEPER_SAVES)
            VALUES (:match_id, :team_id, :ball_possession, :total_passes, :passes_accurate, :passes_percentage, :fouls, :corner_kicks, :offsides,
        :yellow_cards, :red_cards, :goalkeeper_saves)"""
    check_match_stats_query = 'SELECT COUNT(*) FROM TEAM_MATCH_STATISTICS WHERE MATCH_ID = :match_id AND TEAM_ID = :team_id'

    # prepare to insert match shot stats

    insert_match_shot_stats_query = """INSERT INTO TEAM_SHOTS_STATISTICS (MATCH_ID, TEAM_ID, SHOTS_ON_GOAL, SHOTS_OFF_GOAL,
     TOTAL_SHOTS, BLOCKED_SHOTS, SHOTS_INSIDEBOX, SHOTS_OUTSIDEBOX) VALUES (:match_id, :team_id, :shots_on_goal, :shots_off_goal, 
     :total_shots, :blocked_shots, :shots_inside_box, :shots_outside_box)"""

    check_match_shot_stats = 'SELECT COUNT(*) FROM TEAM_SHOTS_STATISTICS WHERE MATCH_ID = :match_id AND TEAM_ID = :team_id'

    match_data = match_stats[0]['team']
    stats = {stat['type']: stat['value'] for stat in match_stats[0]['statistics']}

    # inserts

    match_stats = [{
        'match_id': match_id,
        'team_id': match_data['id'],
        'ball_possession': stats.get('Ball Possession'),
        'total_passes': stats.get('Total passes'),
        'passes_accurate': stats.get('Passes accurate'),
        'passes_percentage': stats.get('Passes %'),
        'fouls': stats.get('Fouls'),
        'corner_kicks': stats.get('Corner Kicks'),
        'offsides': stats.get('Offsides'),
        'yellow_cards': stats.get('Yellow Cards'),
        'red_cards': stats.get('Red Cards'),
        'goalkeeper_saves': stats.get('Goalkeeper Saves')
    }]

    match_shot_stats = [{
        'match_id': match_id,
        'team_id': match_data['id'],
        'shots_on_goal': stats.get('Shots on Goal'),
        'shots_off_goal': stats.get('Shots off Goal'),
        'total_shots': stats.get('Total Shots'),
        'blocked_shots': stats.get('Blocked Shots'),
        'shots_inside_box': stats.get('Shots insidebox'),
        'shots_outside_box': stats.get('Shots outsidebox')
    }]

    # insert match stats
    cursor.execute(check_match_stats_query, {'match_id': match_id, 'team_id': team_id})
    match_stats_exist = cursor.fetchone()[0]
    if match_stats_exist == 0:
        insert_data_into_data_base(insert_match_stats_query, match_stats)
    else:
        print(f'Match stats for match {match_id} and team {team_id} already exists, skipping insertion.')

    # insert match shots

    cursor.execute(check_match_shot_stats, {'match_id': match_id, 'team_id': team_id})
    check_match_shot_stats = cursor.fetchone()[0]
    if check_match_shot_stats == 0:
        insert_data_into_data_base(insert_match_shot_stats_query, match_shot_stats)
    else:
        print(f'Match shots for match {match_id} and team {team_id} already exists, skipping insertion.')

    close_connection()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='match_statistics_etl_dag',
    default_args=default_args,
    schedule='0 */2 * * *',
    description='Create statistics tables and load match stats from football API',
    tags=['oracle', 'football', 'etl']
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_statistics_tables',
        python_callable=create_statistics_tables
    )

    get_next_match_task = PythonOperator(
        task_id='get_next_match',
        python_callable=get_next_match_id,
    )

    fetch_stats_task = PythonOperator(
        task_id='fetch_match_statistics',
        python_callable=fetch_match_statistics,
    )

    insert_stats_task = PythonOperator(
        task_id='insert_match_statistics',
        python_callable=insert_match_statistics,
    )

    update_last_processed_match_task = PythonOperator(
        task_id='update_last_processed_match',
        python_callable=update_last_processed_match,
    )

    create_tables_task >> get_next_match_task >> fetch_stats_task >> insert_stats_task >> update_last_processed_match_task
