from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def wait_few_seconds():
    time.sleep(4)  # Wait few seconds for new data in DB, in order to be sure that new data exists

with DAG(
    "first_trigger_in_correct_order_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description=(
        "This DAG triggers a sequence of ETL DAGs in dependency order: "
        "seasons -> single_season_leagues -> league_teams -> team_matches -> match_statistics. "
        "All DAGs must be enabled to work correctly."
    ),
) as dag:

    trigger_seasons = TriggerDagRunOperator(
        task_id="trigger_seasons",
        trigger_dag_id="seasons_etl_dag",
    )

    wait_1 = PythonOperator(
        task_id="wait_after_seasons",
        python_callable=wait_few_seconds,
    )

    trigger_season_league = TriggerDagRunOperator(
        task_id="trigger_season_league",
        trigger_dag_id="single_season_leagues_etl_dag",
    )

    wait_2 = PythonOperator(
        task_id="wait_after_season_league",
        python_callable=wait_few_seconds,
    )

    trigger_league_teams = TriggerDagRunOperator(
        task_id="trigger_league_teams",
        trigger_dag_id="league_teams_etl_dag",
    )

    wait_3 = PythonOperator(
        task_id="wait_after_league_teams",
        python_callable=wait_few_seconds,
    )

    trigger_team_matches = TriggerDagRunOperator(
        task_id="trigger_team_matches",
        trigger_dag_id="team_matches_etl_dag",
    )

    wait_4 = PythonOperator(
        task_id="wait_after_team_matches",
        python_callable=wait_few_seconds,
    )

    trigger_team_statistics = TriggerDagRunOperator(
        task_id="trigger_team_statistics",
        trigger_dag_id="match_statistics_etl_dag"
    )

    # Define execution order
    (
        trigger_seasons >>
        wait_1 >>
        trigger_season_league >>
        wait_2 >>
        trigger_league_teams >>
        wait_3 >>
        trigger_team_matches >>
        wait_4 >>
        trigger_team_statistics
    )
