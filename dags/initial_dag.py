from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


with DAG(
    "first_trigger_in_correct_order_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="This dag should be triggered first if you don't know what is order to complete the tables that are needed to insert data into the next tables," \
    "like in order: seasons(required)->single_season_leagues->league_teams->team_matches->match_statistics, " \
    "because of the relational database and the primary key that must exist in tables",
) as dag:

    trigger_seasons = TriggerDagRunOperator(
        task_id="trigger_seasons",
        trigger_dag_id="seasons_etl_dag",
    )

    trigger_season_league = TriggerDagRunOperator(
        task_id="trigger_season_league",
        trigger_dag_id="single_season_leagues_etl_dag",
    )

    trigger_league_teams = TriggerDagRunOperator(
        task_id="trigger_league_teams",
        trigger_dag_id="league_teams_etl_dag",
    )

    trigger_team_matches = TriggerDagRunOperator(
        task_id="trigger_team_matches",
        trigger_dag_id="team_matches_etl_dag",
    )

    trigger_team_statistics = TriggerDagRunOperator(
        task_id="trigger_team_statistics",
        trigger_dag_id="match_statistics_etl_dag",
    )

    # Execution order
    (
        trigger_seasons >>
        trigger_season_league >>
        trigger_league_teams >>
        trigger_team_matches >>
        trigger_team_statistics
    )
