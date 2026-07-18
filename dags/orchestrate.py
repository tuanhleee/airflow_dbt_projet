import time
import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


@dag(
    dag_id="orchestrate",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 18, tz="Europe/Paris"),
    catchup=False,
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host="https://dbc-1766c822-14e4.cloud.databricks.com",
            token="i dont know"
        )

        job_trigger = ws.jobs.run_now(
            job_id="your_job_id without string"  
        )

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break
                else:
                    raise Exception(
                        f"Job failed with state: "
                        f"{job_run.state.result_state}"
                    )

            time.sleep(5)

        return "CDC Ingestion Completed"


    @task.bash
    def clean_target():
        return (
            "rm -rf /opt/airflow/dbt_projet/target "
            "&& rm -rf /opt/airflow/dbt_projet/logs"
        )


    @task.bash
    def source_freshness():
        return (
            "cd /opt/airflow/dbt_projet "
            "&& dbt source freshness"
        )


    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt run --select silver_technical",
    )


    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt test --select silver_technical",
    )


    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt run --select silver_bussiness",
    )


    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt test --select silver_bussiness",
    )


    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt run --select gold/ephemeral",
    )


    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt snapshot",
    )


    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/dbt_projet",
        bash_command="dbt run --select gold/fact",
    )


    # Instanciation des tasks décorées
    ingest_task = ingest_cdc()
    clean_task = clean_target()
    freshness_task = source_freshness()


    # Dépendances
    ingest_task >> clean_task

    clean_task >> freshness_task

    freshness_task >> silver_technical

    silver_technical >> silver_technical_tests

    silver_technical_tests >> silver_business

    silver_business >> silver_business_tests

    silver_business_tests >> gold_ephemeral

    gold_ephemeral >> gold_dimensions

    gold_dimensions >> gold_facts


orchestrate_dag = orchestrate()