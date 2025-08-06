from docker.types import Mount
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.decorators import task, dag
import uuid

default_args = {
    "owner": "dbt",
    "description": "dbt idnclimate daily uppdate",
    "depend_on_past": False,
    "start_date": datetime(2021, 1, 1),
    # "end_date": datetime(2022, 6, 30),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def get_uuid():
    myuuid = uuid.uuid4()
    myuuid = str(myuuid)
    return myuuid

@dag(
    dag_id="dbt_idnclimate",
    # schedule_interval="0 0 * * *",
    schedule="@daily",
    # catchup=False,
    default_args=default_args,
    max_active_runs=1,
)
def docker_dag():
    daily_data_update = DockerOperator(
        task_id="daily_data_update",
        image="dbt",
        container_name="dbt_daily_update-{{ds}}-" + get_uuid(),
        auto_remove=True,
        command="python3 python-script/insert_daily_data.py {{ ds }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt",
        container_name="dbt_run-{{ds}}-" + get_uuid(),
        auto_remove=True,
        command="cd dbt_idnclimate && dbt run",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source=".dbt/",
                target="/root/.dbt",
                type="bind",
            )
        ],
    )
    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="dbt",
        container_name="dbt_test-{{ds}}-" + get_uuid(),
        auto_remove=True,
        command="cd dbt_idnclimate && dbt test",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source=".dbt/",
                target="/root/.dbt",
                type="bind",
            )
        ],
    )
    daily_data_update >> dbt_run >> dbt_test

dag = docker_dag()
