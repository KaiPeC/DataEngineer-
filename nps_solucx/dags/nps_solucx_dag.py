# coding: utf-8
import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from sness.datalake.metadata import Environment
from sness.pipeline.custom_operators.cloud_storage_to_big_query_operator import (
    CloudStorageToBigQueryOperator,
)
from sness.pipeline.custom_operators.dataproc_operator import (
    build_ness_etl_path,
)
from sness.pipeline.custom_operators.minecraft_operator import (
    MinecraftOperator,
)
from sness.pipeline.slack import slack_failed_task, slack_miss_sla
from sness.settings.pod_type import PodType


connection_ = BaseHook.get_connection("nps_")


def get_environment() -> str:
    if Variable.get("AIRFLOW_ENVIRONMENT") == "production":
        return "prd"
    return "stg"


ENVIRONMENT = get_environment()

DAG_NAME = "nps_solucx"

default_args = {
    "owner": "Squad Inteligencia Clientes",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2019, 10, 29),
    "on_failure_callback": slack_failed_task,
    "slack_notification_channel": "#alert_inteligencia_clientes_dags",
    "params": {
        "labels": {
            "se_tribe": "ciencia_eng_dados",
            "se_vertical": "ecomm_dados",
        }
    },
}

dag = DAG(
    DAG_NAME,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="0 6 * * *",
    sla_miss_callback=slack_miss_sla,
)
start_dummy = DummyOperator(task_id="Inicio", dag=dag)
end_dummy = DummyOperator(task_id="Fim", dag=dag, sla=datetime.timedelta(hours=3))

############ NPS magalupay ############

# Chamado do job de api to transient
step_transient = MinecraftOperator(
    task_id="nps_api_to_transient",
    main=build_ness_etl_path(
        pipeline="nps_solucx",
        zone="transient",
        name="nps_transient.py",
    ),
    executor_size=PodType.EXECUTOR_MEDIUM,
    arguments=[
        "--api_host",
        connection_magalupay.host,
        "--api_username",
        connection_magalupay.login,
        "--api_password",
        connection_magalupay.password,
        "--environment",
        ENVIRONMENT,
    ],
    dag=dag,
)

# Chamado do job de transient to raw
step_raw = MinecraftOperator(
    task_id="nps_transient_to_raw",
    main=build_ness_etl_path(pipeline="nps_solucx", zone="raw", name="nps_raw.py"),
    executor_size=PodType.EXECUTOR_MEDIUM,
    arguments=[
        "--environment",
        ENVIRONMENT,
    ],
    dag=dag,
)

# Chamado do job de raw to trusted
step_trusted = MinecraftOperator(
    task_id="nps_raw_to_trusted",
    main=build_ness_etl_path(
        pipeline="nps_solucx", zone="trusted", name="nps_trusted.py"
    ),
    executor_size=PodType.EXECUTOR_MEDIUM,
    arguments=[
        "--environment",
        ENVIRONMENT,
    ],
    dag=dag,
)


# Manda os dados do bucket para o BQ (o resultado do scripts)
step_bq_nps = CloudStorageToBigQueryOperator(
    task_id="nps_trusted_to_bq",
    zone="trusted",
    namespace="nps_solucx",
    dataset="satisfaction_survey",
    environment=Environment.PRODUCTION,
    dag=dag,
)

step_bq_nps_revision = CloudStorageToBigQueryOperator(
    task_id="nps_revision_trusted_to_bq",
    zone="trusted",
    namespace="nps_solucx",
    dataset="satisfaction_survey_revisions",
    environment=Environment.PRODUCTION,
    dag=dag,
)


(start_dummy >> step_transient >> step_raw >> step_trusted)

step_trusted >> step_bq_nps >> end_dummy
step_trusted >> step_bq_nps_revision >> end_dummy
