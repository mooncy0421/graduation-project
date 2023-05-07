import yaml
import pendulum
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import settings

from crawl.tasks.load_news_url_title import LoadNewsUrlTitle
from crawl.tasks.load_news_info import LoadNewsInfo


DAG_RUNID_FORMAT = r"%Y-%m-%dT%H:%M:%S%z"
DAG_RUNID_MORE_PRECISION_FORMAT = r"%Y-%m-%dT%H:%M:%S.%f%z"
SNAPSHOT_DT_FORMAT = r"%Y%m%d_%H%M%S_%f"

DAG_ID = "dag_crawl_news_info"

def dag_runid2snapshot(dag_runid):
    datetime_parts = dag_runid.split('__')[-1]
    try:
        dt = datetime.strptime(datetime_parts, DAG_RUNID_FORMAT)
    except:
        dt = datetime.strptime(datetime_parts, DAG_RUNID_MORE_PRECISION_FORMAT)
    return dt.strftime(SNAPSHOT_DT_FORMAT)


def _load_news_url_title(dag_config, **context):
    task_config = dag_config[0]
    
    run_id = context["dag_run"].run_id
    run_snapshot_dt = dag_runid2snapshot(run_id)
    
    LoadNewsUrlTitle(task_config)(context, run_snapshot_dt)

def _load_news_info(dag_config, **context):
    task_config = dag_config[1]
    
    run_id = context["dag_run"].run_id
    run_snapshot_dt = dag_runid2snapshot(run_id)

    LoadNewsInfo(task_config)(context, run_snapshot_dt)

def build_tasks(dag_config, dag):
    start_op = DummyOperator(task_id="start", dag=dag)

    load_news_url_title_op = PythonOperator(
        task_id="load_news_url_title",
        python_callable=_load_news_url_title,
        op_kwargs={
            "dag_config": dag_config
        },
        dag=dag
    )

    load_news_info_op = PythonOperator(
        task_id="load_news_info",
        python_callable=_load_news_info,
        op_kwargs={
            "dag_config": dag_config
        },
        dag=dag
    )

    exit_op = DummyOperator(task_id="exit", dag=dag)

    start_op >> load_news_url_title_op >> load_news_info_op >> exit_op

    return start_op, exit_op

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023,5,7, tzinfo=pendulum.timezone('Asia/Seoul')),
    end_date=None,
    schedule_interval=relativedelta(days=1)
) as dag:
    dag_config_file_path = "/home/hcy/workspace/graduation-project/builds/resources/dag.yml"

    with open(dag_config_file_path, "r") as f:
        dag_config = yaml.load(f, Loader=yaml.Loader)[DAG_ID]

    tasks = build_tasks(dag_config, dag)
    tasks