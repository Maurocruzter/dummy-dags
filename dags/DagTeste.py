from airflow.decorators import dag, task
from datetime import datetime
from utils.functions import parse_ts_dag_run
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

description = "Pipeline de teste multiplos executores"

default_args = {
    "start_date": datetime(2023, 10, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "namespace": "processing",
}

@dag(
    dag_id="Dag_multiplos_executores",
    default_args=default_args,
    schedule_interval="0 3 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    render_template_as_native_obj=True,
    description=description,
    tags=["teste"],
    user_defined_macros={
        "parse_ts_dag_run": parse_ts_dag_run,
    }
)
def dag_headcount():
    @task
    def task_default_executor(**context):
        from utils.fn_testes_executores import func_testes_executores
        func_testes_executores(**context)
        return None

    @task(executor="LocalExecutor")  # Explicitly set LocalExecutor
    def task_local_executor(**context):
        from utils.fn_testes_executores import func_testes_executores
        func_testes_executores(**context)
        return None

    @task
    def task_celery_executor(**context):
        from utils.fn_testes_executores import func_testes_executores
        func_testes_executores(**context)
        return None

    # Explicitly chain the tasks
    first_task = task_default_executor()
    second_task = task_local_executor()
    third_task = task_celery_executor()

    first_task >> second_task >> third_task

# Important: Call the DAG function to instantiate it
dag_instance = dag_headcount()