from airflow.decorators import dag, task
from datetime import datetime
from utils.functions import parse_ts_dag_run
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)


description = (
    "Pipeline para coleta e atualização da base de headcount (anteriormente socios)"
)
data_interval_end = "{{ macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)) }}"
timestamp_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"



default_args = {
    "start_date": datetime(2023, 10, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "namespace": "processing",
    "executor": "LocalExecutor",
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
    },
)
def dag_headcount():

    @task
    def task_default_executor(**context):
        from utils.fn_testes_executores import (
            func_testes_executores,
        )

        func_testes_executores(**context)

    @task(executor="LocalExecutor")
    def task_local_executor(**context):
        from utils.fn_testes_executores import (
            func_testes_executores,
        )

        func_testes_executores(**context)

    @task(executor="CeleryExecutor")
    def task_celery_executor(**context):
        from utils.fn_testes_executores import (
            func_testes_executores,
        )

        func_testes_executores(**context)

    pipeline_flow = (
        task_default_executor() >> task_local_executor() >> task_celery_executor()
    )


dag_headcount()
