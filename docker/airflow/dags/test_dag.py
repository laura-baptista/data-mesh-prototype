from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_teste_data_mesh",
    start_date=datetime(2025, 11, 9),
    schedule_interval="@once",
    catchup=False
):

    t1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Airflow funcionando!'"
    )
