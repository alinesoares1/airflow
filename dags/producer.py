from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("tmp/my_file.txt")

with DAG(
    dag_id="producer",
    scheduler='@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False
):
    # indica ao airflow que a task "update_dataset" atualiza o Dataset my_file, assim que essa tarefa for bem sucedida...a próxima DAG será startada
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    update_dataset()
