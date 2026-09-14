"""TP2 bis — DAG SABOTE : a debugger pendant l'exercice.

Ce fichier contient VOLONTAIREMENT plusieurs erreurs realistes.
Mission : les trouver avec la Grid / le Graph / les logs, puis corriger.

>>> Ne pas lire le corrige (solutions/tp02bis_debug_corrige.py) avant d'avoir cherche ! <<<
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import DummyOperator


def check_data(**context):
    print(f"Verification des donnees du {context['date_du_jour']}")
    return True


with DAG(
    dag_id="snowystore_broken",
    start_date=datetime.now(),
    schedule="@daily",
    catchup=True,
    default_args={"owner": "formation", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["snowystore", "debug"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'extraction du {{ ds }}'",
    )

    check = PythonOperator(
        task_id="check",
        python_callable=check_data,
    )

    load = DummyOperator(task_id="load")

    extract >> check >> load
    load >> extract
