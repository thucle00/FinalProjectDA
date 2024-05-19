import sys
from airflow import configuration

AIRFLOW_DAGS = configuration.get('core', 'dags_folder')
sys.path.append(AIRFLOW_DAGS + '/da_final')

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import time
from datetime import datetime, timedelta
import random
from airflow.utils.db import provide_session
from airflow.models import XCom, Variable
from utils import load_list_rpc, parse_block, parse_transactions, parse_withdrawal, change_w3, write_append_log
from web3 import Web3
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'thucltt',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def keep_10_latest_blocks(**kwargs):
    hook = MongoHook(mongo_conn_id='mongo_da_final')
    client = hook.get_conn()
    db = client.da_final
    
    collection = db.latest10
    
    # Count the total number of documents in the collection
    total_blocks = collection.count_documents({})

    if total_blocks > 10:
        # Sort the documents by the number field and delete the oldest ones
        oldest_blocks = collection.find({}, {'_id': 1}).sort('number', 1).limit(total_blocks - 10)
        for block in oldest_blocks:
            collection.delete_one({'_id': block['_id']})

with DAG('da_final_keep_10_latest_block',
        default_args=default_args,
        description='DA Final Project',
        schedule_interval=timedelta(seconds=15),
        # schedule_interval=None,
        start_date=days_ago(1),
        max_active_runs=1,
        catchup=False,
        tags=['da_final']) as dag:
    
    task_start = DummyOperator(task_id='start')
    
    keep10 = PythonOperator(
        task_id='keep10',
        python_callable=keep_10_latest_blocks,
        provide_context=True,
        # retries=2,  # Retry 2 times
        # retry_delay=1,  # Retry delay in seconds
    )
    
    @provide_session
    def cleanup_xcom(session=None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id 
        # It will delete all xcom of the dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable = cleanup_xcom,
        provide_context=True, 
    )
    
    task_done = DummyOperator(task_id='done')
    
    task_start >> keep10
    keep10 >> clean_xcom
    clean_xcom >> task_done
