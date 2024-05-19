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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import requests

def send_telegram_message(context):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    failed_task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    message = f"[{current_time}] Task {failed_task_id} failed in DAG {dag_id}"

    # Your Telegram bot token and chat ID
    bot_token = ''
    with open(AIRFLOW_DAGS + '/da_final/tgbot') as f:
        for line in f:
            bot_token = line.replace('\n','')
            break
        
    chat_id = ''
    with open(AIRFLOW_DAGS + '/da_final/tgchatid') as f:
        for line in f:
            chat_id = line.replace('\n','')
            break

    telegram_api_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': message
    }
    response = requests.post(telegram_api_url, data=data)
    if response.status_code != 200:
        print("Failed to send Telegram message:", response.text)
        
def send_telegram_message_custom(msg):
    # Your Telegram bot token and chat ID
    bot_token = ''
    with open(AIRFLOW_DAGS + '/da_final/tgbot') as f:
        for line in f:
            bot_token = line.replace('\n','')
            break
        
    chat_id = ''
    with open(AIRFLOW_DAGS + '/da_final/tgchatid') as f:
        for line in f:
            chat_id = line.replace('\n','')
            break

    telegram_api_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': msg
    }
    response = requests.post(telegram_api_url, data=data)
    if response.status_code != 200:
        print("Failed to send Telegram message:", response.text)

default_args = {
    'owner': 'thucltt',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def get_data(**kwargs):
    
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    idx_rpc = int(Variable.get('da_final_idx_rpc'))
    
    rpc_url_list = load_list_rpc(AIRFLOW_DAGS + '/da_final/rpc')
    
    w3, idx_rpc = change_w3(rpc_url_list, idx_rpc)
            
    try:
        cur_block_number = w3.eth.blockNumber
    except Exception as err:
        # print('ERR RPC', rpc_url_list[idx_rpc], err)
        write_append_log('err_log', '{} {} {} {}'.format(formatted_time, 'ERR RPC', rpc_url_list[idx_rpc], err))
        w3, idx_rpc = change_w3(rpc_url_list, idx_rpc)
        
    last_block_number_get = int(Variable.get('da_final_last_block_number_get'))
    
    if last_block_number_get == None:
        last_block_number_get = cur_block_number
        
    if last_block_number_get == cur_block_number:
        return
        
    if last_block_number_get > cur_block_number:
        write_append_log('delay_log', '{} {} {}'.format(formatted_time, 'DELAY RPC', rpc_url_list[idx_rpc]))
        
        w3, idx_rpc = change_w3(rpc_url_list, idx_rpc)
        try:
            cur_block_number = w3.eth.blockNumber
        except Exception as err:
            # print('ERR RPC', rpc_url_list[idx_rpc], err)
            write_append_log('err_log', '{} {} {} {}'.format(formatted_time, 'ERR RPC', rpc_url_list[idx_rpc], err))
            w3, idx_rpc = change_w3(rpc_url_list, idx_rpc)
    
    if (cur_block_number - last_block_number_get) > 1:
        kwargs['ti'].xcom_push(key='delay', value=[last_block_number_get+1, cur_block_number - 1])
    else:
        kwargs['ti'].xcom_push(key='delay', value=[])
    
    # FOR DEBUG
    # kwargs['ti'].xcom_push(key='delay', value=[])
        
    
    try:
        block_data = w3.eth.get_block(cur_block_number, full_transactions=True)
    except Exception as err:
        # print('ERR RPC', rpc_url_list[idx_rpc], err)
        write_append_log('err_log', '{} {} {} {}'.format(formatted_time, 'ERR RPC', rpc_url_list[idx_rpc], err))
        w3, idx_rpc = change_w3(rpc_url_list, idx_rpc)
    
    print(rpc_url_list[idx_rpc])
    
    Variable.set('da_final_idx_rpc', idx_rpc)
    
    block_json = parse_block(block_data)
    transactions_json = parse_transactions(block_data['transactions'])
    withdrawals_json = parse_withdrawal(block_data['withdrawals'], block_data['number'])
    
    if block_json != None:
        Variable.set('da_final_last_block_number_get', cur_block_number)
    
    kwargs['ti'].xcom_push(key='block_json', value=json.dumps(block_json))
    kwargs['ti'].xcom_push(key='transactions_json', value=json.dumps(transactions_json))
    kwargs['ti'].xcom_push(key='withdrawals_json', value=json.dumps(withdrawals_json))
    
def aggregate_data(**kwargs):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_da_final')
    conn = pg_hook.get_conn()
    
    transactions_json = kwargs['ti'].xcom_pull(key='transactions_json')
    try:
        transactions_json = json.loads(transactions_json)
    except Exception as err:
        print(err)
    
    # FOR TABLE address_transactions
    txbyid = dict()
    
    for tx in transactions_json:
        if round(float(tx['value']),3) > 0:
            if tx['from'] not in txbyid:
                txbyid[tx['from']] = [0,0]
                # [x,y] with x is numOfTxs in block, y is sumOfTxs in block
                
            txbyid[tx['from']][0] += 1
            txbyid[tx['from']][1] += tx['value']
    
    data_transactions_to_insert = []
    for idx in txbyid:
        if idx == '0x0000000000000000000000000000000000000000':
            continue
        data_transactions_to_insert.append((idx, txbyid[idx][0], txbyid[idx][1]))
    
    # SQL INSERT statement with ON CONFLICT DO UPDATE
    sql_insert_update = f"""
        INSERT INTO address_transactions (id, transaction_count, total_amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET transaction_count = address_transactions.transaction_count + EXCLUDED.transaction_count,
            total_amount = address_transactions.total_amount + EXCLUDED.total_amount;
    """
    
    # Execute the query
    cursor = conn.cursor()
    for row in data_transactions_to_insert:
        cursor.execute(sql_insert_update, row)
    
    conn.commit()
    
    # FOR TABLE latest10block
    
    block_json = kwargs['ti'].xcom_pull(key='block_json')
    block_json = json.loads(block_json)
    
    data_block_to_insert = [(block_json['number'], block_json['numOfTransactions'], float(block_json['baseFeePerGas'])/1e9, block_json['miner'], block_json['timestamp'])]
    sql_insert_update = f"""
        INSERT INTO latest10block (block_num, num_of_tx, gas_fee, miner, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """
    # Execute the query
    cursor = conn.cursor()
    for row in data_block_to_insert:
        cursor.execute(sql_insert_update, row)
        
    sql_insert_keep10 = f"""
        DELETE FROM latest10block
        WHERE (block_num) IN (
            SELECT block_num
            FROM (
                SELECT 
                    block_num,
                    ROW_NUMBER() OVER (ORDER BY block_num DESC) AS row_number
                FROM latest10block
            ) AS subquery
            WHERE row_number > 10
        );
    """
    
    cursor.execute(sql_insert_keep10)
    conn.commit()
    
    # FOR TABLE miner_inday
    
    # SQL INSERT statement with ON CONFLICT DO UPDATE
    sql_insert_update_miner_inday = f"""
        INSERT INTO miner_inday (miner_address, cnt_block)
        VALUES (%s, %s)
        ON CONFLICT (miner_address) DO UPDATE
        SET cnt_block = miner_inday.cnt_block + EXCLUDED.cnt_block;
    """
    
    data_miner_inday_to_insert = [(block_json['miner'], 1)]
    cursor = conn.cursor()
    for row in data_miner_inday_to_insert:
        cursor.execute(sql_insert_update_miner_inday, row)

    conn.commit()
    
    # FOR TABLE avg_inday
    
    # SQL INSERT statement with ON CONFLICT DO UPDATE
    sql_insert_update_avg_inday = f"""
        INSERT INTO avg_inday (date, tot_txs, tot_gasfee, tot_block, tot_basegasfee)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET tot_txs = avg_inday.tot_txs + EXCLUDED.tot_txs,
            tot_gasfee = avg_inday.tot_gasfee + EXCLUDED.tot_gasfee,
            tot_block = avg_inday.tot_block + EXCLUDED.tot_block,
            tot_basegasfee = avg_inday.tot_basegasfee + EXCLUDED.tot_basegasfee;
    """
    
    current_date = datetime.now().date()

    data_avg_inday_to_insert = [(current_date, block_json['numOfTransactions'], float(block_json['baseFeePerGas'])*float(block_json['gasUsed'])/1e18, 1, float(block_json['baseFeePerGas'])/1e9)]
    
    cursor = conn.cursor()
    for row in data_avg_inday_to_insert:
        cursor.execute(sql_insert_update_avg_inday, row)

    conn.commit()
    
    
     
def store_data(**kwargs):
    
    mongo_hook = MongoHook(mongo_conn_id='mongo_da_final')
    client = mongo_hook.get_conn()
    db = client.da_final
    
    block_json = kwargs['ti'].xcom_pull(key='block_json')
    if block_json == None:
        return
    # currency_collection=db.block
    # currency_collection.insert_one(json.loads(block_json))
    
    currency_collection=db.latest10
    currency_collection.insert_one(json.loads(block_json))
    
    # transactions_json = kwargs['ti'].xcom_pull(key='transactions_json')
    # currency_collection=db.transaction
    # currency_collection.insert_many(json.loads(transactions_json))
    
    # withdrawals_json = kwargs['ti'].xcom_pull(key='withdrawals_json')
    # currency_collection=db.withdrawl
    # currency_collection.insert_many(json.loads(withdrawals_json))
    
    
# def need_run_delay(**kwargs):
#     # delay = kwargs['ti'].xcom_pull(key='delay')
#     # return delay
#     return

with DAG('da_final_get_current_block',
    default_args=default_args,
    description='DA Final Project',
    schedule_interval=timedelta(seconds=15),
    # schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=['da_final']) as dag:

    task_start = DummyOperator(task_id='start')
    
    task_get_data = PythonOperator(
        task_id='get_json_data',
        python_callable=get_data,
        provide_context=True,
        on_failure_callback=send_telegram_message,
        retries=2,  # Retry 2 times
        # retry_delay=1,  # Retry delay in seconds
    )
    
    store_data = PythonOperator(
        task_id='store_data',
        python_callable=store_data,
        provide_context=True,
        on_failure_callback=send_telegram_message,
    )
    
    # delay_block = PythonOperator(
    #     task_id='get_delay_block',
    #     python_callable=need_run_delay,
    #     provide_context=True,
    #     on_failure_callback=send_telegram_message,
    # )
    
    fix_delay = TriggerDagRunOperator(
        task_id='task_fix_delay',
        trigger_dag_id='da_final_get_delay_block',
        conf={'delay_block': '{{ task_instance.xcom_pull(task_ids="get_json_data", key="delay") }}'}, 
        on_failure_callback=send_telegram_message,
        dag=dag
    )
    
    task_aggregate_data = PythonOperator(
        task_id='task_aggregate_data',
        python_callable=aggregate_data,
        provide_context=True,
        # on_failure_callback=send_telegram_message,
        retries = 0,
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
        on_failure_callback=send_telegram_message,
    )
    
    task_done = DummyOperator(task_id='done')
    
    task_start >> task_get_data
    task_get_data >> [fix_delay, store_data, task_aggregate_data]
    [fix_delay, store_data, task_aggregate_data] >> clean_xcom
    clean_xcom >> task_done
    
    # task_start >> task_get_data
    # task_get_data >> task_aggregate_data
    # task_aggregate_data >> clean_xcom
    # clean_xcom >> task_done

    
