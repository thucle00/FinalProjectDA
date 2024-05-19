from web3 import Web3
from web3.types import BlockData, TxData
from typing import List
from datetime import datetime

def write_append_log(file_name, content):
    with open(file_name, 'a') as f:
        f.write(content)
        f.write('\n')
        
def write_replace_log(file_name, content):
    with open(file_name, 'w') as f:
        f.write(content)

def parse_transactions(transactions : List[TxData]):
    transactions_res = []
    
    for transaction in transactions:
        transactions_res.append({
            'blockNumber': transaction['blockNumber'],
            'from' : transaction['from'],
            'gas' : transaction['gas'],
            'gasPrice' : transaction['gasPrice'],
            'hash' : transaction['hash'].hex(),
            'to' : transaction['to'],
            'value' : float(Web3.fromWei(transaction['value'], 'ether')),
        })
    
    return transactions_res

def parse_block(block : BlockData):
    block_res = {
        'baseFeePerGas' : block['baseFeePerGas'],
        'gasUsed' : block['gasUsed'],
        'numOfTransactions' : len(block['transactions']),
        'hash' : block['hash'].hex(),
        'parentHash' : block['parentHash'].hex(),
        'miner' : block['miner'],
        'number' : block['number'],
        'size' : block['size'],
        'timestamp' : block['timestamp'],
    }
    
    return block_res


def parse_withdrawal(withdrawals, blockNumber):
    withdrawals_res = []
    for withdrawal in withdrawals:
        withdrawals_res.append({
            'index': withdrawal['index'],
            'blockNumber' : blockNumber,
            'address': withdrawal['address'],
            'amount': float(Web3.fromWei(int(withdrawal['amount'], 16)*10**9, 'ether')),
        })
    return withdrawals_res

def load_list_rpc(filename):
    list_rpc = []
    with open(filename, 'r') as file:
        for line in file:
            list_rpc.append(line.replace('\n',''))
    return list_rpc

def change_w3(rpc_url_list, idx_rpc):
    
    idx_rpc += 1
    idx_rpc %= len(rpc_url_list)
        
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    ok = False
    
    while not ok:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url_list[idx_rpc]))
            while not w3.isConnected():
                write_append_log('err_log', '{} {} {}'.format(formatted_time, 'LIMIT RPC', rpc_url_list[idx_rpc]))
                idx_rpc += 1
                idx_rpc %= len(rpc_url_list)
            ok = True
        except Exception as err:
            print(rpc_url_list[idx_rpc], err)
            write_append_log('err_log', '{} {} {} {}'.format(formatted_time, 'ERR RPC', rpc_url_list[idx_rpc], err))
            idx_rpc += 1
            idx_rpc %= len(rpc_url_list)
    
    return w3, idx_rpc
