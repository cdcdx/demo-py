import os
import sys
import json
import time
import random
import argparse
import asyncio
import requests
import threading
import concurrent.futures
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from loguru import logger
from datetime import datetime as dt
from decimal import Decimal

from config import DB_ENGINE
from utils.cache import get_redis_data, set_redis_data, del_redis_data
from utils.db import get_db_app, format_query_for_db, convert_row_to_dict
from utils.local import generate_referralcode
from utils.web3_tools import make_request, get_web3_config_by_chainid, get_web3_config_by_network
from utils.log import log as logger

"""
- boxnft event monitoring
"""

hash_file = 'hash_boxnft'
hash_index = 0

# ABI
contract_abi_nftmint = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "buyer", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "superior", "type": "address"},
            {"indexed": False, "internalType": "uint8", "name": "boxId", "type": "uint8"},
            {"indexed": False, "internalType": "uint256", "name": "ethAmount", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "sxpAmount", "type": "uint256"},
            {"indexed": False, "internalType": "uint256[]", "name": "tokenIds", "type": "uint256[]"}
        ],
        "name": "NFTPurchased",
        "type": "event"
    }
]

# ------------------------------------------------------------------------------------

def web3_is_connected_with_retry(web3_obj, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            connected = web3_obj.is_connected()
            return connected
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < max_retries:
                logger.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Max retries reached. Failed to is_connected.")
                return 0

def get_block_number_with_retry(web3_obj, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            current_block = web3_obj.eth.block_number
            current_block -= 3  # Delay 6 seconds, 3 blocks
            return current_block
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < max_retries:
                logger.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Max retries reached. Failed to eth.block_number.")
                return 0

def get_block_with_retry(web3_obj, block_number, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            block = web3_obj.eth.get_block(block_number, full_transactions=True)
            return block
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < max_retries:
                logger.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Max retries reached. Failed to eth.get_block.")
                return None

def get_transaction_with_retry(web3_obj, tx_hash, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            tx_receipt = web3_obj.eth.get_transaction(tx_hash)
            return tx_receipt
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < max_retries:
                logger.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Max retries reached. Failed to eth.get_transaction.")
                return None

def get_transaction_receipt_with_retry(web3_obj, tx_hash, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            tx_receipt = web3_obj.eth.get_transaction_receipt(tx_hash)
            return tx_receipt
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < max_retries:
                logger.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Max retries reached. Failed to eth.get_transaction_receipt.")
                return None

def get_event_logs_with_retry(contract_event, from_block, to_block, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            return contract_event.get_logs(from_block=from_block, to_block=to_block)
        except Exception as e:
            logger.error(f"Event logs attempt {attempt+1} failed: {str(e)}")
            attempt += 1
            time.sleep(retry_interval)
    logger.error("Max retries reached for event logs query")
    return []

# ------------------------------------------------------------------------------------
async def get_eth_price() -> float:
    try:
        eth_price = 0.0
        # 检查缓存
        eth_price = await get_redis_data(False, "box:eth:price")
        logger.debug(f"redis eth_price: {eth_price}")
        if not eth_price:
            # 从API获取
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                "ids": "ethereum",
                "vs_currencies": "usd"
            }
            response = make_request(url, params)
            eth_price = response.get('ethereum', {}).get('usd', 0)
            logger.debug(f"coingecko eth_price: {eth_price}")
            await set_redis_data(False, "box:eth:price", str(eth_price), ex=600)
    except Exception as e:
        logger.error(f"get_eth_price - except ERROR: {str(e)}")
    logger.debug(f"get_eth_price: {eth_price}")
    return eth_price

# ------------------------------------------------------------------------------------

# Monitor blocks and parse related transactions into the database
async def listen_events_start(web3_config, hash_index):
    global hash_file

    config_network = web3_config['network'] # network
    config_chainid = web3_config['chain_id'] # chain_id

    web3_rpc_url = web3_config['server'] # rpc
    if not web3_rpc_url:
        raise Exception("Web3 rpc not found")
    
    web3_obj = Web3(Web3.HTTPProvider(web3_rpc_url))
    if config_chainid in [56, 97]:
        web3_obj.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    # 连接rpc节点
    if not web3_obj.is_connected():
        logger.error(f"Unable to connect to the network: {web3_rpc_url}")
        web3_rpc_url = web3_config['rpc']
        web3_obj = Web3(Web3.HTTPProvider(web3_rpc_url))
        if config_chainid in [56, 97]:
            web3_obj.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        if not web3_obj.is_connected():
            logger.error(f"Unable to connect to the network: {web3_rpc_url}")
            time.sleep(10)
            raise Exception(f"Ooops! Failed to eth.is_connected. {web3_rpc_url}")

    retry_interval = web3_config.get('interval', 10) * 10
    
    logger.info(f"web3_config: {web3_config}")
    
    # nftmint
    nftmint_address = web3_config['nftmint']
    if not (len(nftmint_address) == 42 and nftmint_address.startswith('0x')):
        logger.error(f"Invalid nftmint_contract address - {nftmint_address}")
        return {"code": 401, "success": False, "msg": "Invalid nftmint_contract address"}
    nftmint_contract_address = Web3.to_checksum_address(nftmint_address)
    nftmint_contract = web3_obj.eth.contract(address=nftmint_contract_address, abi=contract_abi_nftmint)
    logger.info(f"nftmint_address: {nftmint_address} config_chainid: {config_chainid}")

    MAX_BLOCK_RANGE = 10000
    while True:
        try:
            current_block = get_block_number_with_retry(web3_obj)
            logger.info(f"hash_index: {hash_index} current_block: {current_block} | calc_block: {current_block-hash_index} start")
            if current_block == 0:
                logger.error(f"Ooops! Failed to eth.block_number.")
                time.sleep(retry_interval)
                continue
            block_diff = current_block - hash_index
            if block_diff <= 0:
                logger.error(f"Ooops! Current block behind index: {block_diff}")
                time.sleep(retry_interval)
                continue
            to_block = min(current_block, hash_index + MAX_BLOCK_RANGE)
            logger.debug(f"Processing blocks: {hash_index} to {to_block} (diff: {to_block - hash_index})")

            ## 购买NFT事件
            events = nftmint_contract.events.NFTPurchased.get_logs(from_block=web3_obj.to_hex(hash_index), to_block=to_block)
            for event in events:
                # logger.info(f"NFTPurchased event: {event}")
                block_number = int(event.blockNumber)
                # logger.debug(f"block_number: {block_number}")
                tx_hash = f"0x{event.transactionHash.hex()}"
                logger.info(f"block_number: {block_number} tx_hash: {tx_hash}")
                
                # 解析事件参数
                event_args = event.args
                logger.debug(f"event_args: {event_args}")
                nft_buyer = event_args['buyer'].lower()
                nft_superior = event_args['superior'].lower()
                nft_boxid = int(event_args['boxId'])
                nft_Ids = event_args['tokenIds']
                ## 金额解析
                nft_ethAmount = int(event_args['ethAmount'])
                nft_sxpAmount = int(event_args['sxpAmount'])
                tx_amount_eth = Web3.from_wei(nft_ethAmount, 'ether')
                tx_amount_sxp = Web3.from_wei(nft_sxpAmount, 'mwei')
                logger.debug(f"nft_ethAmount: {nft_ethAmount} - nft_sxpAmount: {nft_sxpAmount}")
                logger.debug(f"tx_amount_eth: {tx_amount_eth} - tx_amount_sxp: {tx_amount_sxp}")
                # 1ETH = 100000 SXP
                tx_amount_total = round(float(tx_amount_sxp) / 100000 + float(tx_amount_eth), 5)
                if tx_amount_eth == 0 and tx_amount_sxp == 0 and nft_boxid > 0:
                    logger.error(f"Ooops! transaction failed | tx_amount_eth: {tx_amount_eth} tx_amount_sxp: {tx_amount_sxp}")
                    continue
                logger.debug(f"nft_buyer: {nft_buyer} - nft_superior: {nft_superior} / tx_amount_eth: {tx_amount_eth} + tx_amount_sxp: {tx_amount_sxp} = tx_amount_total: {tx_amount_total} / nft_boxid: {nft_boxid} - nft_Ids: {nft_Ids}")
                
                # 解析区块
                tx_block = get_block_with_retry(web3_obj, block_number)
                if tx_block is None:
                    raise Exception("Ooops! Failed to eth.get_block.")
                ## 解析区块时间
                block_timestamp = tx_block.timestamp
                # logger.debug(f"block_timestamp: {block_timestamp}")
                block_time = dt.fromtimestamp(block_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                logger.debug(f"block_timestamp: {block_timestamp} block_time: {block_time}")
                
                # 解析哈希数据
                tx_trans = get_transaction_with_retry(web3_obj, tx_hash)
                if tx_trans is None:
                    logger.error(f"Ooops! transaction not found | tx_hash: {tx_hash}")
                    continue
                else:
                    logger.debug(f"transaction is {tx_trans}")
                # tx_input = Web3.to_hex(tx_trans.get('input'))
                # cool_address = '0x'+tx_input[98:138]
                # logger.info(f"cool_address: {cool_address} ")
                ## 金额解析
                tx_eth = 0
                tx_amount = tx_trans.value
                logger.debug(f"tx_amount: {tx_amount}")
                if tx_amount > 0:
                    tx_eth = Web3.from_wei(tx_amount, 'ether')
                logger.debug(f"tx_amount: {tx_amount} tx_eth: {tx_eth}")
                if tx_eth != tx_amount_eth:
                    logger.error(f"Ooops! transaction amount mismatch | tx_eth: {tx_eth} tx_amount_eth: {tx_amount_eth}")
                    continue
                logger.debug(f"tx_amount: {tx_amount} tx_eth: {tx_eth} / nft_Ids: {nft_Ids} nft_boxid: {nft_boxid}")
                
                tx_from = tx_trans['from'].lower()
                tx_to = tx_trans['to'].lower()
                logger.debug(f"tx_from: {tx_from} tx_to: {tx_to}")
                ## 购买合约校验
                if nftmint_address.lower() != tx_to:
                    logger.error(f"Ooops! nftmint_address not found | tx_hash: {tx_hash}")
                    continue

                # 解析哈希状态
                tx_receipt = get_transaction_receipt_with_retry(web3_obj, tx_hash)
                if tx_receipt is None:
                    logger.error(f"Ooops! transaction_receipt not found | tx_hash: {tx_hash}")
                    continue
                else:
                    logger.debug(f"transaction_receipt is {tx_receipt}")
                if tx_receipt['status'] != 1:
                    logger.error(f"Ooops! transaction_receipt failed | tx: {tx_receipt['from']}")
                    continue
                if len(tx_receipt.get('logs')) == 0:
                    logger.error(f"Ooops! tx_receipt logs not found | tx_hash: {tx_hash}")
                    continue
                tx_from = tx_receipt['from'].lower()
                tx_to = tx_receipt['to'].lower()
                # logger.debug(f"tx_from: {tx_from} tx_to: {tx_to}")
                if nftmint_address.lower() != tx_to:
                    logger.error(f"Ooops! nftmint_address not found | tx_hash: {tx_hash}")
                    continue
                logger.debug(f"tx_from: {tx_from} tx_to: {tx_to} / nft_Ids: {nft_Ids} nft_boxid: {nft_boxid}")
                
                status=1
                note=''
                
                # 使用异步数据库连接
                async with get_db_app() as cursor:
                    # 计算邀请码
                    par_referral_code = generate_referralcode(nft_superior)
                    logger.debug(f"par_address: {nft_superior} par_referral_code: {par_referral_code}")
                    referral_code = generate_referralcode(nft_buyer)
                    logger.debug(f"tx_address: {nft_buyer} referral_code: {referral_code}")
                    
                    # # 更新邀请码
                    # update_query = """
                    #                 UPDATE wenda_users SET referral_code=%s, updated_time=NOW() WHERE address COLLATE utf8mb4_general_ci=%s AND referral_code=''
                    #                 """
                    # values = (referral_code, nft_buyer)
                    # update_query = format_query_for_db(update_query)
                    # logger.debug(f"update_query: {update_query} values: {values}")
                    # await cursor.execute(update_query, values)
                    # if DB_ENGINE == "sqlite": cursor.connection.commit()
                    # else: await cursor.connection.commit()
                    # logger.debug(f"UPDATE wenda_users referral_code success! nft_buyer: {nft_buyer} referral_code: {referral_code}")
                
                    # storage mint
                    nft_count = len(nft_Ids)
                    for nft_id in nft_Ids:
                        insert_query = """
                                    INSERT INTO wenda_nft_onchain 
                                        (contract_address, tx_address, referral_code, par_address, par_referral_code, tx_chainid, tx_blockid, tx_hash, tx_date, tx_amount_sxp, tx_amount_eth, tx_amount_total, nft_id, nft_boxid, nft_timestamp, status, note) 
                                    SELECT 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    WHERE 
                                        NOT EXISTS (SELECT id FROM wenda_nft_onchain WHERE tx_hash=%s AND nft_id=%s);
                                    """
                        values = (nftmint_address, nft_buyer, referral_code, nft_superior, par_referral_code, config_chainid, block_number, tx_hash, block_time, round(tx_amount_sxp/nft_count,3), round(tx_amount_eth/nft_count,6), round(tx_amount_total/nft_count,6),nft_id, nft_boxid, block_timestamp, status, note, tx_hash, nft_id,)
                        insert_query = format_query_for_db(insert_query)
                        logger.debug(f"insert_query: {insert_query} values: {values}")
                        await cursor.execute(insert_query, values)
                        if DB_ENGINE == "sqlite": cursor.connection.commit()
                        else: await cursor.connection.commit()
                        logger.debug(f"Insert wenda_nft_onchain mint success! nft_id: {nft_id} nft_boxid: {nft_boxid}")

            logger.info(f"all items update complete. to_block: {to_block}")

            with open(hash_file, "w", encoding="utf-8") as f:
                f.write(str(to_block))
            hash_index = to_block+1

            time.sleep(retry_interval)
        except Exception as e:
            logger.error(f"listen_contract_event error: {e} , Please wait 600 Seconds")
            time.sleep(600)
            continue

async def listen_events(chainid):
    global hash_file
    global hash_index

    web3_config = get_web3_config_by_chainid(chainid)
    logger.debug(f"web3_config: {web3_config}")
    if not web3_config:
        logger.error(f"STATUS: 400 ERROR: Web3 config not found - chainid: {chainid}")
        raise Exception("Web3 config not found")
    config_chainid = web3_config['chain_id'] # chain_id
    if not config_chainid:
        logger.error(f"STATUS: 400 ERROR: Web3 chain_id not found - chainid: {chainid}")
        raise Exception("Web3 chain_id not found")
    if config_chainid != chainid:
        logger.error(f"STATUS: 400 ERROR: Web3 chainid does not match - chainid: {chainid} != config_chainid: {config_chainid}")
        raise Exception("Web3 chainid does not match")

    hash_file=f"{config_chainid}_boxnft"

    web3_rpc_url = web3_config['server'] # rpc
    if not web3_rpc_url:
        raise Exception("Web3 rpc not found")
    web3_obj = Web3(Web3.HTTPProvider(web3_rpc_url))
    if chainid in [1, 11155111, 8453, 84532, 56, 97]:
        web3_obj.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    while not web3_is_connected_with_retry(web3_obj):
        logger.debug(f"Ooops! Failed to eth.is_connected. {web3_rpc_url}")
        time.sleep(5)

    current_block=0
    while current_block == 0:
        current_block = get_block_number_with_retry(web3_obj)
        time.sleep(5)
    print(f"current_block: {current_block}")

    if not os.path.exists(hash_file):
        with open(hash_file, "w", encoding="utf-8") as f:
            f.write(str(current_block))
        hash_index = current_block
    else:
        with open(hash_file, "r", encoding="utf-8") as f:
            hash_index_str = f.read()
        if not hash_index_str:
            hash_index = current_block
        else:
            hash_index = int(hash_index_str)
    print(f"hash_index: {hash_index}")
    
    if current_block - hash_index > 100:
        logger.error(f"Warning, block height difference. - clac_block: {current_block - hash_index}")

    await listen_events_start(web3_config, hash_index)


if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', type=bool, default=False, action=argparse.BooleanOptionalAction)
    parser.add_argument('-l', '--log', type=str, default="warn")
    parser.add_argument('-c', '--chainid', type=int, default=8453)
    args = parser.parse_args()
    run_debug = bool(args.debug)
    run_log = str(args.log.lower())
    run_chainid = int(args.chainid)

    # log level
    if run_debug:
        log_level = "DEBUG"
    else:
        if run_log == "debug":
            log_level = "DEBUG"
        elif run_log == "info":
            log_level = "INFO"
        elif run_log == "warn":
            log_level = "WARNING"
        elif run_log == "error":
            log_level = "ERROR"
        else:
            log_level = "WARNING"
    logger.remove()
    logger.add(sys.stdout, level=log_level)

    asyncio.run(listen_events(run_chainid))
