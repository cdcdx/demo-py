import os
import json
import time
import requests
from loguru import logger
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from utils.contract_abi import contract_abi_nftmint
from utils.i18n import get_text
from config import WEB3_NETWORK, WEB3_CONFIG, WEB3_WHITE_PRIKEY

# ---------------------------------------

def get_web3_config_by_chainid(chainid):
    assert chainid in [1, 11155111, 8453, 84532, 56, 97]
    web3_configs = json.loads(WEB3_CONFIG)
    for web3_client in web3_configs:
        if chainid>0:
            if web3_client['chain_id'] == chainid:
                return web3_client
        else:
            if web3_client['network'] == WEB3_NETWORK:
                return web3_client
    return web3_configs[0]

def get_web3_config_by_network(network=WEB3_NETWORK):
    assert network in ["Ethereum Mainnet", "Base Sepolia Testnet", "Base Mainnet", "BSC Testnet", "BSC Mainnet"]
    web3_configs: list = json.loads(WEB3_CONFIG)
    logger.info(f"get_web3_config_by_network: {network}")
    for web3_client in web3_configs:
        if web3_client['network'] == network:
            return web3_client
    return web3_configs[0]

# ---------------------------------------
# from urllib3.exceptions import InsecureRequestWarning
# import warnings
# warnings.simplefilter('ignore', InsecureRequestWarning) # 禁用警告

def make_request(hash_url, params):
    # print(f"hash_url: {hash_url} params: {params}")
    # response = requests.get(hash_url, params=params, verify=False)
    response = requests.get(hash_url, params=params, timeout=10)
    if response.status_code == 200:
        data = response.json()
        # print(f"data: {data}")
        return data

# ------------------------------------------------------------------------------------

# 发送交易（重试5次，每次2秒）
def send_transaction_with_retry(web3_obj, transaction, web3_prikey, max_retries=5, retry_interval=2):
    attempt = 0
    while attempt < max_retries:
        try:
            logger.info(f"transaction: {transaction}")
            # === 动态更新 Gas 参数 ===
            latest_block = web3_obj.eth.get_block('latest')
            base_fee = latest_block['baseFeePerGas']
            # 重试时增加优先费（每次增加10%）
            priority_fee = max(web3_obj.eth.max_priority_fee, 100)  # 设置下限
            if attempt > 0:
                priority_fee = int(priority_fee * (1 + 0.1 * attempt))  # 递增
            max_fee = base_fee + priority_fee
            # 更新交易参数
            transaction.update({
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
            })
            logger.info(f"update transaction 1: {transaction}")
            
            # === 估算 Gas ===
            try:
                gas_limit = web3_obj.eth.estimate_gas(transaction)
            except Exception as e:
                logger.error(f"Failed to eth.estimate_gas: {str(e)}")
                gas_limit = 200000
            logger.info(f"gas_limit: {gas_limit}")
            transaction["gas"] = gas_limit
            logger.info(f"update transaction 2: {transaction}")
            
            # 使用私钥签名交易
            signed_transaction = web3_obj.eth.account.sign_transaction(transaction, web3_prikey)
            logger.debug(f"signed_transaction: {signed_transaction}")
            # 发送交易
            try:
                # 发送交易
                if str(signed_transaction).find("raw_transaction") > 0:
                    tx_hash = web3_obj.eth.send_raw_transaction(signed_transaction.raw_transaction)
                elif str(signed_transaction).find("signed_transaction") > 0:
                    tx_hash = web3_obj.eth.send_raw_transaction(signed_transaction.raw_transaction)
                logger.info(f"交易已发送 tx_hash: {tx_hash.hex()}")
                # 等待交易完成
                receipt = web3_obj.eth.wait_for_transaction_receipt(tx_hash)
                logger.info(f"等待交易完成 receipt: {receipt}")
                tx_bytes = f"0x{tx_hash.hex()}"
                
                if receipt['status'] == 1:
                    logger.info(f"交易成功，哈希：{tx_bytes}")
                    return True, {"tx_hash": tx_bytes}
                else:
                    logger.error(f"交易失败，哈希：{tx_bytes}")
                    return False, {"tx_hash": tx_bytes}
            except ValueError as e:
                logger.info(f"Failed to transfer ValueError ETH : {str(e)}")
                try:
                    if e.args[0].get('message') in 'intrinsic gas too low':
                        result = False, {"tx_hash": tx_bytes, "msg": e.args[0].get('message')}
                    else:
                        result = False, {"tx_hash": tx_bytes, "msg": e.args[0].get('message'), "code": e.args[0].get('code')}
                except Exception as e:
                    result = False, {"tx_hash": tx_bytes, "msg": str(e)}
                return result
        except Exception as e:
            error_msg = str(e)
            if "replacement transaction underpriced" in error_msg:
                logger.warning(f"优先费不足，将增加... (尝试 {attempt+1})")
            elif "max fee per gas" in error_msg:
                logger.warning(f"基础费不足，将更新... (尝试 {attempt+1})")
            else:
                logger.error(f"Failed to send transaction: {e} (尝试 {attempt+1})")
            
            attempt += 1
            if attempt < max_retries:
                logger.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Max retries reached. Failed to eth.send_raw_transaction: {str(e)}")
                return False, {"tx_hash": "send_raw_transaction", "msg": str(e)}
# 添加新的公共函数
def build_base_transaction(web3_obj, sender_address, config_chainid):
    """
    构建基础交易参数
    """
    # 获取上个区块Gas
    latest_block = web3_obj.eth.get_block('latest')
    base_fee_per_gas = latest_block['baseFeePerGas']
    priority_fee_per_gas = web3_obj.eth.max_priority_fee  # 获取推荐的小费
    max_fee_per_gas = int(base_fee_per_gas * 1.1) + priority_fee_per_gas  # 增加缓冲
    
    logger.debug(f"Base Fee Per Gas: {base_fee_per_gas} wei")
    logger.debug(f"Max Priority Fee Per Gas: {priority_fee_per_gas} wei")
    logger.debug(f"Max Fee Per Gas: {max_fee_per_gas} wei")
    
    return {
        "chainId": config_chainid,
        "from": sender_address,
        # "nonce": web3_obj.eth.get_transaction_count(sender_address),
        "nonce": web3_obj.eth.get_transaction_count(sender_address, 'pending'),
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": priority_fee_per_gas,
        # "gas": base_fee_per_gas * priority_fee_per_gas,
        # "gas": 20000000,  # 最大 Gas 用量
    }
# 获取web3客户端和配置
def get_web3_client(chainid):
    """获取web3客户端和配置"""
    web3_config = get_web3_config_by_chainid(chainid)
    # logger.debug(f"web3_config: {web3_config}")
    if not web3_config:
        logger.error(f"STATUS: 400 ERROR: Web3 config not found - chainid: {chainid}")
        raise Exception("Web3 config not found")
    config_chainid = web3_config['chain_id']
    if not config_chainid:
        logger.error(f"STATUS: 400 ERROR: Web3 chain_id not found - chainid: {chainid}")
        raise Exception("Web3 chain_id not found")
    if config_chainid != chainid:
        logger.error(f"STATUS: 400 ERROR: Web3 chainid does not match - chainid: {chainid} != config_chainid: {config_chainid}")
        raise Exception("Web3 chainid does not match")
    
    # web3
    web3_rpc_url = web3_config['server'] # rpc
    if not web3_rpc_url:
        raise Exception("Web3 rpc not found")
    web3_obj = Web3(Web3.HTTPProvider(web3_rpc_url))
    if chainid in [56, 97]:
        web3_obj.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    # 连接rpc节点
    if not web3_obj.is_connected():
        logger.error(f"Unable to connect to the network: {web3_rpc_url}")
        web3_rpc_url = web3_config['rpc']
        web3_obj = Web3(Web3.HTTPProvider(web3_rpc_url))
        if chainid in [56, 97]:
            web3_obj.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        if not web3_obj.is_connected():
            raise Exception(f"Unable to connect to the network: {web3_rpc_url}")
    
    return web3_obj, web3_config

# 解析 Solidity 合约的 revert 错误信息
def decode_revert_reason(hex_error):
    try:
        # 移除 '0x' 前缀
        if hex_error.startswith('0x'):
            hex_error = hex_error[2:]
            
        # 检查是否是标准的 Error(string) 选择器
        if hex_error.startswith('08c379a0'):
            # 跳过选择器 (4 bytes = 8 hex chars)
            data = hex_error[8:]
            
            # 获取字符串长度 (offset 32 bytes = 64 hex chars)
            length_hex = data[64:128]
            length = int(length_hex, 16)
            
            # 获取实际的错误消息 (从 128 hex chars 开始)
            message_hex = data[128:128 + length*2]
            message = bytes.fromhex(message_hex).decode('utf-8')
            
            return message
        else:
            # 如果不是标准格式，返回原始错误
            return hex_error
            
    except Exception as e:
        return f"无法解析错误信息: {str(e)}"

# ------------------------------------------------------------------------------------

# 铸造NFT
def contract_nftmint(receiver_address, chainid=0):
    tx_bytes = None
    try:
        # 校验chainid
        if chainid not in [1, 11155111, 8453, 84532, 56, 97]:
            raise Exception("chainid not found")
        
        web3_obj, web3_config = get_web3_client(chainid)
        config_chainid = web3_config['chain_id']
        
        # 获取发送者信息
        sender_address = web3_obj.eth.account.from_key(WEB3_WHITE_PRIKEY).address
        logger.debug(f"白名单地址: {sender_address}")
        sender_balance = web3_obj.eth.get_balance(sender_address)
        logger.debug(f"白名单余额: {web3_obj.from_wei(sender_balance, 'ether')} ETH")
        
        if sender_balance <= 0.0005:
            logger.error(f"Insufficient balance - sender_balance: {sender_balance} ETH")
            return False, {"tx_hash": tx_bytes, "msg": 'Insufficient balance'}

        # 获取合约信息 - nftmint
        nftmint_address = web3_config['nftmint']
        if not (len(nftmint_address) == 42 and nftmint_address.startswith('0x')):
            logger.error(f"Invalid contract address - {nftmint_address}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_ADDRESS')}
        nftmint_contract_address = Web3.to_checksum_address(nftmint_address)
        nftmint_contract = web3_obj.eth.contract(address=nftmint_contract_address, abi=contract_abi_nftmint)

        # 交易参数
        receiver_address = Web3.to_checksum_address(receiver_address)
        cool_address = web3_obj.to_checksum_address(web3_config['cool_address'])
        logger.info(f"给谁铸造: {receiver_address} 上级是谁: {cool_address}")

        ## 是否存在NFT
        is_nft = nftmint_contract.functions.userPurchases( receiver_address ).call()
        logger.debug(f"is_nft: {is_nft}")
        if is_nft > 0:
            logger.error(f"STATUS: 400 ERROR: The address already has NFT - {receiver_address}")
            return False, {"tx_hash": "", "msg": "The address already has NFT"}

        # 使用公共函数构建基础交易参数
        base_transaction = build_base_transaction(web3_obj, sender_address, config_chainid)
        # 构建交易 - 铸造NFT
        transaction = nftmint_contract.functions.genesis(receiver_address, cool_address).build_transaction(base_transaction)
        logger.debug(f"transaction: {transaction}")

        # 发送交易
        tx_success, tx_msg = send_transaction_with_retry(web3_obj, transaction, WEB3_WHITE_PRIKEY)
        logger.debug(f"tx_success: {tx_success}, tx_msg: {tx_msg}")
        if tx_success == False:
            logger.error(f"Ooops! Failed to send_transaction.")
            return False, {"tx_hash": "", "msg": tx_msg['msg']}
        # logger.success(f"The genesis transaction was send successfully! - transaction: {transaction}")
        logger.success(f"genesis successfully - to: {receiver_address} root: {cool_address}")

        return True, {"tx_hash": tx_msg['tx_hash'], "msg": ""}
    except Exception as e:
        logger.error(f"Failed to mintnft ETH: {str(e)}")
        # return False, {"tx_hash": tx_bytes, "msg": str(e)}
        decoded_error = decode_revert_reason(str(e)) if '0x' in str(e) else str(e)
        return False, {"tx_hash": tx_bytes, "msg": decoded_error}

# ------------------------------------------------------------------------------------
