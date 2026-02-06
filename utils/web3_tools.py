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
    logger.info(f"connect to the network: {web3_rpc_url}")
    return web3_obj, web3_config

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

# 检测钱包是否存在卡住的交易
def check_stuck_transactions(web3_obj, wallet_address):
    """
    检测钱包是否存在卡住的交易
    :param web3_obj: Web3实例
    :param wallet_address: 钱包地址
    :return: 是否存在卡住的交易, pending nonce, latest nonce
    """
    pending_nonce = web3_obj.eth.get_transaction_count(wallet_address, 'pending')
    latest_nonce = web3_obj.eth.get_transaction_count(wallet_address, 'latest')
    stuck_transactions_count = pending_nonce - latest_nonce
    
    logger.debug(f"钱包地址: {wallet_address} Pending: {pending_nonce} Latest: {latest_nonce} Stuck: {stuck_transactions_count}")
    
    if stuck_transactions_count > 0:
        logger.error(f"发现卡住的交易 stuck:{stuck_transactions_count}")
        return True, pending_nonce, latest_nonce
    else:
        logger.debug(f"无卡住的交易")
        return False, pending_nonce, latest_nonce

# 撤销卡住的交易
def cancel_stuck_transactions(web3_obj, sender_address, sender_private_key, target_nonce=None):
    """
    撤销卡住的交易
    :param web3_obj: Web3实例
    :param sender_address: 发送者地址
    :param sender_private_key: 发送者私钥
    :param target_nonce: 目标nonce，默认为最新的pending nonce
    :return: 交易结果
    """
    try:
        # 获取pending nonce和latest nonce
        pending_nonce = web3_obj.eth.get_transaction_count(sender_address, 'pending')
        latest_nonce = web3_obj.eth.get_transaction_count(sender_address, 'latest')
        
        if target_nonce is None:
            # 如果没有指定目标nonce，则使用pending nonce
            target_nonce = pending_nonce - 1 if pending_nonce > latest_nonce else latest_nonce
        
        logger.info(f"准备撤销卡住的交易 - 地址: {sender_address}, 目标nonce: {target_nonce}, pending_nonce: {pending_nonce}, latest_nonce: {latest_nonce}")
        
        # 检查目标nonce是否确实卡住了
        if target_nonce < latest_nonce:
            logger.warning(f"目标nonce ({target_nonce}) 小于最新nonce ({latest_nonce}), 无法撤销该nonce的交易")
            return False, {"msg": f"Target nonce {target_nonce} is less than latest nonce {latest_nonce}, cannot cancel"}
        
        # 创建一个简单的自交易（发送到自己的地址），使用高gas价格
        transaction = {
            "from": sender_address,
            "to": sender_address,  # 发送到自己
            "value": 0,  # 不转移资金
            "nonce": target_nonce,
            "gas": 21000,  # 标准转账gas
            "maxFeePerGas": web3_obj.eth.max_priority_fee * 2 + web3_obj.eth.get_block('latest')['baseFeePerGas'] * 2,  # 使用更高gas价格
            "maxPriorityFeePerGas": web3_obj.eth.max_priority_fee * 2,  # 高优先费
            "chainId": web3_obj.eth.chain_id
        }
        
        logger.info(f"创建撤销交易 - nonce: {transaction['nonce']}, gasPrice: {transaction['maxFeePerGas']}")
        
        # 签名并发送交易
        signed_txn = web3_obj.eth.account.sign_transaction(transaction, sender_private_key)
        tx_hash = web3_obj.eth.send_raw_transaction(signed_txn.raw_transaction)
        
        logger.info(f"撤销交易已发送 - tx_hash: 0x{tx_hash.hex()}, nonce: {target_nonce}")
        
        # 等待交易确认
        receipt = web3_obj.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        if receipt['status'] == 1:
            logger.info(f"撤销交易成功 - tx_hash: 0x{tx_hash.hex()}")
            return True, {"tx_hash": f"0x{tx_hash.hex()}"}
        else:
            logger.error(f"撤销交易失败 - tx_hash: 0x{tx_hash.hex()}")
            return False, {"tx_hash": f"0x{tx_hash.hex()}", "msg": "Cancellation transaction failed"}
            
    except Exception as e:
        logger.error(f"撤销卡住交易时出错: {str(e)}")
        return False, {"msg": str(e)}

# 批量撤销卡住的交易
def cancel_all_stuck_transactions(web3_obj, sender_address, sender_private_key):
    """
    批量撤销所有卡住的交易
    :param web3_obj: Web3实例
    :param sender_address: 发送者地址
    :param sender_private_key: 发送者私钥
    :return: 结果列表
    """
    try:
        pending_nonce = web3_obj.eth.get_transaction_count(sender_address, 'pending')
        latest_nonce = web3_obj.eth.get_transaction_count(sender_address, 'latest')
        
        logger.info(f"检查卡住的交易 - 地址: {sender_address}, pending_nonce: {pending_nonce}, latest_nonce: {latest_nonce}")
        
        results = []
        if pending_nonce > latest_nonce:
            logger.info(f"发现 {pending_nonce - latest_nonce} 个卡住的交易，开始批量撤销...")
            
            for nonce in range(latest_nonce, pending_nonce):
                logger.info(f"正在撤销 nonce {nonce} 的卡住交易...")
                success, result = cancel_stuck_transactions(web3_obj, sender_address, sender_private_key, nonce)
                results.append({"nonce": nonce, "success": success, "result": result})
                
                if success:
                    logger.info(f"Nonce {nonce} 撤销成功")
                else:
                    logger.error(f"Nonce {nonce} 撤销失败: {result}")
                
                # 短暂延时，避免nonce冲突
                time.sleep(2)
        else:
            logger.info("没有发现卡住的交易")
        
        return results
        
    except Exception as e:
        logger.error(f"批量撤销卡住交易时出错: {str(e)}")
        return [{"error": str(e)}]

# 发送交易（重试3次，每次延迟2秒）
def send_transaction_with_retry(web3_obj, transaction, web3_prikey, max_retries=3, retry_interval=2):
    
    # 检查是否存在卡住的交易
    sender_address = transaction["from"]
    is_stuck, pending_nonce, latest_nonce = check_stuck_transactions(web3_obj, sender_address)
    if is_stuck:
        logger.error(f"链上有{pending_nonce-latest_nonce}条未完成交易，本次交易终止。")
        return False, {"tx_hash": "exist_stuck_tx", "msg": "There are incomplete transactions on the blockchain, therefore this transaction is terminated."}
    
    attempt = 0
    receipt_timeout = 120  # 默认超时时间
    while attempt < max_retries:
        try:
            logger.debug(f"transaction: {transaction}")
            
            # === 动态更新 Gas 参数 ===
            latest_block = web3_obj.eth.get_block('latest')
            current_base_fee = latest_block['baseFeePerGas']
            # 增加基础费(每次增加10%)
            base_fee = int(current_base_fee * (1 + 0.1 * attempt))
            
            # 增加优先费(每次增加10%)
            current_priority_fee = web3_obj.eth.max_priority_fee
            priority_fee = int(current_priority_fee * (1 + 0.1 * attempt))
            # 设置优先费下限
            priority_fee = max(priority_fee, 100)
            # 计算最大费用
            max_fee = base_fee + priority_fee
            # 更新交易参数
            transaction.update({
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
            })
            logger.debug(f"update transaction fees | base_fee: {base_fee} + priority_fee: {priority_fee} = max_fee: {max_fee}")
            
            # === 估算 Gas ===
            try:
                gas_limit = web3_obj.eth.estimate_gas(transaction)
            except Exception as e:
                logger.error(f"Failed to eth.estimate_gas: {str(e)}")
                gas_limit = 200000
            logger.debug(f"gas_limit: {gas_limit}")
            # 增加gas费(增加10%)
            gas_limit = int(gas_limit * 1.05)
            # 更新交易参数
            transaction["gas"] = gas_limit
            logger.debug(f"update transaction fees | gas_limit: {gas_limit}")
            
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
                logger.info(f"交易已发送 tx_hash: 0x{tx_hash.hex()}")
                
                # 等待交易完成
                receipt = web3_obj.eth.wait_for_transaction_receipt(tx_hash, timeout=receipt_timeout)
                logger.debug(f"等待交易完成 receipt: {receipt}")
                tx_bytes = f"0x{tx_hash.hex()}"
                
                if receipt['status'] == 1:
                    logger.info(f"交易成功 tx_bytes: {tx_bytes}")
                    return True, {"tx_hash": tx_bytes}
                else:
                    logger.error(f"交易失败 tx_bytes: {tx_bytes}")
                    return False, {"tx_hash": tx_bytes}
            except ValueError as e:
                logger.error(f"Failed to transfer ETH ValueError: {str(e)}")
                try:
                    tx_bytes = f"0x{tx_hash.hex()}" if tx_hash is not None else "unknown"
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

# 解析 Solidity 合约的 revert 错误信息
def decode_revert_reason(input_data):
    """
    解析 Solidity 合约的 revert 错误信息，支持多种输入格式
    
    Args:
        input_data: 可能是16进制错误信息、包含错误信息的字符串或元组
            
    Returns:
        str: 解码后的错误信息
    """
    try:
        # 处理不同类型的输入数据
        hex_error = None
        
        if isinstance(input_data, tuple):
            # 如果输入是元组，从中提取十六进制错误信息
            for item in input_data:
                if isinstance(item, str) and item.startswith('0x') and len(item) > 10:
                    hex_error = item
                    break
        elif isinstance(input_data, str):
            if input_data.startswith('0x'):
                # 直接是十六进制字符串
                hex_error = input_data
            else:
                # 字符串中可能包含十六进制错误信息
                # 查找其中的十六进制错误信息
                parts = input_data.split()
                for part in parts:
                    if part.startswith('0x') and len(part) > 10:
                        # 检查是否包含错误选择器
                        check_str = part[2:] if part.startswith('0x') else part
                        if len(check_str) >= 8 and (check_str.startswith('08c379a0') or check_str.startswith('4e487b71')):
                            hex_error = part
                            break
        else:
            # 其他类型转换为字符串处理
            input_str = str(input_data)
            if '0x' in input_str:
                # 从字符串中提取十六进制错误信息
                import re
                hex_matches = re.findall(r'0x[a-fA-F0-9]+', input_str)
                for match in hex_matches:
                    check_str = match[2:]
                    if len(check_str) >= 8 and (check_str.startswith('08c379a0') or check_str.startswith('4e487b71')):
                        hex_error = match
                        break
        
        if not hex_error:
            return str(input_data)  # 如果没找到十六进制错误信息，返回原始数据
        
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
        # 检查是否是 Panic(uint256) 选择器
        elif hex_error.startswith('4e487b71'):
            # 跳过选择器 (4 bytes = 8 hex chars)
            data = hex_error[8:]
            
            # Panic code 位于接下来的32字节
            panic_code_hex = data[:64]
            panic_code = int(panic_code_hex, 16)
            
            # Panic codes mapping
            panic_codes = {
                0x00: "Generic compiler inserted panics",
                0x01: "Assert with an argument that evaluates to false",
                0x11: "Arithmetic operation results in underflow or overflow outside of an unchecked block",
                0x12: "Division or modulo by zero",
                0x21: "Attempt to convert to an invalid type",
                0x22: "Access to a storage byte array that is incorrectly encoded",
                0x31: ".pop() on an empty array",
                0x32: "Array index is out of bounds",
                0x41: "Too much memory was allocated, or an array was created that is too large",
                0x51: "Call to zero-initialized variable of internal function type"
            }
            
            panic_desc = panic_codes.get(panic_code, f"Unknown panic code: {panic_code}")
            return f"Panic({panic_code}): {panic_desc}"
        # 检查是否有其他常见的错误选择器模式
        elif len(hex_error) >= 8:
            # 提取前4个字节（8个十六进制字符）作为函数选择器
            selector = hex_error[:8]
            
            # 自定义错误可能有不同的选择器，尝试通用解析
            if len(hex_error) > 8:
                try:
                    # 尝试按照标准错误格式解析
                    data = hex_error[8:]
                    
                    # 如果数据长度足够，尝试解析
                    if len(data) >= 64:
                        # 获取第一个参数的偏移量（通常在第4-7个字节，即8-15个hex字符位置）
                        offset_hex = data[64:128]  # offset in word 2
                        offset = int(offset_hex, 16) * 2  # Convert word offset to hex char offset
                        
                        # 获取字符串长度（在偏移位置后的32字节）
                        start_pos = 128  # Start after the first two words
                        length_hex = data[start_pos:start_pos+64]
                        
                        if length_hex:  # Check if we have length data
                            length = int(length_hex, 16)
                            
                            # Get the actual error message (after length field)
                            message_start = start_pos + 64
                            message_hex = data[message_start:message_start + length*2]
                            
                            if message_hex:
                                try:
                                    message = bytes.fromhex(message_hex).decode('utf-8')
                                    return message
                                except UnicodeDecodeError:
                                    pass
                except Exception:
                    pass
        
        # 如果以上都不匹配，尝试简单的十六进制转字符串
        try:
            # Remove common prefixes and try direct decoding
            clean_hex = hex_error.replace('0x', '')
            if len(clean_hex) % 2 == 0:
                message = bytes.fromhex(clean_hex).decode('utf-8', errors='ignore').strip('\x00')
                if message:
                    return message
        except Exception:
            pass
        
        # 如果所有尝试都失败，返回原始错误信息
        return str(input_data)
    except Exception as e:
        return f"Unable to decode error: {str(e)}"

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
        sender_nonce = web3_obj.eth.get_transaction_count(sender_address, 'pending')
        logger.info(f"给谁铸造: {receiver_address} 上级是谁: {cool_address} nonce: {sender_nonce}")

        ## 是否存在NFT
        is_nft = nftmint_contract.functions.userPurchases( receiver_address ).call()
        logger.debug(f"is_nft: {is_nft}")
        if is_nft > 0:
            logger.error(f"STATUS: 400 ERROR: NFT already minted - {receiver_address}")
            return False, {"tx_hash": "", "msg": "NFT already minted"}

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
