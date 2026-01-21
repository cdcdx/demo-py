import re
import math
import time
import json
import random
import datetime
from datetime import datetime as dt
from typing import Dict
from fastapi import APIRouter, Depends, BackgroundTasks
from pydantic import BaseModel, EmailStr, Field
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from config import APP_CONFIG, KAFKA_CONFIG, JWT_CONFIG, DB_ENGINE
from utils.bearertoken import md58, create_access_token, decode_access_token
from utils.cache import get_redis_data, set_redis_data, del_redis_data
from utils.db import get_db, format_query_for_db, convert_row_to_dict, format_datetime_fields
from utils.email import send_normal_mail, send_activation_mail, send_reset_mail, send_newpasswd_mail
from utils.i18n import get_text
from utils.local import floor_decimal, generate_registercode, generate_referralcode, validate_email_format
from utils.kafka.produce import kafka_send_produce
from utils.security import get_interface_userid
from utils.web3_tools import make_request, get_web3_config_by_chainid, get_web3_config_by_network
from utils.log import log as logger

router = APIRouter()


## admin

# --------------------------------------------------------------------------------------------------

@router.get("/monitor/info")
async def admin_monitor_info(userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """获取系统信息（管理员权限）"""
    logger.info(f"GET /api/admin/monitor/info")
    if cursor is None:
        logger.error(f"/api/admin/monitor/info cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PERMISSIONS')}

        today = time.strftime("%Y-%m-%d", time.localtime())
        logger.debug(f"today: {today}")

        # 注册用户数
        check_query = "SELECT count(*) as len FROM wenda_users WHERE id>0"
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query}")
        await cursor.execute(check_query)
        all_info = await cursor.fetchone()
        # logger.debug(f"all_info: {all_info}")
        all_info = convert_row_to_dict(all_info, cursor.description)  # 转换字典
        logger.debug(f"all_info: {all_info}")
        if all_info is None:
            total_users = 0
        else:
            total_users = all_info['len']

        # KOL数
        check_query = "SELECT count(*) as len FROM wenda_users WHERE id>0 and LENGTH(username)>0"
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query}")
        await cursor.execute(check_query)
        kol_info = await cursor.fetchone()
        # logger.debug(f"kol_info: {kol_info}")
        kol_info = convert_row_to_dict(kol_info, cursor.description)  # 转换字典
        logger.debug(f"kol_info: {kol_info}")
        if kol_info is None:
            total_kols = 0
        else:
            total_kols = kol_info['len']

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": {
                "total_users": total_users,
                "total_kols": total_kols,
            },
        }
    except Exception as e:
        logger.error(f"/api/admin/monitor/info except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


@router.get("/registercode/generate")
async def admin_registercode_generate(userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """生成注册码（管理员权限）"""
    logger.info(f"GET /api/admin/registercode/generate")
    if cursor is None:
        logger.error(f"/api/admin/registercode/generate cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PERMISSIONS')}

        today = time.strftime("%Y-%m-%d", time.localtime())
        logger.debug(f"today: {today}")

        ## register_code 注册码生成逻辑
        register_code = None
        retry_count = 0
        while retry_count < 5:
            register_code = generate_registercode(userid)
            # Check if the register_code already exists
            check_query = "SELECT id FROM wenda_users WHERE register_code=%s"
            values = (register_code,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_user = await cursor.fetchone()
            logger.debug(f"existing_user: {existing_user}")
            if existing_user is None:
                break
            retry_count += 1
        else:
            logger.error("Failed to generate a registercode after maximum retries.")
            return {"code": 401, "success": False, "msg": "registercode generation failed"}

        ## register_code 注册码入库
        insert_query = "INSERT INTO wenda_users (register_code) VALUES (%s)"
        values = (register_code,)
        insert_query = format_query_for_db(insert_query)
        logger.debug(f"insert_query: {insert_query} values: {values}")
        await cursor.execute(insert_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()
        logger.success(f"register_code: {register_code}")
        
        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": {
                "code": register_code,
                'url': f'{APP_CONFIG["appadmin"]}/register?code={register_code}'
            },
        }
    except Exception as e:
        logger.error(f"/api/admin/registercode/generate except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


@router.get("/registercode/list")
async def admin_registercode_list(page: int | None = 1, limit: int | None = 10, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """未使用的注册码列表（管理员权限）"""
    logger.info(f"GET /api/admin/registercode/list")
    if cursor is None:
        logger.error(f"/api/admin/registercode/list cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PERMISSIONS')}

        today = time.strftime("%Y-%m-%d", time.localtime())
        logger.debug(f"today: {today}")

        # register count
        check_query = "SELECT count(*) as len FROM wenda_users WHERE userid=''"
        logger.debug(f"check_query: {check_query}")
        await cursor.execute(check_query)
        all_info = await cursor.fetchone()
        # logger.debug(f"all_info: {all_info}")
        all_info = convert_row_to_dict(all_info, cursor.description)  # 转换字典
        logger.debug(f"all_info: {all_info}")
        if all_info is None:
            register_count = 0
        else:
            register_count = all_info['len']

        if register_count == 0:
            return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": [],
                "total": 0,
            }
        
        if page == 0: page = 1
        ## register_code 注册码列表
        check_query = "SELECT id,register_code as code,created_time as created FROM wenda_users WHERE userid='' ORDER BY id DESC LIMIT %s, %s"
        values = (limit * (page - 1), limit,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        register_list = await cursor.fetchall()
        # logger.debug(f"register_list: {register_list}")
        if isinstance(register_list, (list, tuple)) and len(register_list) > 0: # 转换字典列表
            converted_list = []
            for row in register_list: # 将每一行转换为字典
                row_dict = convert_row_to_dict(row, cursor.description)  # 转换字典
                formatted_row = format_datetime_fields(row_dict)  # DATETIME转字符串
                converted_list.append(formatted_row)
            register_list = converted_list
        logger.debug(f"register_list: {register_list}")
        
        # 添加url
        if register_list:
            for register_one in register_list:
                register_one['url'] = f'{APP_CONFIG["appadmin"]}/register?code={register_one["code"]}'
        
        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": register_list if register_list else [],
            "total": register_count if register_list else 0,
        }
    except Exception as e:
        logger.error(f"/api/admin/registercode/list except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


@router.get("/registercode/history")
async def admin_registercode_history(page: int | None = 1, limit: int | None = 10, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """注册码生成记录（管理员权限）"""
    logger.info(f"GET /api/admin/registercode/history")
    if cursor is None:
        logger.error(f"/api/admin/registercode/history cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PERMISSIONS')}

        # register count
        check_query = "SELECT count(*) as len FROM wenda_users WHERE id>0"
        logger.debug(f"check_query: {check_query}")
        await cursor.execute(check_query)
        all_info = await cursor.fetchone()
        # logger.debug(f"all_info: {all_info}")
        all_info = convert_row_to_dict(all_info, cursor.description)  # 转换字典
        logger.debug(f"all_info: {all_info}")
        if all_info is None:
            register_count = 0
        else:
            register_count = all_info['len']

        if register_count == 0:
            return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": [],
                "total": 0,
            }
        
        # register list
        if page == 0: page = 1
        ## register_code 注册码列表
        check_query = "SELECT id,register_code as code,userid,username,email,created_time as created FROM wenda_users WHERE id>0 ORDER BY id DESC LIMIT %s, %s"
        values = (limit * (page - 1), limit,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        register_list = await cursor.fetchall()
        # logger.debug(f"register_list: {register_list}")
        if isinstance(register_list, (list, tuple)) and len(register_list) > 0: # 转换字典列表
            converted_list = []
            for row in register_list: # 将每一行转换为字典
                row_dict = convert_row_to_dict(row, cursor.description)  # 转换字典
                formatted_row = format_datetime_fields(row_dict)  # DATETIME转字符串
                converted_list.append(formatted_row)
            register_list = converted_list
        logger.debug(f"register_list: {register_list}")

        # 添加url
        if register_list:
            for register_one in register_list:
                register_one['url'] = f'{APP_CONFIG["appadmin"]}/register?code={register_one["code"]}'
        
        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": register_list if register_list else [],
            "total": register_count if register_list else 0,
        }
    except Exception as e:
        logger.error(f"/api/admin/registercode/history except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# --------------------------------------------------------------------------------------------------

class TxhashRequest(BaseModel):
    tx_chainid: int = Field(default=8453, description="inviter chainid")
    tx_hash: str = Field(..., description="inviter hash")
@router.post("/airdrop/nft")
async def admin_airdrop_nft_hash(post_request: TxhashRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """增加购买NFT (需要管理员权限)"""
    logger.info(f"POST /api/admin/airdrop/nft - {userid}")
    if cursor is None:
        logger.error(f"/api/admin/airdrop/nft - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": "Server error"}
    
    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": "Invalid permissions"}
    
        # action_log airdrop/nft
        log_message = f"{datetime.datetime.now()} - User {userid} action: Check nft hash - tx_chainid: {post_request.tx_chainid} tx_hash: {post_request.tx_hash}\n"
        with open('action_log.txt', 'a') as log_file:
            log_file.write(log_message)

        tx_hash = post_request.tx_hash
        if len(tx_hash) != 66 and len(tx_hash) != 64:
            logger.error(f"STATUS: 400 ERROR: Invalid tx_hash - {tx_hash}")
            return {"code": 400, "success": False, "msg": "Invalid tx_hash"}
            raise Exception("Invalid tx_hash")

        current_timestamp = int(time.time())
        logger.debug(f"current_timestamp: {current_timestamp}")

        # 校验chainid
        chainid = post_request.tx_chainid
        if chainid not in [1, 11155111, 8453, 84532, 56, 97]:
            logger.error(f"STATUS: 400 ERROR: Invalid chainid - {chainid}")
            return {"code": 400, "success": False, "msg": "Invalid chainid"}

        web3_config = get_web3_config_by_chainid(chainid)
        # logger.debug(f"web3_config: {web3_config}")
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

        nft_address = web3_config['nft'].lower() # NFT合约地址
        nftmint_address = web3_config['nftmint'].lower() # NFT购买合约地址
        logger.info(f"nftmint_address: {nftmint_address} config_chainid: {config_chainid}")

        ## 屏蔽用户多次请求 60
        redis_pending = await get_redis_data(f"pending:{tx_hash}:admin:mintnft")
        if redis_pending:
            return {"code": 400, "success": False, "msg": "Too frequent operation"}
        await set_redis_data(f"pending:{tx_hash}:admin:mintnft", value=1, ex=60)

        # 查询 boxnft 是否入库
        check_query = "SELECT id FROM wenda_nft_onchain WHERE tx_hash=%s and status=1"
        values = (tx_hash.lower())
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        boxnft_info = await cursor.fetchone()
        logger.debug(f"mysql boxnft_info: {boxnft_info}")
        boxnft_info = convert_row_to_dict(boxnft_info, cursor.description)  # 转换字典
        logger.debug(f"mysql boxnft_info: {boxnft_info}")
        if boxnft_info:
            logger.success(f"Already stored. tx_hash: {tx_hash}")
            return {"code": 200, "success": False, "msg": "Already stored"}

        # web3
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
        
        logger.info(f"tx_hash: {tx_hash}")

        ## 从哈希解析交易详情
        tx_receipt = web3_obj.eth.get_transaction_receipt(tx_hash)
        if tx_receipt is None:
            logger.error(f"Ooops! Transaction not found | tx_hash: {tx_hash}")
            return {"code": 400, "success": False, "msg": f"Ooops! Transaction not found | tx_hash: {tx_hash}"}
        else:
            logger.debug(f"transaction is {tx_receipt}")
            if tx_receipt['status'] != 1:
                logger.error(f"Ooops! Transaction failed | tx: {tx_receipt['from']}")
                return {"code": 400, "success": False, "msg": f"Ooops! Transaction failed | tx: {tx_receipt['from']}"}
            if len(tx_receipt.get('logs')) == 0:
                logger.error(f"Ooops! tx_receipt logs not found | tx_hash: {tx_hash}")
                return {"code": 400, "success": False, "msg": "tx_receipt logs not found"}

        # tx_from = tx_receipt['from'].lower()
        # tx_to = tx_receipt['to'].lower()
        # logger.debug(f"tx_from: {tx_from} tx_to: {tx_to}")
        # if tx_to != nftmint_address.lower():
        #     logger.error(f"Ooops! nftmint_address not found | tx_hash: {tx_hash}")
        #     return {"code": 400, "success": False, "msg": f"Ooops! nftmint_address not found | tx_hash: {tx_hash}"}
        # logger.debug(f"tx_from: {tx_from} tx_to: {tx_to}")

        # nftmint_data = Web3.to_hex(tx_receipt.get('logs')[-1].get('data'))
        nft_buyer = '0x0000000000000000000000000000000000000000'
        nft_superior = '0x0000000000000000000000000000000000000000'
        nftmint_data = None
        tx_logs = tx_receipt['logs']
        logger.debug(f"len_tx_logs: {len(tx_logs)} tx_logs: {tx_logs}")
        for tx_log in tx_logs:
            # 解析交易合约地址
            tx_to = tx_log.get('address').lower()
            if tx_to != nftmint_address.lower() and tx_to != nft_address.lower():
                continue
            logger.debug(f"tx_to: {tx_to}")
            
            # 解析合约函数签名
            tx_topics = tx_log.get('topics')
            if not tx_topics or len(tx_topics) < 1:
                continue
            logger.debug(f"tx_topics: {tx_topics}")
            
            # 检查事件签名
            topic0 = tx_topics[0]
            event_signature = Web3.to_hex(topic0) if hasattr(topic0, 'hex') else str(topic0)
            logger.debug(f"event_signature: {event_signature}")
            if event_signature == '0xbe79f3b0386b1835b02ea939662d87f12d01f11fc05cc6af94e2a6988a9e28d4': # nftmint Transfer
                # 正确处理HexBytes对象
                nft_buyer_bytes = tx_topics[1][-20:]  # 以太坊地址是20字节
                nft_superior_bytes = tx_topics[2][-20:]    # 以太坊地址是20字节
                # 将HexBytes转换为地址
                nft_buyer = "0x" + nft_buyer_bytes.hex()[-40:].lower()
                nft_superior = "0x" + nft_superior_bytes.hex()[-40:].lower()
                logger.debug(f"nft_superior: {nft_superior} => nft_buyer: {nft_buyer}")
                
                nftmint_data = Web3.to_hex(tx_log.get('data'))
                logger.debug(f"Found nftmint_data: {nftmint_data}")
            else:
                continue
        # 检查是否找到了有效的 nftmint_data
        if nftmint_data is None:
            logger.error(f"No valid nftmint_data found in transaction logs for tx_hash: {tx_hash}")
            return {"code": 400, "success": False, "msg": "No valid nftmint_data found in transaction logs"}
        
        # # 0x
        # # 0000000000000000000000000000000000000000000000000000000000000001 nftboxid
        # # 000000000000000000000000000000000000000000000000015181ff25a98000 eth
        # # 0000000000000000000000000000000000000000000000000000000000000000 sxp
        # # 0000000000000000000000000000000000000000000000000000000000000080 
        # # 0000000000000000000000000000000000000000000000000000000000000001 nftcount
        # # 0000000000000000000000000000000000000000000000000000000000000010 nftid
        # # 0000000000000000000000000000000000000000000000000000000000000011 nftid
        # # 0000000000000000000000000000000000000000000000000000000000000012 nftid
        # 解析交易参数
        nft_boxid = int(nftmint_data[26:66], 16)   # nftboxid
        nft_eth = int(nftmint_data[90:130], 16)   # eth
        nft_sxp = int(nftmint_data[154:194], 16)  # sxp
        nft_count = int(nftmint_data[282:322], 16)   # nftcount
        nft_Ids = [0] * nft_count
        for i in range(nft_count):
            nft_id = int(nftmint_data[346 + i*64:386 + i*64], 16)
            nft_Ids[i] = nft_id
        logger.debug(f"nft_superior: {nft_superior} -> nft_buyer: {nft_buyer} nft_Ids: {nft_Ids}")
        
        tx_amount_eth = Web3.from_wei(nft_eth, 'ether')
        tx_amount_sxp = Web3.from_wei(nft_sxp, 'mwei')
        # 1ETH = 100000 SXP
        tx_amount_total = round(float(tx_amount_sxp) / 100000 + float(tx_amount_eth), 5)
        if tx_amount_eth == 0 and tx_amount_sxp == 0 and nft_boxid > 0:
            logger.error(f"Ooops! transaction failed | tx_amount_eth: {tx_amount_eth} tx_amount_sxp: {tx_amount_sxp}")
            return {"code": 400, "success": False, "msg": "transaction failed"}
        logger.debug(f"nft_buyer: {nft_buyer} - nft_superior: {nft_superior} / tx_amount_eth: {tx_amount_eth} + tx_amount_sxp: {tx_amount_sxp} = tx_amount_total: {tx_amount_total} / nft_boxid: {nft_boxid} - nft_Ids: {nft_Ids}")
        
        ## 从区块解析基本信息
        block_timestamp = 0
        block_time = ''
        
        # 获取交易所在区块
        block_number = tx_receipt['blockNumber']
        block = web3_obj.eth.get_block(block_number, full_transactions=True)
        if block is None:
            logger.error(f"STATUS: 400 ERROR: Block not found | tx_hash: {tx_hash}")
            return {"code": 400, "success": False, "msg": "Block not found"}
        for tx in block.transactions:
            block_tx_hash = Web3.to_hex(tx.hash)
            if block_tx_hash.lower() == tx_hash.lower():
                logger.debug(f"tx: {tx}")
                # 解析区块时间
                block_timestamp = block.timestamp  # 获取区块时间戳
                logger.debug(f"block_timestamp: {block_timestamp}")  # 记录区块时间
                block_time = dt.fromtimestamp(block_timestamp).strftime('%Y-%m-%d %H:%M:%S')  # 将时间戳转换为可读格式
                logger.debug(f"block_time: {block_time}")  # 记录区块时间
                break
        if block_time == '':
            logger.error(f"STATUS: 400 ERROR: block_time not found | tx_hash: {tx_hash}")
            return {"code": 400, "success": False, "msg": "block_time not found"}

        status=1
        note=''
        
        # 计算邀请码
        par_referral_code = generate_referralcode(nft_superior)
        logger.debug(f"par_address: {nft_superior} par_referral_code: {par_referral_code}")
        referral_code = generate_referralcode(nft_buyer)
        logger.debug(f"tx_address: {nft_buyer} referral_code: {referral_code}")
        
        # # 更新邀请码
        # update_query = """
        #                 UPDATE wenda_users SET referral_code=%s,updated_time=NOW() WHERE address COLLATE utf8mb4_general_ci=%s AND referral_code=''
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
                            (contract_address,tx_address,referral_code,par_address,par_referral_code,tx_chainid,tx_blockid,tx_hash,tx_date,tx_amount_sxp,tx_amount_eth,tx_amount_total,nft_id,nft_boxid,nft_timestamp,status,note) 
                        SELECT 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        WHERE 
                            NOT EXISTS (SELECT id FROM wenda_nft_onchain WHERE tx_hash=%s AND nft_id=%s)
                        """
            values = (nftmint_address, nft_buyer, referral_code, nft_superior, par_referral_code, config_chainid, block_number, tx_hash, block_time, round(tx_amount_sxp/nft_count,3), round(tx_amount_eth/nft_count,6), round(tx_amount_total/nft_count,6),nft_id, nft_boxid, block_timestamp, status, note, tx_hash, nft_id)
            # logger.debug(f"insert_query: {insert_query} values: {values}")
            insert_query = format_query_for_db(insert_query)
            logger.debug(f"insert_query: {insert_query} values: {values}")
            await cursor.execute(insert_query, values)
            if DB_ENGINE == "sqlite": cursor.connection.commit()
            else: await cursor.connection.commit()
            logger.debug(f"Insert wenda_nft_onchain mint success! nft_id: {nft_id} nft_boxid: {nft_boxid}")
        return {"code": 200, "success": True, "msg": "Success"}
    except Exception as e:
        logger.error(f"/api/admin/airdrop/nft - except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": "Server error"}

# --------------------------------------------------------------------------------------------------

## Login test
@router.get("/test/login/{uid}")
async def admin_test_login(uid: str, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """生成token (需要管理员权限)"""
    logger.info(f"POST /api/admin/test/login - {uid} ")
    if cursor is None:
        logger.error(f"/api/admin/test/login - {uid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PERMISSIONS')}

        # action_log test/login
        log_message = f"{datetime.datetime.now()} - User {userid} test: Generate {uid} token\n"
        with open('action_log.txt', 'a') as log_file:
            log_file.write(log_message)

        # Check if the userid already exists
        check_query = ("SELECT userid,username,password FROM wenda_users WHERE userid=%s")
        values = (uid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {uid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}

        logger.debug(f"jwt_secret: {md58(existing_user['password'])}")
        expire_timestamp = int(time.time()) + 24*3600
        access_token = create_access_token({
                "userid": existing_user['userid'],
                "username": existing_user['username'],
                "secret": md58(existing_user['password']),
                "expire": expire_timestamp,
            })
        logger.debug(f"username: {existing_user['username']}  token: {access_token}")

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": {
                "token": access_token,
                "user_info": {
                    "uid": existing_user['userid'],
                    "name": existing_user['username'],
                },
            },
        }
    except Exception as e:
        logger.error(f"/api/admin/test/login - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


## email test
class TestEmailRequest(BaseModel):
    email: EmailStr | None = "xxxxxx@gmail.com"
    title: str | None = "测试标题"
    content: str | None = "测试内容"
@router.post("/test/email")
async def admin_test_email(post_request: TestEmailRequest, background_tasks: BackgroundTasks, userid: Dict = Depends(get_interface_userid)):
    """发送测试邮件 (需要管理员权限)"""
    logger.info(f"POST /api/admin/test/email - {post_request}")

    email = post_request.email
    if not validate_email_format(email):  # 邮箱格式判断
        logger.error(f"Invalid email: {email}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}

    try:
        if userid not in APP_CONFIG['admin'] and userid not in APP_CONFIG['action']:  # 有没有管理员权限 或 操作权限
            logger.error(f"STATUS: 401 ERROR: Invalid permissions - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PERMISSIONS')}
    
        # action_log test/email
        log_message = f"{datetime.datetime.now()} - User {userid} test: Sent test email - email: {email} title:{post_request.title} content:{post_request.content}\n"
        with open('action_log.txt', 'a') as log_file:
            log_file.write(log_message)

        ## Send test email
        if APP_CONFIG['startup'] == 'kafka':
            if "mail" in KAFKA_CONFIG['topiclist']:  ## 发送到生产者队列 - 测试邮件 kafka-produce-topic-mail
                value = {
                    "to_email": email,
                    "subject": post_request.title,
                    "context": post_request.content,
                }
                await kafka_send_produce("mail", value)
            else:  ## 添加到后台任务
                background_tasks.add_task(send_normal_mail,
                                        to_email=email,
                                        subject=post_request.title,
                                        context=post_request.content,
                                        )
        elif APP_CONFIG['startup'] == 'background':  ## 添加到后台任务
            background_tasks.add_task(send_normal_mail,
                                    to_email=email,
                                    subject=post_request.title,
                                    context=post_request.content,
                                    )
        elif APP_CONFIG['startup'] == 'direct':  ## 直接调用
            await send_normal_mail(email, post_request.title, post_request.content)

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": "The test mail has been sent to the email",
        }
    except Exception as e:
        logger.error(f"/api/admin/test/email - except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# --------------------------------------------------------------------------------------------------
