import asyncio

from config import APP_CONFIG, DB_ENGINE
from utils.cache import get_redis_data, set_redis_data, del_redis_data
from utils.db import get_db_app, format_query_for_db, convert_row_to_dict
from utils.i18n import get_text
from utils.local import generate_referralcode
from utils.web3_tools import contract_nftmint
from utils.log import log as logger

# 异步: 直接调用  direct kafka
async def async_nft_mintnft(value):
    userid = value.get('userid', '')
    chainid = int(value.get('chainid', 0))
    address = value.get('address', '')

    # .env 用户黑名单
    if userid in APP_CONFIG['black']:
        logger.info(f"STATUS: 401 ERROR: Blacklist - mintnft:{userid} - env:blacklist")
        return {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}

    # 使用新的数据库连接
    async with get_db_app() as cursor:

        ## 从链上查询NFT数量
        check_query = " SELECT COUNT(id) as len FROM wenda_nft_onchain WHERE tx_address COLLATE utf8mb4_general_ci =%s AND tx_chainid=%s AND status=1"
        # 格式化查询语句以适应当前数据库类型
        values = (address, chainid,)
        formatted_check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(formatted_check_query, values)
        existing_record = await cursor.fetchone()
        # logger.debug(f"existing_record: {existing_record}")
        existing_record = convert_row_to_dict(existing_record, cursor.description)  # 转换字典
        logger.debug(f"existing_record: {existing_record}")
        nft_count = existing_record['len'] if existing_record else 0
        if nft_count > 0:
            logger.error(f"STATUS: 400 ERROR: The address already has NFT - {userid}")
            return {"code": 400, "success": False, "msg": "The address already has NFT"}
        
        tx_hash = ''
        status=1
        note = ''

        if status == 1:
            await del_redis_data(f"box:{userid}:session")
            # 白名单发起NFT铸造
            is_success, data = contract_nftmint(address, chainid)
            logger.debug(f"mintnft result: {is_success},  {data}")
            tx_hash = data.get('tx_hash', '')
            if not is_success:
                status = 2
                note = data.get('msg', '').replace('\n', ' ').replace('\r', ' ')[:250]
                # logger.error(f"Failed to mint: {note}")
                logger.error(f"STATUS: 400 ERROR: Failed to mint NFT - {userid} - {note}")
                return {"code": 400, "success": False, "msg": "Failed to mint NFT"}
            await del_redis_data(f"box:{userid}:session")
        
        # 计算邀请码
        referral_code = generate_referralcode(address)
        logger.debug(f"tx_address: {address} referral_code: {referral_code}")
        
        # # 更新邀请码
        # update_query = "UPDATE wenda_users SET referral_code=%s WHERE address COLLATE utf8mb4_general_ci=%s"
        # values = (referral_code, address,)
        # formatted_update_query = format_query_for_db(update_query)
        # logger.debug(f"update_query: {update_query} values: {values}")
        # await cursor.execute(formatted_update_query, values)
        # if DB_ENGINE == "sqlite": cursor.connection.commit()
        # else: await cursor.connection.commit()
        
        logger.success(f"update wenda_users {address} - {userid} => {referral_code}")
        return {
            "code": 200, 
            "success": True, 
            "msg": "Success"
        }

## 同步: 后台任务  background
def sync_nft_mintnft(value):
    userid = value.get('userid', '')
    chainid = int(value.get('chainid', 0))
    address = value.get('address', '')

    # .env 用户黑名单
    if userid in APP_CONFIG['black']:
        logger.info(f"STATUS: 401 ERROR: Blacklist - mintnft:{userid} - env:blacklist")
        return {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}

    # 由于同步函数不能使用异步上下文管理器，我们需要使用事件循环
    async def run_async_logic():
        async with get_db_app() as cursor:

            ## 从链上查询NFT数量
            check_query = " SELECT COUNT(id) as len FROM wenda_nft_onchain WHERE tx_address COLLATE utf8mb4_general_ci=%s AND tx_chainid=%s AND status=1"
            values = (address, chainid,)
            formatted_check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(formatted_check_query, values)
            existing_record = await cursor.fetchone()
            # logger.debug(f"existing_record: {existing_record}")
            existing_record = convert_row_to_dict(existing_record, cursor.description)  # 转换字典
            logger.debug(f"existing_record: {existing_record}")
            nft_count = existing_record['len'] if existing_record else 0
            if nft_count > 0:
                logger.error(f"STATUS: 400 ERROR: The address already has NFT - {userid}")
                return {"code": 400, "success": False, "msg": "The address already has NFT"}
            
            tx_hash = ''
            status=1
            note = ''

            if status == 1:
                await del_redis_data(f"box:{userid}:session")
                # 白名单发起NFT铸造
                is_success, data = contract_nftmint(address, chainid)
                logger.debug(f"mintnft result: {is_success},  {data}")
                tx_hash = data.get('tx_hash', '')
                if not is_success:
                    status = 2
                    note = data.get('msg', '').replace('\n', ' ').replace('\r', ' ')[:250]
                    # logger.error(f"Failed to mint: {note}")
                    logger.error(f"STATUS: 400 ERROR: Failed to mint NFT - {userid} - {note}")
                    return {"code": 400, "success": False, "msg": "Failed to mint NFT"}
                await del_redis_data(f"box:{userid}:session")

            # 计算邀请码
            referral_code = generate_referralcode(address)
            logger.debug(f"tx_address: {address} referral_code: {referral_code}")
            
            # # 更新邀请码
            # update_query = "UPDATE wenda_users SET referral_code=%s WHERE address COLLATE utf8mb4_general_ci=%s"
            # values = (referral_code, address,)
            # formatted_update_query = format_query_for_db(update_query)
            # logger.debug(f"update_query: {update_query} values: {values}")
            # await cursor.execute(formatted_update_query, values)
            # if DB_ENGINE == "sqlite": cursor.connection.commit()
            # else: await cursor.connection.commit()
            
            logger.success(f"update wenda_users {address} - {userid} => {referral_code}")
            return {
                "code": 200, 
                "success": True, 
                "msg": "Success"
            }

    # 运行异步逻辑
    return asyncio.run(run_async_logic())
