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

from config import APP_CONFIG, KAFKA_CONFIG, JWT_CONFIG, DB_ENGINE
from utils.bearertoken import md58, create_access_token, decode_access_token
from utils.cache import get_redis_data, set_redis_data, del_redis_data
from utils.db import get_db, format_query_for_db, convert_row_to_dict, format_datetime_fields
from utils.email import send_normal_mail, send_activation_mail, send_reset_mail, send_newpasswd_mail
from utils.i18n import get_text
from utils.local import floor_decimal, generate_registercode, validate_email_format
from utils.kafka.produce import kafka_send_produce
from utils.security import get_interface_userid
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
                'url': f'{APP_CONFIG["appbase"]}/register?code={register_code}'
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
                register_one['url'] = f'{APP_CONFIG["appbase"]}/register?code={register_one["code"]}'
        
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
                register_one['url'] = f'{APP_CONFIG["appbase"]}/register?code={register_one["code"]}'
        
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
