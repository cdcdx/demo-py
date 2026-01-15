import hashlib
import json
import random
import re
import string
import time
import datetime
from datetime import datetime as dt
from typing import Dict
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, EmailStr, Field

from config import APP_CONFIG, KAFKA_CONFIG, DB_ENGINE, JWT_CONFIG
from utils.bearertoken import md58, create_access_token, decode_access_token
from utils.cache import del_redis_data, get_redis_data, set_redis_data
from utils.captcha import validate_captcha
from utils.db import get_db, format_query_for_db, convert_row_to_dict, format_datetime_fields
from utils.email import send_activation_mail, send_reset_mail
from utils.i18n import get_text
from utils.kafka.produce import kafka_send_produce
from utils.mintnft import sync_nft_mintnft, async_nft_mintnft
from utils.local import generate_userid, generate_registercode, hash_password, check_password, validate_password_strength, validate_email_format, verify_recaptcha_token
from utils.security import get_current_userid, get_interface_userid
from utils.log import log as logger

router = APIRouter()


## auth

# --------------------------------------------------------------------------------------------------

# 查询3次 插入2次
class AuthRegiterRequest(BaseModel):
    email: EmailStr | None = "xxxxxx@gmail.com"
    username: str
    password: str
    register_code: str = Field(..., description="register code", pattern=r"(^[0-9a-zA-Z]{5}$){0,1}")
    recaptcha_token: str
@router.post("/register")  # {email,username,password,register_code,recaptcha_token}
async def register(post_request: AuthRegiterRequest, background_tasks: BackgroundTasks, cursor=Depends(get_db)):
    """商户注册 一户一码"""
    logger.info(f"POST /api/auth/register - {post_request.username} {post_request.email}")
    if cursor is None:
        logger.error(f"/api/auth/register - {post_request.username} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    email = post_request.email
    if not validate_email_format(email):  # 邮箱格式判断
        logger.error(f"Invalid email: {email}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}

    username = post_request.username
    if len(username) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
    
    try:
        password = validate_password_strength(post_request.password)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    recaptcha_token = post_request.recaptcha_token
    logger.debug(f"recaptcha_token: {recaptcha_token}")
    ## captcha_token 图形验证码校验
    if not verify_recaptcha_token(recaptcha_token):
        logger.error(f"STATUS: 401 ERROR: Invalid captcha")
        return {"code": 401, "success": False, "msg": get_text('INVALID_CAPTCHA')}

    ## 屏蔽用户多次请求 60
    redis_pending = await get_redis_data(f"pending:{email}:register")
    if redis_pending:
        return {"code": 400, "success": False, "msg": "Please wait for the last completion"}
    await set_redis_data(f"pending:{email}:register", value=1, ex=60)

    try:
        register_code = post_request.register_code
        if len(register_code) != 5:
            logger.error(f"STATUS: 401 ERROR: Invalid registercode - {register_code}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_REGISTERCODE')}
        logger.debug(f"register_code: {register_code}")
        
        ## 根据 register_code 查询注册码是否存在
        check_query = "SELECT id FROM wenda_users WHERE register_code=%s AND userid='' "
        values = (register_code,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_registercode = await cursor.fetchone()
        # logger.debug(f"existing_registercode: {existing_registercode}")
        if existing_registercode is None:  # 注册码不存在 或 注册码已使用
            logger.error(f"STATUS: 401 ERROR: Invalid registercode {register_code}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_REGISTERCODE')}
        existing_registercode = convert_row_to_dict(existing_registercode, cursor.description)  # 转换字典
        logger.debug(f"existing_registercode: {existing_registercode}")

        register_id = existing_registercode['id']
        logger.debug(f"register_id: {register_id}")

        # Check if the email already exists
        check_query = "SELECT email FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Email already registered - {email}")
            return {"code": 400, "success": False, "msg": get_text('ALREADY_EMAIL_REGISTERED')}

        # Check if the username already exists
        check_query = "SELECT username FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Username already registered - {username}")
            return {"code": 400, "success": False, "msg": get_text('ALREADY_USERNAME_REGISTERED')}

        ## userid生成逻辑
        userid = None
        retry_count = 0
        while retry_count < 5:
            userid = generate_userid(email)
            # Check if the userid already exists
            check_query = "SELECT id FROM wenda_users WHERE userid=%s"
            values = (userid,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_user = await cursor.fetchone()
            # logger.debug(f"existing_user: {existing_user}")
            if existing_user is None:
                break
            existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
            logger.debug(f"existing_user: {existing_user}")
            retry_count += 1
        else:
            logger.error("Failed to generate a userid after maximum retries.")
            return {"code": 400, "success": False, "msg": "userid generation failed"}
        logger.debug(f"email: {email} => userid: {userid}")
        
        hashed_password = hash_password(password)
        logger.debug(f"username: {username} hashed_password: {hashed_password}")

        email_state = "UNVERIFIED"
        update_query = "UPDATE wenda_users SET userid=%s,email=%s,username=%s,password=%s,state=%s,updated_time=NOW() WHERE id=%s AND userid='' "
        values = (userid, email, username, hashed_password, email_state, register_id,)
        update_query = format_query_for_db(update_query)
        logger.debug(f"update_query: {update_query} values: {values}")
        await cursor.execute(update_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()

        ## 生成确认注册链接，并发送邮件
        logger.debug(f"userid: {userid}, username: {username}, secret: {md58(hashed_password)}")
        expire_timestamp = int(time.time()) + JWT_CONFIG['expire']
        verify_token = create_access_token({
                "userid": userid,
                "username": username,
                "secret": md58(hashed_password),
                "expire": expire_timestamp,
            })
        logger.debug(f"verify_token: {verify_token}")
        verify_url = APP_CONFIG['apibase'] + "/api/validate/token?token=" + verify_token
        logger.info(f"verify_url: {verify_url}")

        ## Send activation email
        if APP_CONFIG['startup'] == 'kafka':
            if "mail" in KAFKA_CONFIG['topiclist']:  ## 发送到生产者队列 - 激活邮件 kafka-produce-topic-mail
                value = {
                    "to_email": email,
                    "subject": "activation",
                    "context": verify_url,
                    "username": username,
                }
                await kafka_send_produce("mail", value)
            else:  ## 添加到后台任务
                background_tasks.add_task(send_activation_mail,
                                        to_email=email,
                                        username=username,
                                        verify_url=verify_url,
                                        )
        elif APP_CONFIG['startup'] == 'background':  ## 添加到后台任务
            background_tasks.add_task(send_activation_mail,
                                    to_email=email,
                                    username=username,
                                    verify_url=verify_url,
                                    )
        elif APP_CONFIG['startup'] == 'direct':  ## 直接调用
            send_activation_mail(username, email, verify_url)

        return {
            "success": True,
            "code": 200,
            "msg": "Success",
            "data": {
                "code": register_code,
                "uid": userid,
                "name": username,
                "email": email,
                "state": email_state,
            },
        }
    except Exception as e:
        await cursor.connection.rollback()
        logger.error(f"/api/auth/register except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次
class AuthLoginRequest(BaseModel):
    username: str  # 可以是 username or email
    password: str
    recaptcha_token: str
@router.post("/login")  # {username,password,recaptcha_token}
async def login(post_request: AuthLoginRequest, cursor=Depends(get_db)):
    """登录: 支持用户名或邮箱登录"""
    logger.info(f"POST /api/auth/login - {post_request.username}")
    if cursor is None:
        logger.error(f"/api/auth/login - {post_request.username} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}
    
    username = post_request.username
    if len(username) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}

    try:
        password = validate_password_strength(post_request.password)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    recaptcha_token = post_request.recaptcha_token
    logger.debug(f"recaptcha_token: {recaptcha_token}")
    ## captcha_token 图形验证码校验
    if not verify_recaptcha_token(recaptcha_token):
        logger.error(f"STATUS: 401 ERROR: Invalid captcha")
        return {"code": 401, "success": False, "msg": get_text('INVALID_CAPTCHA')}

    try:
        # Check if the username or email already exists
        if validate_email_format(username): # 邮箱格式判断
            check_query = "SELECT userid,username,password FROM wenda_users WHERE email=%s"
        else:
            check_query = "SELECT userid,username,password FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        logger.debug(f'username: {username} password: {password.encode("utf-8")} existing_user["password"]: {existing_user["password"].encode("utf-8")}')
        if check_password(password, existing_user['password']):
            logger.debug(f"jwt_secret: {md58(existing_user['password'])}")
            expire_timestamp = int(time.time()) + JWT_CONFIG['expire']
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
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid password - {username} {password}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    except Exception as e:
        logger.error(f"/api/auth/login except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次 更新1次
class AuthForgetRequest(BaseModel):
    email: EmailStr | None = 'xxxxxx@gmail.com'
    recaptcha_token: str
@router.post("/forget-password")  # {email,recaptcha_token}
async def forget_password(post_request: AuthForgetRequest, background_tasks: BackgroundTasks, cursor=Depends(get_db)):
    """忘记密码"""
    logger.info(f"POST /api/auth/forget-password - {post_request.email}")
    if cursor is None:
        logger.error(f"/api/auth/forget-password - {post_request.email} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    email = post_request.email
    if not validate_email_format(email):  # 邮箱格式判断
        logger.error(f"Invalid email: {email}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}

    recaptcha_token = post_request.recaptcha_token
    logger.debug(f"recaptcha_token: {recaptcha_token}")
    ## captcha_token 图形验证码校验
    if not verify_recaptcha_token(recaptcha_token):
        logger.error(f"STATUS: 401 ERROR: Invalid captcha")
        return {"code": 401, "success": False, "msg": get_text('INVALID_CAPTCHA')}

    try:
        # Check if the email already exists
        check_query = "SELECT username,updated_time as updated,unix_timestamp(cooldown_time) as cooldown FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid email - {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        existing_user = format_datetime_fields(existing_user)  # DATETIME转字符串
        logger.debug(f"existing_user: {existing_user}")

        if existing_user:
            # 计算冷却时间，10分钟后才能再次更新
            cooldownSecord = 600.0
            if existing_user['cooldown'] is not None:
                now_timestamp = int(dt.now().timestamp())
                # cooldown_timestamp = existing_user['cooldown']
                cooldown_timestamp = int(time.mktime(time.strptime(existing_user['cooldown'], '%Y-%m-%d %H:%M:%S')))
                cooldownSecord = now_timestamp - cooldown_timestamp
                logger.debug(f"now_timestamp: {now_timestamp} cooldown_timestamp: {cooldown_timestamp}")
            if APP_CONFIG['level'] == "debug":  # debug数据：30秒后可以再次更新
                cooldownSecord += 3570
            logger.debug(f"username: {existing_user['username']} cooldownSecord: {cooldownSecord}")
            if cooldownSecord >= 600:
                ## 生成重置密码链接，并发送邮件
                timestamp = APP_CONFIG['key']
                logger.debug(f"timestamp: {timestamp}")
                if existing_user['updated'] is not None:
                    timestamp = existing_user['updated'] # str(dt.timestamp(existing_user['updated']))
                    logger.debug(f"timestamp: {timestamp}")
                logger.debug(f"email: {email}, key: {md58(timestamp)}")
                expire_timestamp = int(time.time()) + JWT_CONFIG['expire']
                payload = {
                    "email": email,
                    "key": md58(timestamp),
                    "expire": expire_timestamp,
                }
                logger.debug(f"payload: {payload}")
                reset_token = create_access_token(payload)
                logger.debug(f"reset_token: {reset_token}")

                reset_url = APP_CONFIG['redirect'] + reset_token + "&path=reset"
                logger.info(f"reset_url: {reset_url}")

                ## Send reset email
                if APP_CONFIG['startup'] == 'kafka':
                    if "mail" in KAFKA_CONFIG['topiclist']:  ## 发送到生产者队列 - 重置邮件 kafka-produce-topic-mail
                        value = {
                            "to_email": email,
                            "subject": "reset",
                            "context": reset_url,
                            "username": existing_user['username'],
                        }
                        await kafka_send_produce("mail", value)
                    else:  ## 添加到后台任务
                        background_tasks.add_task(send_reset_mail,
                                                  to_email=email,
                                                  username=existing_user['username'],
                                                  reset_url=reset_url,
                                                  )
                elif APP_CONFIG['startup'] == 'background':  ## 添加到后台任务
                    background_tasks.add_task(send_reset_mail,
                                              to_email=email,
                                              username=existing_user['username'],
                                              reset_url=reset_url,
                                              )
                elif APP_CONFIG['startup'] == 'direct':  ## 直接调用
                    send_reset_mail(existing_user['username'], email, reset_url)

                ## 更新账户冷却时间
                update_query = "UPDATE wenda_users set cooldown_time=NOW() where email=%s"
                values = (email,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "You will receive an email with the reset link if an account exists with the email provided. Remember to look in your spam or junk mail folder as well.",
                }
            else:
                logger.error(f"STATUS: 402 ERROR: Too many requests, please try again later. - cooldownSecord: {3600 - cooldownSecord}")
                return { "code": 402, "success": False, "msg": "Too many requests, please try again later."}
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid email - {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
    except Exception as e:
        logger.error(f"/api/auth/forget-password except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次 更新1次
class AuthResetRequest(BaseModel):
    token: str
    password_new: str
@router.post("/reset-password")  # {token,password_new}
async def reset_password(post_request: AuthResetRequest, cursor=Depends(get_db)):
    """重置密码,前端传token来授权修改密码"""
    logger.info(f"POST /api/auth/reset-password - {post_request.password_new}")
    if cursor is None:
        logger.error(f"/api/auth/reset-password - {post_request.password_new} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}
    credentials_exception = HTTPException(status_code=401, detail="Invalid JWT Token")

    token = post_request.token

    try:
        password_new = validate_password_strength(post_request.password_new)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    try:
        payload = decode_access_token(token)
        logger.debug(f"payload: {payload}")
        current_timestamp = int(time.time())
        expire = payload.get('expire')
        if expire is None:
            raise credentials_exception
        elif expire <= current_timestamp:
            raise credentials_exception

        if payload.get('email', None):  ## 重置密码
            email = payload.get('email')
            jwt_key = payload.get('key')
            if email is None:
                return {"code": 401, "success": False, "msg": get_text('INVALID_JWT_TOKEN')}

            # 账户是否存在
            check_query = "SELECT userid,username,updated_time as updated FROM wenda_users WHERE email=%s"
            values = (email,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_user = await cursor.fetchone()
            # logger.debug(f"existing_user: {existing_user}")
            existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
            existing_user = format_datetime_fields(existing_user)  # DATETIME转字符串
            logger.debug(f"existing_user: {existing_user}")

            if existing_user:
                # key 有效性校验
                timestamp = APP_CONFIG['key']
                if existing_user['updated'] is not None:
                    timestamp = existing_user['updated'] # str(dt.timestamp(existing_user['updated']))
                    logger.debug(f"timestamp: {timestamp}")
                sql_key = md58(timestamp)
                logger.debug(f"sql.key: {sql_key} jwt.key: {jwt_key}")
                if sql_key != jwt_key:
                    return {"code": 401, "success": False, "msg": get_text('INVALID_JWT_TOKEN')}

                logger.debug(f"username: {existing_user['username']} password_new: {password_new}")
                hashed_password_new = hash_password(password_new)
                logger.debug(f"username: {existing_user['username']} hashed_password_new: {hashed_password_new}")
                ## 更新账户密码
                update_query = "UPDATE wenda_users set password=%s,updated_time=NOW(),cooldown_time=NOW() where userid=%s"
                values = (hashed_password_new, existing_user['userid'],)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(f"box:{existing_user['userid']}:session")

                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Password changed successfully, please login again.",
                }
            else:
                logger.error(f"STATUS: 401 ERROR: Invalid email - {email}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
        else:
            return {"code": 401, "success": False, "msg": get_text('INVALID_JWT_TOKEN')}
    except Exception as e:
        logger.error(f"/api/auth/reset-password except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次 更新1次
class AuthChangePasswordRequest(BaseModel):
    password: str
    password_new: str
@router.post("/change-password")  # {password,password_new}
async def change_password(post_request: AuthChangePasswordRequest,userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """修改密码"""
    logger.info(f"POST /api/auth/change-password - {userid} - {post_request.password_new}")
    if cursor is None:
        logger.error(f"/api/auth/change-password - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        password = validate_password_strength(post_request.password)
        password_new = validate_password_strength(post_request.password_new)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    try:
        # Check if the userid already exists
        check_query = "SELECT username,password,unix_timestamp(cooldown_time) as cooldown FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        existing_user = format_datetime_fields(existing_user)  # DATETIME转字符串
        logger.debug(f"existing_user: {existing_user}")

        if check_password(password, existing_user['password']):
            # 计算冷却时间，10分钟后才能再次更新
            cooldownSecord = 600
            if existing_user['cooldown'] is not None:
                now_timestamp = int(dt.now().timestamp())
                # cooldown_timestamp = existing_user['cooldown']
                cooldown_timestamp = int(time.mktime(time.strptime(existing_user['cooldown'], '%Y-%m-%d %H:%M:%S')))
                cooldownSecord = now_timestamp - cooldown_timestamp
                logger.debug(f"now_timestamp: {now_timestamp} cooldown_timestamp: {cooldown_timestamp}")
            if APP_CONFIG['level'] == "debug":  # debug数据：30秒后可以再次更新
                cooldownSecord += 3570
            logger.debug(f"username: {existing_user['username']} cooldownSecord: {cooldownSecord}")
            if cooldownSecord >= 600:
                hashed_password_new = hash_password(password_new)
                logger.debug(f"username: {existing_user['username']} hashed_password_new: {hashed_password_new}")
                ## 更新账户密码
                update_query = "UPDATE wenda_users set password=%s,updated_time=NOW(),cooldown_time=NOW() where userid=%s"
                values = (hashed_password_new, userid,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(f"box:{userid}:session")

                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Password changed successfully, please login again.",
                }
            else:
                logger.error(f"STATUS: 402 ERROR: Too many requests, please try again later. - cooldownSecord: {3600 - cooldownSecord}")
                return { "code": 402, "success": False, "msg": "Too many requests, please try again later."}
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid password - {userid} {password} => {password_new}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    except Exception as e:
        logger.error(f"/api/auth/change-password - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新1次
class AuthChangeEmailRequest(BaseModel):
    email: EmailStr | None = 'xxxxxx@gmail.com'
    password: str
@router.post("/change-email")  # {password,email}
async def change_email(post_request: AuthChangeEmailRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """修改邮件"""
    logger.info(f"POST /api/auth/change-email - {userid} - {post_request.email}")
    if cursor is None:
        logger.error(f"/api/auth/change-email - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    email = post_request.email
    if not validate_email_format(email):  # 邮箱格式判断
        logger.error(f"Invalid email: {email} - {userid}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
    try:
        password = validate_password_strength(post_request.password)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    try:
        # 查询邮箱是否已使用
        check_query = "SELECT userid,username FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            if existing_user['userid'].strip() == userid:
                logger.debug(f"Same email, no need to modify. - {existing_user['username']} - {email}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Same email, no need to modify.",
                }
            logger.error(f"STATUS: 400 ERROR: Email already registered - {existing_user['username']} - {email}")
            return {"code": 400, "success": False, "msg": get_text('ALREADY_EMAIL_REGISTERED')}

        # 查询token里的UID是否真实存在
        check_query = "SELECT email,username,password,address,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid} - {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        if check_password(password, existing_user['password']):
            if existing_user['state'] == "UNVERIFIED":
                logger.debug(f"username: {existing_user['username']} - {existing_user['email']} => email: {email}")
                ## 更新账户邮箱
                update_query = "UPDATE wenda_users set email=%s,updated_time=NOW() where userid=%s"
                values = (email, userid,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(f"box:{userid}:session")

                logger.debug(f"STATUS: 200 Success: Email changed successfully, please verify again. - {existing_user['username']} - {email}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Email changed successfully, please verify again.",
                }
            else:
                logger.error(f"STATUS: 400 ERROR: Email already modified - {existing_user['username']} - {existing_user['email']} => {email}")
                return {"code": 400, "success": False, "msg": get_text('ALREADY_EMAIL_MODIFIED')}
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid password - {userid} {password} => {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    except Exception as e:
        logger.error(f"/api/auth/change-email - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新1次
class AuthChangeUsernameRequest(BaseModel):
    username: str
    password: str
@router.post("/change-username")  # {username}
async def change_username(post_request: AuthChangeUsernameRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """修改用户名"""
    logger.info(f"POST /api/auth/change-username - {userid} - {post_request.username}")
    if cursor is None:
        logger.error(f"/api/auth/change-username - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    username = post_request.username
    if len(username) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
    try:
        password = validate_password_strength(post_request.password)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    try:
        ## 查询用户名是否已使用
        check_query = "SELECT userid,username FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            if existing_user['userid'].strip() == userid:
                logger.debug(f"Same username, no need to modify. - {existing_user['username']} - {username}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Same username, no need to modify.",
                }
            logger.error(f"STATUS: 400 ERROR: Username already registered - {existing_user['username']} - {username}")
            return {"code": 400, "success": False, "msg": get_text('ALREADY_USERNAME_REGISTERED')}

        # 查询token里的UID是否真实存在
        check_query = "SELECT email,username,password,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid} - {username}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        if check_password(password, existing_user['password']):
            if existing_user['username'] != username:
                ## 更新用户名
                update_query = "UPDATE wenda_users set username=%s,updated_time=NOW() where userid=%s"
                values = (username, userid,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(f"box:{userid}:session")

                logger.debug(f"STATUS: 200 Success: Username changed successfully, please verify again. - {username}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Username changed successfully, please verify again.",
                }
            else:
                logger.error(f"STATUS: 400 ERROR: Username already modified - {existing_user['username']} - {existing_user['username']} => {username}")
                return {"code": 400, "success": False, "msg": get_text('ALREADY_USERNAME_MODIFIED')}
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid password - {userid} {password} => {username}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    except Exception as e:
        logger.error(f"/api/auth/change-username - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新1次
class AuthChangeAddressRequest(BaseModel):
    address: str | None = '0x***'
    password: str
@router.post("/change-address")  # {address}
async def change_address(post_request: AuthChangeAddressRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """修改地址"""
    logger.info(f"POST /api/auth/change-address - {userid} - {post_request.address}")
    if cursor is None:
        logger.error(f"/api/auth/change-address - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    address = post_request.address
    if not (len(address) == 42 and address[:2] == '0x'):  # 钱包地址判断
        logger.error(f"STATUS: 401 ERROR: Invalid address - {address}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_ADDRESS')}
    try:
        password = validate_password_strength(post_request.password)
    except ValueError:
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}

    try:
        ## 查询地址是否已使用
        check_query = "SELECT userid,username FROM wenda_users WHERE address=%s"
        values = (address,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            if existing_user['userid'].strip() == userid:
                logger.debug(f"Same address, no need to modify. - {existing_user['username']} - {address}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Same address, no need to modify.",
                }
            logger.error(f"STATUS: 400 ERROR: Address already registered - {existing_user['username']} - {address}")
            return {"code": 400, "success": False, "msg": get_text('ALREADY_ADDRESS_REGISTERED')}

        # 查询token里的UID是否真实存在
        check_query = "SELECT email,username,password,address,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid} - {address}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        if check_password(password, existing_user['password']):
            if existing_user['address'] != address:
                ## 更新地址
                update_query = "UPDATE wenda_users set address=%s,updated_time=NOW() where userid=%s"
                values = (address, userid,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(f"box:{userid}:session")

                logger.debug(f"STATUS: 200 Success: Address changed successfully, please verify again. - {address}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Address changed successfully, please verify again.",
                }
            else:
                logger.error(f"STATUS: 400 ERROR: Address already modified - {existing_user['username']} - {existing_user['address']} => {address}")
                return {"code": 400, "success": False, "msg": get_text('ALREADY_ADDRESS_MODIFIED')}
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid password - {userid} {password} => {address}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    except Exception as e:
        logger.error(f"/api/auth/change-address - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次
@router.post("/send-verifyemail")
async def send_verifyemail(background_tasks: BackgroundTasks, userid: Dict = Depends(get_interface_userid),cursor=Depends(get_db)):
    """发送验证邮件"""
    logger.info(f"POST /api/auth/send-verifyemail - {userid}")
    if cursor is None:
        logger.error(f"/api/auth/send-verifyemail - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        ## 屏蔽用户多次请求 60
        redis_pending = await get_redis_data(f"pending:{userid}:send:verifyemail")
        if redis_pending:
            return {"code": 400, "success": False, "msg": "Please wait for the last completion"}
        await set_redis_data(f"pending:{userid}:send:verifyemail", value=1, ex=60)

        ## 根据 userid 查询是否已验证
        check_query = "SELECT email,username,password,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        if existing_user:
            ## 用户社交信息表 更新积分记录
            if existing_user['state'] != 'UNVERIFIED':
                # 邮件已验证
                logger.error(f"STATUS: 400 ERROR: Email already verified - {existing_user['email']}")
                return {"code": 400, "success": False, "msg": get_text('ALREADY_EMAIL_VERIFIED')}
            else:
                # 重发验证邮件
                ## 生成确认注册链接，并发送邮件
                logger.debug(f"userid: {userid}, username: {existing_user['username']}, secret: {md58(existing_user['password'])}")
                expire_timestamp = int(time.time()) + JWT_CONFIG['expire']
                verify_token = create_access_token({
                        "userid": userid,
                        "username": existing_user['username'],
                        "secret": md58(existing_user['password']),
                        "expire": expire_timestamp
                    })
                logger.debug(f"verify_token: {verify_token}")
                verify_url = APP_CONFIG['apibase'] + '/api/validate/token?token=' + verify_token
                logger.debug(f"verify_url: {verify_url}")

                ## Send verify email
                if APP_CONFIG['startup'] == 'kafka':
                    if "mail" in KAFKA_CONFIG['topiclist']:  ## 发送到生产者队列 - 激活邮件 kafka-produce-topic-mail
                        value = {
                            "to_email": existing_user['email'],
                            "subject": "activation",
                            "context": verify_url,
                            "username": existing_user['username'],
                        }
                        await kafka_send_produce("mail", value)
                    else:  ## 添加到后台任务
                        background_tasks.add_task(send_activation_mail,
                                                  to_email=existing_user['email'],
                                                  username=existing_user['username'],
                                                  verify_url=verify_url,
                                                  )
                elif APP_CONFIG['startup'] == 'background':  ## 添加到后台任务
                    background_tasks.add_task(send_activation_mail,
                                              to_email=existing_user['email'],
                                              username=existing_user['username'],
                                              verify_url=verify_url,
                                              )
                elif APP_CONFIG['startup'] == 'direct':
                    send_activation_mail(existing_user['username'], existing_user['email'], verify_url)

                return {
                    "success": True,
                    "code": 200,
                    "msg": "Success",
                    "data": "You will receive an email with the reset link if an account exists with the email provided. Remember to look in your spam or junk mail folder as well."
                }
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
    except Exception as e:
        logger.error(f"/api/auth/send-verifyemail - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新1次
class BindAddressRequest(BaseModel):
    address: str | None = '0x***'
@router.post("/bind-address")  # {address}
async def bind_address(post_request: BindAddressRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """绑定钱包地址"""
    logger.info(f"POST /api/auth/bind-address - {userid} - {post_request.address}")
    if cursor is None:
        logger.error(f"/api/auth/bind-address - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    address = post_request.address
    if not (len(address) == 42 and address[:2] == '0x'):  # 钱包地址判断
        logger.error(f"STATUS: 401 ERROR: Invalid address - {address}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_ADDRESS')}
    if not address:  # None
        logger.error(f"STATUS: 401 ERROR: No wallet address - {address}")
        return {"code": 401, "success": False, "msg": "No wallet address"}

    try:
        ## 屏蔽用户多次请求 60
        redis_pending = await get_redis_data(f"pending:{userid}:bind:address")
        if redis_pending:
            return {"code": 400, "success": False, "msg": "Please wait for the last completion"}
        await set_redis_data(f"pending:{userid}:bind:address", value=1, ex=60)

        address = address.lower()

        # Check if the address already exists
        check_query = "SELECT userid FROM wenda_users WHERE address COLLATE utf8mb4_general_ci=%s"
        values = (address,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_address = await cursor.fetchone()
        # logger.debug(f"existing_address: {existing_address}")
        existing_address = convert_row_to_dict(existing_address, cursor.description)  # 转换字典
        logger.debug(f"existing_address: {existing_address}")
        if existing_address:
            if existing_address['userid'].strip() == userid:
                logger.error(f"STATUS: 400 ERROR: The address has been bound - address: {address}")
                return {"code": 400, "success": False, "msg": "The address has been bound"}
            else:
                logger.error(f"STATUS: 400 ERROR: The address has been bound by someone - userid: {userid}")
                return {"code": 400, "success": False, "msg": "The address has been bound by someone"}

        # Check if the userid already exists
        check_query = "SELECT address FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            if existing_user['address'] == address:
                logger.error(f"STATUS: 400 ERROR: The user has bound an address - userid: {userid} address: {address}")
                return {"code": 400, "success": False, "msg": "The user has bound an address"}
            elif existing_user['address'] != '':
                logger.error(f"STATUS: 400 ERROR: The user has already bound another address - userid: {userid} address: {address}")
                return {"code": 400, "success": False, "msg": "The user has already bound another address"}

        # 开始绑定逻辑
        update_query = "UPDATE wenda_users set address=%s,updated_time=NOW() where userid=%s"
        values = (address, userid,)
        update_query = format_query_for_db(update_query)
        logger.debug(f"update_query: {update_query} values: {values}")
        await cursor.execute(update_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()

        ## redis 删除
        await del_redis_data(f"box:{userid}:session")

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": "Successfully bound address"
        }
    except Exception as e:
        logger.error(f"/api/auth/bind-address - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


@router.get("/mintnft") # ?chainid=84532
async def address_mintnft_B0(chainid:int, background_tasks: BackgroundTasks, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """赠送B0级NFT一张:用于生成邀请码"""
    logger.info(f"POST /api/auth/mintnft - {userid}")
    if cursor is None:
        logger.error(f"/api/auth/mintnft - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": "Server error"}

    try:
        ## B0铸造状态 - 0: 维护中   None/1: 允许B0铸造
        box_mintnft_status = await get_redis_data(f"box:mintnft:status")
        if box_mintnft_status == 0:
            logger.error(f"Mintnft maintenance, will be restored later. - {userid}")
            return {"code": 400, "success": False, "msg": "Mintnft maintenance, will be restored later."}

        ## 屏蔽用户多次请求 600
        redis_pending = await get_redis_data(f"pending:{userid}:mintnft")
        if redis_pending:
            return {"code": 400, "success": False, "msg": "Please wait for the last completion"}
        await set_redis_data(f"pending:{userid}:mintnft", value=1, ex=600)

        # Check if the userid already exists
        check_query = " SELECT address FROM wenda_users WHERE userid=%s "
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if not existing_user:
            logger.error(f"STATUS: 400 ERROR: User not found - {userid}")
            return {"code": 400, "success": False, "msg": "User not found"}
        ## 是否绑定地址
        address = existing_user['address'].lower()
        logger.debug(f"address: {address}")
        if address == '':
            logger.error(f"STATUS: 400 ERROR: Please bind the address first - {userid}")
            return {"code": 400, "success": False, "msg": "Please bind the address first"}
        
        ## 查询NFT持有状态
        # Check if the address already exists
        check_query = " SELECT id FROM wenda_nft_onchain WHERE tx_chainid=%s and tx_address COLLATE utf8mb4_general_ci=%s AND status=1 order by id desc limit 1 "
        values = (chainid, address)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            logger.error(f"STATUS: 400 ERROR: The address already has NFT - {userid}")
            return {"code": 400, "success": False, "msg": "The address already has NFT"}

        ## 开始铸造NFT B0

        # 发送到 kafka
        value = {
            "userid": userid,
            "chainid": chainid,
            "address": address,
        }
        if APP_CONFIG['startup'] == 'kafka':
            if "mintnft" in KAFKA_CONFIG['topiclist']:  ## 发送到生产者队列 - 划转 kafka-produce-topic-mintnft
                await kafka_send_produce("mintnft", value)
            else:  ## 添加到后台任务
                background_tasks.add_task(sync_nft_mintnft, value=value)
        elif APP_CONFIG['startup'] == 'background':  ## 添加到后台任务
            background_tasks.add_task(sync_nft_mintnft, value=value)
        elif APP_CONFIG['startup'] == 'direct':  ## 直接调用
            await async_nft_mintnft(value)
        
        ## redis 删除
        await del_redis_data(f"box:{userid}:session")
        
        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": "Waiting for minting to complete"
        }
    except Exception as e:
        logger.error(f"/api/auth/mintnft - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": "Server error"}


@router.get("/referral-history")  # ?page1&limit=10
async def referral_history(page: int | None = 1, limit: int | None = 10, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """钱包邀请记录"""
    logger.info(f"POST /api/auth/referral-history - {userid}")
    if cursor is None:
        logger.error(f"/api/auth/referral-history - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": "Server error"}

    try:
        ## 查询用户基础信息
        sessioninfo = await get_redis_data(f"box:{userid}:session")
        logger.debug(f"redis sessioninfo: {sessioninfo}")
        if sessioninfo is None:
            # Check if the userid already exists
            check_query = "SELECT userid,username,email,address,social_dc,social_x,state FROM wenda_users WHERE userid=%s"
            values = (userid,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            sessioninfo = await cursor.fetchone()
            # logger.debug(f"sessioninfo: {sessioninfo}")
            sessioninfo = convert_row_to_dict(sessioninfo, cursor.description)  # 转换字典
            logger.debug(f"sessioninfo: {sessioninfo}")
            ## redis 设置 sessioninfo
            await set_redis_data(f"box:{userid}:session", value=json.dumps(sessioninfo), ex=300)
        if sessioninfo is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}

        address = sessioninfo.get('address', '').lower()
        if not address or len(address) != 42 or address[:2] != '0x':
            return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": [],
                "total": 0,
            }
        logger.info(f"address: {address}")

        # referral_count
        referral_count = await get_redis_data(f"box:{userid}:referral:count")
        logger.debug(f"redis referral_count: {referral_count}")
        if not referral_count:
            ## 根据address查询子账号购买数量
            check_query = """
                            SELECT 
                                COUNT(DISTINCT tx_hash) as len
                            FROM wenda_nft_onchain 
                            WHERE par_address COLLATE utf8mb4_general_ci=%s AND status=1 AND nft_boxid>0
                            """
            values = (address,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            referral_info = await cursor.fetchone()
            # logger.debug(f"mysql referral_info: {referral_info}")
            referral_info = convert_row_to_dict(referral_info, cursor.description)  # 转换字典
            logger.debug(f"mysql referral_info: {referral_info}")
            referral_count = int(referral_info['len']) if referral_info else 0
            logger.debug(f"mysql referral_count: {referral_count}")
            ## redis 设置 referral_points
            await set_redis_data(f"box:{userid}:referral:count", value=referral_count, ex=300)
        if referral_count == 0:
            return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": [],
                "total": 0,
            }
        
        if page == 0: page = 1
        # referral_list
        referral_list = await get_redis_data(f"box:{userid}:referral:list:{page}:{limit}")
        logger.debug(f"redis referral_list: {referral_list}")
        if not referral_list:
            ## 根据address查询子账号购买金额
            check_query = """
                            SELECT 
                                MIN(nft_boxid) as type,
                                MIN(tx_date) as date,
                                MIN(tx_address) as address,
                                MIN(tx_chainid) as chainid,
                                tx_hash,
                                ROUND(SUM(tx_amount_eth),6) as amount_eth,
                                ROUND(SUM(tx_amount_sxp),6) as amount_sxp,
                                COUNT(tx_hash) as count
                            FROM wenda_nft_onchain 
                            WHERE par_address COLLATE utf8mb4_general_ci=%s AND status=1 AND nft_boxid>0
                            GROUP BY tx_hash
                            ORDER BY date DESC
                            LIMIT %s, %s
                            """
            values = (address, (page - 1) * limit, limit,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            referral_list = await cursor.fetchall()
            # logger.debug(f"mysql referral_list: {referral_list}")
            if isinstance(referral_list, (list, tuple)) and len(referral_list) > 0: # 转换字典列表
                converted_list = []
                for row in referral_list: # 将每一行转换为字典
                    row_dict = convert_row_to_dict(row, cursor.description)  # 转换字典
                    formatted_row = format_datetime_fields(row_dict)  # DATETIME转字符串
                    converted_list.append(formatted_row)
                referral_list = converted_list
            logger.debug(f"mysql referral_list: {referral_list}")
            
            ## redis 设置 referral_list
            await set_redis_data(f"box:{userid}:referral:list:{page}:{limit}", value=json.dumps(referral_list), ex=300)
        
        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": referral_list if referral_list else [],
            "total": referral_count,
        }
    except Exception as e:
        logger.error(f"/api/auth/referral-history - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": "Server error"}


# 查询2次
@router.get("/session")  # {}
async def session(userid: Dict = Depends(get_current_userid), cursor=Depends(get_db)):
    """获取用户账户信息"""
    logger.info(f"GET /api/auth/session - {userid}")
    if cursor is None:
        logger.error(f"/api/auth/session - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        ## 查询用户基础信息
        sessioninfo = await get_redis_data(f"box:{userid}:session")
        logger.debug(f"redis sessioninfo: {sessioninfo}")
        if sessioninfo is None:
            # Check if the userid already exists
            check_query = "SELECT userid,username,email,address,social_dc,social_x,state FROM wenda_users WHERE userid=%s"
            values = (userid,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            sessioninfo = await cursor.fetchone()
            # logger.debug(f"sessioninfo: {sessioninfo}")
            sessioninfo = convert_row_to_dict(sessioninfo, cursor.description)  # 转换字典
            logger.debug(f"sessioninfo: {sessioninfo}")
            ## redis 设置 sessioninfo
            await set_redis_data(f"box:{userid}:session", value=json.dumps(sessioninfo), ex=300)
        if sessioninfo is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        
        return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": {
                    "uid": sessioninfo['userid'],
                    "name": sessioninfo['username'],
                    "email": sessioninfo['email'],
                    "address": sessioninfo['address'],
                    "social_dc": sessioninfo['social_dc'],
                    "social_x": sessioninfo['social_x'],
                },
            }
    except Exception as e:
        logger.error(f"/api/auth/session - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}
