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

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, EmailStr, Field

from config import APP_CONFIG, KAFKA_CONFIG, DB_ENGINE, JWT_CONFIG
from utils.bearertoken import md58, create_access_token, decode_access_token
from utils.captcha import validate_captcha
from utils.db import get_db, format_query_for_db
from utils.email import send_activation_mail, send_reset_mail
from utils.i18n import get_text
from utils.kafka.produce import kafka_send_produce
from utils.local import generate_userid, generate_register_code
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
    register_code: str  # = Field(..., description="register code", pattern=r"(^[0-9a-zA-Z]{6}$){0,1}")
    recaptcha_token: str
@router.post("/register")  # {email,username,password,register_code,recaptcha_token}
async def register(post_request: AuthRegiterRequest, background_tasks: BackgroundTasks, cursor=Depends(get_db)):
    """商户注册 一户一码"""
    logger.info(f"POST /api/auth/register - {post_request.username} {post_request.email}")
    if cursor is None:
        logger.error(f"/api/auth/register - {post_request.username} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    email = post_request.email
    REGEX_PATTERN = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$"
    if not (re.search(REGEX_PATTERN, email)):  # 邮箱格式判断
        logger.error(f"Invalid email: {email}")
        return {"code": 401, "success": False, "msg": f"Invalid email"}

    username = post_request.username
    if len(username) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
    
    password = post_request.password
    if len(password) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    else:
        ## 判断密码是不是32/64位哈希,不是则对密码格式进行校验 [ 长度至少8个; 必须含有:大写字母/小写字母/数字/特殊字符@#$%^&+= ]
        if len(password) != 32 and len(password) != 64:
            pattern = r'^.*(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*(),.?":{}|<>]).*$'
            result = re.findall(pattern, password)
            if not (result):
                logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
            ### 密码格式满足规则进行哈希运算
            password = hashlib.sha256(str(password).encode()).hexdigest()[:20]
        ## 判断密码是不是32/64位哈希,是则截取前20位作为密码
        else:
            password = password[:20]

    recaptcha_token = post_request.recaptcha_token
    logger.debug(f"recaptcha_token: {recaptcha_token}")
    ## captcha_token 图形验证码校验
    if len(recaptcha_token) > 256 or (APP_CONFIG['level'] != 'debug'):  # debug不校验
        res_captcha = validate_captcha(recaptcha_token)
        if res_captcha is None:
            logger.error(f"STATUS: 403 ERROR: Captcha exception")
            return {"code": 403, "success": False, "msg": f"Captcha exception"}
        elif res_captcha['success'] == False:
            logger.error(f"STATUS: 401 ERROR: Invalid captcha - {res_captcha['error-codes'][0]}")
            return {"code": 401, "success": False, "msg": f"Invalid captcha"}

    try:
        register_code = post_request.register_code
        if len(register_code) != 5:
            logger.error(f"STATUS: 401 ERROR: Invalid registercode - {register_code}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_REGISTER_CODE')}
        else:
            logger.debug(f"register_code: {register_code}")
            ## 根据 register_code 查询注册码是否存在
            check_query = "SELECT 1 FROM wenda_users WHERE register_code=%s AND userid='' "
            values = (register_code,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_registercode = await cursor.fetchone()
            logger.debug(f"existing_registercode: {existing_registercode}")
            if existing_registercode is None:  # 注册码不存在 或 已使用,注册无效
                logger.error(f"STATUS: 401 ERROR: Invalid registercode {register_code}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_REGISTER_CODE')}
            # 如果是元组，转换为字典
            if isinstance(existing_registercode, tuple):
                existing_registercode = dict(zip([desc[0] for desc in cursor.description], existing_registercode))
            elif hasattr(existing_registercode, 'keys'):
                existing_registercode = dict(existing_registercode)
            logger.debug(f"existing_registercode: {existing_registercode}")

        # Check if the email already exists
        check_query = "SELECT email FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Email already registered - {email}")
            return {"code": 400, "success": False, "msg": "Email already registered"}

        # Check if the username already exists
        check_query = "SELECT username FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Username already registered - {username}")
            return {"code": 400, "success": False, "msg": "Username already registered"}

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
            logger.debug(f"existing_user: {existing_user}")
            if existing_user is None:
                break
            # 如果是元组，转换为字典
            if isinstance(existing_user, tuple):
                existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
            elif hasattr(existing_user, 'keys'):
                existing_user = dict(existing_user)
            logger.debug(f"existing_user: {existing_user}")
            retry_count += 1
        else:
            logger.error("Failed to generate a userid after maximum retries.")
            return {"code": 500, "success": False, "msg": "userid generation failed"}
        logger.debug(f"email: {email} => userid: {userid}")
        
        hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        logger.debug(f"username: {username} hashed_password: {hashed_password}")

        email_state = "UNVERIFIED"
        update_query = "UPDATE wenda_users SET userid=%s, email=%s, username=%s, password=%s, state=%s, updated_time=NOW() WHERE register_code=%s AND userid='' "
        values = (userid, email, username, hashed_password, email_state, register_code,)
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
        verify_url = APP_CONFIG["apibase"] + "/api/validate/token?token=" + verify_token
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

    password = post_request.password
    if len(password) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    else:
        ## 判断密码是不是32/64位哈希,不是则对密码格式进行校验 [ 长度至少8个; 必须含有:大写字母/小写字母/数字/特殊字符@#$%^&+= ]
        if len(password) != 32 and len(password) != 64:
            pattern = r'^.*(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*(),.?":{}|<>]).*$'
            result = re.findall(pattern, password)
            if not (result):
                logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
            ### 密码格式满足规则进行哈希运算
            password = hashlib.sha256(str(password).encode()).hexdigest()[:20]
        ## 判断密码是不是32/64位哈希,是则截取前20位作为密码
        else:
            password = password[:20]

    recaptcha_token = post_request.recaptcha_token
    ## captcha_token 图形验证码校验
    if len(recaptcha_token) > 256 or (APP_CONFIG['level'] != 'debug'):  # debug不校验
        res_captcha = validate_captcha(recaptcha_token)
        if res_captcha is None:
            logger.error(f"STATUS: 403 ERROR: Captcha exception")
            return {"code": 403, "success": False, "msg": f"Captcha exception"}
        elif res_captcha['success'] == False:
            logger.error(f"STATUS: 401 ERROR: Invalid captcha - {res_captcha['error-codes'][0]}")
            return {"code": 401, "success": False, "msg": f"Invalid captcha"}

    try:
        # Check if the username or email already exists
        REGEX_PATTERN = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$"
        if re.search(REGEX_PATTERN, username):  # 邮箱格式判断
            check_query = "SELECT userid,username,password FROM wenda_users WHERE email=%s"
        else:
            check_query = "SELECT userid,username,password FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")

        logger.debug(f'username: {username} password: {password.encode("utf-8")} existing_user["password"]: {existing_user["password"].encode("utf-8")}')
        if bcrypt.checkpw(password.encode("utf-8"), existing_user["password"].encode("utf-8"),):
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
    REGEX_PATTERN = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$"
    if not (re.search(REGEX_PATTERN, email)):  # 邮箱格式判断
        logger.error(f"Invalid email: {email}")
        return {"code": 401, "success": False, "msg": f"Invalid email"}

    recaptcha_token = post_request.recaptcha_token
    ## captcha_token 图形验证码校验
    if len(recaptcha_token) > 256 or (APP_CONFIG['level'] != 'debug'):  # debug不校验
        res_captcha = validate_captcha(recaptcha_token)
        if res_captcha is None:
            logger.error(f"STATUS: 403 ERROR: Captcha exception")
            return {"code": 403, "success": False, "msg": f"Captcha exception"}
        elif res_captcha['success'] == False:
            logger.error(f"STATUS: 401 ERROR: Invalid captcha - {res_captcha['error-codes'][0]}")
            return {"code": 401, "success": False, "msg": f"Invalid captcha"}

    try:
        # Check if the email already exists
        check_query = "SELECT username,updated_time,unix_timestamp(cooldown_time) as cooldown FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid email - {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        for key, value in existing_user.items(): # DATETIME转字符串
            if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                existing_user[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, Decimal):
                existing_user[key] = int(value)
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
                if existing_user['updated_time'] is not None:
                    timestamp = existing_user['updated_time'] # str(dt.timestamp(existing_user['updated_time']))
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
                logger.error(f"STATUS: 402 ERROR: Too frequent, please try again later - cooldownSecord: {3600 - cooldownSecord}")
                return {
                    "code": 402,
                    "success": False,
                    "msg": "Too frequent, please try again later",
                }
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

    password = post_request.password_new
    if len(password) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    else:
        ## 判断密码是不是32/64位哈希,不是则对密码格式进行校验 [ 长度至少8个; 必须含有:大写字母/小写字母/数字/特殊字符@#$%^&+= ]
        if len(password) != 32 and len(password) != 64:
            pattern = r'^.*(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*(),.?":{}|<>]).*$'
            result = re.findall(pattern, password)
            if not (result):
                logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
            ### 密码格式满足规则进行哈希运算
            password = hashlib.sha256(str(password).encode()).hexdigest()[:20]
        ## 判断密码是不是32/64位哈希,是则截取前20位作为密码
        else:
            password = password[:20]
    password_new = password

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
            check_query = "SELECT userid,username,updated_time FROM wenda_users WHERE email=%s"
            values = (email,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_user = await cursor.fetchone()
            logger.debug(f"existing_user: {existing_user}")
            # 如果是元组，转换为字典
            if isinstance(existing_user, tuple):
                existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
            elif hasattr(existing_user, 'keys'):
                existing_user = dict(existing_user)
            for key, value in existing_user.items(): # DATETIME转字符串
                if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                    existing_user[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(value, Decimal):
                    existing_user[key] = int(value)
            logger.debug(f"existing_user: {existing_user}")

            if existing_user:
                # key 有效性校验
                timestamp = APP_CONFIG['key']
                if existing_user['updated_time'] is not None:
                    timestamp = existing_user['updated_time'] # str(dt.timestamp(existing_user['updated_time']))
                    logger.debug(f"timestamp: {timestamp}")
                sql_key = md58(timestamp)
                logger.debug(f"sql.key: {sql_key} jwt.key: {jwt_key}")
                if sql_key != jwt_key:
                    return {"code": 401, "success": False, "msg": get_text('INVALID_JWT_TOKEN')}

                logger.debug(f"username: {existing_user['username']} password_new: {password_new}")
                hashed_password_new = bcrypt.hashpw(password_new.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                logger.debug(f"username: {existing_user['username']} hashed_password_new: {hashed_password_new}")
                ## 更新账户密码
                update_query = "UPDATE wenda_users set password=%s,updated_time=NOW(),cooldown_time=NOW() where userid=%s"
                values = (hashed_password_new, existing_user['userid'],)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

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

    password = post_request.password
    password_new = post_request.password_new
    if len(password) < 6 or len(password_new) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    else:
        ## 判断密码是不是32/64位哈希,不是则对密码格式进行校验 [ 长度至少8个; 必须含有:大写字母/小写字母/数字/特殊字符@#$%^&+= ]
        if len(password) != 32 and len(password) != 64:
            pattern = r'^.*(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*(),.?":{}|<>]).*$'
            result = re.findall(pattern, password_new)
            if not (result):
                logger.error(f"STATUS: 401 ERROR: Invalid password_new - {password_new} {userid}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
            ### 密码格式满足规则进行哈希运算
            password = hashlib.sha256(str(password).encode()).hexdigest()[:20]
            password_new = hashlib.sha256(str(password_new).encode()).hexdigest()[:20]
        ## 判断密码是不是32/64位哈希,是则截取前20位作为密码
        else:
            password = password[:20]
            password_new = password_new[:20]

    try:
        # Check if the userid already exists
        check_query = "SELECT username,password,unix_timestamp(cooldown_time) as cooldown FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")

        if bcrypt.checkpw(password.encode("utf-8"), existing_user['password'].encode("utf-8"),):
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
                hashed_password_new = bcrypt.hashpw(password_new.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                logger.debug(f"username: {existing_user['username']} hashed_password_new: {hashed_password_new}")
                ## 更新账户密码
                update_query = "UPDATE wenda_users set password=%s,updated_time=NOW(),cooldown_time=NOW() where userid=%s"
                values = (hashed_password_new, userid,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Password changed successfully, please login again.",
                }
            else:
                logger.error(f"STATUS: 402 ERROR: Too frequent, please try again later - cooldownSecord: {3600 - cooldownSecord}")
                return {
                    "code": 402,
                    "success": False,
                    "msg": "Too frequent, please try again later",
                }
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
    REGEX_PATTERN = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$"
    if not (re.search(REGEX_PATTERN, email)):  # 邮箱格式判断
        logger.error(f"Invalid email: {email} - {userid}")
        return {"code": 401, "success": False, "msg": f"Invalid email"}

    password = post_request.password
    if len(password) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    else:
        ## 判断密码是不是32/64位哈希,不是则对密码格式进行校验 [ 长度至少8个; 必须含有:大写字母/小写字母/数字/特殊字符@#$%^&+= ]
        if len(password) != 32 and len(password) != 64:
            pattern = r'^.*(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*(),.?":{}|<>]).*$'
            result = re.findall(pattern, password)
            if not (result):
                logger.error(f"STATUS: 401 ERROR: Invalid password - {password} {userid}")
                return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
            ### 密码格式满足规则进行哈希运算
            password = hashlib.sha256(str(password).encode()).hexdigest()[:20]
        ## 判断密码是不是32/64位哈希,是则截取前20位作为密码
        else:
            password = password[:20]

    try:
        # 查询邮箱是否已使用
        check_query = "SELECT userid,username FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            if existing_user['userid'].strip() == userid:
                logger.debug(f"The email is the same and does not need to be modified. - {existing_user['username']} - {email}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "The email is the same and does not need to be modified.",
                }
            logger.error(f"STATUS: 400 ERROR: Email already registered - {existing_user['username']} - {email}")
            return {"code": 400, "success": False, "msg": "Email already registered"}

        # 查询token里的UID是否真实存在
        check_query = "SELECT email,username,password,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid} - {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")

        if bcrypt.checkpw(password.encode("utf-8"), existing_user['password'].encode("utf-8"),):
            if existing_user['state'] == "UNVERIFIED":
                logger.debug(f"username: {existing_user['username']} - {existing_user['email']} => email: {email}")
                ## 更新账户邮箱
                update_query = "UPDATE wenda_users set email=%s, updated_time=NOW() where userid=%s"
                values = (email, userid,)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                logger.debug(f"STATUS: 200 Success: Email changed successfully, please verify again. - {existing_user['username']} - {email}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Email changed successfully, please verify again.",
                }
            else:
                logger.error(f"STATUS: 400 ERROR: Email already verified - {existing_user['username']} - {existing_user['email']} => {email}")
                return {"code": 400, "success": False, "msg": "Email already verified"}
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid password - {userid} {password} => {email}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_PASSWORD')}
    except Exception as e:
        logger.error(f"/api/auth/change-email - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新1次
class AuthChangeUsernameRequest(BaseModel):
    username: str
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
        ## 查询用户名是否已使用
        check_query = "SELECT userid FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")
        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Username already exists - {username}")
            return {"code": 400, "success": False, "msg": "Username already exists"}
        # 如果是元组，转换为字典
        if isinstance(existing_user, tuple):
            existing_user = dict(zip([desc[0] for desc in cursor.description], existing_user))
        elif hasattr(existing_user, 'keys'):
            existing_user = dict(existing_user)
        logger.debug(f"existing_user: {existing_user}")

        ## 查询用户基础信息
        # Check if the userid already exists
        check_query = "SELECT userid,username,email,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        sessioninfo = await cursor.fetchone()
        logger.debug(f"sessioninfo: {sessioninfo}")
        if sessioninfo is None:
            logger.error(f"STATUS: 401 ERROR: Invalid data - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}
        # 如果是元组，转换为字典
        if isinstance(sessioninfo, tuple):
            sessioninfo = dict(zip([desc[0] for desc in cursor.description], sessioninfo))
        elif hasattr(sessioninfo, 'keys'):
            sessioninfo = dict(sessioninfo)
        logger.debug(f"sessioninfo: {sessioninfo}")

        if sessioninfo['username'] != username:
            ## 更新用户名
            update_query = "UPDATE wenda_users set username=%s, updated_time=NOW() where userid=%s"
            values = (username, userid,)
            update_query = format_query_for_db(update_query)
            logger.debug(f"update_query: {update_query} values: {values}")
            await cursor.execute(update_query, values)
            if DB_ENGINE == "sqlite": cursor.connection.commit()
            else: await cursor.connection.commit()

            logger.debug(f"STATUS: 200 Success: Username changed successfully, please verify again. - {username}")
            return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": "Username changed successfully, please verify again.",
            }
        else:
            logger.error(f"STATUS: 400 ERROR: Username already modified - {username}")
            return {"code": 400, "success": False, "msg": "Username already modified"}

    except Exception as e:
        logger.error(f"/api/auth/change-username - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


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
        # Check if the userid already exists
        check_query = "SELECT userid,username,email,state FROM wenda_users WHERE userid=%s"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        sessioninfo = await cursor.fetchone()
        logger.debug(f"sessioninfo: {sessioninfo}")
        if sessioninfo is None: # 2
            logger.error(f"STATUS: 401 ERROR: Invalid data - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}
        # 如果是元组，转换为字典
        if isinstance(sessioninfo, tuple):
            sessioninfo = dict(zip([desc[0] for desc in cursor.description], sessioninfo))
        elif hasattr(sessioninfo, 'keys'):
            sessioninfo = dict(sessioninfo)
        logger.debug(f"sessioninfo: {sessioninfo}")
        
        if sessioninfo:
            return {
                "code": 200,
                "success": True,
                "msg": "Success",
                "data": {
                    "uid": sessioninfo['userid'],
                    "name": sessioninfo['username'],
                    "email": sessioninfo['email'],
                },
            }
        else:
            logger.error(f"STATUS: 401 ERROR: Invalid userid - {userid}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
    except Exception as e:
        logger.error(f"/api/auth/session - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}
