import re
import hashlib
import random
import string
import time
import datetime
from datetime import datetime as dt
from typing import Dict
from decimal import Decimal

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from jose import JWTError
from pydantic import BaseModel, EmailStr, Field
from starlette.responses import RedirectResponse

from config import APP_CONFIG, KAFKA_CONFIG, DB_ENGINE
from utils.bearertoken import md58, decode_access_token
from utils.cache import get_redis_data, set_redis_data, del_redis_data
from utils.db import get_db, format_query_for_db
from utils.email import send_newpasswd_mail
from utils.i18n import get_text
from utils.kafka.produce import kafka_send_produce
from utils.security import get_interface_userid
from utils.log import log as logger

router = APIRouter()


## validate

# --------------------------------------------------------------------------------------------------

# 注册验证 查询1次 更新1次
# 重置密码 查询3次 更新3次
@router.get("/token")
async def validate_token( token: str, background_tasks: BackgroundTasks, cursor=Depends(get_db)):
    """校验TOKEN: 注册验证 / 重置密码"""
    logger.info(f"GET /api/validate/token - {token}")
    if cursor is None:
        logger.error(f"/api/validate/token cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}
    credentials_exception = HTTPException(status_code=401, detail="Invalid JWT Token")

    userid = ''
    try:
        # jwt解码
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
                raise credentials_exception

            # 账户是否存在
            check_query = "SELECT id,userid,username,password,updated_time FROM wenda_users WHERE email=%s"
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
                userid = existing_user['userid']
                # key 有效性校验
                timestamp = APP_CONFIG['key']
                if existing_user['updated_time'] != None:
                    timestamp = existing_user['updated_time'] # str(dt.timestamp(existing_user['updated_time']))
                    logger.debug(f"timestamp: {timestamp}")
                sql_key = md58(timestamp)
                logger.debug(f"sql.key: {sql_key} jwt.key: {jwt_key}")
                if sql_key != jwt_key:
                    raise credentials_exception

                ## 生成8位随机密码，并发送邮件
                new_passwd = ''.join(random.sample(string.ascii_letters + string.digits, 8))
                logger.debug(f"new_passwd: {new_passwd}")
                ### 密码格式满足规则进行哈希运算
                new_passwd_hash = hashlib.sha256(str(new_passwd).encode()).hexdigest()[:20]
                logger.debug(f"new_passwd_hash: {new_passwd_hash}")
                ## 哈希加密
                hashed_password = bcrypt.hashpw(new_passwd_hash.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                logger.debug(f"username: {existing_user['username']} hashed_password: {hashed_password}")
                # logger.debug(f"passwd: {new_passwd}")

                ## Send new password email
                
                if APP_CONFIG['startup'] == 'kafka':
                    if "mail" in KAFKA_CONFIG['topiclist']:  ## 发送到生产者队列 - 新密码邮件 kafka-produce-topic-mail
                        value = {
                            "to_email": email,
                            "subject": "newpasswd",
                            "context": new_passwd,
                            "username": existing_user['username'],
                        }
                        await kafka_send_produce("mail", value)
                    else:  ## 添加到后台任务
                        background_tasks.add_task(send_newpasswd_mail,
                                                  to_email=email,
                                                  username=existing_user['username'],
                                                  password=new_passwd,
                                                  )
                elif APP_CONFIG['startup'] == 'background':  ## 添加到后台任务
                    background_tasks.add_task(send_newpasswd_mail,
                                              to_email=email,
                                              username=existing_user['username'],
                                              password=new_passwd,
                                              )
                elif APP_CONFIG['startup'] == 'direct':  ## 直接调用
                    send_newpasswd_mail(existing_user['username'], existing_user['email'], new_passwd)

                ## 更新账户密码
                update_query = "UPDATE wenda_users set password=%s,updated_time=NOW() where userid=%s"
                values = (hashed_password, userid)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(False, f"box:{userid}:pwd")

                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "The new password has been sent to the email"
                }
            else:
                logger.error(f"STATUS: 400 ERROR: Account reset failed - {username}")
                return {"code": 400, "success": False, "msg": "Account reset failed"}
        else:  ## 注册验证
            username = payload.get('username')
            jwt_secret = payload.get('secret')
            if username is None:
                raise credentials_exception

            # 账户是否未验证
            check_query = "SELECT id,userid,username,password FROM wenda_users WHERE username=%s AND state=%s"
            values = (username, "UNVERIFIED")
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
                # secret有效性校验
                userid = existing_user['userid']
                sql_secret = md58(existing_user['password'])
                logger.debug(f"sql.secret: {sql_secret} jwt.secret: {jwt_secret}")
                if sql_secret != jwt_secret:
                    raise credentials_exception

                ## 更新账户注册状态
                update_query = "UPDATE wenda_users set state=%s,updated_time=NOW() where userid=%s"
                values = ("VERIFIED", userid)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                ## redis 删除
                await del_redis_data(False, f"box:{userid}:session")

                logger.debug(f"STATUS: 200 ERROR: Account verification successful - {username}")
                return RedirectResponse(url=APP_CONFIG['appadmin'])
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": "Account verification successful"
                }
            else:
                logger.error(f"STATUS: 400 ERROR: Account already verified - {username}")
                return RedirectResponse(url=APP_CONFIG['appadmin'])
                return {"code": 400, "success": False, "msg": "Account already verified"}
    except JWTError:
        raise credentials_exception
    except Exception as e:
        logger.error(f"/api/validate/token except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次
class ValidateUsernameRequest(BaseModel):
    username: str
@router.post("/username")  # {username}
async def validate_username(post_request: ValidateUsernameRequest, cursor=Depends(get_db)):
    """校验用户名"""
    logger.info(f"POST /api/validate/username - {post_request.username}")
    if cursor is None:
        logger.error(f"/api/validate/username - {post_request.username} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    username = post_request.username
    if len(username) < 6:
        logger.error(f"STATUS: 401 ERROR: Invalid username - {username}")
        return {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}

    try:
        # Check if the username already exists
        check_query = "SELECT userid FROM wenda_users WHERE username=%s"
        values = (username,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")

        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Username already registered - {username}")
            return {"code": 400, "success": False, "msg": "Username already registered"}
        else:
            return {"code": 200, "success": True, "msg": "Success", "data": "Username can be used"}
    except Exception as e:
        logger.error(f"/api/validate/username except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次
class ValidateEmailRequest(BaseModel):
    email: EmailStr | None = 'xxxxxx@gmail.com'
@router.post("/email")  # {email}
async def validate_email(post_request: ValidateEmailRequest, cursor=Depends(get_db)):
    """校验邮箱是否存在"""
    logger.info(f"POST /api/validate/email - {post_request.email}")
    if cursor is None:
        logger.error(f"/api/validate/email - {post_request.email} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    email = post_request.email
    REGEX_PATTERN = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$"
    if not (re.search(REGEX_PATTERN, email)):  # 邮箱格式判断
        logger.error(f"Invalid email: {email}")
        return {"code": 401, "success": False, "msg": f"Invalid email"}

    try:
        # Check if the email already exists
        check_query = "SELECT userid FROM wenda_users WHERE email=%s"
        values = (email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")

        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Email already registered - {email}")
            return {"code": 200, "success": False, "msg": "Email already registered"}
        else:
            return {"code": 200, "success": True, "msg": "Success", "data": "Email can be used"}
    except Exception as e:
        logger.error(f"/api/validate/email except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次  # ok
class ValidateAddressRequest(BaseModel):
    address: str | None = '0x***'
@router.post("/address")  # {address}
async def validate_address(post_request: ValidateAddressRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """校验钱包地址"""
    logger.info(f"POST /api/validate/address - {post_request.address}")
    if not post_request.address:  # None
        logger.error(f"STATUS: 401 ERROR: No wallet address - {address}")
        return {"code": 401, "success": False, "msg": "No wallet address"}

    try:
        address = post_request.address
        if not (len(address) == 42 and address[:2] == '0x'):  # 钱包地址判断
            logger.error(f"STATUS: 401 ERROR: Invalid address - {address}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_ADDRESS')}

        # Check if the address already exists
        check_query = "SELECT userid FROM wenda_users WHERE address COLLATE utf8mb4_general_ci = %s"
        values = (address,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        logger.debug(f"existing_user: {existing_user}")

        if existing_user:
            logger.error(f"STATUS: 400 ERROR: Address already registered - {address}")
            return {"code": 200, "success": False, "msg": "Address already registered"}
        else:
            return {"code": 200, "success": True, "msg": "Success", "data": "Address can be used"}
    except Exception as e:
        logger.error(f"/api/validate/address except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次
class RegisterCodeRequest(BaseModel):
    register_code: str = Field(..., description="register code", pattern=r"(^[0-9a-zA-Z]{5}$){0,1}")
@router.post("/registercode")  # {register_code}
async def validate_registercode(post_request: RegisterCodeRequest, cursor=Depends(get_db)):
    """校验注册码"""
    logger.info(f"POST /api/validate/registercode - {post_request.register_code}")
    if cursor is None:
        logger.error(f"/api/validate/registercode - {post_request.register_code} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        register_code = post_request.register_code
        if len(register_code) != 5:
            logger.error(f"STATUS: 401 ERROR: Invalid registercode - {register_code}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_REGISTER_CODE')}
        logger.debug(f"register_code: {register_code}")
        
        ## 根据 register_code 查询注册码是否存在
        check_query = "SELECT id FROM wenda_users WHERE register_code = %s AND userid='' "
        values = (register_code,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_registercode = await cursor.fetchone()
        logger.debug(f"existing_registercode: {existing_registercode}")
        
        if existing_registercode is None:  # 注册码不存在 或 已使用,注册无效
            logger.error(f"STATUS: 401 ERROR: Invalid registercode {register_code}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_REGISTER_CODE')}
        else:
            return {"code": 200, "success": True, "msg": "Success", "data": "Registercode can be used"}
    except Exception as e:
        logger.error(f"/api/validate/registercode except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

