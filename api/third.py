import base64
import hashlib
import json
import random
import re
import string
import time
import uuid
from datetime import datetime as dt
from typing import Dict

import requests
from authlib.integrations.httpx_client import AsyncOAuth2Client
from fastapi import APIRouter, Depends, Request, BackgroundTasks
from pydantic import BaseModel, EmailStr, Field
from requests_oauthlib import OAuth1Session
from starlette.responses import RedirectResponse

from config import APP_CONFIG, JWT_CONFIG, SOCIAL_CONFIG, DB_ENGINE
from utils.bearertoken import md58, create_access_token
from utils.cache import del_redis_data, get_redis_data, set_redis_data
from utils.db import get_db, format_query_for_db, convert_row_to_dict, format_datetime_fields
from utils.i18n import get_text
from utils.local import generate_userid, generate_registercode, hash_password, validate_email_format
from utils.security import get_interface_userid
from utils.log import log as logger

router = APIRouter()


# third

# 查询0次
@router.get("/google/auth")
async def google_auth():
    """生成google登录授权链接"""
    logger.info(f"GET /api/auth/google/auth")

    try:
        if SOCIAL_CONFIG['google_id'] == "":
            logger.error(f"STATUS: 401 ERROR: Invalid GOOGLE_CLIENT_ID")
            return {"code": 401, "success": False, "msg": get_text('INVALID_GOOGLE_CLIENT_ID')}

        authorization_url = f"https://accounts.google.com/o/oauth2/auth?response_type=code&client_id={SOCIAL_CONFIG['google_id']}&redirect_uri={SOCIAL_CONFIG['google_callback']}&scope=openid%20profile%20email&access_type=offline"
        logger.debug(f"authorization_url: {authorization_url}")

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": {
                "url": authorization_url,
            }
        }
    except Exception as e:
        logger.error(f"/api/auth/google/auth except ERROR: {str(e).splitlines()[0]}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新2次
@router.get("/google/callback")
async def google_callback(code: str, cursor=Depends(get_db)):
    """google用户授权回调,获取用户google信息"""
    logger.info(f"POST /api/auth/google/callback - {code}")
    if cursor is None:
        logger.error(f"/api/auth/google/callback - {code} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        # logger.debug(f"request: {str(request.url)}")
        token_url = "https://accounts.google.com/o/oauth2/token"
        data = {
            "code": code,
            "client_id": SOCIAL_CONFIG['google_id'],
            "client_secret": SOCIAL_CONFIG['google_secret'],
            "redirect_uri": SOCIAL_CONFIG['google_callback'],
            "grant_type": "authorization_code",
        }
        response = requests.post(token_url, data=data)
        logger.debug(f"data: {data}")
        google_access_token = response.json().get('access_token')
        logger.debug(f"google_access_token: {google_access_token}")
        user_url = "https://www.googleapis.com/oauth2/v1/userinfo"
        response = requests.get(user_url, headers={"Authorization": f"Bearer {google_access_token}"})
        user_info = response.json()
        logger.debug(f"user_info: {user_info}")
        if str(user_info).find("email") <= 0:
            logger.error(f"STATUS: 401 ERROR: Invalid google email - {user_info}")
            error = {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
            error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
            return RedirectResponse(url=APP_CONFIG['error'] + error_base64)

        login_email = user_info['email']
        login_username = user_info['name'].replace(' ', '_')
        if len(login_username) < 6:
            login_username += '_' + ''.join(random.sample(string.ascii_letters + string.digits, 6-len(login_username)))

        if not validate_email_format(login_email): # 邮箱格式判断
            logger.error(f"STATUS: 401 ERROR: Invalid google email - {login_email}")
            error = {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}
            error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
            return RedirectResponse(url=APP_CONFIG['error'] + error_base64)

        # Check if the email already exists
        check_query = "SELECT userid,username,password FROM wenda_users WHERE email=%s"
        values = (login_email,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        logger.debug(f"existing_user: {existing_user}")

        if existing_user:  # 账号已存在则登录
            logger.debug(f"jwt_secret: {md58(existing_user['password'])}")
            expire_timestamp = int(time.time()) + JWT_CONFIG['expire']
            access_token = create_access_token({
                    "userid": existing_user['userid'], 
                    "username": existing_user['username'], 
                    "secret": md58(existing_user['password']), 
                    "expire": expire_timestamp
                })
            logger.debug(f"username: {existing_user['username']}  token: {access_token}")

            dashboard_url = APP_CONFIG['redirect'] + access_token + '&path=dashboard'
            logger.debug(f"dashboard_url: {dashboard_url}")
            return RedirectResponse(url=dashboard_url)
        else:  # 账号不存在则注册
            # Check if the username already exists
            check_query = "SELECT userid,username FROM wenda_users WHERE username=%s"
            values = (login_username,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_user = await cursor.fetchone()
            # logger.debug(f"existing_user: {existing_user}")
            existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
            logger.debug(f"existing_user: {existing_user}")
            if existing_user:
                login_username += '_' + ''.join(random.sample(string.ascii_letters + string.digits, 6-len(login_username)))
            logger.debug(f"final login_username: {login_username}")

            ## userid生成逻辑
            userid = None
            retry_count = 0
            while retry_count < 5:
                userid = generate_userid(login_email)
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
            
            logger.debug(f"email: {login_email} userid: {userid} username: {login_username}")

            # google_token_password = google_access_token[:20]
            ### 密码格式满足规则则进行哈希运算
            google_token_password = hashlib.sha256(str(google_access_token[:20]).encode()).hexdigest()[:20]
            hashed_password = hash_password(google_token_password)

            logger.debug(f"jwt_secret: {md58(hashed_password)}")
            expire_timestamp = int(time.time()) + JWT_CONFIG['expire']
            access_token = create_access_token({
                    "userid": userid, 
                    "username": login_username, 
                    "secret": md58(hashed_password),
                    "expire": expire_timestamp
                })
            logger.debug(f"username: {login_username}  token: {access_token}")

            insert_query = "INSERT INTO wenda_users (userid, email, username, password, register_code, state) SELECT %s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM wenda_users WHERE email=%s)"
            values = (userid, login_email, login_username, hashed_password, "GOOGLE", "VERIFIED", login_email,)
            insert_query = format_query_for_db(insert_query)
            logger.debug(f"insert_query: {insert_query} values: {values}")
            await cursor.execute(insert_query, values)
            if DB_ENGINE == "sqlite": cursor.connection.commit()
            else: await cursor.connection.commit()

            dashboard_url = APP_CONFIG['redirect'] + access_token + '&path=dashboard&type=bind'
            logger.debug(f"dashboard_url: {dashboard_url}")
            return RedirectResponse(url=dashboard_url)
    except Exception as e:
        logger.error(f"/api/auth/google/callback - {code} except ERROR: {str(e).splitlines()[0]}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询2次 更新1次
class RegisterCodeRequest(BaseModel):
    register_code: str = Field(..., description="address code", pattern=r"(^[0-9a-zA-Z]{5}$){0,1}")
@router.post("/google/bind-registercode")  # {register_code}
async def google_bind_registercode(post_request: RegisterCodeRequest, userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """绑定注册码"""
    logger.info(f"POST /api/auth/google/bind-registercode - {userid} - {post_request.register_code}")
    if cursor is None:
        logger.error(f"/api/auth/google/bind-registercode - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

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

        ## 根据 用户邮箱、注册时间 判断绑定是否有效
        check_query = "SELECT id,register_code,email,unix_timestamp(created_time) as created FROM wenda_users WHERE userid=%s and register_code='GOOGLE'"
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        existing_user = await cursor.fetchone()
        # logger.debug(f"existing_user: {existing_user}")
        if existing_user is None:
            logger.error(f"Invalid userid - {userid} - GOOGLE")
            return {"code": 401, "success": False, "msg": get_text('INVALID_USERID')}
        existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
        existing_user = format_datetime_fields(existing_user)  # DATETIME转字符串
        logger.debug(f"existing_user: {existing_user}")

        if not validate_email_format(existing_user['email']):  # 邮箱格式判断
            logger.error(f"Invalid email: {existing_user['email']} - {userid} - {register_code}")
            return {"code": 401, "success": False, "msg": get_text('INVALID_EMAIL')}

        ## register_code 注册码生成逻辑
        new_register_code = None
        retry_count = 0
        while retry_count < 5:
            new_register_code = generate_registercode(userid)
            # Check if the register_code already exists
            check_query = "SELECT id FROM wenda_users WHERE register_code=%s"
            values = (new_register_code,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_registercode_new = await cursor.fetchone()
            # logger.debug(f"existing_registercode_new: {existing_registercode_new}")
            if existing_registercode_new is None:
                break
            existing_registercode_new = convert_row_to_dict(existing_registercode_new, cursor.description)  # 转换字典
            logger.debug(f"existing_registercode_new: {existing_registercode_new}")
            retry_count += 1
        else:
            logger.error("Failed to generate a registercode after maximum retries.")
            return {"code": 400, "success": False, "msg": "registercode generation failed"}
        # 新注册码替换原注册码
        update_query = "UPDATE wenda_users set register_code=%s,updated_time=NOW() WHERE id=%s"
        values = (new_register_code, register_id,)
        update_query = format_query_for_db(update_query)
        logger.debug(f"update_query: {update_query} values: {values}")
        await cursor.execute(update_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()

        # 用户更新注册码
        update_query = "UPDATE wenda_users set register_code=%s,updated_time=NOW() WHERE id=%s"
        values = (register_code, existing_user['id'],)
        update_query = format_query_for_db(update_query)
        logger.debug(f"update_query: {update_query} values: {values}")
        await cursor.execute(update_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()

        ## 删除缓存
        await del_redis_data(f"box:{userid}:session")

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": "Successfully bound register code"
        }
    except Exception as e:
        logger.error(f"/api/auth/google/bind-registercode - {userid} except ERROR: {str(e)}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次 插入1次
@router.get("/discord/connect")
async def discord_connect(userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """生成discord用户授权链接"""
    logger.info(f"GET /api/auth/discord/connect - {userid}")
    if cursor is None:
        logger.error(f"/api/auth/discord/connect - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if SOCIAL_CONFIG['discord_id'] == "":
            logger.error(f"STATUS: 401 ERROR: Invalid DISCORD_CLIENT_ID")
            return {"code": 401, "success": False, "msg": get_text('INVALID_DISCORD_CLIENT_ID')}

        ## 根据 userid 查询是否已绑定
        check_query = "SELECT social_uuid,social_action,social_global_name,unix_timestamp(created_time) as created FROM wenda_users_social_dc WHERE userid=%s and social_id!='' and social_uuid!='' and social_action!='' and status>0 order by id desc "
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        users_social = await cursor.fetchone()
        # logger.debug(f"users_social: {users_social}")
        users_social = convert_row_to_dict(users_social, cursor.description)  # 转换字典
        users_social = format_datetime_fields(users_social)  # DATETIME转字符串
        logger.debug(f"users_social: {users_social}")

        social_uuid = ''
        social_type = 'authorize'
        if users_social:
            now_timestamp = dt.now().timestamp()
            created_timestamp = users_social['created']
            createdSecord = now_timestamp - created_timestamp
            logger.debug(f"now_timestamp: {now_timestamp} created_timestamp: {created_timestamp} createdSecord: {createdSecord}")
            ## 用户社交信息表 更新积分记录
            if users_social['social_action'] == '1':  # 已加入
                ## 用户表 更新社交信息记录
                update_query = """
                                UPDATE wenda_users gu
                                JOIN ( SELECT userid,social_dc FROM wenda_users WHERE userid=%s AND social_dc='' ) temp ON gu.userid = temp.userid
                                SET gu.social_dc=%s,gu.updated_time=NOW();
                                """
                values = (userid, users_social.get('social_global_name'),)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                logger.error(f"STATUS: 400 ERROR: Already join to Discord channel - userid: {userid}")
                return {"code": 400, "success": False, "msg": get_text('ALREADY_JOIN_DISCORD_CHANNEL')}
            elif users_social['social_action'] == '0':  # 已授权
                social_type = 'join'
            # elif users_social['created'] < 300: # 已申请, 等待授权
            elif createdSecord < 300:  # 已申请, 等待授权
                social_uuid = users_social['social_uuid']
                logger.info(f"STATUS: 200 ERROR: Please do not apply again - userid: {userid} - {social_uuid}")
        if social_uuid == '':
            social_uuid = str(uuid.uuid4())
        logger.debug(f"social_uuid: {social_uuid} social_type: {social_type}")

        # 未绑定
        client = AsyncOAuth2Client(
            client_id=SOCIAL_CONFIG['discord_id'],
            client_secret=SOCIAL_CONFIG['discord_secret'],
            redirect_uri=SOCIAL_CONFIG['discord_callback'],
            scope='identify guilds',
        )
        authorization_url, state = client.create_authorization_url('https://discord.com/api/oauth2/authorize', state=social_uuid)
        logger.debug(f"authorization_url: {authorization_url}")

        ## 用户社交信息表 增加积分记录
        insert_query = "INSERT INTO wenda_users_social_dc (userid, social_uuid, social_type, status) VALUES (%s,%s,%s,%s)"
        values = (userid, state, social_type, 0,)
        insert_query = format_query_for_db(insert_query)
        logger.debug(f"insert_query: {insert_query} values: {values}")
        await cursor.execute(insert_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": {
                "url": authorization_url,
            }
        }
    except Exception as e:
        logger.error(f"/api/auth/discord/connect - {userid} except ERROR: {str(e).splitlines()[0]}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# authorize / join
@router.get("/discord/callback")
async def discord_callback(request: Request, code: str, state: str, cursor=Depends(get_db)):
    """discord用户授权回调,获取用户discord信息"""
    logger.info(f"POST /api/auth/discord/callback")
    if cursor is None:
        logger.error(f"/api/auth/discord/callback cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        logger.debug(f"request: {str(request.url)}")

        client = AsyncOAuth2Client(
            client_id=SOCIAL_CONFIG['discord_id'],
            client_secret=SOCIAL_CONFIG['discord_secret'],
            redirect_uri=SOCIAL_CONFIG['discord_callback'],
            state=state,
        )
        token = await client.fetch_token('https://discord.com/api/oauth2/token',
                                         grant_type='authorization_code',
                                         authorization_response=str(request.url))
        logger.debug(f"token: {token}")
        API_HEADERS = {
            'Authorization': 'Bearer %s' % token['access_token'],
        }

        ## 根据 social_uuid 更新账户信息
        check_query = "SELECT id,userid,social_type FROM wenda_users_social_dc WHERE social_uuid=%s"
        values = (state,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        users_social = await cursor.fetchone()
        # logger.debug(f"users_social: {users_social}")
        users_social = convert_row_to_dict(users_social, cursor.description)  # 转换字典
        logger.debug(f"users_social: {users_social}")

        if users_social['social_type'] == 'authorize':
            ## 授权成功获取用户个人信息
            user_response = await client.get('https://discord.com/api/users/@me', headers=API_HEADERS)
            if user_response.status_code == 200:
                user_data = user_response.json()
                logger.debug(f"user_data: {user_data}")

                social_global_name = user_data.get('username', None)
                if social_global_name is None:
                    logger.error(f"STATUS: 401 ERROR: Invalid username - user_data: {user_data}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                social_name = user_data.get('global_name', social_global_name)
                
                ## 根据 social_id 查询是否已绑定
                check_query = "SELECT id,userid,status FROM wenda_users_social_dc WHERE social_id=%s and status>0"
                values = (user_data.get('id'),)
                check_query = format_query_for_db(check_query)
                logger.debug(f"check_query: {check_query} values: {values}")
                await cursor.execute(check_query, values)
                users_socialid = await cursor.fetchone()
                # logger.debug(f"users_socialid: {users_socialid}")
                if users_socialid:
                    logger.error(f"STATUS: 400 ERROR: Already bound to Discord - social_id: {user_data['id']}")
                    error = {"code": 400, "success": False, "msg": get_text('ALREADY_BOUND_DISCORD')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                users_socialid = convert_row_to_dict(users_socialid, cursor.description)  # 转换字典
                logger.debug(f"users_socialid: {users_socialid}")

                if users_social:
                    # https://cdn.discordapp.com/avatars/1086374091071696978/5693ae76f81199edcb4c378ba1382437.webp?size=48
                    avatar_url = f"https://cdn.discordapp.com/avatars/{user_data['id']}/{user_data['avatar']}.webp?size=48"
                    ## 用户社交信息表 更新积分记录
                    update_query = "UPDATE wenda_users_social_dc set social_id=%s,social_name=%s,social_global_name=%s,social_avatar=%s,social_locale=%s,social_action=%s,status=%s,updated_time=NOW() where id=%s"
                    values = (user_data.get('id'), social_name, social_global_name, avatar_url, user_data.get('locale'), '0', 2, users_social['id'])
                    update_query = format_query_for_db(update_query)
                    logger.debug(f"update_query: {update_query} values: {values}")
                    await cursor.execute(update_query, values)
                    if DB_ENGINE == "sqlite": cursor.connection.commit()
                    else: await cursor.connection.commit()

                    ## 用户表 更新社交信息记录
                    update_query = "UPDATE wenda_users set social_dc=%s,updated_time=NOW() where userid=%s"
                    values = (social_global_name, users_social['userid'],)
                    update_query = format_query_for_db(update_query)
                    logger.debug(f"update_query: {update_query} values: {values}")
                    await cursor.execute(update_query, values)
                    if DB_ENGINE == "sqlite": cursor.connection.commit()
                    else: await cursor.connection.commit()

                    ## redis 删除
                    await del_redis_data(f"box:{users_social['userid']}:session")

                    return RedirectResponse(url=APP_CONFIG['appbase'])
                    return {
                        "code": 200,
                        "success": True,
                        "msg": "Success",
                        "data": "Authorization successful"
                    }
                else:
                    logger.error(f"STATUS: 401 ERROR: Invalid social_uuid - {state}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_SOCIAL_UUID')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
            else:
                logger.debug(f"user_response: {user_response}")
                logger.error(f"STATUS: {user_response.status_code} ERROR: Failed to fetch user details")
                error = {"code": user_response.status_code, "success": False, "msg": get_text('FAILED_FETCH_USER_DETAILS')}
                error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                logger.debug(f"error_base64: {error_base64}")
                return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
        elif users_social['social_type'] == 'join':
            ## 授权成功获取用户个人信息
            user_response = await client.get('https://discord.com/api/users/@me', headers=API_HEADERS)
            if user_response.status_code == 200:
                user_data = user_response.json()
                logger.debug(f"user_data: {user_data}")
                ## 授权成功获取用户频道信息
                guilds_response = await client.get('https://discord.com/api/users/@me/guilds', headers=API_HEADERS)
                if guilds_response.status_code == 200:
                    guilds_data = guilds_response.json()
                    logger.debug(f"guilds_data: {guilds_data}")

                    social_global_name = user_data.get('username', None)
                    if social_global_name is None:
                        logger.error(f"STATUS: 401 ERROR: Invalid username - user_data: {user_data}")
                        error = {"code": 401, "success": False, "msg": get_text('INVALID_USERNAME')}
                        error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                        return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                    social_name = user_data.get('global_name', social_global_name)
                    
                    # guilds = [guild for guild in guilds_data if guild['permissions'] == 2147483647]
                    join_status = 0
                    for guild in guilds_data:
                        if guild['id'] == '1264832793536630816' or guild['name'] == 'GAEA':
                            join_status = 1

                    if not join_status:
                        logger.error(f"STATUS: 404 ERROR: Please join Discord channel - username: {user_data['username']} len(guilds_data): {len(guilds_data)}")
                        error = {"code": 401, "success": False, "msg": get_text('PLEASE_JOIN_DISCORD_CHANNEL')}
                        error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                        return RedirectResponse(url=APP_CONFIG['error'] + error_base64)

                    ## 根据 social_id 查询是否已绑定
                    check_query = "SELECT id,userid,status FROM wenda_users_social_dc WHERE social_id=%s and social_type='authorize'"
                    values = (user_data.get('id'),)
                    check_query = format_query_for_db(check_query)
                    logger.debug(f"check_query: {check_query} values: {values}")
                    await cursor.execute(check_query, values)
                    users_socialid = await cursor.fetchone()
                    # logger.debug(f"users_socialid: {users_socialid}")
                    if not users_socialid:
                        logger.error(f"STATUS: 404 ERROR: Please authorize first - social_id: {user_data['id']}")
                        error = {"code": 404, "success": False, "msg": "Please authorize first"}
                        error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                        return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                    users_socialid = convert_row_to_dict(users_socialid, cursor.description)  # 转换字典
                    logger.debug(f"users_socialid: {users_socialid}")

                    if users_social:
                        # https://cdn.discordapp.com/avatars/1086374091071696978/5693ae76f81199edcb4c378ba1382437.webp?size=48
                        avatar_url = f"https://cdn.discordapp.com/avatars/{user_data['id']}/{user_data['avatar']}.webp?size=48"
                        ## 用户社交信息表 更新积分记录
                        update_query = "UPDATE wenda_users_social_dc set social_id=%s,social_name=%s,social_global_name=%s,social_avatar=%s,social_locale=%s,social_action=%s,status=%s,updated_time=NOW() where id=%s"
                        values = (user_data.get('id'), social_name, social_global_name, avatar_url, user_data.get('locale'), str(join_status), 2, users_social['id'])
                        update_query = format_query_for_db(update_query)
                        logger.debug(f"update_query: {update_query} values: {values}")
                        await cursor.execute(update_query, values)
                        if DB_ENGINE == "sqlite": cursor.connection.commit()
                        else: await cursor.connection.commit()
                        return RedirectResponse(url=APP_CONFIG['appbase'])
                        return {
                            "code": 200,
                            "success": True,
                            "msg": "Success",
                            "data": "Join successful"
                        }
                    else:
                        logger.error(f"STATUS: 401 ERROR: Invalid social_uuid - {state}")
                        error = {"code": 401, "success": False, "msg": get_text('INVALID_SOCIAL_UUID')}
                        error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                        return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                else:
                    logger.debug(f"guilds_response: {guilds_response}")
                    logger.error(f"STATUS: {guilds_response.status_code} ERROR: Failed to fetch user details")
                    error = {"code": guilds_response.status_code, "success": False, "msg": get_text('FAILED_FETCH_USER_DETAILS')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    logger.debug(f"error_base64: {error_base64}")
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
            else:
                logger.debug(f"user_response: {user_response}")
                logger.error(f"STATUS: {user_response.status_code} ERROR: Failed to fetch user details")
                error = {"code": user_response.status_code, "success": False, "msg": get_text('FAILED_FETCH_USER_DETAILS')}
                error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                logger.debug(f"error_base64: {error_base64}")
                return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
    except Exception as e:
        logger.error(f"/api/auth/discord/callback except ERROR: {str(e).splitlines()[0]}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# 查询1次 插入1次
@router.get("/x/connect")
async def twitter_connect(userid: Dict = Depends(get_interface_userid), cursor=Depends(get_db)):
    """生成twitter用户授权链接"""
    logger.info(f"GET /api/auth/x/connect - {userid}")
    if cursor is None:
        logger.error(f"/api/auth/x/connect - {userid} cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        if SOCIAL_CONFIG['twitter_key'] == "":
            logger.error(f"STATUS: 401 ERROR: Invalid TWITTER_CONSUMER_KEY")
            return {"code": 401, "success": False, "msg": get_text('INVALID_TWITTER_CONSUMER_KEY')}

        ## 根据 social_id 查询是否已绑定
        check_query = "SELECT social_id,social_uuid,social_action,social_global_name,unix_timestamp(created_time) as created FROM wenda_users_social_x WHERE userid=%s and social_id!='' and social_uuid!='' and social_action!='' and status>0 order by id desc "
        values = (userid,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        users_social = await cursor.fetchone()
        # logger.debug(f"users_social: {users_social}")
        users_social = convert_row_to_dict(users_social, cursor.description)  # 转换字典
        users_social = format_datetime_fields(users_social)  # DATETIME转字符串
        logger.debug(f"users_social: {users_social}")

        social_uuid = ''
        social_type = 'authorize'
        if users_social:
            now_timestamp = dt.now().timestamp()
            created_timestamp = users_social['created']
            createdSecord = now_timestamp - created_timestamp
            logger.debug(f"now_timestamp: {now_timestamp} created_timestamp: {created_timestamp} createdSecord: {createdSecord}")
            ## 用户社交信息表 更新积分记录
            if users_social['social_action'] == '1':  # 已关注
                ## 用户表 更新社交信息记录
                update_query = """
                                UPDATE wenda_users gu
                                JOIN ( SELECT userid,social_x FROM wenda_users WHERE userid=%s AND social_x='' ) temp ON gu.userid = temp.userid
                                SET gu.social_x=%s,gu.updated_time=NOW();
                                """
                values = (userid, users_social.get('social_global_name'),)
                update_query = format_query_for_db(update_query)
                logger.debug(f"update_query: {update_query} values: {values}")
                await cursor.execute(update_query, values)
                if DB_ENGINE == "sqlite": cursor.connection.commit()
                else: await cursor.connection.commit()

                logger.error(f"STATUS: 400 ERROR: Already follow to Twitter - userid: {userid}")
                return {"code": 400, "success": False, "msg": get_text('ALREADY_FOLLOW_TWITTER')}
            elif users_social['social_action'] == '0':  # 已授权
                social_type = 'follow'
            # elif users_social['created'] < 300: # 已申请, 等待授权
            elif createdSecord < 300:  # 已申请, 等待授权
                social_uuid = users_social['social_uuid']
                logger.info(f"STATUS: 200 ERROR: Please do not apply again - userid: {userid} - {social_uuid}")
                authorization_url = f'https://api.twitter.com/oauth/authenticate?oauth_token={social_uuid}'
                logger.debug(f"authorization_url: {authorization_url}")
                return {
                    "code": 200,
                    "success": True,
                    "msg": "Success",
                    "data": {
                        "url": authorization_url,
                    }
                }
        logger.debug(f"social_uuid: {social_uuid} social_type: {social_type}")

        # 未绑定
        oauth = OAuth1Session(
            client_key=SOCIAL_CONFIG['twitter_key'],
            client_secret=SOCIAL_CONFIG['twitter_secret'],
            callback_uri=SOCIAL_CONFIG['twitter_callback'],
        )
        fetch_response = oauth.fetch_request_token('https://api.twitter.com/oauth/request_token')
        logger.debug(f"fetch_response: {fetch_response}")
        request_token = fetch_response.get('oauth_token')
        request_token_secret = fetch_response.get('oauth_token_secret')
        logger.debug(f"request_token: {request_token} request_token_secret: {request_token_secret}")
        # 生成授权链接
        authorization_url = oauth.authorization_url('https://api.twitter.com/oauth/authenticate')
        logger.debug(f"authorization_url: {authorization_url}")

        ## 用户社交信息表 增加积分记录
        insert_query = "INSERT INTO wenda_users_social_x (userid, social_uuid, social_type, status) VALUES (%s,%s,%s,%s)"
        values = (userid, request_token, social_type, 0,)
        insert_query = format_query_for_db(insert_query)
        logger.debug(f"insert_query: {insert_query} values: {values}")
        await cursor.execute(insert_query, values)
        if DB_ENGINE == "sqlite": cursor.connection.commit()
        else: await cursor.connection.commit()

        return {
            "code": 200,
            "success": True,
            "msg": "Success",
            "data": {
                "url": authorization_url,
            }
        }
    except Exception as e:
        logger.error(f"/api/auth/x/connect - {userid} except ERROR: {str(e).splitlines()[0]}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}


# authorize / follow
@router.get("/x/callback")
async def twitter_callback(request: Request, oauth_token: str, oauth_verifier: str, cursor=Depends(get_db)):
    """twitter用户授权回调,获取用户twitter信息"""
    logger.info(f"POST /api/auth/x/callback")
    if cursor is None:
        logger.error(f"/api/auth/x/callback cursor: None")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

    try:
        logger.debug(f"request: {str(request.url)}")

        oauth = OAuth1Session(
            client_key=SOCIAL_CONFIG['twitter_key'],
            client_secret=SOCIAL_CONFIG['twitter_secret'],
            resource_owner_key=oauth_token,
            verifier=oauth_verifier,
        )
        fetch_response = oauth.fetch_access_token('https://api.twitter.com/oauth/access_token')
        logger.debug(f"fetch_response: {fetch_response}")
        access_token = fetch_response['oauth_token']
        access_token_secret = fetch_response['oauth_token_secret']
        logger.debug(f"access_token: {access_token} access_token_secret: {access_token_secret}")

        ## 根据 social_uuid 更新账户信息
        check_query = "SELECT id,userid,social_type FROM wenda_users_social_x WHERE social_uuid=%s"
        values = (oauth_token,)
        check_query = format_query_for_db(check_query)
        logger.debug(f"check_query: {check_query} values: {values}")
        await cursor.execute(check_query, values)
        users_social = await cursor.fetchone()
        # logger.debug(f"users_social: {users_social}")
        users_social = convert_row_to_dict(users_social, cursor.description)  # 转换字典
        logger.debug(f"users_social: {users_social}")

        if users_social['social_type'] == 'authorize':
            ## 授权成功获取用户信息
            include_email = True
            oauth = OAuth1Session(
                client_key=SOCIAL_CONFIG['twitter_key'],
                client_secret=SOCIAL_CONFIG['twitter_secret'],
                resource_owner_key=access_token,
                resource_owner_secret=access_token_secret,
            )
            params = {
                "include_email": "true" if include_email else "false",
                "skip_status": "true",  # Adjust this as needed to include/exclude status
                "include_entities": "true"
            }
            response = oauth.get('https://api.twitter.com/1.1/account/verify_credentials.json', params=params)
            if response.status_code == 200:
                user_data = response.json()
                logger.debug(f"user_data: {user_data}")
                if user_data.get('id_str', None) is None:
                    logger.error(f"STATUS: 401 ERROR: Invalid user_data - user_data: {user_data}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                
                social_global_name = user_data.get('screen_name', None)
                if social_global_name is None:
                    logger.error(f"STATUS: 401 ERROR: Invalid screen_name - user_data: {user_data}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                social_name = user_data.get('name', social_global_name)
                
                ## 获取用户信息 并入库
                ## 根据 social_id 查询是否已绑定
                check_query = "SELECT id,userid,status FROM wenda_users_social_x WHERE social_id=%s and status>0"
                values = (user_data.get('id_str'),)
                check_query = format_query_for_db(check_query)
                logger.debug(f"check_query: {check_query} values: {values}")
                await cursor.execute(check_query, values)
                users_socialid = await cursor.fetchone()
                # logger.debug(f"users_socialid: {users_socialid}")
                if users_socialid:
                    logger.error(f"STATUS: 400 ERROR: Already bound to Twitter - social_id: {user_data['id_str']}")
                    error = {"code": 400, "success": False, "msg": get_text('ALREADY_BOUND_TWITTER')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                users_socialid = convert_row_to_dict(users_socialid, cursor.description)  # 转换字典
                logger.debug(f"users_socialid: {users_socialid}")

                if users_social:
                    ## 用户社交信息表 更新积分记录
                    update_query = "UPDATE wenda_users_social_x set social_id=%s,social_name=%s,social_global_name=%s,social_avatar=%s,social_locale=%s,\
                                                    x_followers_count=%s,x_friends_count=%s,x_listed_count=%s,x_favourites_count=%s,x_statuses_count=%s,\
                                                    x_created_at=%s,x_email=%s,social_action=%s,status=%s,access_token=%s,access_token_secret=%s,updated_time=NOW() where id=%s"
                    values = (user_data.get('id_str'), social_name, social_global_name, user_data.get('profile_image_url_https'), user_data.get('location'), \
                              user_data.get('followers_count'), user_data.get('friends_count'), user_data.get('listed_count'), user_data.get('favourites_count'), user_data.get('statuses_count'), \
                              user_data.get('created_at'), user_data.get('email'), '0', 2, access_token, access_token_secret, users_social['id'])
                    update_query = format_query_for_db(update_query)
                    logger.debug(f"update_query: {update_query} values: {values}")
                    await cursor.execute(update_query, values)
                    if DB_ENGINE == "sqlite": cursor.connection.commit()
                    else: await cursor.connection.commit()

                    ## 用户表 更新社交信息记录
                    update_query = "UPDATE wenda_users set social_x=%s,updated_time=NOW() where userid=%s"
                    values = (social_global_name, users_social['userid'],)
                    update_query = format_query_for_db(update_query)
                    logger.debug(f"update_query: {update_query} values: {values}")
                    await cursor.execute(update_query, values)
                    if DB_ENGINE == "sqlite": cursor.connection.commit()
                    else: await cursor.connection.commit()

                    ## redis 删除
                    await del_redis_data(f"box:{users_social['userid']}:session")

                    return RedirectResponse(url=APP_CONFIG['appbase'])
                    return {
                        "code": 200,
                        "success": True,
                        "msg": "Success",
                        "data": "Authorization successful"
                    }
                else:
                    logger.error(f"STATUS: 401 ERROR: Invalid social_uuid - {oauth_token}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_SOCIAL_UUID')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
            else:
                logger.debug(f"response: {response}")
                logger.error(f"STATUS: {response.status_code} ERROR: Failed to fetch user details")
                error = {"code": response.status_code, "success": False, "msg": get_text('FAILED_FETCH_USER_DETAILS')}
                error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                logger.debug(f"error_base64: {error_base64}")
                return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
        elif users_social['social_type'] == 'follow':
            ## 授权成功获取用户信息
            include_email = True
            oauth = OAuth1Session(
                client_key=SOCIAL_CONFIG['twitter_key'],
                client_secret=SOCIAL_CONFIG['twitter_secret'],
                resource_owner_key=access_token,
                resource_owner_secret=access_token_secret,
            )
            params = {
                "include_email": "true" if include_email else "false",
                "skip_status": "true",  # Adjust this as needed to include/exclude status
                "include_entities": "true"
            }
            response = oauth.get('https://api.twitter.com/1.1/account/verify_credentials.json', params=params)
            if response.status_code == 200:
                user_data = response.json()
                logger.debug(f"user_data: {user_data}")
                if user_data.get('id_str', None) is None:
                    logger.error(f"STATUS: 401 ERROR: Invalid user_data - user_data: {user_data}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)

                social_global_name = user_data.get('screen_name', None)
                if social_global_name is None:
                    logger.error(f"STATUS: 401 ERROR: Invalid screen_name - user_data: {user_data}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_DATA')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                social_name = user_data.get('name', social_global_name)
                
                ## 获取用户信息 并入库
                ## 根据 social_id 查询是否已绑定
                check_query = "SELECT id,userid,x_friends_count,status FROM wenda_users_social_x WHERE social_id=%s and social_type='authorize'"
                values = (user_data.get('id_str'),)
                check_query = format_query_for_db(check_query)
                logger.debug(f"check_query: {check_query} values: {values}")
                await cursor.execute(check_query, values)
                users_socialid = await cursor.fetchone()
                # logger.debug(f"users_socialid: {users_socialid}")
                if users_socialid is None:
                    logger.error(f"STATUS: 404 ERROR: Please authorize first - social_id: {user_data['id_str']}")
                    error = {"code": 404, "success": False, "msg": "Please authorize first"}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
                users_socialid = convert_row_to_dict(users_socialid, cursor.description)  # 转换字典
                logger.debug(f"users_socialid: {users_socialid}")

                follow_status = 0
                if users_socialid['x_friends_count'] <= user_data.get('friends_count'):
                    follow_status = 1
                if not follow_status:
                    logger.error(f"STATUS: 404 ERROR: Please follow Twitter - friends_count: {user_data.get('friends_count')}")
                    error = {"code": 401, "success": False, "msg": get_text('PLEASE_FOLLOW_TWITTER')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)

                if users_social:
                    ## 用户社交信息表 更新积分记录
                    update_query = "UPDATE wenda_users_social_x set social_id=%s,social_name=%s,social_global_name=%s,social_avatar=%s,social_locale=%s,\
                                                    x_followers_count=%s,x_friends_count=%s,x_listed_count=%s,x_favourites_count=%s,x_statuses_count=%s,\
                                                    x_created_at=%s,x_email=%s,social_action=%s,status=%s,access_token=%s,access_token_secret=%s,updated_time=NOW() where id=%s"
                    values = (user_data.get('id_str'), social_name, social_global_name, user_data.get('profile_image_url_https'), user_data.get('location'), \
                              user_data.get('followers_count'), user_data.get('friends_count'), user_data.get('listed_count'), user_data.get('favourites_count'), user_data.get('statuses_count'), \
                              user_data.get('created_at'), user_data.get('email'), str(follow_status), 2, access_token, access_token_secret, users_social['id'])
                    update_query = format_query_for_db(update_query)
                    logger.debug(f"update_query: {update_query} values: {values}")
                    await cursor.execute(update_query, values)
                    if DB_ENGINE == "sqlite": cursor.connection.commit()
                    else: await cursor.connection.commit()

                    ## 用户表 更新社交信息记录
                    update_query = "UPDATE wenda_users set social_x=%s,updated_time=NOW() where userid=%s"
                    values = (social_global_name, users_social['userid'],)
                    update_query = format_query_for_db(update_query)
                    logger.debug(f"update_query: {update_query} values: {values}")
                    await cursor.execute(update_query, values)
                    if DB_ENGINE == "sqlite": cursor.connection.commit()
                    else: await cursor.connection.commit()

                    return RedirectResponse(url=APP_CONFIG['appbase'])
                    return {
                        "code": 200,
                        "success": True,
                        "msg": "Success",
                        "data": "Authorization successful"
                    }
                else:
                    logger.error(f"STATUS: 401 ERROR: Invalid social_uuid - {oauth_token}")
                    error = {"code": 401, "success": False, "msg": get_text('INVALID_SOCIAL_UUID')}
                    error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                    return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
            else:
                logger.debug(f"response: {response}")
                logger.error(f"STATUS: {response.status_code} ERROR: Failed to fetch user details")
                error = {"code": response.status_code, "success": False, "msg": get_text('FAILED_FETCH_USER_DETAILS')}
                error_base64 = bytes.decode(base64.b64encode(json.dumps(error).encode()))
                logger.debug(f"error_base64: {error_base64}")
                return RedirectResponse(url=APP_CONFIG['error'] + error_base64)
    except Exception as e:
        logger.error(f"/api/auth/x/callback except ERROR: {str(e).splitlines()[0]}")
        return {"code": 500, "success": False, "msg": get_text('SERVER_ERROR')}

