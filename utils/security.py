import json
import time

from fastapi import HTTPException, Depends, Request
from fastapi.security import HTTPAuthorizationCredentials
from jose import JWTError
from loguru import logger

from utils.bearertoken import md58, bearer, decode_access_token
from utils.cache import get_redis_data, set_redis_data, del_redis_data, increment_redis_data
from utils.db import get_db, format_query_for_db, convert_row_to_dict
from config import JWT_CONFIG, APP_CONFIG, set_envsion, get_envsion


# 校验:Token密码
async def get_current_userid(authorization: HTTPAuthorizationCredentials = Depends(bearer), cursor=Depends(get_db)):
    credentials_exception = HTTPException(status_code=401, detail="Invalid JWT Token")
    userid = ''
    try:
        token = authorization.credentials
        # jwt解码
        payload = decode_access_token(token)
        logger.debug(f"payload: {payload}")
        current_timestamp = int(time.time())
        expire = payload.get('expire', None)
        if expire is None:
            raise credentials_exception
        elif expire <= current_timestamp:
            raise credentials_exception

        if payload.get('email', None):  # token错误
            raise credentials_exception

        userid = payload.get('userid', None)
        if userid is None:
            raise credentials_exception
        username = payload.get('username', None)
        if username is None:
            raise credentials_exception
        jwt_secret = payload.get('secret', None)
        if jwt_secret is None:
            raise credentials_exception

        ## redis 获取账号密码
        redis_user_passwd = await get_redis_data(f"box:{userid}:pwd")
        # logger.debug(f"redis redis_user_passwd: {redis_user_passwd}")
        if not redis_user_passwd:
            # secret有效性校验
            check_query = "SELECT password FROM wenda_users WHERE username=%s"
            values = (username,)
            check_query = format_query_for_db(check_query)
            logger.debug(f"check_query: {check_query} values: {values}")
            await cursor.execute(check_query, values)
            existing_user = await cursor.fetchone()
            # logger.debug(f"existing_user: {existing_user}")
            if existing_user is None:
                raise credentials_exception
            existing_user = convert_row_to_dict(existing_user, cursor.description)  # 转换字典
            logger.debug(f"existing_user: {existing_user}")
            sql_user_passwd = existing_user['password']
            db_secret = md58(sql_user_passwd)
            logger.debug(f"mysql sql.secret: {db_secret} jwt.secret: {jwt_secret}")
            if db_secret != jwt_secret:
                raise credentials_exception
            # redis 设置
            await set_redis_data(f"box:{userid}:pwd", value=sql_user_passwd, ex=JWT_CONFIG['expire'])
        else:
            db_secret = md58(redis_user_passwd)
            logger.debug(f"redis redis.secret: {db_secret} jwt.secret: {jwt_secret}")
            if db_secret != jwt_secret:
                # redis 删除
                await del_redis_data(f"box:{userid}:pwd")
                raise credentials_exception
    except JWTError:
        raise credentials_exception
    return userid


# 校验:userid访问接口次数 90秒内访问超过 10次503 30次拉黑用户
async def get_interface_userid(request: Request, authorization: HTTPAuthorizationCredentials = Depends(bearer)):
    credentials_exception = HTTPException(status_code=401, detail="Invalid JWT Token")
    notauthorized_exception = HTTPException(status_code=403, detail="Not authenticated")  # 用户黑名单
    forbidden_exception = HTTPException(status_code=503, detail="Frequent operation")  # 超过10次503
    userid = ''
    try:
        # token解码userid
        token = authorization.credentials
        ## jwt解码
        payload = decode_access_token(token)
        logger.debug(f"payload: {payload}")
        if payload.get('email', None):  ## 重置密码 ({"email": post_request.email, "key": md58(timestamp), "expire": 3600})
            raise credentials_exception
        ## 注册验证 ({"userid": userid, "username": username, "secret": md58(hashed_password), "expire": JWT_CONFIG['expire']})
        current_timestamp = int(time.time())
        expire = payload.get('expire', None)
        if expire is None:
            raise credentials_exception
        elif expire <= current_timestamp:
            raise credentials_exception

        userid = payload.get('userid', None)
        if userid is None:
            raise credentials_exception

        # debug模式
        if APP_CONFIG['level'] == 'debug':
            return userid

        # .env 用户黑名单
        if userid in APP_CONFIG['black']:
            logger.info(f"STATUS: 401 ERROR: Unauthorized - env:blacklist:{userid} - {userid}")
            raise notauthorized_exception
        
    except JWTError:
        raise credentials_exception
    return userid

