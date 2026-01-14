# -*- coding: UTF8 -*-
import re
import os
import sys
import cv2
import time
import math
import string
import random
import asyncio
import hashlib
import platform
import subprocess
import shlex
import shutil
import ssl
import bcrypt
from zlib import crc32
from pathlib import Path
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.responses import StreamingResponse

from utils.captcha import validate_captcha
from utils.log import log as logger
from config import *

# 2FA
import pyotp
secret_key = 'UTVVO2XNK3PD525OFQUQCQYL5DRU5SOA'

# colorama
from colorama import Fore, Style, init
init(autoreset=True)
red = Fore.LIGHTRED_EX
blue = Fore.LIGHTBLUE_EX
green = Fore.LIGHTGREEN_EX
black = Fore.LIGHTBLACK_EX
magenta = Fore.LIGHTMAGENTA_EX
reset = Style.RESET_ALL

# ------------------------------------------------------

def generate_userid(email):
    input_str = f"{email}{time.time()}"
    hash_val = hashlib.sha256(input_str.encode()).hexdigest()
    randint = int(hash_val[:5], 16)
    userid = str(int(round(time.time() * 590) + randint))
    # print(f"email: {email} => userid: {userid}")
    return userid

def shift_char(char: str, shift: int) -> str:
    """
    对单个字符 char 进行偏移, shift 为偏移量
    """
    if 'A' <= char <= 'Z':
        return chr((ord(char) - ord('A') + shift) % 26 + ord('A'))
    elif 'a' <= char <= 'z':
        return chr((ord(char) - ord('a') + shift) % 26 + ord('a'))
    elif '0' <= char <= '9':
        return chr((ord(char) - ord('0') + shift) % 10 + ord('0'))
    else:
        return chr(ord(char) + shift)

def generate_registercode(userid, length=5):
    # 生成随机码
    src_code = "".join(random.sample(string.ascii_letters + string.digits, length))
    # 生成随机顺序
    input_str = f"{userid}BOXMINER{time.time()}"
    hash_val = hashlib.sha256(input_str.encode()).hexdigest()
    sort_code = hash_val[:length]
    # 随机码按序偏移
    referral_result = []
    for char, shift_digit in zip(src_code, sort_code):
        shift = int(shift_digit, 16)  # 将数字字符转为整数
        referral_result.append(shift_char(char, shift))
    register_code = ''.join(referral_result).upper()
    # logger.debug(f"src_code: {src_code} sort_code: {sort_code} => register_code: {register_code}")
    return register_code

def generate_referralcode(address, length=8):
    # 生成随机顺序
    input_str = f"{address}BOXMINER"
    hash_val = hashlib.sha256(input_str.encode()).hexdigest()
    sort_code = hash_val[:length]
    referral_code = sort_code.upper()
    # logger.debug(f"sort_code: {sort_code} => referral_code: {referral_code}")
    return referral_code

# ------------------------------------------------------

def floor_decimal(n, decimals=0):
    """
    小数向下取整
    """
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier

def ceil_decimal(n, decimals=0):
    """
    小数向上取整
    """
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier) / multiplier

# ------------------------------------------------------

# 验证邮箱格式
EMAIL_PATTERN = r"^[a-zA-Z0-9_+&*-]+(?:\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,7}$"

def validate_email_format(email: str) -> bool:
    """验证邮箱格式"""
    return bool(re.match(EMAIL_PATTERN, email))

# ------------------------------------------------------

# 验证密码强度
PASSWORD_PATTERN = r'^.*(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*(),.?":{}|<>]).*$'
def validate_password_strength(password: str) -> str:
    """
    验证密码强度并返回处理后的密码
    如果密码不是32/64位哈希，则根据规则验证并哈希
    """
    if len(password) != 32 and len(password) != 64:
        if not re.match(PASSWORD_PATTERN, password):
            logger.error(f"STATUS: 401 ERROR: Invalid password - {password}")
            raise ValueError("密码不符合要求")
        # 哈希密码并截取前20位
        return hashlib.sha256(str(password).encode()).hexdigest()[:20]
    else:
        # 如果是32/64位哈希，截取前20位
        return password[:20]

def hash_password(password: str) -> str:
    """安全地哈希密码"""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def check_password(plain_password: str, hashed_password: str) -> bool:
    """检查密码是否匹配"""
    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except Exception:
        return False

# ------------------------------------------------------

# 验证旋转门Token
def verify_recaptcha_token(recaptcha_token: str) -> bool:
    """验证reCAPTCHA令牌"""
    if len(recaptcha_token) > 256 or (APP_CONFIG['level'] != 'debug'):  # debug不校验
        res_captcha = validate_captcha(recaptcha_token)
        if res_captcha is None:
            logger.error(f"STATUS: 403 ERROR: Captcha exception")
            return False
        elif res_captcha['success'] == False:
            logger.error(f"STATUS: 401 ERROR: Invalid captcha - {res_captcha['error-codes'][0]}")
            return False
    return True

# ------------------------------------------------------

def check_ssl_files(certfile, keyfile):
    if not os.path.exists(certfile):
        certfile = os.path.join(BASE_DIR, certfile)
    if not os.path.exists(keyfile):
        keyfile = os.path.join(BASE_DIR, keyfile)
    if os.path.exists(certfile) and os.path.exists(keyfile):
        try:
            ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ctx.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
        except ssl.SSLError as e:
            logger.error(f"Failed to load SSL cert or key: {e}")
            certfile=''
            keyfile=''
        except FileNotFoundError as e:
            logger.info(f"SSL cert or key file not found: {e}")
            certfile=''
            keyfile=''
    else:
        certfile=''
        keyfile=''
    return certfile, keyfile

def check_2fa(pwd: str):
    """
    2FA校验
    """
    ## 无效PWD
    if len(pwd) != 6:
        logger.error(f"STATUS: 400 ERROR: Invalid pwd")
        return False

    ## 生成当前的2FA密码
    totp = pyotp.TOTP(secret_key)
    current_password = totp.now()
    logger.info(f"current_password: {green}{current_password} {black}pwd: {green}{pwd}")
    ## 密码校验
    if current_password != pwd:
        logger.error(f"STATUS: 400 ERROR: Password verification failed")
        return False
    logger.success(f"STATUS: 200 INFO: Password verification successful")
    return True

def is_base58_encoded(s):
    """
    检查字符串是否为有效的Base58编码字符串
    
    Base58字符集: 123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz
    (去除了容易混淆的字符: 0, O, I, l)
    
    Args:
        s (str): 要检查的字符串
        
    Returns:
        bool: 如果是有效的Base58编码返回True，否则返回False
    """
    if not isinstance(s, str) or len(s) == 0:
        return False
    
    # Base58允许的字符集
    base58_chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    
    # 检查字符串是否只包含Base58字符
    if not all(c in base58_chars for c in s):
        return False
    
    return True

def is_base58_encoded_regex(s):
    """
    使用正则表达式检查字符串是否为有效的Base58编码字符串
    
    Args:
        s (str): 要检查的字符串
        
    Returns:
        bool: 如果是有效的Base58编码返回True，否则返回False
    """
    if not isinstance(s, str) or len(s) == 0:
        return False
    
    # Base58正则表达式模式
    base58_pattern = r'^[1-9A-HJ-NP-Za-km-z]+$'
    
    return bool(re.match(base58_pattern, s))

def get_hostname():
    hostname = platform.node()
    return hostname

# ------------------------------------------------------

def run_command(cmd):
    """
    执行命令
    """
    output = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #rst = output.stdout.read().decode("UTF8").strip()
    rst = output.stdout.readlines()
    return rst

# ------------------------------------------------------
