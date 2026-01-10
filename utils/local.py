# -*- coding: UTF8 -*-
import re
import os
import sys
import cv2
import time
import math
import string
import random
import ffmpeg
import asyncio
import hashlib
import platform
import subprocess
import shlex
import shutil
import ssl
from zlib import crc32
from pathlib import Path
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.responses import StreamingResponse

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

def floor_decimal(n, decimals=0):
    """
    小数向下取整
    """
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier

def generate_register_code(userid, length=5):
    # 生成随机码
    src_code = "".join(random.sample(string.ascii_letters + string.digits, length))
    # 生成随机顺序
    input_str = f"{userid}NODEREGISTER{time.time()}"
    hash_val = hashlib.sha256(input_str.encode()).hexdigest()
    sort_code = hash_val[:length]
    # 随机码按序偏移
    referral_result = []
    for char, shift_digit in zip(src_code, sort_code):
        shift = int(shift_digit, 16)  # 将数字字符转为整数
        referral_result.append(shift_char(char, shift))
    register_code = ''.join(referral_result).upper()
    # register_code = "ga" + ''.join(referral_result)
    # logger.debug(f"src_code: {src_code} sort_code: {sort_code} => register_code: {register_code}")
    return register_code


def validate_sfzid(sfzid: str) -> bool:
    """
    验证18位身份证号码的合法性
    
    Args:
        sfzid (str): 身份证号码字符串
        
    Returns:
        bool: 验证结果，True表示合法，False表示不合法
    """
    # 基本格式检查
    if not sfzid or len(sfzid) != 18:
        return False
    
    # 检查是否全部为数字，除了最后一位可能是X
    if not re.match(r'^\d{17}[\dXx]$', sfzid):
        return False
    
    # 各位权重值
    weight_factors = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    
    # 校验码对应值
    check_codes = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
    
    # 计算前17位与权重的乘积和
    sum_value = 0
    for i in range(17):
        sum_value += int(sfzid[i]) * weight_factors[i]
    
    # 计算校验码
    mod = sum_value % 11
    expected_check_code = check_codes[mod]
    
    # 比较校验码
    return sfzid[17].upper() == expected_check_code

def validate_email(email: str) -> bool:
    """
    验证邮箱地址格式
    
    Args:
        email (str): 邮箱地址字符串
        
    Returns:
        bool: 验证结果，True表示合法，False表示不合法
    """
    if not email:
        return False
    
    # 邮箱正则表达式
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def generate_random_sfzid():
    """
    随机生成一个符合规则的身份证号码
    
    身份证号码构成：
    1-6位：地址码
    7-14位：出生日期码 YYYYMMDD
    15-17位：顺序码，奇数为男，偶数为女
    18位：校验码
    """
    import random
    from datetime import datetime, timedelta
    
    # 常用地址码列表（部分）
    address_codes = [
        "110000", "120000", "130000", "140000", "150000",  # 北京、天津、河北、山西、内蒙古
        "210000", "220000", "230000",                      # 辽宁、吉林、黑龙江
        "310000", "320000", "330000", "340000", "350000", "360000", "370000",  # 上海、江苏、浙江、安徽、福建、江西、山东
        "410000", "420000", "430000", "440000", "450000", "460000",            # 河南、湖北、湖南、广东、广西、海南
        "500000", "510000", "520000", "530000", "540000",                      # 重庆、四川、贵州、云南、西藏
        "610000", "620000", "630000", "640000", "650000",                      # 陕西、甘肃、青海、宁夏、新疆
        "810000", "820000"                                                     # 香港、澳门
    ]
    
    # 随机选择一个地址码
    address_code = random.choice(address_codes)
    
    # 生成随机出生日期 (1950-01-01 到 2005-12-31)
    start_date = datetime(1950, 1, 1)
    end_date = datetime(2005, 12, 31)
    random_date = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days)
    )
    birth_date = random_date.strftime("%Y%m%d")
    
    # 生成顺序码 (15-17位)
    order_code = f"{random.randint(0, 999):03d}"
    
    # 构造前17位
    id_without_check = address_code[:6] + birth_date + order_code
    
    # 计算校验码
    check_code = calculate_check_digit(id_without_check)
    
    # 返回完整的身份证号码
    return id_without_check + check_code

def calculate_check_digit(id_number):
    """
    根据身份证前17位计算第18位校验码
    
    Args:
        id_number: 身份证前17位
        
    Returns:
        校验码 (0-9 或 X)
    """
    # 加权因子
    weights = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    
    # 校验码对应表
    check_codes = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
    
    # 计算加权和
    sum_value = 0
    for i in range(17):
        sum_value += int(id_number[i]) * weights[i]
    
    # 取模并返回对应的校验码
    return check_codes[sum_value % 11]

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
