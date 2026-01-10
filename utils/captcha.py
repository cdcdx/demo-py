import requests
from loguru import logger

from config import CAPTCHA_CONFIG

def validate_turnstile(captcha_data):
    try:
        if CAPTCHA_CONFIG['turnstile_url'] == '' or CAPTCHA_CONFIG['turnstile_secret'] == '':
            return None
        response = requests.post(
            CAPTCHA_CONFIG['turnstile_url'],  # 'https://challenges.cloudflare.com/turnstile/v0/siteverify',
            {
                "secret": CAPTCHA_CONFIG['turnstile_secret'], 
                "response": captcha_data,
            },
            timeout=5  # 设置请求超时
        )
        logger.debug(f"validate_turnstile response: {response.json()}")
        if response.ok:
            return response.json()
        return None
    except requests.exceptions.RequestException as err:
        logger.error(err)
        return None

def validate_recaptcha(captcha_data):
    try:
        if CAPTCHA_CONFIG['recaptcha_url'] == '' or CAPTCHA_CONFIG['recaptcha_secret'] == '':
            return None
        response = requests.post(
            CAPTCHA_CONFIG['recaptcha_url'],  # 'https://www.google.com/recaptcha/api/siteverify',
            {
                "secret": CAPTCHA_CONFIG['recaptcha_secret'], 
                "response": captcha_data,
                # "ip": ip,
            },
            timeout=5  # 设置请求超时
        )
        logger.debug(f"validate_recaptcha response: {response.json()}")
        if response.ok:
            return response.json()
        return None
    except requests.exceptions.RequestException as err:
        logger.error(err)
        return None

def validate_hcaptcha(captcha_data):
    try:
        if CAPTCHA_CONFIG['hcaptcha_url'] == '' or CAPTCHA_CONFIG['hcaptcha_secret'] == '':
            return None
        response = requests.post(
            CAPTCHA_CONFIG['hcaptcha_url'],  # 'https://hcaptcha.com/siteverify',
            {
                "secret": CAPTCHA_CONFIG['hcaptcha_secret'], 
                "response": captcha_data,
            },
            timeout=5  # 设置请求超时
        )
        logger.debug(f"validate_hcaptcha response: {response.json()}")
        if response.ok:
            return response.json()
        return None
    except requests.exceptions.RequestException as err:
        logger.error(err)
        return None

# 多合一验证码: 根据验证码特征判断验证码来源
def validate_captcha(captcha_data):
    res_captcha=None
    if captcha_data.startswith('0.'):
        res_captcha = validate_turnstile(captcha_data)
    elif captcha_data.startswith('P1_'):
        res_captcha = validate_hcaptcha(captcha_data)
    else:
        res_captcha = validate_recaptcha(captcha_data)
    logger.debug(f"res_captcha: {res_captcha}")
    return res_captcha
