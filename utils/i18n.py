import gettext
import os
from contextvars import ContextVar
from typing import Dict, Any
from fastapi import Request

# 存储当前语言的上下文变量
current_language: ContextVar[str] = ContextVar('current_language', default='en')

# 多语言翻译字典
TRANSLATIONS = {
    'en': {
        'ALREADY_BOUND_TWITTER': 'Already bound to Twitter',
        'ALREADY_FOLLOW_TWITTER': 'Already follow Twitter',
        'PLEASE_FOLLOW_TWITTER': 'Please follow Twitter',
        'ALREADY_BOUND_DISCORD': 'Already bound to Discord',
        'ALREADY_JOIN_DISCORD_CHANNEL': 'Already join Discord channel',
        'PLEASE_JOIN_DISCORD_CHANNEL': 'Please join Discord channel',
        'ALREADY_ADDRESS_REGISTERED': 'Address already registered',
        'ALREADY_EMAIL_REGISTERED': 'Email already registered',
        'ALREADY_EMAIL_VERIFIED': 'Email already verified',
        'ALREADY_USERNAME_REGISTERED': 'Username already registered',
        'ALREADY_ADDRESS_MODIFIED': 'Address already modified',
        'ALREADY_EMAIL_MODIFIED': 'Email already modified',
        'ALREADY_USERNAME_MODIFIED': 'Username already modified',
        'FAILED_FETCH_USER_DETAILS': 'Failed to fetch user details',
        'INVALID_ADDRESS': 'Invalid address',
        'INVALID_CAPTCHA': 'Invalid captcha',
        'INVALID_DATA': 'Invalid data',
        'INVALID_EMAIL': 'Invalid email',
        'INVALID_GOOGLE_CLIENT_ID': 'Invalid GOOGLE_CLIENT_ID',
        'INVALID_DISCORD_CLIENT_ID': 'Invalid DISCORD_CLIENT_ID',
        'INVALID_TWITTER_CONSUMER_KEY': 'Invalid TWITTER_CONSUMER_KEY',
        'INVALID_JWT_TOKEN' : 'Invalid JWT Token',
        'INVALID_PERMISSIONS': 'Invalid permissions',
        'INVALID_REGISTERCODE': 'Invalid registercode',
        'INVALID_SOCIAL_UUID': 'Invalid social_uuid',
        'INVALID_PASSWORD': 'Invalid password',
        'INVALID_USERNAME': 'Invalid username',
        'INVALID_USERID': 'Invalid userid',
        'PLEASE_AUTHORIZE_FIRST': 'Please authorize first',
        'PLEASE_DO_NOT_APPLY_AGAIN': 'Please do not apply again',
        'REGISTERCODE_GENERATION_FAILED': 'registercode generation failed',
        'SERVER_ERROR': 'Server error',
        'SUCCESS_MESSAGE': 'Success',
        'SUCCESS_BOUND_REGISTER_CODE': 'Successfully bound registercode',
        'USERID_GENERATION_FAILED': 'userid generation failed',
    },
    'zh': {
        'ALREADY_BOUND_TWITTER': '已绑定Twitter',
        'ALREADY_FOLLOW_TWITTER': '已关注Twitter',
        'PLEASE_FOLLOW_TWITTER': '请关注Twitter',
        'ALREADY_BOUND_DISCORD': '已绑定Discord',
        'ALREADY_JOIN_DISCORD_CHANNEL': '已加入Discord频道',
        'PLEASE_JOIN_DISCORD_CHANNEL': '请先加入Discord频道',
        'ALREADY_ADDRESS_REGISTERED': '地址已注册',
        'ALREADY_EMAIL_REGISTERED': '邮箱已注册',
        'ALREADY_EMAIL_VERIFIED': '邮箱已验证',
        'ALREADY_USERNAME_REGISTERED': '用户名已注册',
        'ALREADY_ADDRESS_MODIFIED': '地址已修改',
        'ALREADY_EMAIL_MODIFIED': '邮箱已修改',
        'ALREADY_USERNAME_MODIFIED': '用户名已修改',
        'FAILED_FETCH_USER_DETAILS': '获取详情失败',
        'INVALID_ADDRESS': '无效地址',
        'INVALID_CAPTCHA': '无效验证码',
        'INVALID_DATA': '无效数据',
        'INVALID_EMAIL': '无效邮箱',
        'INVALID_GOOGLE_CLIENT_ID': '无效GOOGLE_CLIENT_ID',
        'INVALID_DISCORD_CLIENT_ID': '无效DISCORD_CLIENT_ID',
        'INVALID_TWITTER_CONSUMER_KEY': '无效TWITTER_CONSUMER_KEY',
        'INVALID_JWT_TOKEN' : '无效Token',
        'INVALID_PERMISSIONS': '无效权限',
        'INVALID_REGISTERCODE': '无效注册码',
        'INVALID_SOCIAL_UUID': '无效社交UUID',
        'INVALID_PASSWORD': '无效密码',
        'INVALID_USERNAME': '无效用户名',
        'INVALID_USERID': '无效用户ID',
        'PLEASE_AUTHORIZE_FIRST': '请先授权',
        'PLEASE_DO_NOT_APPLY_AGAIN': '请勿重复申请',
        'REGISTERCODE_GENERATION_FAILED': '注册码生成失败',
        'SERVER_ERROR': '服务器错误',
        'SUCCESS_MESSAGE': '成功',
        'SUCCESS_BOUND_REGISTER_CODE': '成功绑定注册码',
        'USERID_GENERATION_FAILED': '用户ID生成失败',
    }
}

def get_text(key: str) -> str:
    """获取当前语言对应的文本"""
    lang = current_language.get()
    return TRANSLATIONS.get(lang, TRANSLATIONS['en']).get(key, key)

def get_user_preferred_language(request: Request) -> str:
    """
    根据请求获取用户的首选语言
    """
    # 优先级: 请求参数 > cookie > 请求头 > 默认值
    preferred_lang = request.query_params.get('lang') or \
                     request.cookies.get('language') or \
                     request.headers.get('Accept-Language', '').split(',')[0].split('-')[0] or \
                     'en'
    
    # 验证语言是否支持
    supported_languages = ['en', 'zh']  # 根据实际支持的语言调整
    return preferred_lang if preferred_lang in supported_languages else 'en'
