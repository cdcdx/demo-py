from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import re
from utils.i18n import current_language

class LanguageMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 从请求头、查询参数或cookie中获取语言设置
        language = (
            request.headers.get('Accept-Language', '').split(',')[0].split('-')[0] or
            request.query_params.get('lang', '') or
            request.cookies.get('language', 'en')
        ).lower()
        
        # 验证语言是否支持
        if language not in ['en', 'zh']:  # 可根据需要添加更多语言
            language = 'en'
        
        # 设置当前语言上下文
        token = current_language.set(language)
        
        try:
            response = await call_next(request)
            return response
        finally:
            # 恢复上下文
            current_language.reset(token)
