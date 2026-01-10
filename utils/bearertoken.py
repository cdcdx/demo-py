import hashlib
from jose import jwt
from fastapi.security import HTTPBearer

from config import JWT_CONFIG

bearer = HTTPBearer()

def md58(s):
    md = hashlib.md5()
    md.update(s.encode('utf-8'))
    return md.hexdigest()[:8]

# Function to JWT token
def create_access_token(data: dict):
    return jwt.encode(data, JWT_CONFIG['secretkey'], algorithm=JWT_CONFIG['algorithm'])

def decode_access_token(token: str):
    return jwt.decode(token, JWT_CONFIG['secretkey'], algorithms=JWT_CONFIG['algorithm'])

