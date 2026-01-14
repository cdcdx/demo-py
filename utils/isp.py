"""Fetch info from the short url click"""
try:
    # Optional package
    import os
    from loguru import logger
    from urllib.parse import urlparse, parse_qs
    import geoip2.database
    GEOIP2_AVAILABLE = True
except ImportError:
    print("ModuleNotFoundError: No module named 'geoip2'")
    GEOIP2_AVAILABLE = False
    geoip2 = None  # 定义为None以避免NameError

def parse_request(request):
    """Pass request object and returns parsed data dict.
    Country is fetched from IP using maxmind db.

    :param request:
    :return:
    """
    ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    data = dict(
        referrer=request.referrer,
        user_agent=request.headers.get('User-Agent'),
        country=get_ip_country(ip)
    )
    return data

def parse_header(request):
    """Pass request object and returns parsed data dict.
    Country is fetched from IP using maxmind db.

    :param request:
    :return:
    """
    ip = request.headers.get('Pygmy-App-User-Ip')
    data = dict(
        referrer=request.headers.get('Pygmy-Http-Rreferrer'),
        user_agent=request.headers.get('Pygmy-Http-User-Agent'),
        country=get_ip_country(ip)
    )
    return data

def get_real_ip(request):
    x_forwarded_for = request.headers.get('x-forwarded-for')
    # print(f"x_forwarded_for: {x_forwarded_for}")
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]  # 多次反向代理后会有多个ip值，第一个ip才是真实ip
    else:
        ip = request.headers.get('remote-host')  # 这里获得代理ip
        if not ip:
            ip = request.client.host
    return ip

## 获取url路径
def get_api_path(request):
    url=urlparse(request.url.__str__())
    logger.debug(f"url: {url}")
    apipath=url.path.replace('/api/','').replace('/','_').replace('-','_')
    logger.debug(f"apipath 1: {apipath}")
    apiquery=parse_qs(url.query)
    logger.debug(f"apiquery: {apiquery} len: {len(apiquery)}")
    if len(apiquery) > 0 and url.query.find('page') >= 0:
        apipath=f"{apipath}_page_{apiquery['page'][0]}"
        logger.debug(f"apipath 2: {apipath}")
    return apipath

# https://www.maxmind.com/en/accounts/1050515/geoip/downloads
# https://github.com/wp-statistics/GeoLite2-City
def get_geodb_reader(db_name):
    """获取GeoIP数据库读取器"""
    if not GEOIP2_AVAILABLE:
        print("geoip2 module not available")
        return None
        
    db_path = f'./utils/{db_name}.mmdb'
    if os.getcwd().find('utils') > 0:
        db_path = f'./{db_name}.mmdb'
    if os.path.exists(db_path):
        return geoip2.database.Reader(db_path)
    else:
        print(f"ERROR: {db_path} does not exist.")
        return None

def get_ip_country(ip):
    """Get country from ip. Uses Geoip2 db.

    :param ip:
    :return: None/str
    """
    if not GEOIP2_AVAILABLE:
        return {"country": None, "isocode": None}
        
    c_country = None
    c_isocode = None
    try:
        reader = get_geodb_reader('GeoLite2-Country')
        if reader:
            c = reader.country(ip)
            c_isocode = c.country.iso_code
            c_country = c.country.names.get('en', None)
            if c_country == 'Hong Kong' or c_country == 'Macao' or c_country == 'Taiwan':
                c_country = 'China'
    except geoip2.errors.GeoIP2Error as e:
        logger.debug(f"ERROR: GeoIP2Error: {str(e)}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
    return {
        "country": c_country,
        "isocode": c_isocode,
    }

def get_ip_city(ip):
    """Get location from ip. Uses Geoip2 db.

    :param ip:
    :return: None/str
    """
    if not GEOIP2_AVAILABLE:
        return {"country": None, "isocode": None, "latitude": 0, "longitude": 0}
        
    c_country = None
    c_isocode = None
    c_latitude = 0
    c_longitude = 0
    try:
        reader = get_geodb_reader('GeoLite2-City')
        if reader:
            city = reader.city(ip)
            c_isocode = city.country.iso_code
            c_country = city.country.names.get('en', None)
            c_longitude = city.location.longitude
            c_latitude = city.location.latitude
            if c_country == 'Hong Kong' or c_country == 'Macao' or c_country == 'Taiwan':
                c_country = 'China'
    except geoip2.errors.GeoIP2Error as e:
        logger.debug(f"ERROR: GeoIP2Error: {str(e)}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
    return {
        "country": c_country,
        "isocode": c_isocode,
        "latitude": c_latitude,
        "longitude": c_longitude,
    }

def get_ip_score(timestamp) -> float:
    score = 0.0
    if timestamp >= 100000:
        score = 10.0 - timestamp/10000
    elif timestamp >= 10000:
        score = 20.0 - timestamp/900
    elif timestamp >= 1000:
        score = 70.0 - timestamp/180
    elif timestamp >= 100:
        score = 90.0 - timestamp/45
    else:
        score = 100.0 - timestamp/9
    
    if score < 5.2:
        return 0
    return round(score-5.2, 2)

if __name__ == "__main__":
    client_ips=['127.0.0.1', '155.70.191.11', '101.9.108.19', '2.2.2.2']
    for client_ip in client_ips:
        # import random
        # client_ip = ".".join(map(str,(random.randint(1,255) for _ in range(4))))
        city = get_ip_city(client_ip)
        print(f"client_ip: {client_ip} city: {city}")
        country = get_ip_country(client_ip)
        print(f"client_ip: {client_ip} country: {country} ")
    for timestamp in range(10,10000,100):
        ip_score = get_ip_score(timestamp)
        print(f"{timestamp} -> {ip_score}")
