import os
import re
import urllib
import aiosqlite
import aiomysql
import asyncpg
import datetime
from datetime import datetime as dt
from decimal import Decimal
from contextlib import asynccontextmanager
from abc import ABC, abstractmethod
from typing import AsyncGenerator
from loguru import logger
# from fastapi import HTTPException

from config import DB_ENGINE, SQLITE_URL, MYSQL_URL, DB_MAXCONNECT, POSTGRESQL_URL, BASE_DIR

class Database(ABC):
    """数据库抽象基类"""

    def __init__(self, url: str):
        self.url = url
        self.pool = None

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator:
        pass

class SQLiteDatabase(Database):
    def __init__(self, url: str):
        super().__init__(url)
        db_path = url.split("://")[1]
        if db_path.startswith('./'):
            self.url = os.path.join(BASE_DIR, db_path.replace('./', ''))
        else:
            self.url = db_path
        logger.info(f"SQLite URL: {self.url}")

    async def connect(self) -> None:
        """连接数据库并创建表"""
        try:
            # 检查数据库文件是否存在，不存在则创建
            if not os.path.exists(self.url):
                await self._initialize_database()
            
            # 创建连接池
            self.pool = await aiosqlite.connect(self.url, uri=True, check_same_thread=False)
            if self.pool:
                logger.info("Connected to SQLite database")
                # 连接成功后检查并创建表
                await self.create_tables()
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise
            raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

    async def _initialize_database(self):
        """初始化新的数据库文件"""
        conn = await aiosqlite.connect(self.url, uri=True)
        try:
            await self.create_tables(conn)
        finally:
            await conn.close()

    async def create_tables(self, conn=None) -> None:
        """创建数据库表"""
        should_commit = conn is None
        if conn is None:
            conn = await aiosqlite.connect(self.url, uri=True)
        
        try:
            # 创建用户表
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wenda_users (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    register_code  TEXT     DEFAULT '',
                    email          TEXT     DEFAULT '',
                    userid         TEXT     DEFAULT '',
                    username       TEXT     DEFAULT '',
                    password       TEXT     DEFAULT '',
                    address        TEXT     DEFAULT '',
                    social_dc      TEXT     DEFAULT '',
                    social_x       TEXT     DEFAULT '',
                    state          TEXT     DEFAULT 'UNVERIFIED',
                    cooldown_time  DATETIME DEFAULT NULL,
                    created_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_time   DATETIME DEFAULT NULL
                );
            """)
            
            # 检查是否有初始数据
            cursor = await conn.execute("SELECT COUNT(*) FROM wenda_users")
            count = await cursor.fetchone()
            if count and count[0] == 0:
                # 插入数据
                await conn.execute("""
                    INSERT INTO wenda_users (id, register_code, email, userid, username, password, state, created_time) VALUES 
                    (1, 'KOMAC', 'admin@liuhai.com', '1033809395880', 'admin',   '$2b$12$YoWDyiLyzZr/GU9nCT3rQeN7x7e5V7WtSRHkc0KP3.pwGWjxShDyO', 'VERIFIED', '2025-07-11 07:28:39'),
                    (2, 'PMOCR', '370887876@qq.com', '1033963391743', 'pangtou', '$2b$12$sq3jCm0Z6B9W.H5MsLXh7.O.zlMDmLgwUrQM2V3XX144AL0GKqpFS', 'VERIFIED', '2025-07-11 07:28:39'),
                    (3, 'ZD3JU', 'psh001@qq.com', '1040492267738', 'psh001', '$2b$12$hnJh2jqkn2etSAjhq19saORj0z5NiwE4znZTp6c5y/H4cOB1B5Yku', 'VERIFIED', '2025-07-11 07:28:39');
                """)
            
            # 创建社交用户表 x
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wenda_users_social_x (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    userid              TEXT     DEFAULT '',
                    social_uuid         TEXT     DEFAULT '',
                    social_type         TEXT     DEFAULT '',  -- authorize/follow/retweet
                    social_action       TEXT     DEFAULT '',  -- '' or 0: - follow false / 1: follow true
                    social_id           TEXT     DEFAULT '',
                    social_name         TEXT     DEFAULT '',
                    social_global_name  TEXT     DEFAULT '',
                    social_avatar       TEXT     DEFAULT '',
                    social_locale       TEXT     DEFAULT '',
                    access_token        TEXT     DEFAULT '',
                    access_token_secret TEXT     DEFAULT '',
                    x_followers_count   INTEGER  DEFAULT 0,
                    x_friends_count     INTEGER  DEFAULT 0,
                    x_listed_count      INTEGER  DEFAULT 0,
                    x_favourites_count  INTEGER  DEFAULT 0,
                    x_statuses_count    INTEGER  DEFAULT 0,
                    x_created_at        TEXT     DEFAULT '',
                    x_email             TEXT     DEFAULT '',
                    status              INTEGER  DEFAULT 0,  -- 状态: 0 failed, 1 doing, 2 success
                    created_time        DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_time        DATETIME DEFAULT NULL
                );
            """)
            
            # 创建社交用户表 dc
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wenda_users_social_dc (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    userid              TEXT     DEFAULT '',
                    social_uuid         TEXT     DEFAULT '',
                    social_type         TEXT     DEFAULT '',  -- authorize/join
                    social_action       TEXT     DEFAULT '',  -- '' or 0: - join false / 1: join true
                    social_id           TEXT     DEFAULT '',
                    social_name         TEXT     DEFAULT '',
                    social_global_name  TEXT     DEFAULT '',
                    social_avatar       TEXT     DEFAULT '',
                    social_locale       TEXT     DEFAULT '',
                    status              INTEGER  DEFAULT 0,  -- 状态: 0 failed, 1 doing, 2 success
                    created_time        DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_time        DATETIME DEFAULT NULL
                );
            """)
            
            # 创建NFT表
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wenda_nft_onchain (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_address    TEXT     DEFAULT '',
                    tx_chainid          INTEGER  DEFAULT 0,  -- chainid
                    tx_blockid          INTEGER  DEFAULT 0,  -- blockid
                    tx_hash             TEXT     DEFAULT '',
                    tx_date             TEXT     DEFAULT '',
                    tx_amount_sxp       float    DEFAULT 0.0,
                    tx_amount_eth       float    DEFAULT 0.0,
                    tx_amount_total     float    DEFAULT 0.0,
                    tx_address          TEXT     DEFAULT '',
                    referral_code       TEXT     DEFAULT '',
                    par_address         TEXT     DEFAULT '',
                    par_referral_code   TEXT     DEFAULT '',
                    nft_id              INTEGER  DEFAULT 0,
                    nft_boxid           INTEGER  DEFAULT 0,
                    nft_timestamp       INTEGER  DEFAULT 0,
                    status              TINYINT  DEFAULT 0,  -- -1 failed, 0 pending, 1 mint, 2 addkol
                    note                TEXT     DEFAULT '',
                    created_time        DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_time        DATETIME DEFAULT NULL
                );
            """)
            
            if should_commit:
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            if should_commit:
                await conn.rollback()
            raise
            raise HTTPException(status_code=500, detail="Failed to create tables")
        finally:
            if should_commit:
                await conn.close()

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        if self.pool is None:
            await self.connect()
        if self.pool is None:  # 再次检查连接是否成功建立
            raise RuntimeError("Failed to establish database connection")
        try:
            yield self.pool
        finally:
            pass  # SQLite 不需要显式关闭连接池中的连接

class MySQLDatabase(Database):
    def __init__(self, url: str):
        super().__init__(url)
        parsed_url = urllib.parse.urlparse(url)
        self.host = parsed_url.hostname
        self.port = parsed_url.port or 3306
        self.username = urllib.parse.unquote(parsed_url.username)
        self.password = urllib.parse.unquote(parsed_url.password)
        self.db = parsed_url.path.lstrip('/')
        logger.info(f"MySQL URL: host={self.host}, port={self.port}, user={self.username}, db={self.db}")

    async def connect(self) -> None:
        try:
            # 创建连接池
            self.pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.db,
                maxsize=DB_MAXCONNECT
            )
            if self.pool:
                logger.info("Connected to PostgreSQL database")
                # 连接成功后检查并创建表
                await self.create_tables()
        except aiomysql.Error as e:
            logger.error(f"Failed to connect to MySQL database: {e}")
            # 检查错误是否与用户不存在相关
            if "does not exist" in str(e) or "authentication" in str(e).lower():
                logger.error(f"Database authentication failed: {e}")
                raise Exception(f"Database authentication failed: {e}")
            else:
                # 尝试创建数据库
                try:
                    await self.create_database()
                    # 重新尝试创建连接池
                    self.pool = await aiomysql.create_pool(
                        host=self.host,
                        port=self.port,
                        user=self.username,
                        password=self.password,
                        db=self.db,
                        maxsize=DB_MAXCONNECT
                    )
                    if self.pool:
                        logger.info("Connected to MySQL database")
                        # 连接成功后检查并创建表
                        await self.create_tables()
                except Exception as create_error:
                    logger.error(f"Failed to create or connect to database: {create_error}")
                    raise Exception(f"Failed to initialize database: {create_error}")

    async def create_database(self) -> None:
        try:
            async with aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password
            ) as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
                    await conn.commit()
        except aiomysql.Error as e:
            logger.error(f"Failed to create MySQL database: {e}")
            # return False
            raise
            raise HTTPException(status_code=500, detail="Failed to create database")

    async def create_tables(self) -> None:
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # 检查表是否存在 mysql wenda_users
                    await cursor.execute("SHOW TABLES LIKE 'wenda_users'")
                    table_exists = await cursor.fetchone()
                    if not table_exists:
                        # 表不存在，创建表
                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS wenda_users (
                                `id`                   int           NOT NULL AUTO_INCREMENT COMMENT 'id',
                                `register_code`        varchar(16)   NOT NULL    COMMENT '注册码',      -- 一人一码
                                `email`                varchar(128)  DEFAULT ''  COMMENT '邮箱',
                                `userid`               varchar(32)   DEFAULT ''  COMMENT '用户ID',
                                `username`             varchar(64)   DEFAULT ''  COMMENT '用户名',
                                `password`             varchar(255)  DEFAULT ''  COMMENT '密码哈希',
                                `address`              varchar(64)   DEFAULT ''  COMMENT 'Address',
                                `social_dc`            varchar(64)   DEFAULT ''  COMMENT 'Discord',
                                `social_x`             varchar(64)   DEFAULT ''  COMMENT 'Twitter',
                                `state`                varchar(32)   DEFAULT 'UNVERIFIED' COMMENT '用户状态',    -- UNVERIFIED/VERIFIED/ACTIVE
                                `cooldown_time`        datetime      DEFAULT NULL,
                                `created_time`         datetime      DEFAULT CURRENT_TIMESTAMP,
                                `updated_time`         datetime      DEFAULT NULL,
                                PRIMARY KEY (`id`)  USING BTREE,
                                INDEX idx_email (email),
                                INDEX idx_userid (userid),
                                INDEX idx_username (username)
                            ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
                        """)
                        # 插入数据
                        await cursor.execute("""
                            INSERT INTO wenda_users (`id`, `register_code`, `email`, `userid`, `username`, `password`, `state`, `created_time`) VALUES 
                            (1, 'KOMAC', 'admin@liuhai.com', '1033809395880', 'admin',   '$2b$12$YoWDyiLyzZr/GU9nCT3rQeN7x7e5V7WtSRHkc0KP3.pwGWjxShDyO', 'VERIFIED', '2025-07-11 07:28:39'),
                            (2, 'PMOCR', '370887876@qq.com', '1033963391743', 'pangtou', '$2b$12$sq3jCm0Z6B9W.H5MsLXh7.O.zlMDmLgwUrQM2V3XX144AL0GKqpFS', 'VERIFIED', '2025-07-11 07:28:39'),
                            (3, 'ZD3JU', 'psh001@qq.com', '1040492267738', 'psh001', '$2b$12$hnJh2jqkn2etSAjhq19saORj0z5NiwE4znZTp6c5y/H4cOB1B5Yku', 'VERIFIED', '2025-07-11 07:28:39');
                        """)
                        await conn.commit()
                    
                    # 检查表是否存在 mysql wenda_users_social_x
                    await cursor.execute("SHOW TABLES LIKE 'wenda_users_social_x'")
                    table_exists = await cursor.fetchone()
                    if not table_exists:
                        # 表不存在，创建表
                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS wenda_users_social_x (
                                `id`                  int           NOT NULL AUTO_INCREMENT COMMENT 'id',
                                `userid`              varchar(32)   DEFAULT '' COMMENT '用户ID',
                                `social_uuid`         varchar(64)   DEFAULT '' COMMENT 'twitter',
                                `social_type`         varchar(16)   DEFAULT '' COMMENT 'twitter',  -- authorize/follow/retweet
                                `social_action`       varchar(8)    DEFAULT '' COMMENT 'twitter',  -- '' or 0: - follow false / 1: follow true
                                `social_id`           varchar(32)   DEFAULT '' COMMENT 'ID',
                                `social_name`         varchar(64)   DEFAULT '' COMMENT '名字',
                                `social_global_name`  varchar(64)   DEFAULT '' COMMENT '全名',
                                `social_avatar`       varchar(128)  DEFAULT '' COMMENT '头像',
                                `social_locale`       varchar(32)   DEFAULT '' COMMENT '语言',
                                `access_token`        varchar(64)   DEFAULT '' COMMENT 'twitter',
                                `access_token_secret` varchar(64)   DEFAULT '' COMMENT 'twitter',
                                `x_followers_count`   int           DEFAULT 0,
                                `x_friends_count`     int           DEFAULT 0,
                                `x_listed_count`      int           DEFAULT 0,
                                `x_favourites_count`  int           DEFAULT 0,
                                `x_statuses_count`    int           DEFAULT 0,
                                `x_created_at`        varchar(32)   DEFAULT '',
                                `x_email`             varchar(64)   DEFAULT '',
                                `status`              int           DEFAULT 0  COMMENT '状态',    -- 0 failed, 1 doing, 2 success
                                `created_time`        datetime   DEFAULT NOW() COMMENT '创建时间',
                                `updated_time`        datetime   DEFAULT NULL  COMMENT '更新时间',
                                PRIMARY KEY (`id`)  USING BTREE,
                                INDEX idx_userid (userid),
                                INDEX idx_social_uuid (social_uuid)
                            ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
                        """)
                        await conn.commit()
                    
                    # 检查表是否存在 mysql wenda_users_social_dc
                    await cursor.execute("SHOW TABLES LIKE 'wenda_users_social_dc'")
                    table_exists = await cursor.fetchone()
                    if not table_exists:
                        # 表不存在，创建表
                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS wenda_users_social_dc (
                                `id`                  int           NOT NULL AUTO_INCREMENT COMMENT 'id',
                                `userid`              varchar(32)   DEFAULT '' COMMENT '用户ID',
                                `social_uuid`         varchar(64)   DEFAULT '' COMMENT 'discord',
                                `social_type`         varchar(16)   DEFAULT '' COMMENT 'discord',  -- authorize/join
                                `social_action`       varchar(8)    DEFAULT '' COMMENT 'discord',  -- '' or 0: - join false / 1: join true
                                `social_id`           varchar(32)   DEFAULT '' COMMENT 'ID',
                                `social_name`         varchar(64)   DEFAULT '' COMMENT '名字',
                                `social_global_name`  varchar(64)   DEFAULT '' COMMENT '全名',
                                `social_avatar`       varchar(128)  DEFAULT '' COMMENT '头像',
                                `social_locale`       varchar(32)   DEFAULT '' COMMENT '语言',
                                `status`              int           DEFAULT 0  COMMENT '状态',    -- 0 failed, 1 doing, 2 success
                                `created_time`        datetime   DEFAULT NOW() COMMENT '创建时间',
                                `updated_time`        datetime   DEFAULT NULL  COMMENT '更新时间',
                                PRIMARY KEY (`id`)  USING BTREE,
                                INDEX idx_userid (userid),
                                INDEX idx_social_uuid (social_uuid)
                            ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
                        """)
                        await conn.commit()
                    
                    # 检查表是否存在 mysql wenda_nft_onchain
                    await cursor.execute("SHOW TABLES LIKE 'wenda_nft_onchain'")
                    table_exists = await cursor.fetchone()
                    if not table_exists:
                        # 表不存在，创建表
                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS wenda_nft_onchain (
                                `id`                  int           NOT NULL AUTO_INCREMENT COMMENT 'id',
                                `contract_address`    varchar(64)   NOT NULL    COMMENT '合约地址',
                                `tx_chainid`          int           DEFAULT 0,  -- chainid
                                `tx_blockid`          int           DEFAULT 0,  -- blockid
                                `tx_hash`             varchar(128)  DEFAULT '',
                                `tx_date`             varchar(32)   DEFAULT '',
                                `tx_amount_sxp`       float         DEFAULT 0.0 COMMENT 'SXP金额',
                                `tx_amount_eth`       float         DEFAULT 0.0 COMMENT 'ETH金额',
                                `tx_amount_total`     float         DEFAULT 0.0 COMMENT '换算后ETH总额',
                                `tx_address`          varchar(64)   NOT NULL    COMMENT '钱包地址',
                                `referral_code`       varchar(16)   NOT NULL    COMMENT '邀请码',  -- 固定不变 由钱包生成
                                `par_address`         varchar(64)   DEFAULT ''  COMMENT '父级地址',
                                `par_referral_code`   varchar(32)   DEFAULT ''  COMMENT '父级邀请码',
                                `nft_id`              int           DEFAULT 0   COMMENT 'NFTID',
                                `nft_boxid`           int           DEFAULT 0   COMMENT 'NFT等级',  -- B0-B1
                                `nft_timestamp`       int           DEFAULT 0   COMMENT 'NFT时间戳',
                                `status`              TINYINT       DEFAULT 0   COMMENT '交易状态', -- -1 failed, 0 pending, 1 mint, 2 addkol
                                `note`                varchar(256)  DEFAULT ''  COMMENT '备注',
                                `created_time`        datetime      DEFAULT NOW(),
                                `updated_time`        datetime      DEFAULT NULL ,
                                PRIMARY KEY (`id`)  USING BTREE,
                                INDEX idx_txhash_nftid (tx_hash,nft_id)
                            ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
                        """)
                        await conn.commit()
                    
        except aiomysql.Error as e:
            logger.error(f"Failed to create or check table: {e}")
            raise
            raise HTTPException(status_code=500, detail="Failed to create or check table")

    async def disconnect(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiomysql.Connection, None]:
        if self.pool is None:
            await self.connect()
        if self.pool is None:  # 再次检查连接是否成功建立
            raise RuntimeError("Failed to establish database connection")
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            if self.pool:
                self.pool.release(conn)

class PostgreSQLDatabase(Database):
    def __init__(self, url: str):
        super().__init__(url)
        parsed_url = urllib.parse.urlparse(url)
        self.host = parsed_url.hostname
        self.port = parsed_url.port or 5432
        self.username = urllib.parse.unquote(parsed_url.username)
        self.password = urllib.parse.unquote(parsed_url.password)
        self.db = parsed_url.path.lstrip('/')
        logger.info(f"PostgreSQL URL: host={self.host}, port={self.port}, user={self.username}, db={self.db}")

    async def connect(self) -> None:
        try:
            # 创建连接池
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.db,
                min_size=5,
                max_size=DB_MAXCONNECT
            )
            if self.pool:
                logger.info("Connected to PostgreSQL database")
                # 连接成功后检查并创建表
                await self.create_tables()
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL database: {e}")
            # 检查错误是否与用户不存在相关
            if "does not exist" in str(e) or "authentication" in str(e).lower():
                logger.error(f"Database authentication failed: {e}")
                raise Exception(f"Database authentication failed: {e}")
            else:
                # 尝试创建数据库
                try:
                    await self.create_database()
                    # 重新尝试创建连接池
                    self.pool = await asyncpg.create_pool(
                        host=self.host,
                        port=self.port,
                        user=self.username,
                        password=self.password,
                        database=self.db,
                        min_size=5,
                        max_size=20
                    )
                    if self.pool:
                        logger.info("Connected to PostgreSQL database")
                        # 连接成功后检查并创建表
                        await self.create_tables()
                except Exception as create_error:
                    logger.error(f"Failed to create or connect to database: {create_error}")
                    raise Exception(f"Failed to initialize database: {create_error}")

    async def create_database(self) -> None:
        try:
            # 连接到默认数据库以创建新数据库
            default_pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database='postgres'  # 使用默认数据库
            )
            
            async with default_pool.acquire() as conn:
                db_exists = await conn.fetchval(f"SELECT 1 FROM pg_database WHERE datname = '{self.db}'")
                if not db_exists:
                    await conn.execute(f"CREATE DATABASE {self.db}")
                else:
                    logger.info(f"Database {self.db} already exists")
            await default_pool.close()
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL database: {e}")
            # return False
            raise
            raise HTTPException(status_code=500, detail="Failed to create database")

    async def create_tables(self) -> None:
        try:
            async with self.pool.acquire() as conn:
                # 检查表是否存在 postgresql wenda_users
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'wenda_users'
                    );
                """)
                if not table_exists:
                    # 表不存在，创建表
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS wenda_users (
                            id            SERIAL PRIMARY KEY,
                            register_code VARCHAR(16)  NOT NULL, -- 一人一码
                            email         VARCHAR(128) DEFAULT '',
                            userid        VARCHAR(32)  DEFAULT '',
                            username      VARCHAR(64)  DEFAULT '',
                            password      VARCHAR(255) DEFAULT '',
                            address       VARCHAR(64)  DEFAULT '', -- Address
                            social_dc     VARCHAR(64)  DEFAULT '', -- Discord
                            social_x      VARCHAR(64)  DEFAULT '', -- Twitter
                            state         VARCHAR(32)  DEFAULT 'UNVERIFIED', -- 用户状态: UNVERIFIED/VERIFIED/ACTIVE
                            cooldown_time TIMESTAMP    DEFAULT NULL,
                            created_time  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                            updated_time  TIMESTAMP    DEFAULT NULL
                        );
                    """)
                    # 创建索引
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON wenda_users(email);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_userid ON wenda_users(userid);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_username ON wenda_users(username);")
                    
                    # 插入数据
                    await conn.execute("""
                        INSERT INTO wenda_users (id, register_code, email, userid, username, password, state, created_time) VALUES 
                        (1, 'KOMAC', 'admin@liuhai.com', '1033809395880', 'admin',   '$2b$12$YoWDyiLyzZr/GU9nCT3rQeN7x7e5V7WtSRHkc0KP3.pwGWjxShDyO', 'VERIFIED', '2025-07-11 07:28:39'),
                        (2, 'PMOCR', '370887876@qq.com', '1033963391743', 'pangtou', '$2b$12$sq3jCm0Z6B9W.H5MsLXh7.O.zlMDmLgwUrQM2V3XX144AL0GKqpFS', 'VERIFIED', '2025-07-11 07:28:39'),
                        (3, 'ZD3JU', 'psh001@qq.com', '1040492267738', 'psh001', '$2b$12$hnJh2jqkn2etSAjhq19saORj0z5NiwE4znZTp6c5y/H4cOB1B5Yku', 'VERIFIED', '2025-07-11 07:28:39');
                    """)
                    # 设置自增ID
                    await conn.execute("""
                        SELECT setval('wenda_users_id_seq', (SELECT MAX(id) FROM wenda_users));
                    """)
                
                # 检查表是否存在 postgresql wenda_users_social_x
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'wenda_users_social_x'
                    );
                """)
                if not table_exists:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS wenda_users_social_x (
                            id                  SERIAL        NOT NULL PRIMARY KEY,
                            userid              VARCHAR(32)   DEFAULT '',
                            social_uuid         VARCHAR(64)   DEFAULT '',
                            social_type         VARCHAR(16)   DEFAULT '',  -- authorize/follow/retweet
                            social_action       VARCHAR(8)    DEFAULT '',  -- '' or 0: - follow false / 1: follow true
                            social_id           VARCHAR(32)   DEFAULT '',
                            social_name         VARCHAR(64)   DEFAULT '',
                            social_global_name  VARCHAR(64)   DEFAULT '',
                            social_avatar       VARCHAR(128)  DEFAULT '',
                            social_locale       VARCHAR(32)   DEFAULT '',
                            access_token        VARCHAR(64)   DEFAULT '',
                            access_token_secret VARCHAR(64)   DEFAULT '',
                            x_followers_count   INTEGER       DEFAULT 0,
                            x_friends_count     INTEGER       DEFAULT 0,
                            x_listed_count      INTEGER       DEFAULT 0,
                            x_favourites_count  INTEGER       DEFAULT 0,
                            x_statuses_count    INTEGER       DEFAULT 0,
                            x_created_at        VARCHAR(32)   DEFAULT '',
                            x_email             VARCHAR(64)   DEFAULT '',
                            status              INTEGER       DEFAULT 0,    -- 状态: 0 failed, 1 doing, 2 success
                            created_time        TIMESTAMP     DEFAULT NOW(),
                            updated_time        TIMESTAMP     DEFAULT NULL
                        );
                    """)
                    # 创建索引
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_userid ON wenda_users_social_x(userid);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_social_uuid ON wenda_users_social_x(social_uuid);")
                
                # 检查表是否存在 postgresql wenda_users_social_dc
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'wenda_users_social_dc'
                    );
                """)
                if not table_exists:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS wenda_users_social_dc (
                            id                  SERIAL        NOT NULL PRIMARY KEY,
                            userid              VARCHAR(32)   DEFAULT '',
                            social_uuid         VARCHAR(64)   DEFAULT '',
                            social_type         VARCHAR(16)   DEFAULT '',  -- authorize/join
                            social_action       VARCHAR(8)    DEFAULT '',  -- '' or 0: - join false / 1: join true
                            social_id           VARCHAR(32)   DEFAULT '',
                            social_name         VARCHAR(64)   DEFAULT '',
                            social_global_name  VARCHAR(64)   DEFAULT '',
                            social_avatar       VARCHAR(128)  DEFAULT '',
                            social_locale       VARCHAR(32)   DEFAULT '',
                            status              INTEGER       DEFAULT 0,    -- 状态: 0 failed, 1 doing, 2 success
                            created_time        TIMESTAMP     DEFAULT NOW(),
                            updated_time        TIMESTAMP     DEFAULT NULL
                        );
                    """)
                    # 创建索引
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_userid ON wenda_users_social_dc(userid);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_social_uuid ON wenda_users_social_dc(social_uuid);")
                
                # 检查表是否存在 postgresql wenda_nft_onchain
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'wenda_nft_onchain'
                    );
                """)
                if not table_exists:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS wenda_nft_onchain (
                            id                  SERIAL        NOT NULL PRIMARY KEY,
                            contract_address    VARCHAR(64)   NOT NULL,
                            tx_chainid          INTEGER       DEFAULT 0,  -- chainid
                            tx_blockid          INTEGER       DEFAULT 0,  -- blockid
                            tx_hash             VARCHAR(128)  DEFAULT '',
                            tx_date             VARCHAR(32)   DEFAULT '',
                            tx_amount_sxp       FLOAT         DEFAULT 0.0,
                            tx_amount_eth       FLOAT         DEFAULT 0.0,
                            tx_amount_total     FLOAT         DEFAULT 0.0,
                            tx_address          VARCHAR(64)   NOT NULL,
                            referral_code       VARCHAR(16)   NOT NULL,
                            par_address         VARCHAR(64)   DEFAULT '',
                            par_referral_code   VARCHAR(32)   DEFAULT '',
                            nft_id              INTEGER       DEFAULT 0 ,
                            nft_boxid           INTEGER       DEFAULT 0 ,
                            nft_timestamp       INTEGER       DEFAULT 0 ,
                            status              INTEGER       DEFAULT 0 ,
                            note                VARCHAR(256)  DEFAULT '',
                            created_time        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
                            updated_time        TIMESTAMP     DEFAULT NULL 
                        );
                    """)
                    # 创建索引
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_tx_hash ON wenda_nft_onchain(tx_hash);")
                    # 创建约束
                    await conn.execute("ALTER TABLE wenda_nft_onchain ADD CONSTRAINT uk_wenda_nft_onchain_txhash_nftid UNIQUE (tx_hash, nft_id);")
                
        except Exception as e:
            logger.error(f"Failed to create or check table: {e}")
            raise
            raise HTTPException(status_code=500, detail="Failed to create or check table")

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        if self.pool is None:
            await self.connect()
        if self.pool is None:  # 再次检查连接是否成功建立
            raise RuntimeError("Failed to establish database connection")
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            if self.pool:
                await self.pool.release(conn)

class PostgreSQLCursor:
    def __init__(self, conn):
        self.conn = conn
        self._last_result = None
        self._rowcount_value = 0
        self.description = None  # 用于兼容性
    
    async def execute(self, query, params=None):
        # 首先处理LIMIT语法转换
        import re
        # 处理参数化的LIMIT子句: LIMIT %s, %s 或 LIMIT %s,%s
        query = re.sub(r'LIMIT\s*%s\s*,\s*%s', 'LIMIT $2 OFFSET $1', query, flags=re.IGNORECASE)
        query = re.sub(r'LIMIT\s*%s,%s', 'LIMIT $2 OFFSET $1', query, flags=re.IGNORECASE)
        # 处理数字的LIMIT子句: LIMIT 1, 20 或 LIMIT 1,20
        query = re.sub(r'LIMIT\s*(\d+)\s*,\s*(\d+)', r'LIMIT \2 OFFSET \1', query, flags=re.IGNORECASE)
        query = re.sub(r'LIMIT\s*(\d+),(\d+)', r'LIMIT \2 OFFSET \1', query, flags=re.IGNORECASE)
        
        # 然后将 %s 替换为 $1, $2, $3...
        formatted_query = query.replace('%s', '$1')
        param_count = formatted_query.count('%s')
        for i in range(param_count):
            formatted_query = formatted_query.replace('%s', f'${i+1}', 1)
        
        # 区分SELECT和其他操作
        is_select = query.strip().upper().startswith('SELECT')
        
        if params:
            if isinstance(params, tuple):
                if is_select:
                    # 对于SELECT查询，使用fetch
                    self._last_result = await self.conn.fetch(formatted_query, *params)
                    self._rowcount_value = len(self._last_result) if self._last_result is not None else 0
                else:
                    # 对于INSERT/UPDATE/DELETE，使用execute获取影响的行数
                    result = await self.conn.execute(formatted_query, *params)
                    # 从结果中提取影响的行数
                    if result:
                        # 结果通常是类似 "UPDATE 2" 或 "INSERT 0 1" 的字符串
                        parts = result.split()
                        if len(parts) > 1 and parts[-1].isdigit():
                            self._rowcount_value = int(parts[-1])
                        else:
                            self._rowcount_value = 0
                    self._last_result = []
            else:
                if is_select:
                    self._last_result = await self.conn.fetch(formatted_query, params)
                    self._rowcount_value = len(self._last_result) if self._last_result is not None else 0
                else:
                    result = await self.conn.execute(formatted_query, params)
                    if result:
                        parts = result.split()
                        if len(parts) > 1 and parts[-1].isdigit():
                            self._rowcount_value = int(parts[-1])
                        else:
                            self._rowcount_value = 0
                    self._last_result = []
        else:
            if is_select:
                self._last_result = await self.conn.fetch(formatted_query)
                self._rowcount_value = len(self._last_result) if self._last_result is not None else 0
            else:
                result = await self.conn.execute(formatted_query)
                if result:
                    parts = result.split()
                    if len(parts) > 1 and parts[-1].isdigit():
                        self._rowcount_value = int(parts[-1])
                    else:
                        self._rowcount_value = 0
                self._last_result = []
    
    async def fetchone(self):
        if self._last_result and len(self._last_result) > 0:
            first_row = self._last_result[0]
            # 设置description属性，用于元组到字典的转换
            if hasattr(first_row, 'keys'):
                self.description = [(key, None) for key in first_row.keys()]
            return first_row
        return None
    
    async def fetchall(self):
        # return self._last_result or []
        if self._last_result is not None:
            return self._last_result
        else:
            return []
    
    @property
    def rowcount(self):
        return self._rowcount_value
    
    @property
    def connection(self): # 返回连接对象本身，但提供一个空的commit方法
        class ConnectionWrapper:
            def __init__(self, conn):
                self._conn = conn
            
            async def commit(self):
                # PostgreSQL (asyncpg) 通常不需要手动提交
                # 因为它是自动提交的，除非在显式的事务中
                pass
            
            async def rollback(self):
                # 如需要可实现回滚逻辑
                pass
        
        return ConnectionWrapper(self.conn)

# print(f"DB_ENGINE: {DB_ENGINE}")
# 根据配置创建数据库实例
def create_database_instance():
    """根据配置创建相应的数据库实例"""
    if DB_ENGINE == "mysql":
        return MySQLDatabase(url=MYSQL_URL)
    elif DB_ENGINE == "postgresql":
        return PostgreSQLDatabase(url=POSTGRESQL_URL)
    else:  # 默认使用SQLite
        return SQLiteDatabase(url=SQLITE_URL)

database = create_database_instance()

async def get_db() -> AsyncGenerator:
    try:
        async with database.get_connection() as conn:
            if isinstance(database, SQLiteDatabase): # SQLite 使用不同的游标获取方式
                cursor = await conn.cursor()
                try:
                    yield cursor
                finally:
                    await cursor.close()
            elif isinstance(database, MySQLDatabase): # MySQL 使用不同的游标获取方式
                cursor = await conn.cursor()
                try:
                    yield cursor
                finally:
                    await cursor.close()
            elif isinstance(database, PostgreSQLDatabase): # PostgreSQL 兼容的游标实现
                cursor = PostgreSQLCursor(conn)
                yield cursor
    except Exception as e:
            logger.error(f"Database connection error: {str(e)} | Engine: {DB_ENGINE} | URL: {database.url}")
            raise
            raise HTTPException(status_code=500, detail="Database connection error")

@asynccontextmanager
async def get_db_app():
    async with database.get_connection() as conn:
        if isinstance(database, SQLiteDatabase):  # SQLite 使用不同的游标获取方式
            async with conn.cursor() as cursor:
                yield cursor
        elif isinstance(database, MySQLDatabase):  # MySQL 使用不同的游标获取方式
            async with conn.cursor() as cursor:
                yield cursor
        elif isinstance(database, PostgreSQLDatabase):  # PostgreSQL 连接可以直接使用
            cursor = PostgreSQLCursor(conn)
            yield cursor

def format_query_for_db(query: str) -> str:
    """根据数据库类型格式化查询语句"""
    if DB_ENGINE == "sqlite": # 将 %s 替换为 ?
        # 将 %s 替换为 ?
        query = query.replace('%s', '?')
        query = query.replace('NOW()', 'CURRENT_TIMESTAMP')
        
        # 处理 unix_timestamp 语法
        query = re.sub(r'unix_timestamp\(([^)]+)\)', r'strftime(\1)', query)
        
        # 处理 COLLATE utf8mb4_general_ci 语法
        query = re.sub(r'\bCOLLATE\s+utf8mb4_general_ci\b', 'COLLATE NOCASE', query, flags=re.IGNORECASE)
        # query = re.sub(r'([a-zA-Z_][a-zA-Z0-9_]*)\s+COLLATE\s+utf8mb4_general_ci\s+=\s+(%s)', r'LOWER(\1) = LOWER(\2)', query, flags=re.IGNORECASE)
        
        return query
    elif DB_ENGINE == "postgresql": # 将 %s 替换为 $1, $2, $3...
        # 将 %s 替换为 $1, $2, $3...
        param_count = query.count('%s')
        for i in range(param_count):
            query = query.replace('%s', f'${i+1}', 1)
        
        # 处理 LIMIT 语法
        # 处理数字的LIMIT子句: LIMIT 1, 20 或 LIMIT 1,20
        query = re.sub(r'LIMIT\s*(\d+)\s*,\s*(\d+)', r'LIMIT \2 OFFSET \1', query, flags=re.IGNORECASE)
        query = re.sub(r'LIMIT\s*(\d+),(\d+)', r'LIMIT \2 OFFSET \1', query, flags=re.IGNORECASE)
        # 处理参数化的LIMIT子句: LIMIT $2, $3 -> LIMIT $3 OFFSET $2
        query = re.sub(r'LIMIT\s*\$(\d+)\s*,\s*\$(\d+)', lambda m: f'LIMIT ${m.group(2)} OFFSET ${m.group(1)}', query, flags=re.IGNORECASE)
        query = re.sub(r'LIMIT\s*\$(\d+),\$(\d+)', lambda m: f'LIMIT ${m.group(2)} OFFSET ${m.group(1)}', query, flags=re.IGNORECASE)
        
        # 处理 UPDATE JOIN 语法
        # 将 UPDATE table1 alias1 JOIN (subquery) temp ON alias1.key = temp.key SET xxxx 转换为 
        #    UPDATE table1 alias1 SET xxxx FROM (subquery) WHERE alias1.key = temp.key
        query = re.sub(
            r'UPDATE\s+(\w+)\s+(\w+)\s+JOIN\s+\(\s*(.*?)\s*\)\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s+SET\s+(.+?)(?:;|$)',
            lambda m: f"UPDATE {m.group(1)} {m.group(2)} SET {re.sub(f'{m.group(2)}\\.', '', m.group(9))} FROM ({m.group(3)}) {m.group(4)} WHERE {m.group(2)}.{m.group(6)} = {m.group(4)}.{m.group(8)};",
            query,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # 处理 unix_timestamp 语法
        query = re.sub(r'unix_timestamp\(([^)]+)\)', r'EXTRACT(EPOCH FROM \1)', query)
        # 处理 GROUP_CONCAT 函数参数类型
        query = re.sub(r'GROUP_CONCAT\(([^)]+?)\)', r"STRING_AGG(\1::text, ',')", query)
        # 处理 ROUND 函数参数类型
        query = re.sub(r'ROUND\(([^,]+),\s*(\d+)\)', r'ROUND(\1::NUMERIC, \2)', query)
        # 处理 COLLATE utf8mb4_general_ci 语法
        query = re.sub(r'COLLATE\s+utf8mb4_general_ci\s*=', 'ILIKE ', query, flags=re.IGNORECASE)
        query = query.replace('ILIKE  ', 'ILIKE ')
        
        return query
    else:  # MySQL %s 
        return query

def convert_row_to_dict(row, cursor_description=None):
    """将数据库查询结果行转换为字典"""
    if row is None:
        return None
    if isinstance(row, tuple) and cursor_description:
        return dict(zip([desc[0] for desc in cursor_description], row))
    elif hasattr(row, 'keys'):
        return dict(row)
    return row

def format_datetime_fields(data: dict) -> dict:
    """格式化日期时间字段"""
    if data is None:
        return None
    formatted_data = {}
    for key, value in data.items():
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            formatted_data[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, Decimal):
            formatted_data[key] = float(value)
        else:
            formatted_data[key] = value
    return formatted_data

if __name__ == "__main__":
    query='SELECT COUNT(id) as len FROM wenda_nft_onchain WHERE tx_address COLLATE utf8mb4_general_ci=%s AND tx_chainid=%s AND status=1'
    logger.info(f"Formatted query: {query}")
    query1 = format_query_for_db(query)
    logger.info(f"Formatted query1: {query1}")
