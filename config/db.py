from pymongo import MongoClient
from urllib.parse import urlparse
from config.env import ENV
from utils.logger import get_logger

uri = ENV.get('MONGO_URI', '')

_client = None
_db_name = None

logger = get_logger("Database")


def _extract_database_name(uri: str) -> str:
    try:
        parsed = urlparse(uri)
        db_name = parsed.path.lstrip('/')
        if db_name:
            return db_name
        return 'polymarket_copy_trading'
    except Exception:
        return 'polymarket_copy_trading'


async def connect_db():
    global _client, _db_name
    if not uri or not uri.strip():
        logger.info("No MONGO_URI configured — skipping database")
        return
    _db_name = _extract_database_name(uri)
    logger.info(f"Connecting to MongoDB database={_db_name}")
    _client = MongoClient(uri)
    _client.admin.command('ping')
    logger.info("MongoDB connected and responding to ping")


def get_client():
    return _client


def get_database():
    if _client is None:
        return None
    global _db_name
    if _db_name is None:
        _db_name = _extract_database_name(uri)
    return _client[_db_name]
