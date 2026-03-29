from pymongo.collection import Collection
from config.db import get_database


def get_user_position_model(wallet_address: str) -> Collection:
    db = get_database()
    return db[f'user_positions_{wallet_address}']


def get_user_activity_model(wallet_address: str) -> Collection:
    db = get_database()
    return db[f'user_activities_{wallet_address}']


def get_trading_collection() -> Collection:
    db = get_database()
    return db['trading']
