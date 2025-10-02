from pymongo import MongoClient


def mongodb_connect(connection_string: str) -> MongoClient:
    mongo_client: MongoClient = MongoClient(connection_string)
    return mongo_client
