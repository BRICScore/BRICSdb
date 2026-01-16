from pymongo import AsyncMongoClient
import os

def connectToDB():
    MONGO_URI = os.getenv("MONGO_URI")
    client = AsyncMongoClient(MONGO_URI, maxPoolSize=10)
    if client:
        return client.get_database("brics")
    else:
        raise Exception("Failed to connect to DB")

