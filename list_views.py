import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
uri = os.getenv("MONGO_URI")  # ponelo en .env
db_name = os.getenv("MONGO_DB", "handwash_db")

client = MongoClient(uri)
db = client[db_name]

for c in db.list_collections():
    if c.get("type") == "view":
        print("VIEW :", c["name"])
    else:
        print("COLL :", c["name"])
