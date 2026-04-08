import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "handwash_db")
VIEW_NAME = os.getenv("MONGO_VIEW")  # nombre de la view
LIMIT = int(os.getenv("LIMIT", "0"))  # 0 = sin limit

def flatten_docs(docs: list[dict]) -> pd.DataFrame:
    if not docs:
        return pd.DataFrame()
    df = pd.json_normalize(docs, sep=".")
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
    return df

def main():
    if not MONGO_URI or not VIEW_NAME:
        raise SystemExit("Falta MONGO_URI o MONGO_VIEW en .env")

    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][VIEW_NAME]  # en pymongo se consulta igual que una colección

    cursor = coll.find({}, batch_size=2000)
    if LIMIT > 0:
        cursor = cursor.limit(LIMIT)

    docs = list(cursor)
    df = flatten_docs(docs)

    os.makedirs("exports", exist_ok=True)
    base = os.path.join("exports", VIEW_NAME)

    df.to_csv(base + ".csv", index=False, encoding="utf-8-sig")
    try:
        df.to_parquet(base + ".parquet", index=False)
    except Exception as e:
        print("No pude exportar Parquet (instalá pyarrow). Error:", e)

    print(f"OK ✅ view={VIEW_NAME} filas={len(df)} cols={len(df.columns)}")
    print(base + ".csv")

if __name__ == "__main__":
    main()
