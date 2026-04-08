import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

load_dotenv()

URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "handwash_db")
VIEW = os.getenv("MONGO_VIEW")

if not URI or not VIEW:
    raise SystemExit("Falta MONGO_URI o MONGO_VIEW en .env")

def to_str(s: pd.Series) -> pd.Series:
    return s.astype("string")

def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)

client = MongoClient(URI)
docs = list(client[DB][VIEW].find({}))
df = pd.json_normalize(docs, sep=".")

# -------------------------
# 1) PLANA (exploración)
# -------------------------
flat = df.copy()
if "_id" in flat.columns:
    flat["_id"] = to_str(flat["_id"])
if "userId" in flat.columns:
    flat["userId"] = to_str(flat["userId"])
if "employee._id" in flat.columns:
    flat["employee._id"] = to_str(flat["employee._id"])
if "sector._id" in flat.columns:
    flat["sector._id"] = to_str(flat["sector._id"])
if "date" in flat.columns:
    flat["date"] = to_dt(flat["date"])

# -------------------------
# 2) MODELO BI (estrella)
# -------------------------
# FACT
fact_cols = [
    "_id", "id", "type", "date",
    "userId",
    "score.total",
    "employee._id", "employee.id",
    "sector._id", "sector.id",
]
fact_cols = [c for c in fact_cols if c in df.columns]

fact = df[fact_cols].copy().rename(columns={
    "_id": "event_oid",
    "id": "event_id",
    "userId": "user_oid",
    "score.total": "score_total",
    "employee._id": "employee_oid",
    "employee.id": "employee_id",
    "sector._id": "sector_oid",
    "sector.id": "sector_id",
})

for c in ["event_oid", "user_oid", "employee_oid", "sector_oid"]:
    if c in fact.columns:
        fact[c] = to_str(fact[c])
if "date" in fact.columns:
    fact["date"] = to_dt(fact["date"])

# DIM employee
emp_cols = [c for c in df.columns if c.startswith("employee.")]
dim_employee = pd.DataFrame()
if emp_cols:
    dim_employee = df[emp_cols].copy()
    dim_employee = dim_employee.rename(columns=lambda c: c.replace("employee.", ""))
    if "_id" in dim_employee.columns:
        dim_employee["_id"] = to_str(dim_employee["_id"])
        dim_employee = dim_employee.drop_duplicates(subset=["_id"]).rename(columns={"_id": "employee_oid"})
    for col in ["createdAt", "updatedAt", "lastActivity"]:
        if col in dim_employee.columns:
            dim_employee[col] = to_dt(dim_employee[col])

# DIM sector
sec_cols = [c for c in df.columns if c.startswith("sector.")]
dim_sector = pd.DataFrame()
if sec_cols:
    dim_sector = df[sec_cols].copy()
    dim_sector = dim_sector.rename(columns=lambda c: c.replace("sector.", ""))
    if "_id" in dim_sector.columns:
        dim_sector["_id"] = to_str(dim_sector["_id"])
        dim_sector = dim_sector.drop_duplicates(subset=["_id"]).rename(columns={"_id": "sector_oid"})

# FACT steps (si existe y trae algo)
steps = pd.DataFrame()
if "score.steps" in df.columns:
    tmp = df[["_id", "score.steps"]].copy().rename(columns={"_id": "event_oid"})
    tmp["event_oid"] = to_str(tmp["event_oid"])
    tmp = tmp.explode("score.steps", ignore_index=True)
    if tmp["score.steps"].dropna().empty:
        steps = pd.DataFrame()
    else:
        if tmp["score.steps"].apply(lambda x: isinstance(x, dict)).any():
            steps_norm = pd.json_normalize(tmp["score.steps"].dropna().tolist(), sep=".")
            steps_norm.index = tmp.loc[tmp["score.steps"].notna()].index
            steps = tmp.drop(columns=["score.steps"]).join(steps_norm)
        else:
            steps = tmp.dropna(subset=["score.steps"]).rename(columns={"score.steps": "step_value"})

try:
    from bson import ObjectId
except Exception:
    ObjectId = None

def coerce_objectids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte cualquier bson.ObjectId dentro del DataFrame a string.
    Esto evita que pyarrow falle al escribir Parquet.
    """
    if ObjectId is None:
        return df

    for col in df.columns:
        s = df[col]
        # Solo tocamos columnas 'object' / mixtas (las problemáticas)
        if s.dtype == "object":
            # si hay al menos un ObjectId, convertimos toda la columna a string (preservando NaN)
            has_oid = s.map(lambda x: isinstance(x, ObjectId)).any()
            if has_oid:
                df[col] = s.map(lambda x: str(x) if isinstance(x, ObjectId) else x)
    return df

# -------------------------
# EXPORT
# -------------------------
os.makedirs("exports", exist_ok=True)

flat = coerce_objectids(flat)
fact = coerce_objectids(fact)
if not dim_employee.empty: dim_employee = coerce_objectids(dim_employee)
if not dim_sector.empty: dim_sector = coerce_objectids(dim_sector)
if not steps.empty: steps = coerce_objectids(steps)

flat.to_parquet(f"exports/{VIEW}_flat.parquet", index=False)
fact.to_parquet("exports/fact_handwashing.parquet", index=False)
if not dim_employee.empty:
    dim_employee.to_parquet("exports/dim_employee.parquet", index=False)
if not dim_sector.empty:
    dim_sector.to_parquet("exports/dim_sector.parquet", index=False)
if not steps.empty:
    steps.to_parquet("exports/fact_handwashing_steps.parquet", index=False)

print("✅ Export OK")
print("Flat:", flat.shape, f"-> exports/{VIEW}_flat.parquet")
print("Fact:", fact.shape, "-> exports/fact_handwashing.parquet")
if not dim_employee.empty: print("Dim employee:", dim_employee.shape, "-> exports/dim_employee.parquet")
if not dim_sector.empty: print("Dim sector:", dim_sector.shape, "-> exports/dim_sector.parquet")
if not steps.empty: print("Steps:", steps.shape, "-> exports/fact_handwashing_steps.parquet")
