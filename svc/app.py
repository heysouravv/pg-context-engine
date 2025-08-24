import os, hashlib, orjson, time
from typing import Optional, Dict, Any, List, Literal
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from temporalio.client import Client

from db import (
    fetchone, fetchall, execute, executemany,
    ensure_user_table, ensure_generated_index, _hash_name,
)

from cache import pub

app = FastAPI(title="PG Context Engine", version="0.0.1")

TEMPORAL_TARGET = os.getenv("TEMPORAL_TARGET","temporal:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE","edge-tq")
_temporal = {"client": None}

async def tclient():
    if not _temporal["client"]:
        _temporal["client"] = await Client.connect(TEMPORAL_TARGET)
    return _temporal["client"]

# ======== GLOBAL MIRROR ========

class MirrorPayload(BaseModel):
    version: str
    rows: List[Dict[str, Any]]

@app.post("/mirror/global/{dataset_id}")
async def upload_mirror(dataset_id: str, body: MirrorPayload):
    if not body.rows:
        raise HTTPException(400, "rows[] required")
    checksum = hashlib.sha256(orjson.dumps(body.rows)).hexdigest()
    client = await tclient()
    handle = await client.start_workflow(
        "MirrorIngestWorkflow",
        args=[dataset_id, body.version, checksum, body.rows],
        id=f"mirror-{dataset_id}-{body.version}-{int(time.time())}",
        task_queue=TASK_QUEUE,
    )
    return {"ok": True, "workflow_id": handle.id, "checksum": checksum}

class Ctx(BaseModel):
    filters: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, Any]] = None
    selection: Optional[Dict[str, Any]] = None

@app.post("/context/{user_id}/{dataset_id}")
async def set_ctx(user_id: str, dataset_id: str, ctx: Ctx):
    client = await tclient()
    handle = await client.start_workflow(
        "UserContextWorkflow",
        args=[user_id, dataset_id, ctx.dict()],
        id=f"uctx-{user_id}-{dataset_id}-{int(time.time())}",
        task_queue=TASK_QUEUE,
    )
    return {"ok": True, "workflow_id": handle.id}

@app.get("/sql/urls")
def sql_urls():
    return {
        "read_write_mysql": f"mysql://{os.getenv('MYSQL_USER', 'edge')}:{os.getenv('MYSQL_PASS', 'edgepass')}@{os.getenv('MYSQL_HOST', 'localhost')}:{os.getenv('MYSQL_PORT', '3307')}/{os.getenv('MYSQL_DB', 'edge')}",
        "read_only_mysql":  f"mysql://reader:readpass@{os.getenv('MYSQL_HOST', 'localhost')}:{os.getenv('MYSQL_PORT', '3307')}/{os.getenv('MYSQL_DB', 'edge')}",
        "notes": "Query user tables directly; names are hashed, use /userdb/{user}/{table}/info to discover.",
    }

# ======== USERDB: SCHEMA & INTROSPECTION ========

class IndexSpec(BaseModel):
    name: str = Field(..., pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    path: str
    type: Literal["string","number","integer","boolean","datetime"] = "string"

class SchemaSpec(BaseModel):
    pk_path: str
    ts_path: str = "$.updated_at"
    indexes: List[IndexSpec] = []

@app.post("/userdb/{user_id}/{table}/schema")
def register_schema(user_id: str, table: str, spec: SchemaSpec):
    phy = ensure_user_table(user_id, table, spec.pk_path, spec.ts_path)
    for idx in spec.indexes:
        ensure_generated_index(user_id, table, idx.name, idx.path, idx.type)
    return {"ok": True, "phy_table": phy}

@app.get("/userdb/{user_id}/{table}/info")
def table_info(user_id: str, table: str):
    row = fetchone("SELECT * FROM userdb_tables WHERE user_id=%s AND table_name=%s", (user_id, table))
    if not row:
        raise HTTPException(404, "No schema registered")
    idxs = fetchall(
        "SELECT col_name, json_path, col_type FROM userdb_table_indexes WHERE user_id=%s AND table_name=%s",
        (user_id, table),
    )
    return {"table": row, "indexes": idxs}

# ======== USERDB: DML ========

def _extract_path(obj: Dict[str, Any], path: str) -> Any:
    if not path or not path.startswith("$."):
        return None
    cur = obj
    for part in path[2:].split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

class UpsertRow(BaseModel):
    item: Dict[str, Any]
    client_ts: Optional[int] = None

class UpsertBatch(BaseModel):
    rows: List[UpsertRow]
    mode: Literal["lww","force"] = "lww"

@app.post("/userdb/{user_id}/{table}/upsert")
def upsert_rows(user_id: str, table: str, body: UpsertBatch):
    meta = fetchone("SELECT * FROM userdb_tables WHERE user_id=%s AND table_name=%s", (user_id, table))
    if not meta:
        raise HTTPException(404, "Register schema first")
    phy = meta["phy_table"]
    pk_path = meta["pk_path"]
    ts_path = meta["ts_path"]
    if not body.rows:
        return {"ok": True, "count": 0}

    values = []
    now = int(time.time())
    for r in body.rows:
        pk = _extract_path(r.item, pk_path)
        if pk is None or pk == "":
            raise HTTPException(400, f"Missing PK at {pk_path}")
        ts_val = r.client_ts if r.client_ts is not None else _extract_path(r.item, ts_path)
        ts = int(ts_val) if isinstance(ts_val, (int, float, str)) and str(ts_val).isdigit() else now
        values.append((str(pk), orjson.dumps(r.item).decode(), ts))

    if body.mode == "force":
        sql = f"""
        INSERT INTO `{phy}` (pk, item, updated_at)
        VALUES (%s, CAST(%s AS JSON), %s)
        ON DUPLICATE KEY UPDATE item=VALUES(item), updated_at=VALUES(updated_at)
        """
    else:
        sql = f"""
        INSERT INTO `{phy}` (pk, item, updated_at)
        VALUES (%s, CAST(%s AS JSON), %s)
        ON DUPLICATE KEY UPDATE
          item = IF(VALUES(updated_at) >= updated_at, VALUES(item), item),
          updated_at = GREATEST(updated_at, VALUES(updated_at))
        """
    n = executemany(sql, values)

    for pk, item_json, ts in values:
        pub(f"userdb:{user_id}:{table}", orjson.dumps({
            "type": "upsert", "user_id": user_id, "table": table, "pk": pk, "updated_at": ts, "item": orjson.loads(item_json)
        }))

    return {"ok": True, "count": n, "phy_table": phy}

class DeleteBatch(BaseModel):
    pks: List[str]

@app.post("/userdb/{user_id}/{table}/delete")
def delete_rows(user_id: str, table: str, body: DeleteBatch):
    meta = fetchone("SELECT * FROM userdb_tables WHERE user_id=%s AND table_name=%s", (user_id, table))
    if not meta:
        raise HTTPException(404, "Register schema first")
    phy = meta["phy_table"]
    if not body.pks:
        return {"ok": True, "count": 0}

    sql = f"DELETE FROM `{phy}` WHERE pk=%s"
    n = executemany(sql, [(pk,) for pk in body.pks])

    for pk in body.pks:
        pub(f"userdb:{user_id}:{table}", orjson.dumps({
            "type": "delete", "user_id": user_id, "table": table, "pk": pk
        }))

    return {"ok": True, "count": n, "phy_table": phy}

# ======== USERDB: FAST QUERY ========

@app.get("/userdb/{user_id}/{table}/query")
def query_rows(
    user_id: str,
    table: str,
    since: Optional[int] = Query(None, description="updated_after (epoch seconds)"),
    limit: int = Query(200, ge=1, le=5000),
    order: Literal["asc","desc"] = "desc",
    idx_status: Optional[str] = Query(None, description="Filter by status"),
    idx_country: Optional[str] = Query(None, description="Filter by country"),
    idx_amount: Optional[int] = Query(None, description="Filter by amount"),
):
    meta = fetchone("SELECT * FROM userdb_tables WHERE user_id=%s AND table_name=%s", (user_id, table))
    if not meta:
        raise HTTPException(404, "No schema registered")
    phy = meta["phy_table"]

    where = ["1=1"]
    params: List[Any] = []

    if since is not None:
        where.append("updated_at >= %s")
        params.append(int(since))

    # Handle specific index filters
    filters = {}
    if idx_status is not None:
        filters["idx_status"] = idx_status
    if idx_country is not None:
        filters["idx_country"] = idx_country
    if idx_amount is not None:
        filters["idx_amount"] = idx_amount

    idx_meta = fetchall(
        "SELECT col_name FROM userdb_table_indexes WHERE user_id=%s AND table_name=%s",
        (user_id, table),
    )
    allowed = {row["col_name"] for row in idx_meta}
    
    for k, v in filters.items():
        if k in allowed:
            where.append(f"`{k}` = %s")
            params.append(v)

    sql = f"SELECT pk, item, updated_at FROM `{phy}` WHERE {' AND '.join(where)} ORDER BY updated_at {order.upper()} LIMIT %s"
    params.append(limit)
    rows = fetchall(sql, tuple(params))
    out = [{"pk": r["pk"], "updated_at": r["updated_at"], "item": orjson.loads(r["item"])} for r in rows]
    return {"ok": True, "rows": out, "count": len(out), "phy_table": phy}
