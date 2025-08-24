import os, hashlib, orjson, time
from typing import Optional, Dict, Any, List, Literal
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from temporalio.client import Client

from db import (
    fetchone, fetchall, execute, executemany,
    ensure_user_table, ensure_generated_index, _hash_name,
    get_conn,
)

from cache import pub, REDIS

app = FastAPI(title="PG Context Engine", version="0.0.1")

TEMPORAL_TARGET = os.getenv("TEMPORAL_TARGET","temporal:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE","edge-tq")
_temporal = {"client": None}

async def tclient():
    if not _temporal["client"]:
        _temporal["client"] = await Client.connect(TEMPORAL_TARGET)
    return _temporal["client"]

# ======== CONTINENT DATA VERSIONING SYSTEM ========

class ContinentPayload(BaseModel):
    rows: List[Dict[str, Any]]

@app.post("/continent/{dataset_id}")
async def upload_continent(dataset_id: str, body: ContinentPayload):
    """POST: Immediately cache data and return success, then process asynchronously"""
    if not body.rows:
        raise HTTPException(400, "rows[] required")
    
    # Generate version internally based on timestamp and checksum
    ts = int(time.time())
    checksum = hashlib.sha256(orjson.dumps(body.rows)).hexdigest()
    version = f"v{ts}.{checksum[:8]}"  # Format: v{timestamp}.{first8chars_of_checksum}
    
    # Start async workflow for background processing
    client = await tclient()
    handle = await client.start_workflow(
        "ContinentIngestWorkflow",
        args=[dataset_id, version, checksum, body.rows],
        id=f"continent-{dataset_id}-{version}-{ts}",
        task_queue=TASK_QUEUE,
    )
    
    return {
        "ok": True, 
        "workflow_id": handle.id, 
        "version": version,
        "checksum": checksum,
        "message": "Data cached immediately, processing in background"
    }

@app.put("/continent/{dataset_id}")
async def update_continent(dataset_id: str, body: ContinentPayload):
    """PUT: Same as POST - immediately cache and process asynchronously"""
    return await upload_continent(dataset_id, body)

@app.get("/continent/{dataset_id}")
async def get_continent(dataset_id: str, version: Optional[str] = None):
    """GET: Fast response from cache, falls back to database if needed"""
    
    # Try to get from Redis cache first
    if version:
        cache_key = f"continent:{dataset_id}:{version}"
    else:
        # Get latest version from cache
        latest_key = f"continent:{dataset_id}:latest"
        latest_version = REDIS.get(latest_key)
        if latest_version:
            cache_key = f"continent:{dataset_id}:{latest_version.decode()}"
        else:
            cache_key = None
    
    if cache_key:
        cached_data = REDIS.get(cache_key)
        if cached_data:
            try:
                data = orjson.loads(cached_data)
                return {
                    "ok": True,
                    "source": "cache",
                    "data": data
                }
            except:
                pass  # Fall through to database
    
    # Fallback to database
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    if version:
        # Get specific version
        cur.execute("""
            SELECT cv.*, COUNT(cr.id) as row_count
            FROM continent_versions cv
            LEFT JOIN continent_rows cr ON cv.dataset_id = cr.dataset_id AND cv.version = cr.version
            WHERE cv.dataset_id = %s AND cv.version = %s AND cv.status = 'ready'
            GROUP BY cv.id
        """, (dataset_id, version))
    else:
        # Get latest version
        cur.execute("""
            SELECT cv.*, COUNT(cr.id) as row_count
            FROM continent_versions cv
            LEFT JOIN continent_rows cr ON cv.dataset_id = cr.dataset_id AND cv.version = cr.version
            WHERE cv.dataset_id = %s AND cv.status = 'ready'
            GROUP BY cv.id
            ORDER BY cv.ts DESC
            LIMIT 1
        """, (dataset_id,))
    
    version_info = cur.fetchone()
    if not version_info:
        cur.close()
        conn.close()
        raise HTTPException(404, "Dataset or version not found")
    
    # Get the actual rows
    cur.execute("SELECT item FROM continent_rows WHERE dataset_id = %s AND version = %s", 
                (dataset_id, version_info['version']))
    rows = [orjson.loads(row['item']) for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    # Cache this result for future fast access
    cache_data = {
        "version": version_info['version'],
        "checksum": version_info['checksum'],
        "ts": version_info['ts'],
        "rows": rows,
        "count": len(rows),
        "parent_version": version_info.get('parent_version'),
        "diff_checksum": version_info.get('diff_checksum')
    }
    
    cache_key = f"continent:{dataset_id}:{version_info['version']}"
    REDIS.setex(cache_key, 86400, orjson.dumps(cache_data).decode())
    
    # Update latest version cache
    REDIS.setex(f"continent:{dataset_id}:latest", 86400, version_info['version'])
    
    return {
        "ok": True,
        "source": "database",
        "data": cache_data
    }

@app.get("/continent/{dataset_id}/versions")
async def list_continent_versions(dataset_id: str, limit: int = Query(10, ge=1, le=100)):
    """Get list of available versions for a dataset"""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT cv.*, COUNT(cr.id) as row_count
        FROM continent_versions cv
        LEFT JOIN continent_rows cr ON cv.dataset_id = cr.dataset_id AND cv.version = cr.version
        WHERE cv.dataset_id = %s AND cv.status = 'ready'
        GROUP BY cv.id
        ORDER BY cv.ts DESC
        LIMIT %s
    """, (dataset_id, limit))
    
    versions = cur.fetchall()
    cur.close()
    conn.close()
    
    return {
        "ok": True,
        "dataset_id": dataset_id,
        "versions": versions
    }

@app.get("/continent/{dataset_id}/diff/{version}")
async def get_continent_diff(dataset_id: str, version: str):
    """Get diffs for a specific version (useful for island systems)"""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # Get version info
    cur.execute("""
        SELECT * FROM continent_versions 
        WHERE dataset_id = %s AND version = %s AND status = 'ready'
    """, (dataset_id, version))
    
    version_info = cur.fetchone()
    if not version_info:
        cur.close()
        conn.close()
        raise HTTPException(404, "Version not found")
    
    # Get diffs
    cur.execute("""
        SELECT * FROM continent_diffs 
        WHERE dataset_id = %s AND version = %s
        ORDER BY ts ASC
    """, (dataset_id, version))
    
    diffs = cur.fetchall()
    cur.close()
    conn.close()
    
    return {
        "ok": True,
        "dataset_id": dataset_id,
        "version": version,
        "parent_version": version_info.get('parent_version'),
        "diff_checksum": version_info.get('diff_checksum'),
        "diffs": diffs,
        "diff_count": len(diffs)
    }

@app.get("/continent/{dataset_id}/incremental/{from_version}/{to_version}")
async def get_incremental_update(dataset_id: str, from_version: str, to_version: str):
    """Get incremental update between two versions (for island systems)"""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # Validate versions exist
    cur.execute("""
        SELECT version FROM continent_versions 
        WHERE dataset_id = %s AND version IN (%s, %s) AND status = 'ready'
    """, (dataset_id, from_version, to_version))
    
    versions = [row['version'] for row in cur.fetchall()]
    if len(versions) != 2:
        cur.close()
        conn.close()
        raise HTTPException(404, "One or both versions not found")
    
    # Get diffs for the target version
    cur.execute("""
        SELECT * FROM continent_diffs 
        WHERE dataset_id = %s AND version = %s
        ORDER BY ts ASC
    """, (dataset_id, to_version))
    
    diffs = cur.fetchall()
    cur.close()
    conn.close()
    
    return {
        "ok": True,
        "dataset_id": dataset_id,
        "from_version": from_version,
        "to_version": to_version,
        "diffs": diffs,
        "diff_count": len(diffs),
        "message": "Apply these diffs to your local copy of from_version to get to_version"
    }

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
        "read_only_mysql":  f"mysql://reader:readpass@{os.getenv('MYSQL_HOST', 'localhost')}:{os.getenv('MYSQL_PORT', '3307')}:{os.getenv('MYSQL_DB', 'edge')}",
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
        raise HTTPException(404, "Delete schema first")
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
