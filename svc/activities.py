import time, hashlib, orjson
from typing import Any, Dict, List
from temporalio import activity
from db import get_conn
from cache import REDIS, pub

def _now() -> int:
    return int(time.time())

@activity.defn
async def validate_mirror(dataset_id: str, version: str, checksum: str, n_rows: int):
    if not dataset_id or not version or not checksum:
        raise RuntimeError("Missing fields")
    if n_rows < 0:
        raise RuntimeError("Invalid rows len")
    key = f"seen:{dataset_id}:{version}"
    if REDIS.setnx(key, checksum):
        REDIS.expire(key, 86400)
        return True
    prev = REDIS.get(key)
    if prev and prev.decode() != checksum:
        raise RuntimeError("Checksum mismatch for same dataset+version")
    return True

@activity.defn
async def commit_global_mirror(dataset_id: str, version: str, checksum: str, rows: List[Dict[str, Any]]):
    ts = _now()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO global_mirror_versions (dataset_id, version, checksum, ts)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE checksum = VALUES(checksum), ts = VALUES(ts)
        """,
        (dataset_id, version, checksum, ts),
    )
    cur.execute("DELETE FROM global_rows WHERE dataset_id=%s AND version=%s", (dataset_id, version))
    if rows:
        sql = "INSERT INTO global_rows (dataset_id, version, item) VALUES (%s, %s, CAST(%s AS JSON))"
        data = [(dataset_id, version, orjson.dumps(r).decode() if isinstance(r, (dict, list)) else r) for r in rows]
        B = 1000
        for i in range(0, len(data), B):
            cur.executemany(sql, data[i:i+B])
    cur.close()
    conn.close()

    REDIS.hset(f"global:{dataset_id}:current", mapping={"version": version, "checksum": checksum, "ts": ts})
    return {"dataset_id": dataset_id, "version": version, "count": len(rows)}

@activity.defn
async def fanout_global_update(dataset_id: str, version: str):
    msg = orjson.dumps({"type": "global_update", "dataset_id": dataset_id, "version": version})
    pub(f"topic:{dataset_id}", msg)
    return True

@activity.defn
async def store_user_ctx(user_id: str, dataset_id: str, ctx: Dict[str, Any]):
    ts = _now()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_contexts (user_id, dataset_id, ctx, ts)
        VALUES (%s, %s, CAST(%s AS JSON), %s)
        ON DUPLICATE KEY UPDATE ctx=VALUES(ctx), ts=VALUES(ts)
        """,
        (user_id, dataset_id, orjson.dumps(ctx).decode(), ts),
    )
    cur.close()
    conn.close()
    return True

def _apply_filters(rows: List[Dict[str, Any]], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    filters = (ctx or {}).get("filters") or {}
    if not filters:
        return rows

    def match(row):
        for k, v in filters.items():
            rv = row.get(k, None)
            if isinstance(v, list):
                if rv not in v:
                    return False
            else:
                if rv != v:
                    return False
        return True

    out = [r for r in rows if match(r)]

    sort = (ctx or {}).get("sort")
    if sort and "by" in sort:
        by = sort["by"]
        desc = bool(sort.get("desc", False))
        out.sort(key=lambda x: x.get(by), reverse=desc)
    return out

@activity.defn
async def project_view(user_id: str, dataset_id: str):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
      SELECT mv.version
      FROM global_mirror_versions mv
      JOIN (
        SELECT dataset_id, MAX(ts) AS max_ts
        FROM global_mirror_versions
        GROUP BY dataset_id
      ) lv ON lv.dataset_id = mv.dataset_id AND lv.max_ts = mv.ts
      WHERE mv.dataset_id=%s
      LIMIT 1
    """, (dataset_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return False
    version = row["version"]

    cur.execute("SELECT ctx FROM user_contexts WHERE user_id=%s AND dataset_id=%s", (user_id, dataset_id))
    ctxrow = cur.fetchone()
    ctx = orjson.loads(ctxrow["ctx"]) if ctxrow and ctxrow.get("ctx") else {}

    cur.execute("SELECT item FROM global_rows WHERE dataset_id=%s AND version=%s", (dataset_id, version))
    raw = cur.fetchall()
    all_rows = [orjson.loads(r["item"]) for r in raw]

    projected = _apply_filters(all_rows, ctx)

    cur.execute("DELETE FROM user_views WHERE user_id=%s AND dataset_id=%s AND version=%s",
                (user_id, dataset_id, version))
    if projected:
        data = [(user_id, dataset_id, version, orjson.dumps(it).decode(), int(time.time())) for it in projected]
        cur.executemany(
            "INSERT INTO user_views (user_id, dataset_id, version, item, ts) VALUES (%s,%s,%s,CAST(%s AS JSON),%s)",
            data
        )
    cur.close()
    conn.close()

    msg = orjson.dumps({"type": "view_ready", "dataset_id": dataset_id, "version": version, "user_id": user_id})
    pub(f"topic:{dataset_id}:{user_id}", msg)
    return True
