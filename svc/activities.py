import time, hashlib, orjson
from typing import Any, Dict, List, Optional, Tuple
from temporalio import activity
from db import get_conn
from cache import REDIS, pub

def _now() -> int:
    return int(time.time())

def _extract_id(item: Dict[str, Any]) -> str:
    """Extract unique ID from item - assumes 'id' field exists"""
    item_id = item.get('id')
    if item_id is None:
        raise ValueError(f"Item missing 'id' field: {item}")
    return str(item_id)

def _compute_diff(old_items: List[Dict[str, Any]], new_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compute differences between old and new datasets - optimized for large datasets"""
    if not old_items and not new_items:
        return []
    
    # Use more efficient data structures for large datasets
    old_map = {}
    new_map = {}
    
    # Build maps with error handling
    for item in old_items:
        try:
            item_id = _extract_id(item)
            old_map[item_id] = item
        except ValueError as e:
            print(f"Warning: Skipping item with invalid ID: {e}")
            continue
    
    for item in new_items:
        try:
            item_id = _extract_id(item)
            new_map[item_id] = item
        except ValueError as e:
            print(f"Warning: Skipping item with invalid ID: {e}")
            continue
    
    diffs = []
    ts = _now()
    
    # Find added/updated items
    for item_id, new_item in new_map.items():
        if item_id not in old_map:
            # New item
            diffs.append({
                'diff_type': 'add',
                'item_id': item_id,
                'old_item': None,
                'new_item': new_item,
                'ts': ts
            })
        elif old_map[item_id] != new_item:
            # Updated item
            diffs.append({
                'diff_type': 'update',
                'item_id': item_id,
                'old_item': old_map[item_id],
                'new_item': new_item,
                'ts': ts
            })
    
    # Find deleted items
    for item_id, old_item in old_map.items():
        if item_id not in new_map:
            diffs.append({
                'diff_type': 'delete',
                'item_id': item_id,
                'old_item': old_item,
                'new_item': None,
                'ts': ts
            })
    
    return diffs

@activity.defn
async def validate_continent(dataset_id: str, version: str, checksum: str, n_rows: int):
    if not dataset_id or not version or not checksum:
        raise RuntimeError("Missing fields")
    if n_rows < 0:
        raise RuntimeError("Invalid rows len")
    if n_rows > 10000:  # Set reasonable upper limit
        raise RuntimeError(f"Dataset too large: {n_rows} rows (max: 10,000)")
    
    key = f"seen:{dataset_id}:{version}"
    if REDIS.setnx(key, checksum):
        REDIS.expire(key, 86400)
        return True
    prev = REDIS.get(key)
    if prev and prev.decode() != checksum:
        raise RuntimeError("Checksum mismatch for same dataset+version")
    return True

@activity.defn
async def cache_continent_data(dataset_id: str, version: str, checksum: str, rows: List[Dict[str, Any]]):
    """Immediately cache the new continent data for fast GET responses"""
    ts = _now()
    cache_key = f"continent:{dataset_id}:{version}"
    
    # Cache in Redis for immediate access
    cache_data = {
        "version": version,
        "checksum": checksum,
        "ts": ts,
        "rows": rows,
        "count": len(rows)
    }
    
    # Cache for 24 hours
    REDIS.setex(cache_key, 86400, orjson.dumps(cache_data).decode())
    
    # Also cache in database for persistence
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO continent_cache (dataset_id, version, data, checksum, ts, expires_at)
            VALUES (%s, %s, CAST(%s AS JSON), %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
              data = VALUES(data), 
              checksum = VALUES(checksum), 
              ts = VALUES(ts), 
              expires_at = VALUES(expires_at)
            """,
            (dataset_id, version, orjson.dumps(cache_data).decode(), checksum, ts, ts + 86400),
        )
    except Exception as e:
        print(f"Warning: Failed to cache in database: {e}")
        # Continue even if database caching fails
    finally:
        cur.close()
        conn.close()
    
    return {"dataset_id": dataset_id, "version": version, "cached": True}

@activity.defn
async def compute_continent_diff(dataset_id: str, version: str, checksum: str, rows: List[Dict[str, Any]]):
    """Compute differences between new version and previous version - optimized for large datasets"""
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Get the previous version
        cur.execute(
            "SELECT version FROM continent_versions WHERE dataset_id=%s ORDER BY ts DESC LIMIT 1",
            (dataset_id,)
        )
        prev_row = cur.fetchone()
        parent_version = prev_row[0] if prev_row else None
        
        if parent_version:
            # Get previous rows for diff computation
            cur.execute("SELECT item FROM continent_rows WHERE dataset_id=%s AND version=%s", (dataset_id, parent_version))
            prev_rows = [orjson.loads(row[0]) for row in cur.fetchall()]
            
            # Compute diffs
            diffs = _compute_diff(prev_rows, rows)
            
            # Store diffs in batches for large datasets
            if diffs:
                diff_data = []
                for diff in diffs:
                    try:
                        diff_data.append((
                            dataset_id, version, diff['diff_type'], diff['item_id'],
                            orjson.dumps(diff['old_item']).decode() if diff['old_item'] else None,
                            orjson.dumps(diff['new_item']).decode() if diff['new_item'] else None,
                            diff['ts']
                        ))
                    except Exception as e:
                        print(f"Warning: Failed to prepare diff for item {diff.get('item_id')}: {e}")
                        continue
                
                # Use smaller batch size for diffs to avoid memory issues
                B = 500
                for i in range(0, len(diff_data), B):
                    try:
                        cur.executemany(
                            "INSERT INTO continent_diffs (dataset_id, version, diff_type, item_id, old_item, new_item, ts) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            diff_data[i:i+B]
                        )
                    except Exception as e:
                        print(f"Warning: Failed to insert diff batch {i//B + 1}: {e}")
                        # Continue with next batch
            
            diff_checksum = hashlib.sha256(orjson.dumps(diffs)).hexdigest()
        else:
            # First version - all items are additions
            diffs = []
            for item in rows:
                try:
                    item_id = _extract_id(item)
                    diffs.append({
                        'diff_type': 'add',
                        'item_id': item_id,
                        'old_item': None,
                        'new_item': item,
                        'ts': _now()
                    })
                except ValueError as e:
                    print(f"Warning: Skipping item with invalid ID: {e}")
                    continue
            
            diff_checksum = hashlib.sha256(orjson.dumps(diffs)).hexdigest()
            
            # Store initial diffs
            if diffs:
                diff_data = []
                for diff in diffs:
                    try:
                        diff_data.append((
                            dataset_id, version, diff['diff_type'], diff['item_id'],
                            None, orjson.dumps(diff['new_item']).decode(), diff['ts']
                        ))
                    except Exception as e:
                        print(f"Warning: Failed to prepare initial diff: {e}")
                        continue
                
                # Use smaller batch size for diffs
                B = 500
                for i in range(0, len(diff_data), B):
                    try:
                        cur.executemany(
                            "INSERT INTO continent_diffs (dataset_id, version, diff_type, item_id, old_item, new_item, ts) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            diff_data[i:i+B]
                        )
                    except Exception as e:
                        print(f"Warning: Failed to insert initial diff batch {i//B + 1}: {e}")
        
        return {"dataset_id": dataset_id, "version": version, "diff_count": len(diffs), "diff_checksum": diff_checksum, "parent_version": parent_version}
    
    except Exception as e:
        print(f"Error in compute_continent_diff: {e}")
        raise
    finally:
        cur.close()
        conn.close()

@activity.defn
async def commit_continent_data(dataset_id: str, version: str, checksum: str, rows: List[Dict[str, Any]], parent_version: Optional[str] = None, diff_checksum: Optional[str] = None):
    """Commit continent data to database (runs asynchronously after caching) - optimized for large datasets"""
    ts = _now()
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Insert version record
        cur.execute(
            """
            INSERT INTO continent_versions (dataset_id, version, checksum, ts, parent_version, diff_checksum, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'ready')
            ON DUPLICATE KEY UPDATE 
              checksum = VALUES(checksum), 
              ts = VALUES(ts), 
              parent_version = VALUES(parent_version), 
              diff_checksum = VALUES(diff_checksum),
              status = 'ready'
            """,
            (dataset_id, version, checksum, ts, parent_version, diff_checksum),
        )
        
        # Clear old rows for this version
        cur.execute("DELETE FROM continent_rows WHERE dataset_id=%s AND version=%s", (dataset_id, version))
        
        # Insert new rows in optimized batches
        if rows:
            sql = "INSERT INTO continent_rows (dataset_id, version, item) VALUES (%s, %s, CAST(%s AS JSON))"
            data = []
            for r in rows:
                try:
                    if isinstance(r, (dict, list)):
                        data.append((dataset_id, version, orjson.dumps(r).decode()))
                    else:
                        data.append((dataset_id, version, r))
                except Exception as e:
                    print(f"Warning: Failed to prepare row for insertion: {e}")
                    continue
            
            # Use larger batch size for rows (more efficient)
            B = 1000
            for i in range(0, len(data), B):
                try:
                    cur.executemany(sql, data[i:i+B])
                except Exception as e:
                    print(f"Warning: Failed to insert row batch {i//B + 1}: {e}")
                    # Continue with next batch
        
        return {"dataset_id": dataset_id, "version": version, "count": len(rows), "committed": True}
    
    except Exception as e:
        print(f"Error in commit_continent_data: {e}")
        raise
    finally:
        cur.close()
        conn.close()

@activity.defn
async def fanout_continent_update(dataset_id: str, version: str):
    msg = orjson.dumps({"type": "continent_update", "dataset_id": dataset_id, "version": version})
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
      FROM continent_versions mv
      JOIN (
        SELECT dataset_id, MAX(ts) AS max_ts
        FROM continent_versions
        WHERE status = 'ready'
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

    cur.execute("SELECT item FROM continent_rows WHERE dataset_id=%s AND version=%s", (dataset_id, version))
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
