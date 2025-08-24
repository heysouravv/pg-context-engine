import os, re, hashlib, time
import mysql.connector
from mysql.connector import pooling

POOL = pooling.MySQLConnectionPool(
    pool_name="edgepool",
    pool_size=12,
    host=os.getenv("MYSQL_HOST", "mysql"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "edge"),
    password=os.getenv("MYSQL_PASS", "edgepass"),
    database=os.getenv("MYSQL_DB", "edge"),
    autocommit=True,
)

def get_conn():
    return POOL.get_connection()

def _now() -> int:
    return int(time.time())

def _hash_name(user_id: str, table: str) -> str:
    h = hashlib.sha1(f"{user_id}:{table}".encode()).hexdigest()[:16]
    return f"udb_{h}"

def fetchone(sql: str, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        try: cur.close()
        except: pass
        conn.close()

def fetchall(sql: str, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        try: cur.close()
        except: pass
        conn.close()

def execute(sql: str, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.rowcount
    finally:
        try: cur.close()
        except: pass
        conn.close()

def executemany(sql: str, seq_params):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, seq_params)
        return cur.rowcount
    finally:
        try: cur.close()
        except: pass
        conn.close()

def ensure_user_table(user_id: str, table_name: str, pk_path: str, ts_path: str):
    phy = _hash_name(user_id, table_name)
    row = fetchone(
        "SELECT phy_table FROM userdb_tables WHERE user_id=%s AND table_name=%s",
        (user_id, table_name),
    )
    if row:
        return row["phy_table"]

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{phy}` (
      pk VARCHAR(191) NOT NULL PRIMARY KEY,
      item JSON NOT NULL,
      updated_at BIGINT NOT NULL,
      KEY idx_updated_at (updated_at DESC)
    ) ENGINE=InnoDB;
    """
    execute(ddl)

    execute(
        """INSERT INTO userdb_tables (user_id, table_name, phy_table, pk_path, ts_path, created_at)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (user_id, table_name, phy, pk_path, ts_path, _now()),
    )
    return phy

def _safe_colname(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$", name):
        raise ValueError("Invalid column name")
    return name

def ensure_generated_index(user_id: str, table_name: str, col_name: str, json_path: str, col_type: str):
    col_name = _safe_colname(col_name)
    row = fetchone(
        "SELECT phy_table FROM userdb_tables WHERE user_id=%s AND table_name=%s",
        (user_id, table_name),
    )
    if not row:
        raise RuntimeError("Table schema not registered")
    phy = row["phy_table"]

    execute(
        """INSERT IGNORE INTO userdb_table_indexes (user_id, table_name, col_name, json_path, col_type)
           VALUES (%s,%s,%s,%s,%s)""",
        (user_id, table_name, col_name, json_path, col_type),
    )

    if col_type == "string":
        expr = f"JSON_UNQUOTE(JSON_EXTRACT(item, '{json_path}'))"
        coldef = f"`{col_name}` VARCHAR(255) GENERATED ALWAYS AS ({expr}) STORED"
    elif col_type == "number":
        expr = f"JSON_EXTRACT(item, '{json_path}')"
        coldef = f"`{col_name}` DOUBLE GENERATED ALWAYS AS ({expr}) STORED"
    elif col_type == "integer":
        expr = f"JSON_EXTRACT(item, '{json_path}')"
        coldef = f"`{col_name}` BIGINT GENERATED ALWAYS AS (CAST({expr} AS SIGNED)) STORED"
    elif col_type == "boolean":
        expr = f"JSON_EXTRACT(item, '{json_path}')"
        coldef = f"`{col_name}` TINYINT(1) GENERATED ALWAYS AS (JSON_EXTRACT(item,'{json_path}') IN (true, 1, 'true')) STORED"
    elif col_type == "datetime":
        expr = f"JSON_UNQUOTE(JSON_EXTRACT(item, '{json_path}'))"
        coldef = f"`{col_name}` DATETIME GENERATED ALWAYS AS (STR_TO_DATE({expr}, '%Y-%m-%dT%H:%i:%s')) STORED"
    else:
        raise ValueError("Unsupported col_type")

    try:
        execute(f"ALTER TABLE `{phy}` ADD COLUMN {coldef}")
    except mysql.connector.Error as e:
        if e.errno not in (1060,):  # duplicate column
            raise

    try:
        execute(f"CREATE INDEX `{col_name}` ON `{phy}`(`{col_name}`)")
    except mysql.connector.Error as e:
        if e.errno not in (1061,):  # duplicate key
            raise

    return phy
