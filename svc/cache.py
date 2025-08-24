import os, redis

REDIS = redis.from_url(os.getenv("REDIS_URL"))

def pub(topic: str, payload: bytes):
    REDIS.publish(topic, payload)

def set_kv(key: str, val: bytes, ex: int = None):
    REDIS.set(key, val, ex=ex)

def get_kv(key: str):
    return REDIS.get(key)

def hset_json(pipe, key: str, mapping: dict):
    import orjson
    for k, v in mapping.items():
        pipe.hset(key, k, orjson.dumps(v) if isinstance(v, (dict, list)) else v)
