import json
import redis

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_record(record_type, record_key):
    return redis_client.hget(record_type, record_key)