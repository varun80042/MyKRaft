import json
import redis

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def delete_record(record_type, record_key):
    redis_client.hdel(record_type, record_key)