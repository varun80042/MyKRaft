import json
import redis

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def add_record(record_type, record_key, record_data):
    redis_client.hset(record_type, record_key, json.dumps(record_data))