from flask import Flask, jsonify, request
from pyraft import raft
import uuid
import json
from datetime import datetime
import logging
import redis

# from helper.logger import log_message
# from helper.add_record import add_record
# from helper.get_record import get_record
# from helper.delete_record import delete_record

app = Flask(__name__)

node = raft.make_default_node()
node.start()

node_metadata = {"TopicRecord": {}, "PartitionRecord": {}, "BrokerRecord": {}, "ProducerRecord": {}, "RegistrationChangeRecord": {}}

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

log_filename = f'log{node.port}.log'
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

def log_message(message):
    log_entry = f"Node Port: {node.port}, Node ID: {node.nid}, Node Status: {node.state} - {message}"
    logging.info(log_entry)

def add_record(record_type, record_key, record_data):
    redis_client.hset(record_type, record_key, json.dumps(record_data))

def get_record(record_type, record_key):
    return redis_client.hget(record_type, record_key)

def delete_record(record_type, record_key):
    redis_client.hdel(record_type, record_key)

@app.route('/', methods=["GET"])
def home():
    nid = node.nid
    port = node.port
    status = node.state
    log_message("Node hosted successfully!")
    return jsonify({"nid": nid, "port": port, "status": status})

@app.route('/get_node_metadata', methods=["GET"])
def get_node_metadata():
    try:
        for record_type in node_metadata.keys():
            keys = redis_client.hkeys(record_type)
            for key in keys:
                data = redis_client.hget(record_type, key)
                node_metadata[record_type][key.decode("utf-8")] = json.loads(data)
        log_message("Node metadata fetched successfully")
        return jsonify({"message": f"Success! Metadata added to raft node with port {node.port}."})
    except Exception as e:
        log_message(f"Error fetching node metadata: {e}")
        return jsonify({"error": str(e)})
    
@app.route('/show_node_metadata', methods=["GET"])
def show_node_metadata():
    log_message("Node metadata displayed successfully!")
    return jsonify(node_metadata)

@app.route('/add_broker/<broker_name>', methods=["POST"])
def add_broker(broker_name):
    try:
        curl_data = request.json
        if redis_client.hexists("BrokerRecord", broker_name):
            return jsonify({"message": f"Failure. Broker '{broker_name}' already exists!"})
        if node.state == "l":
            internal_uuid = str(uuid.uuid4())
            broker_data = {
                "internalUUID": internal_uuid,
                "brokerId": curl_data.get("brokerId"),
                "brokerHost": curl_data.get("brokerHost"),
                "brokerPort": curl_data.get("brokerPort"),
                "securityProtocol": curl_data.get("securityProtocol"),
                "brokerStatus": "ALIVE",
                "rackId": curl_data.get("rackId"),
                "epoch": 0,
                "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            }
            add_record("BrokerRecord", broker_name, broker_data)
            log_message(f"Broker '{broker_name}' added successfully")
            return jsonify({"message": "Success!"})
        else:
            log_message("Failed to add broker - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error adding broker '{broker_name}': {e}")
        return jsonify({"error": str(e)})

@app.route('/get_broker/<broker_name>', methods=["GET"])
def get_broker(broker_name):
    try:
        broker_data = get_record("BrokerRecord", broker_name)
        if broker_data:
            log_message(f"Broker '{broker_name}' fetched successfully")
            return jsonify(json.loads(broker_data))
        else:
            log_message(f"Broker '{broker_name}' not found")
            return jsonify({"message": f"Failure. Broker '{broker_name}' not found!"})
    except Exception as e:
        log_message(f"Error fetching broker '{broker_name}': {e}")
        return jsonify({"error": str(e)})

@app.route('/delete_broker/<broker_name>', methods=["DELETE"])
def delete_broker(broker_name):
    try:
        if not redis_client.hexists("BrokerRecord", broker_name):
            log_message(f"Broker '{broker_name}' does not exist")
            return jsonify({"message": f"Failure. Broker '{broker_name}' does not exist!"})
        if node.state == "l":
            delete_record("BrokerRecord", broker_name)
            log_message(f"Broker '{broker_name}' deleted successfully")
            return jsonify({"message": "Success. Broker deleted!"})
        else:
            log_message("Failed to delete broker - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error deleting broker '{broker_name}': {e}")
        return jsonify({"error": str(e)})

@app.route('/add_topic/<topic_name>', methods=["POST"])
def add_topic(topic_name):
    try:
        curl_data = request.json
        if redis_client.hexists("TopicRecord", topic_name):
            log_message(f"Topic '{topic_name}' already exists")
            return jsonify({"message": f"Failure. Topic '{topic_name}' already exists!"})
        if node.state == "l":
            topic_uuid = str(uuid.uuid4())
            topic_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            topic_data = {
                "topicUUID": topic_uuid,
                "name": topic_name,
                "timestamp": topic_timestamp
            }
            add_record("TopicRecord", topic_name, topic_data)
            log_message(f"Topic '{topic_name}' added successfully")
            return jsonify({"message": "Success!"})
        else:
            log_message("Failed to add topic - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error adding topic '{topic_name}': {e}")
        return jsonify({"error": str(e)})

@app.route('/get_topic/<topic_name>', methods=["GET"])
def get_topic(topic_name):
    try:
        topic_data = get_record("TopicRecord", topic_name)
        if topic_data:
            log_message(f"Topic '{topic_name}' fetched successfully")
            return jsonify(json.loads(topic_data))
        else:
            log_message(f"Topic '{topic_name}' not found")
            return jsonify({"message": f"Failure. Topic '{topic_name}' not found!"})
    except Exception as e:
        log_message(f"Error fetching topic '{topic_name}': {e}")
        return jsonify({"error": str(e)})

@app.route('/delete_topic/<topic_name>', methods=["DELETE"])
def delete_topic(topic_name):
    try:
        if not redis_client.hexists("TopicRecord", topic_name):
            log_message(f"Topic '{topic_name}' does not exist")
            return jsonify({"message": f"Failure. Topic '{topic_name}' does not exist!"})
        if node.state == "l":
            delete_record("TopicRecord", topic_name)
            log_message(f"Topic '{topic_name}' deleted successfully")
            return jsonify({"message": "Success. Topic deleted!"})
        else:
            log_message("Failed to delete topic - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error deleting topic '{topic_name}': {e}")
        return jsonify({"error": str(e)})

@app.route('/add_partition/<partition_id>', methods=["POST"])
def add_partition(partition_id):
    try:
        curl_data = request.json
        topic_uuid = curl_data.get("topicUUID")
        if redis_client.hexists("PartitionRecord", partition_id):
            log_message(f"Partition '{partition_id}' already exists")
            return jsonify({"message": f"Failure. Partition '{partition_id}' already exists!"})
        if node.state == "l":
            if not redis_client.hexists("TopicRecord", topic_uuid):
                log_message(f"Topic with UUID '{topic_uuid}' does not exist")
                return jsonify({"message": f"Failure. Topic with UUID '{topic_uuid}' does not exist!"})
            else:
                partition_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
                partition_data = {
                    "partitionId": int(partition_id),
                    "topicUUID": topic_uuid,
                    "replicas": curl_data.get("replicas", []),
                    "ISR": curl_data.get("ISR", []),
                    "removingReplicas": [],
                    "addingReplicas": [],
                    "leader": curl_data.get("leader"),
                    "partitionEpoch": 0,
                    "timestamp": partition_timestamp
                }
                add_record("PartitionRecord", partition_id, partition_data)
                log_message(f"Partition '{partition_id}' added successfully")
                return jsonify({"message": "Success!"})
        else:
            log_message("Failed to add partition - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error adding partition '{partition_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/get_partition/<partition_id>', methods=["GET"])
def get_partition(partition_id):
    try:
        partition_data = get_record("PartitionRecord", partition_id)
        if partition_data:
            log_message(f"Partition '{partition_id}' fetched successfully")
            return jsonify(json.loads(partition_data))
        else:
            log_message(f"Partition '{partition_id}' not found")
            return jsonify({"message": f"Failure. Partition '{partition_id}' not found!"})
    except Exception as e:
        log_message(f"Error fetching partition '{partition_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/delete_partition/<partition_id>', methods=["DELETE"])
def delete_partition(partition_id):
    try:
        if not redis_client.hexists("PartitionRecord", partition_id):
            log_message(f"Partition '{partition_id}' does not exist")
            return jsonify({"message": f"Failure. Partition '{partition_id}' does not exist!"})
        if node.state == "l":
            delete_record("PartitionRecord", partition_id)
            log_message(f"Partition '{partition_id}' deleted successfully")
            return jsonify({"message": "Success. Partition deleted!"})
        else:
            log_message("Failed to delete partition - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error deleting partition '{partition_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/add_producer/<producer_id>', methods=["POST"])
def add_producer(producer_id):
    try:
        curl_data = request.json
        if redis_client.hexists("ProducerRecord", producer_id):
            log_message(f"Producer '{producer_id}' already exists")
            return jsonify({"message": f"Failure. Producer '{producer_id}' already exists!"})
        if node.state == "l":
            producer_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            producer_data = {
                "producerId": int(producer_id),
                "producerEpoch": curl_data.get("producerEpoch"),
                "lastSequence": curl_data.get("lastSequence"),
                "coordinatorEpoch": curl_data.get("coordinatorEpoch"),
                "timestamp": producer_timestamp
            }
            add_record("ProducerRecord", producer_id, producer_data)
            log_message(f"Producer '{producer_id}' added successfully")
            return jsonify({"message": "Success!"})
        else:
            log_message("Failed to add producer - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error adding producer '{producer_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/get_producer/<producer_id>', methods=["GET"])
def get_producer(producer_id):
    try:
        producer_data = get_record("ProducerRecord", producer_id)
        if producer_data:
            log_message(f"Producer '{producer_id}' fetched successfully")
            return jsonify(json.loads(producer_data))
        else:
            log_message(f"Producer '{producer_id}' not found")
            return jsonify({"message": f"Failure. Producer '{producer_id}' not found!"})
    except Exception as e:
        log_message(f"Error fetching producer '{producer_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/delete_producer/<producer_id>', methods=["DELETE"])
def delete_producer(producer_id):
    try:
        if not redis_client.hexists("ProducerRecord", producer_id):
            log_message(f"Producer '{producer_id}' does not exist")
            return jsonify({"message": f"Failure. Producer '{producer_id}' does not exist!"})
        if node.state == "l":
            delete_record("ProducerRecord", producer_id)
            log_message(f"Producer '{producer_id}' deleted successfully")
            return jsonify({"message": "Success. Producer deleted!"})
        else:
            log_message("Failed to delete producer - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error deleting producer '{producer_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/add_registration_change/<change_id>', methods=["POST"])
def add_registration_change(change_id):
    try:
        curl_data = request.json
        if redis_client.hexists("RegistrationChangeRecord", change_id):
            log_message(f"Registration change '{change_id}' already exists")
            return jsonify({"message": f"Failure. Registration change '{change_id}' already exists!"})
        if node.state == "l":
            change_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            change_data = {
                "changeId": int(change_id),
                "brokerId": curl_data.get("brokerId"),
                "changeType": curl_data.get("changeType"),
                "changeTimestamp": change_timestamp
            }
            add_record("RegistrationChangeRecord", change_id, change_data)
            log_message(f"Registration change '{change_id}' added successfully")
            return jsonify({"message": "Success!"})
        else:
            log_message("Failed to add registration change - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error adding registration change '{change_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/get_registration_change/<change_id>', methods=["GET"])
def get_registration_change(change_id):
    try:
        change_data = get_record("RegistrationChangeRecord", change_id)
        if change_data:
            log_message(f"Registration change '{change_id}' fetched successfully")
            return jsonify(json.loads(change_data))
        else:
            log_message(f"Registration change '{change_id}' not found")
            return jsonify({"message": f"Failure. Registration change '{change_id}' not found!"})
    except Exception as e:
        log_message(f"Error fetching registration change '{change_id}': {e}")
        return jsonify({"error": str(e)})

@app.route('/delete_registration_change/<change_id>', methods=["DELETE"])
def delete_registration_change(change_id):
    try:
        if not redis_client.hexists("RegistrationChangeRecord", change_id):
            log_message(f"Registration change '{change_id}' does not exist")
            return jsonify({"message": f"Failure. Registration change '{change_id}' does not exist!"})
        if node.state == "l":
            delete_record("RegistrationChangeRecord", change_id)
            log_message(f"Registration change '{change_id}' deleted successfully")
            return jsonify({"message": "Success. Registration change deleted!"})
        else:
            log_message("Failed to delete registration change - not a leader")
            return jsonify({"message": "Failure. Not a leader!"})
    except Exception as e:
        log_message(f"Error deleting registration change '{change_id}': {e}")
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(port=node.port)