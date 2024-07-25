import logging

def log_message(message, node):
    log_entry = f"Node Port: {node.port}, Node ID: {node.nid}, Node Status: {node.state} - {message}"
    logging.info(log_entry)