# MyKRaft Consensus
- KRaft is a event based, distributed metadata management system that was written to replace Zookeeper in the ecosystem of Kafka.
- It uses Raft as an underlying consensus algorithm to do log replication and manage consistency of state.
- This project aims to mimic the working of the KRaft Protocol used in Apache Kafka.

### Start the raft nodes using the following commands (three nodes) - 
```
python raft.py -i 1 -a 127.0.0.1:5010
```
```
python raft.py -i 2 -a 127.0.0.1:5020 -e 1/127.0.0.1:5010
```
```
python raft.py -i 3 -a 127.0.0.1:5030 -e 1/127.0.0.1:5010,2/127.0.0.1:5020
```