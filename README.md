# Distributed Systems Principles - Assignment 3

## Pub/Sub MQ Implementation w/ Load Balancing/Broker Replication
### Richard White, Max Coursey 

 PUB/SUB model supported by the ZeroMQ (ZMQ) middleware. Application maintains a broker, well known to the publishers and subscribers. This broker performs matchmaking. Data is disseminated from publishers to subscribers in a globally configurable way using one of the two approaches.

## Simulated "Flooding" 
- Publisher’s middleware layer directly send the data to the subscribers who are interested in the topic being published by this publisher. 
- Look-up is now handled via Zookeeper, instead of a lame-broker of scanning the entire network. 
- Look-up is done via a root znode "/topic", which contains children registered by the Publisher
- The value stored at the given topic is the IP of the publisher which pushes data
- eg: /topic/12345 -> {id: 1, ip:10.0.0.2, strength:10, history: 10}
- ie: topic '12345' is being published via host at IP 10.0.0.2

## Centralized 
- Replicas play 2 roles: Load Balancing, and Coordination
- 'Workers' play the role of Broker for the given topics they are prescribed.
- 'Load' plays the role of coordinator to parse out the load across the workers.
- There is no limit to the number of 'Workers' that can be deployed, but generally there will be 1 active 'Loader' (Multiples can be set up for redundancy/election if it fails)
- When a Publisher registers itself in ZK, the Loader assigns it to a 'Worker' or active Broker.
- The Loader uses thresholds, by topic count, to determine where to assign the Pubs.
- Subscribers are able to look-up their topic, and recieve a list of publishers which are active on the given topic.
- Subscribers then filter down for their criteria (eg: Greatest Strength pub on a given topic that meets the required 'history' criteria)


>Subscribers generate their own plots/graphs of the delta times between the publisher pushing the data, and the subscriber receipting the data. The latency is calculated in two ways - First with wireshark sniffing and a second way by utilizing timestamps for the publishers sent time and the subscriber's received time.  The plots of latency are generated with Matplotlib.

The main.py method can take an arugment of the number of pub/subs and whether to utilize the centralized or simulated flooding method.

## Built With
- Ubuntu 20.04 (on VirtualBox)
- Python3
- Mininet
- ZeroMQ
- Zookeeper
- Wireshark

Python libraries:
 - kazoo
 - pyshark
 - matplotlib
 - pyzmq
 - mininet
 - netifaces

**What you need to install on Ubuntu:**
- mininet - http://mininet.org/download/
- python3/pip3  - sudo apt install python3-pip
- wireshark - sudo pip install wireshark (also included with mininet)
- pyshark - sudo pip install pyshark
- zmq - sudo -H pip3 install pyzmq
- matplotlib - sudo apt-get install python3-matplotlib

## SET-UP:
- Clone repo - https://github.com/rich-java-dev/cs6381-assignment3.git

- Navigate to cs6381-assignment3 folder
- run **pip install -r requirements.txt**
 - This will ensure all the of python packages used are installed.
- run **sudo mn -c** between runs
- run **ps -e | grep java** to get a list of pids for java apps to ensure zookeeper is not already running
- run **sudo kill {pid}** to kill any existing java apps (We will deploy Zookeeper on a mininet host)

- watch the following demo - https://youtu.be/BV68_xNUDXI


## **Running mininet and commands manually in xterm windows**
>
 - sudo mn -x --topo=linear,10
 - **host1**: /path/to/zookeeper/bin/zkServer.sh start
 - **host2**: python3 replica.py --type=worker
 - **host3**: python3 replica.py --type=worker
 - **host4**: python3 replica.py --type=worker
 - **host5**: python3 replica.py
 - **host6**: python3 publisher.py --proxy --topic="a"
 - **host7**: python3 publisher.py --proxy --topic="a"
 - **host8**: python3 subscriber.py --proxy --topic="a"
 - add more pubs/subs as needed.



## App Structure

**replica.py**
usage: replica.py --xin=5555 --xout=5556 --zkserver=10.0.0.1 [-h] [--zkserver ZKSERVER] [--xin XIN] [--xout XOUT]

>optional arguments:
  -h, --help            show this help message and exit
  --zkserver ZKSERVER, --zkintf ZKSERVER
  --xin XIN, --in_bound XIN
  --xout XOUT, --out_bound XOUT


**publisher.py**
usage: publisher.py [-h] [--zkserver ZKSERVER] [--port PORT] [--topic TOPIC] [--proxy]

>optional arguments:
  -h, --help            show this help message and exit
  --zkserver ZKSERVER, --zkintf ZKSERVER
  --port PORT
  --topic TOPIC
  --proxy

**subscriber.py**
usage: subscriber.py [-h] [--proxy] [--zkserver ZKSERVER] [--port PORT] [--topic TOPIC]
                     [--sample_size SAMPLE_SIZE] [--label LABEL]

>optional arguments:
  -h, --help            show this help message and exit
  --proxy
  --zkserver ZKSERVER, --zkintf ZKSERVER
  --port PORT
  --topic TOPIC
  --sample_size SAMPLE_SIZE, --samples SAMPLE_SIZE
  --label LABEL

**monitor.py** 
(pyshark api for monitoring TCP packets (TTDs)) - must be ran as root/sudo
usage: monitor.py [-h] [--interface INTERFACE] [--net_size NET_SIZE]
                  [--sample_size SAMPLE_SIZE]
>optional arguments:
  -h, --help            show this help message and exit
  --interface INTERFACE
  --net_size NET_SIZE
  --sample_size SAMPLE_SIZE, --samples SAMPLE_SIZE

**main.py** 
(driver for configuring network)
usage: main.py [-h] [--zkserver ZKSERVER] [--zkpath ZKPATH] [--proxy_mode] [--xin XIN] [--xout XOUT]
               [--pub_count PUB_COUNT] [--proxy_count PROXY_COUNT]

>optional arguments:
  -h, --help            show this help message and exit
  --zkserver ZKSERVER, --zkintf ZKSERVER
  --zkpath ZKPATH, --zk_bin_path ZKPATH
  --proxy_mode
  --xin XIN
  --xout XOUT
  --pub_count PUB_COUNT
  --proxy_count PROXY_COUNT
