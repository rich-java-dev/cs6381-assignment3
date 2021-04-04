import zmq
from zmq.utils.monitor import recv_monitor_message
import threading
import time
import matplotlib.pyplot as plt
import netifaces as ni
from math import ceil
from random import randrange, randint
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
from kazoo.exceptions import (
    ConnectionClosedError,
    NoNodeError,
    KazooException
)

TOPIC_THRESHOLD = 3
context = zmq.Context()

class Worker():
    
    def __init__(self, zkserver="10.0.0.1", in_bound=5555, out_bound=5556):
        self.in_bound = in_bound
        self.out_bound = out_bound
        self.topic_threshold = 3
        self.context = zmq.Context()
        self.ip = get_ip()
        self.topic_root_path = "/topic"
        #replicas change from replica standby to broker when determined by load balancer
        self.replica_root_path = "/brokers"
        self.replica_standby_path = "/replicas"
        self.my_path = None
        self.replicas = []
        self.standbys =[]
        self.topics = []
        self.front_end = None
        self.back_end = None
        self.replica_socket = None # set up PUSH/PULL socket for state transfer
        self.replica_port = "5557   "
        

        #TODO - watch leader and connect to replica socket to get pub_data registry state
        #ensure that broker root path exists
        if self.zk.exists(self.replica_root_path) is None:
            self.zk.create(self.path, value=b'', ephemeral=True, makepath=True)
        
        #ensure that replica standby root path exists
        if self.zk.exists(self.replica_standby_path) is None:
            self.zk.create(self.path, value=b'', ephemeral=True, makepath=True)
 
        #create replica standby sequential path for each instance created
        self.my_path = self.zk.create(self.replica_standby_path, value=self.ip, sequence=True, ephemeral=True, makepath=True)
        
        print(f'\n#### My Znode Details ####')
        print(f'- ZNode Path: {self.my_path}')
        print(f'- IP: {self.ip}')
        
        #Set up zmq proxy socket
        self.front_end = self.context.socket(zmq.SUB)
        self.front_end.bind(f"tcp://*:{self.in_bound}")
        self.front_end.subscribe("")
        self.back_end = self.context.socket(zmq.PUB)
        self.back_end.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.back_end.bind(f"tcp://*:{self.out_bound}") 
        print(
                f'- Pub/Sub Ports: in_bound={self.in_bound}, out_bound={self.out_bound}')            
        zmq.proxy(self.front_end, self.back_end)


def get_ip():
    intf_name = ni.interfaces()[1]
    print('\n#### Start-Up Details ####')
    print(f'Interface: {intf_name}')
    return ni.ifaddresses(intf_name)[ni.AF_INET][0]['addr']


def start_kazoo_client(intf="10.0.0.1", port="2181"):
    url = f'{intf}:{port}'
    print(f"ZK client Started: {url}")
    zk = KazooClient(hosts=url)
    zk.start()
    return zk