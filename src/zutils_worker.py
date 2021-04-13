import zmq
from zmq.utils.monitor import recv_monitor_message
import matplotlib.pyplot as plt
import netifaces as ni
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
from kazoo.exceptions import (
    ConnectionClosedError,
    NoNodeError,
    KazooException
)

class Worker():
    
    def __init__(self, zkserver="10.0.0.1", in_bound=5555, out_bound=5556):
        self.in_bound = in_bound
        self.out_bound = out_bound
        self.context = zmq.Context()
        self.zk = start_kazoo_client(zkserver)
        self.ip = get_ip()
        self.my_path = None
        self.front_end = None
        self.back_end = None
        self.replica_standby_path = "/brokers/"

        #if self.zk.exists(self.replica_root_path) is None:
        #    self.zk.create(self.path, value=b'', ephemeral=True, makepath=True)
        
        if not self.zk.exists(self.replica_standby_path):
            self.zk.create(self.replica_standby_path)
 
        encoded_ip = self.ip.encode('utf-8')
        self.my_path = self.zk.create(self.replica_standby_path, value=encoded_ip, sequence=True, ephemeral=True, makepath=True)
        
        print(f'\n#### My WORKER Details ####')
        print(f'- ZNode Path: {self.my_path}')
        print(f'- IP: {self.ip}')
        
    def start(self):
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