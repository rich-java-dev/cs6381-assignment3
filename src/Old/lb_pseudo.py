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

class Replica():
    
    def __init__(self):
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

    #watch topic children and recalculate replica count based on any changes       
        def lb_topics():
            @self.zk.ChildrenWatch(self.topic_root_path)
            def topic_watch(children):
                print(f"Topic Children Change")
                self.topics = children
                if self.topics is not None:
                    new_repl_cnt = ceil(len(self.topics)/TOPIC_THRESHOLD)
                    old_repl_cnt = self.zk.get_children(self.replica_root_path)
                if old_repl_cnt != new_repl_cnt:
                    #create/delete replicase to match count - then call distribute topics
                    update_replicas(new_repl_cnt)
                else:
                    print('Topics changed but count remained same')
                    #skip over create/delete replicachanges and check to see if we need to redistribute
                    distribute_topics_to_replicas()
        
        #only called if need to change count of replicas            
        def update_replicas(self, rep_count):
            #list of working brokers, in order, if any e.g. ["xxxxxxxx1", "xxxxxxxx2", ...]
            self.replicas = self.zk.get_children(self.replica_root_path).sort()
            print(f'UpdateReplicas - Current Working Reps: {self.replicas}')
            
            #list of sorted standbys
            self.standbys = self.zk.get_children(self.replica_standby_path).sort()
            
            crnt_stdby_idx = len(self.replicas) - 1
            change_rep_num = rep_count - len(self.replicas)
            
            #TODO - Need data watch for both replicas and standbys
            
            #create
            if change_rep_num > 0:
                for i in range(1, change_rep_num + 1):
                    #get ip value from standby - path listed in self.replicas
                    tmp_ip = self.zk.get(path=self.replica_standby_path + "/" + self.standbys[crnt_stdby_idx + i])
                    print(f'{i} -new lb broker ip: {tmp_ip}')
                    self.zk.create(self.replica_root_path + "/" + self.standbys[crnt_stdby_idx + i], ephemeral=True, value=tmp_ip)
            
            #delete
            elif change_rep_num < 0:
                for i in range(1, -change_rep_num + 1):
                    path = self.replica_root_path + "/" + self.replicas[-i]
                    self.zk.delete(path)
                    self.replicas.pop()
                    print(f'Deleted replica {path}')
                    
            self.distribute_topics_to_replicas()
                        
            #register dicts for pubs subs and brokers?
        def distribute_topics_to_replicas():
            #for topics a..n,n+1..n+3, n+4...n+7,...each set updates the pubs and sub sockets
            for i in len(self.replicas):
                rep_ip = self.zk.server.get(self.replicas[i]).decode('utf-8') #might need to get path instead of object
                idx = 0
                for j in range (idx * TOPIC_THRESHOLD, TOPIC_THRESHOLD + i * TOPIC_THRESHOLD):
                    self.zk.server.set(f'/topics/{self.topics[j]}',rep_ip)
                    #update topic pubs/subs fo
                    
                    
                    
        def replica_watch():
            @self.zk.ChildrenWatch(self.replica_root_path)
            def replica_watcher(children):
                #find which replica a topic belongs to
                #update_publisher
                pub_path = "/topic/" + self.topic
                intf = zk.server.get(pub.path).decode('utf-8')
                conn_str = f'tcp://{inf}:{self.port}'
                self.socket.connect(conn_str)
                time.sleep(5)
                #return lamdbda to publisher
                
                
                #update_subscriber
            @self.zk.ChildrenWatch(self.replica_root_path)
            def replica_watcher(children):
                #find which replica a topic belongs to
                #update_publisher
                pub_path = "/topic/" + self.topic
                intf = zk.server.get(pub.path).decode('utf-8')
                conn_str = f'tcp://{inf}:{self.port}'
                self.socket.connect(conn_str)
                time.sleep(5)
                #return lamdbda to publisher
                
                
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