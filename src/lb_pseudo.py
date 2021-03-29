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

class LoadBalancer():
    
    def __init__(self):
        self.exist_replica_cnt = None
        
        def replica_sockets(self):
            #set up sockets REQ/RES to listen for a "become a primary" command
            
        def update_children_connections(self):
            #get children
            #for each child update ip address
            #do we keep ip addresss in node? own dict?
            #depending on where we store, how do we use to connect?
            
            
        def lb_topics():
            @self.zk.ChildrenWatch(self.topic_root_path)
            def topic_watch(children):
                print(f"Topic Children Change")
                if children is not None:
                    new_repl_cnt = ceil(len(children)/TOPIC_THRESHOLD)
                    old_repl_cnt = self.zk.get_children(replica_root_path)
                if old_repl_cnt != new_repl_cnt:
                    update_replicas(new_repl_cnt)
                else:
                    print('Topics changed but count remained same')
                    #skip over replica changes and check to see if we need to redistribute
                    distribute_topics_to_replicas()
                    
        def update_replicas(self, count):
            replicas = self.zk.get_children(replica_root_path)
            change_num = count - len(replicas)
            
            #create
            if change_num > 0:
                for _ in range(change_num):
                    self.zk.create(replica_root_path, ephemeral=True, sequence=True, makepath=True, value="??") #How to get IP?
            #delete
            elif change_num < 0:
                for i in range(-change_num):
                    path = replica_root_path + "/" + replicas[i]
                   self.zk.delete(path)
                   print(f'Deleted replica {path}')
          
             distribute_topics_to_replicas()
                        
            #register dicts for pubs subs and brokers?
        def distribute_topics_to_replicas():
            replicas = self.zk.get_children(replica_root_path)
            topics_sorted = self.zk.get_children(self.topic_root_path).sort()
            #for topics a..n,n+1..n+3, n+4...n+7,...each set updates the pubs and sub sockets
            for i in len(replicas):
                rep_ip = zk.server.get(replicas[i]).decode('utf-8') #might need to get path instead of object
                for j in range (TOPIC_THRESHOLD):
                    t = topics_sorted[j]
                    zk.server.set(f'/{t}',rep_ip)
                    #update topic pubs/subs for 
                    
                    
                    
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
                