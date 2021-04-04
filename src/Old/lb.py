import zmq
from zmq.utils.monitor import recv_monitor_message
import threading
import time
import matplotlib.pyplot as plt
import netifaces as ni
from random import randrange, randint
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
from kazoo.exceptions import (
    ConnectionClosedError,
    NoNodeError,
    KazooException
)

context = zmq.Context()

##NOTES
#Should pub/subs have a leader watch (leader creates new replica children) then update their leader ip
#based on a new req?

class LoadBalancer():

    def __init__(self):
        self.topic
        self.broker_root_path = "/leader"
        self.broker_path = None #broker path underwhich topics exist
        self.num_topics = 3 #max num of topics under broker
    
        broker_list = self.zk.get_children(self.broker_root_path)
        topic_list = self.zk.get_children(self.broker_path)
        topic_num = len(topic_list)
        
        
        def handle_pub_msgs(req_type):
            if req_type == "get_leader_ip":
                #get leader will need to find first leader with availability
                lb_leader_ip = get_avail_leader()
                self.pub_cs_socket.send_string(f'{lb_leader_ip}:{self.leader_port}')
            
            elif req_type == "register_pub":
                register_pub(topic, ip, id, strength)
                
            elif req_type == "publish":
            
        def handle_sub_msgs(req_type):
            if req_type == "get_leader_ip":
                #get leader will need to find first leader with availability
                lb_leader_ip = get_leader_ip()
                self.sub_cs_socket.send_string(f'{lb_leader_ip}:{self.leader_port}')
            
            elif req_type == "register_pub":
                register_sub(topic, ip, id, strength)
              
        def get_avail_leader():
            broker_list = self.zk.get_children(self.broker_root_path).sort()

            for b in broker_list: 
                b_topics = self.zk.get_children(self.broker_root_path + "/" + b)
                b_topic_cnt = len(b_topics)
                if b_topic_cnt < self.num_topics:
                    return b
                else:
                    pass
            #this needs catch - also how to evenly distribute load?