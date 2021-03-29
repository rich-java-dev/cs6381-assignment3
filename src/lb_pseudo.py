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

class LoadBalancer():
    
    def __init__(self):
        
        
        def replica_sockets(self):
            #set up sockets REQ/RES to listen for a "become a primary" command
            
        def update_children_connections(self):
            #get children
            #for each child update ip address
            #do we keep ip addresss in node? own dict?
            #depending on where we store, how do we use to connect?
            
            
            #register dicts for pubs subs and brokers?