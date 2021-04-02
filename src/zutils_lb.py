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

class Proxy():

    def __init__(self, zkserver="10.0.0.1", in_bound=5555, out_bound=5556):
        self.in_bound = in_bound
        self.out_bound = out_bound
        self.path = '/proxy'
        self.elect_root_path = '/election'
        self.elect_candidate_path = '/election/candidate_sessionid_'
        self.pub_root_path = '/publishers'
        self.my_path = ""
        self.candidate_list = []
        self.ip = get_ip()
        self.zk = start_kazoo_client(zkserver)
        self.leader_node = None
        self.isleader = False
        self.context = zmq.Context()
        self.pub_data = {} #{topic: {pubid: {pub_values: [], strength: someval}}}
        self.front_end = None
        self.back_end = None
        self.replica_socket = None # set up PUSH/PULL socket for state transfer
        self.replica_port = "5557"
        self.register_socket = None
        self.register_port = "5558"
        
        #Create znode for each instance of this class
        self.my_path = self.zk.create(self.elect_candidate_path, value=b'', sequence=True, ephemeral=True, makepath=True)
        print(f'\n#### My Znode Details ####')
        print(f'- ZNode Path: {self.my_path}')
        print(f'- IP: {self.ip}')

        #ensure that leader path exists
        if self.zk.exists(self.path) is None:
            self.zk.create(self.path, value=b'', ephemeral=True, makepath=True)
        
        #get all children as candidates under '/election' parent
        self.candidate_list = self.zk.get_children(self.elect_root_path)
        
        #candidate list removes parent node (/election/) from name, not in seq order
        self.candidate_list.sort()
        print(f'- ZK Cluster Candidates: {self.candidate_list}')

    def start(self):
        #Current Znode is first in sequential candidate list and becomes LEADER
        if self.my_path.endswith(self.candidate_list[0]):
            self.leader_node = self.my_path
            self.isleader = True
            print(f'\n- My Status: **LEADER**')
            
            #set method requires byte string
            encoded_ip = self.ip.encode('utf-8')
            self.zk.set('/proxy', encoded_ip)
            self.zk.set(self.my_path, encoded_ip)
            
            #Set up zmq proxy socket
            self.front_end = self.context.socket(zmq.SUB)
            self.front_end.bind(f"tcp://*:{self.in_bound}")
            self.front_end.subscribe("")
            self.back_end = self.context.socket(zmq.PUB)
            self.back_end.setsockopt(zmq.XPUB_VERBOSE, 1)
            self.back_end.bind(f"tcp://*:{self.out_bound}")            
            print(
                f'- Pub/Sub Ports: in_bound={self.in_bound}, out_bound={self.out_bound}')            
            
            #Replica socket allow communications between all candidates in cluster
            self.replica_socket = self.context.socket(zmq.PUB)
            self.replica_socket.bind(f"tcp://*:{self.replica_port}")
            print(f'- Cluster Port: {self.replica_port}')
            
            #Register Socket - receives register request from pub - sends resp with IP of load balanced broker(s) for that topic
            self.register_socket = self.context.socket(zmq.REP)
            self.register_socket.bind(f'tpc://*:{self.register_port}')
            
            #Removed blocking proxy to set up registration vs. publish
            #zmq.proxy(front_end, back_end)
            self.get_pub_msg()
                
        #Znodes exist earlier in candidate list - Current node becomes FOLLOWER
        else:
            #def watch function on previous node in candidate list
            def watch_prev_node(prev_node):
                 @self.zk.DataWatch(path=prev_node)
                 def proxy_watcher(data, stat, event):

                     #need to check first if None - then check event type
                    if event is not None:
                        if event.type == "DELETED":
                            print('\n#### Leader Delete Event  ####')
                            print(f'- {event}\n')
                            election = self.zk.Election('election/', self.ip)
                            election.run(self.won_election) # blocks until wins or canceled
                        
            print(f'\n- My Status: **FOLLOWER**')
            self.isleader = False
            leader_node = self.candidate_list[0]
            print(f'- Following leader: {leader_node}')
            leader_ip = self.zk.get(self.path)[0].decode('utf-8')
            print(f'- Following leader: {leader_ip}')
            
            #Find previous seq node in candidate list
            crnt_node_idx = self.candidate_list.index(self.my_path[10:])
            prev_node_path = self.candidate_list[crnt_node_idx - 1]            
            prev_node = self.zk.exists(self.elect_root_path + "/" + prev_node_path, watch_prev_node)
            print(f'- Watching Prev/Seq. Node: {prev_node_path}')
            
            #TODO - check if need while self.isleader is False:
            watch_prev_node(self.elect_root_path + "/" + prev_node_path)

            print("start -> initial replica socket and data func")
            self.replica_socket = self.context.socket(zmq.SUB)
            self.replica_socket.connect(f'tcp://{leader_ip}:{self.replica_port}')
            self.replicate_data()
            
    def won_election(self): 
        print(f'- **New Leader Won Election**')
        print(f'- Won election, self.path: {self.my_path}')
        #check leader barrier node
        if self.zk.exists(self.path) is None:
            self.zk.create(self.path, value=self.ip.encode(
                    'utf-8'), ephemeral=True, makepath=True)
            print(f'- My Status: **LEADER**')
            print(f'- MY IP: {self.ip}')
            
            #Flag used to differentiate recieve message and state replication funcs
            self.isleader = True

            self.front_end = context.socket(zmq.SUB)
            self.front_end.bind(f"tcp://*:{self.in_bound}")
            self.front_end.subscribe("")
            self.back_end = context.socket(zmq.PUB)
            self.back_end.setsockopt(zmq.XPUB_VERBOSE, 1)
            self.back_end.bind(f"tcp://*:{self.out_bound}")            
            print(
                f"- Pub/Sub Ports: in_bound={self.in_bound}, out_bound={self.out_bound}")
            
            self.replica_socket = self.context.socket(zmq.PUB)
            self.replica_socket.bind(f"tcp://*:{self.replica_port}")
            print(f'- Cluster Port: {self.replica_port}')
            
            #Removed blocking proxy to set up registration vs. publish
            #zmq.proxy(front_end, back_end)
            self.get_pub_msg()
              
    def get_pub_msg(self):
        print('\n####  Run get pub message func ####')
        #barrier until become leader
        while self.isleader is False:
            pass
        while True:
            msg = self.front_end.recv_string()
            message = msg.split(' ')
            if message[0] == 'register':
                print('- Pub Request Register: Update Local State')
                pubid = message[1]
                topic = message[2]
                strength = message[3]
                self.update_data('publisher', pubid, topic, strength, '')
                self.replica_socket.send_string(msg)
                print('#### Sent register pub state transaction to cluster ####')
            else:
                self.back_end.send_string(msg)
                print("- Forward publication...")
   
    def update_data(self, add_this, pubid, topic, strength, publication):
        print('\n#### Run update pub state func ####')
        try:
            if add_this == 'publisher':
                if topic not in self.pub_data.keys():
                    self.pub_data.update({topic: {pubid: {'strength': strength, 'publications': []}}})
                elif pubid not in self.pub_data[topic].keys():
                    self.pub_data[topic].update({pubid: {'strength': strength, 'publications': []}})
            elif add_this == 'publication':
                stored_publication = publication + '--' + str(time.time())
                self.pub_data[topic][pubid]['publications'].append(stored_publication)
            print(f'- Updated Local State: {self.pub_data}')
        except KeyError as ex:
            print(ex)

    def replicate_data(self):
        #barrier - must be follower - may need to be update if replicated leaders
        while self.isleader is False:
            print('\n#### Waiting for new state from leader and update locally ####')
            try:
                #IM HERE - recv string is blocking and not getting message
                self.replica_socket.subscribe("")
                recv_pushed_pub_data = self.replica_socket.recv_string()
                print(f'- received replica data from leader: {recv_pushed_pub_data}')
            except Exception as e:
                print(f'- timeout error {e}')
                continue
            message = recv_pushed_pub_data.split(' ')
            msg_type = message[0]
            pubid = message[1]
            topic = message[2]
            strength = message[3]
            if msg_type == 'register':
                self.update_data('publisher', pubid, topic, strength, '')
            #IM HERE - not sure I'll use to store publications below
            # elif msg_type == 'publish':
            #     publication = message[4]
            #     self.update_data('publication', pubid, topic, strength, publication)
                
class Publisher():

    def __init__(self, port=5555, zkserver="10.0.0.1", topic=12345, proxy=True):
        self.port = port
        self.proxy = proxy
        self.topic = topic
        self.path = f"/topic/{topic}"
        self.proxy_path = "/proxy"
        self.zk = start_kazoo_client(zkserver)
        self.ip = get_ip()
        self.socket = context.socket(zmq.PUB)
        self.strength = randrange(1,11)
        self.pubid = randrange(1000,10000)
        
    def start(self):
        print(f"Publisher: {self.ip}")
        self.init_monitor()

        #create parent znode
        if not self.zk.exists("/topic"):
            self.zk.create("/topic")

        print(f'Publishing w/ proxy={self.proxy} and topic:{self.topic}')

        if self.proxy:  # PROXY MODE

            @self.zk.DataWatch(self.proxy_path)
            def proxy_watcher(data, stat):
                print(f"Publisher: proxy watcher triggered. data:{data}")
                if data is not None:
                    intf = data.decode('utf-8')
                    conn_str = f'tcp://{intf}:{self.port}'
                    print(f"connecting: {conn_str}")
                    self.socket.connect(conn_str)
                    
                    #socket needs time to connect otherwise will pass over send string
                    time.sleep(5)
                    self.socket.send_string(f'register {self.pubid} {self.topic} {self.strength}')

        else:  # FLOOD MODE
            conn_str = f'tcp://{self.ip}:{self.port}'
            print(f"binding: {conn_str}")
            self.socket.bind(conn_str)

            print(f"Publisher: creating znode {self.path}:{self.ip}")
            self.zk.create(self.path, value=self.ip.encode(
                'utf-8'), ephemeral=True)

        return lambda topic, msg: self.socket.send_string(f'{topic} {msg}')

    def init_monitor(self):
        evt_map = {}
        for val in dir(zmq):
            if val.startswith('EVENT_'):
                key = getattr(zmq, val)
                print("%21s : %4i" % (val, key))
                evt_map[key] = val

        def evt_monitor(monitor):
            while monitor.poll():
                evt = recv_monitor_message(monitor)
                evt.update({'description': evt_map[evt['event']]})
                #TODO - uncomment - old connection not dying
                # print("Event: {}".format(evt))
                if evt['event'] == zmq.EVENT_MONITOR_STOPPED:
                    break
            monitor.close()
            print()
            print('event monitor stopped.')

        monitor = self.socket.get_monitor_socket()
        t = threading.Thread(target=evt_monitor, args=(monitor,))
        t.start()

class Subscriber():

    def __init__(self, port=5556, zkserver="10.0.0.1", topic='12345', proxy=True):
        self.port = port
        self.topic = topic
        self.proxy_path = "/proxy"
        self.path = f"/topic/{topic}"
        self.proxy = proxy
        self.ip = get_ip()
        self.socket = context.socket(zmq.SUB)
        self.zk = start_kazoo_client(zkserver)

    def start(self):
        print(f"Subscriber: {self.ip}")

        if self.proxy:  # PROXY MODE

            @self.zk.DataWatch(self.proxy_path)
            def proxy_watcher(data, stat):
                print(f"Subscriber: proxy watcher triggered. data:{data}")
                if data is not None:
                    intf = data.decode('utf-8')
                    conn_str = f'tcp://{intf}:{self.port}'
                    print(f"connecting: {conn_str}")
                    self.socket.connect(conn_str)

        else:  # FLOOD MODE

            @self.zk.DataWatch(self.path)
            def pub_watcher(data, stat):
                print(f"Publisher: topic watcher triggered. data:{data}")
                if data is not None:
                    intf = data.decode('utf-8')
                    conn_str = f'tcp://{intf}:{self.port}'
                    print(f"connecting: {conn_str}")
                    self.socket.connect(conn_str)

        self.socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)

        return lambda: self.socket.recv_string()

    def plot_data(self, data_set, label=""):

        # plot the time deltas
        fig, axs = plt.subplots(1)
        axs.plot(range(len(data_set)), data_set)
        axs.set_title(
            f"RTTs '{label}' - topic '{self.topic}' - host '{self.ip}'")
        axs.set_ylabel("Delta Time (Pub - Sub)")
        axs.set_xlabel("Number of Samples")
        plt.show()


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