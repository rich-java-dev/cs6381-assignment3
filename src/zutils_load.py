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
from math import ceil
import json

context = zmq.Context()
db_data = {}

class Proxy():

    def __init__(self, zkserver="10.0.0.1", in_bound=5555, out_bound=5556):
        self.in_bound = in_bound
        self.out_bound = out_bound
        self.leader_path = '/leader'
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
        self.register_port = "5559"
        self.replicas=[]
        self.replica_root_path = "/brokers"
        self.replica_standby_path = "/workers"
        self.topic_root_path = "/topic"
        self.standbys = []
        self.topics = []
        self.TOPIC_THRESHOLD = 3

        
        #Create znode for each instance of this class
        self.my_path = self.zk.create(self.elect_candidate_path, value=b'', sequence=True, ephemeral=True, makepath=True)
        print(f'\n#### My LOAD REPLICA Details ####')
        print(f'- ZNode Path: {self.my_path}')
        print(f'- IP: {self.ip}')

        #ensure that leader path exists
        if self.zk.exists(self.leader_path) is None:
            self.zk.create(self.leader_path, value=b'', ephemeral=True, makepath=True)
        
        #get all chsldren as candidates under '/election' parent
        self.candidate_list = self.zk.get_children(self.elect_root_path)
        
        #candidate list removes parent node (/election/) from name, not in seq order
        self.candidate_list.sort()
        print(f'- ZK Cluster Candidates: {self.candidate_list}')

    def start(self):
        #Current Znode is first in sequential candidate list and becomes LEADER
        if self.my_path.endswith(self.candidate_list[0]):
            self.leader_node = self.my_path #leader node takes /election/client_sessionid_xxxxxxxx
            self.isleader = True
            print(f'\n- My Status: **LEADER**')
            
            #set method requires byte string
            encoded_ip = self.ip.encode('utf-8')
            self.zk.set(self.leader_path, encoded_ip) #set leader value to current node ip
            self.zk.set(self.my_path, encoded_ip) # set candidate value to current node ip
            
            #Set up zmq proxy socket
            self.front_end = self.context.socket(zmq.SUB)
            self.front_end.bind(f"tcp://*:{self.in_bound}")
            #TODO - Workaround to get replicate state messages on same socket as publications
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
            
            #Register socket just sends pub_data publisher registry dict to new subs
            self.register_socket = self.context.socket(zmq.PUB)
            self.register_socket.bind(f"tcp://*:{self.register_port}")
            print(f'- Set Up Sub Register {self.register_port}')
            sub_regis_thr = threading.Thread(target=self.get_sub_register)
            sub_regis_thr.start()
            
           
            @self.zk.ChildrenWatch(self.replica_standby_path)
            def standby_watch(children):
                print(f'\nStandby Worker Change Triggered')
            #IM HERE - if an ip in re
                repls = self.zk.get_children(self.replica_root_path)
                print(repls)
                if repls:
                    for p in children:
                        temp = self.zk.get(self.replica_root_path + "/" + p[0])
                        #TODO - MIGHT get rid of standbys and just have replicas
            

                if not self.zk.exists(self.replica_root_path):
                    self.zk.create(self.replica_root_path)
                if not self.zk.get_children(self.replica_root_path):
                    topic_num = len(self.topics)
                    standby_num = len(children)
                    broker_num = min(ceil(len(self.topics)/self.TOPIC_THRESHOLD),standby_num)
                    print("broker_num:" + str(broker_num))
                    for i in range(broker_num):
                        broker_ip = self.zk.get(self.replica_standby_path +"/" + children[i])[0]
                        self.zk.create(self.replica_root_path + "/" + children[i], value = broker_ip, sequence=True, ephemeral=True)
                
                print("replica children")
                c = self.zk.get_children(self.replica_root_path)
                print(c)
                #for each standby change to broker for the number of topics up to threshold
                #if too many topics just set all standbys to broker
                
            #TODO - set up topic watch to check and redistribute load
            @self.zk.ChildrenWatch(self.topic_root_path)
            def topic_watch(children):
                print(f"\nTopic Children Watch Triggered")
                print("Current topic znodes")
                
                child_topics = []
                for c in children:
                    child_topics.append(c[:-10])
                print(child_topics)
                #update pub data
                for child in children:
                    #get dict value with topic, strength,history for each topic
                    b_val = self.zk.get(self.topic_root_path + "/" + child)[0]
                    val = eval(b_val.decode('utf-8'))

                    if val['topic'] not in self.pub_data.keys():
                        self.pub_data.update({val['topic']: {val['pub_ip']: {'strength': val['strength'], 'history': val['history'], 'broker_ip': "", 'publications': []}}})
                    
                    elif val['pubid'] not in self.pub_data[val['topic']].keys():
                        self.pub_data[val['topic']].update({val['pub_ip']: {'strength': val['strength'], 'history': val['history'], 'broker_ip': "", 'publications': []}})
                
                self.replicas = self.zk.get_children(self.replica_root_path)
                print("Checking Workers: ")
                print(self.replicas)
                if not self.replicas:
                    print("  - No Active Workers")
                    
                else:
                    print(f"  - Active Workers: {len(self.replicas)}\n")
                    for t in children:
                        print("topic watch iterate through topic vals")
                        b_val = self.zk.get(self.topic_root_path + "/" + t)[0]
                        val = eval(b_val.decode('utf-8'))
                        print(val)
                    try: 
                        self.topics = child_topics
                        if self.topics:
                            print(self.topics)
                            new_repl_cnt = max(1,ceil(len(self.topics)/self.TOPIC_THRESHOLD))
                            old_repl_cnt = len(self.zk.get_children(self.replica_root_path))
                            print("new repl cnt")
                            print(new_repl_cnt)
                            print("old repl cnt")
                            print(old_repl_cnt)
                            if new_repl_cnt > len(self.replicas):
                                print("Need Additional Workers")
                                
                            elif old_repl_cnt != new_repl_cnt:
                                #create/delete replicase to match count - then call distribute topics
                                print("Change in count - create new replicas")
                                self.update_replicas(new_repl_cnt)
                            else:
                                print('- Topics changed but count remained same')
                                #skip over create/delete replicachanges and check to see if we need to redistribute
                                self.distribute_topics_to_replicas()
                    except NoNodeError:
                        print("- No Topics yet - Pass")        
                        
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
            leader_ip = self.zk.get(self.leader_path)[0].decode('utf-8')
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
        if self.zk.exists(self.leader_path) is None:
            self.zk.create(self.leader_path, value=self.ip.encode(
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
            
            self.register_socket = self.context.socket(zmq.PUB)
            self.register_socket.bind(f"tcp://*:{self.register_port}")
            
            sub_regis_thr = threading.Thread(target=self.get_sub_register)
            sub_regis_thr.start()
            #Removed blocking proxy to set up registration vs. publish
            #zmq.proxy(front_end, back_end)
            self.get_pub_msg()
            
    def get_sub_register(self):
        print('********SUB REG THREAD*******')
        while True:
            self.register_socket.send_string(f'{self.pub_data}')
            time.sleep (5)  
          
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
                pub_str = str(self.pub_data)
                self.replica_socket.send_string(pub_str)
                print('#### Sent register pub state transaction to load cluster ####')
                
            else:
                #CHECK registry then send to correct broker ip xpub
                self.back_end.send_string(msg)
                #print(f"- Forward publication...{msg}")
   
    def update_data(self, add_this, pubid, topic, strength, history, publication):
        print('\n#### Run update pub state func ####')
        try:
            if add_this == 'publisher':
                if topic not in self.pub_data.keys():
                    self.pub_data.update({topic: {pubid: {'strength': strength, 'history': history, 'broker_ip': "", 'publications': []}}})
                elif pubid not in self.pub_data[topic].keys():
                    self.pub_data[topic].update({pubid: {'strength': strength, 'history': history, 'broker_ip': "", 'publications': []}})
            elif add_this == 'publication':
                stored_publication = publication + '--' + str(time.time())
                self.pub_data[topic][pubid]['publications'].append(stored_publication)
            
            print(f'- Updated Local State: {self.pub_data}')
            db_data = self.pub_data
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
            history = message[4]
            if msg_type == 'register':
                self.update_data('publisher', pubid, topic, strength, history, '')
            #IM HERE - not sure I'll use to store publications below
            # elif msg_type == 'publish':
            #     publication = message[4]
            #     self.update_data('publication', pubid, topic, strength, publication)
            #only called if need to change count of replicas            
    
    def update_replicas(self, rep_count):
        #list of working brokers, in order, if any e.g. ["xxxxxxxx1", "xxxxxxxx2", ...]
        self.replicas = self.zk.get_children(self.replica_root_path)
        time.sleep(1)
        
        print(f'Update Replicas - Current Working Reps: {self.replicas}')
        print(f"New replica request count: {rep_count}")
        
        #list of sorted standbys
        self.standbys = self.zk.get_children(self.replica_standby_path)
        
        crnt_stdby_idx = len(self.replicas) - 1
        change_rep_num = rep_count - len(self.replicas)
     
        #create
        if change_rep_num > 0:
            if len(self.standbys) - len(self.replicas) >= change_rep_num:
                for i in range(1, change_rep_num + 1):
                    #get ip value from standby - path listed in self.replicas
                    #TODO - need to decode ip?
                    tmp_ip = self.zk.get(path=self.replica_standby_path + "/" + self.standbys[crnt_stdby_idx + i])[0]
                    print(f'{i} -new lb broker ip: {tmp_ip}')
                    self.zk.create(self.replica_root_path + "/" + self.standbys[crnt_stdby_idx + i], ephemeral=True, value=tmp_ip)
            else:
                print("Error - need to startup some more replica workers")
        #delete
        elif change_rep_num < 0:
            if len(self.replicas) > -change_rep_num:
                # for i in range(1, -change_rep_num + 1):
                #     path = self.replica_root_path + "/" + self.replicas[-i]
                #     self.zk.delete(path)
                #     self.replicas.pop()
                    print(f'TODO - Remove replica from list: {self.replicas[-change_rep_num]}')
            else:
                print("Error - local replica list is smaller than than the workers it expects to delete")
                
        self.distribute_topics_to_replicas()
                    
        #register dicts for pubs subs and brokers?
    def distribute_topics_to_replicas(self):
        print("\nDistributing Topics to Replicas")
        print(self.replicas)
        replica_count = len(self.replicas)
        topic_count = len(self.zk.get_children(self.topic_root_path))
        idx = 0
        for i in range(0, replica_count):
            rep_ip = self.zk.get(self.replica_root_path + "/" + self.replicas[i])[0]

            for j in range (idx * self.TOPIC_THRESHOLD, min(self.TOPIC_THRESHOLD + i * self.TOPIC_THRESHOLD, topic_count)):
                print(self.topics[j])
                time.sleep(1)
                if self.topics[j] in self.pub_data:
                    print("----topic in pub data")
                    print(self.topics[j])
                    print(self.pub_data)
                    for t in self.pub_data[self.topics[j]]:
                        self.pub_data[self.topics[j]][t]["broker_ip"] = rep_ip.decode('utf-8')
                else:
                    print("----topic not in pub data")
            idx += 1
            print(self.pub_data)
            b_pub_data = str(self.pub_data).encode('utf-8')
            self.zk.set(self.leader_path, b_pub_data)
            print("Distr - exiting good\n")
                           
class Publisher():

    def __init__(self, port=5555, zkserver="10.0.0.1", topic=12345, proxy=True):
        self.port = port
        self.proxy = proxy
        self.topic = topic
        self.topic_root_path = '/topic'
        self.path = f"/topic/{topic}"
        self.leader_path = "/leader"
        self.zk = start_kazoo_client(zkserver)
        self.ip = get_ip()
        self.socket = context.socket(zmq.PUB)
        self.strength = randrange(1,11)
        self.history = randrange(0,30)
        self.pubid = randrange(1000,10000)
        
    def start(self):
        print(f"Publisher: {self.ip}")
        print(f'My Root: ' + self.path)
        self.init_monitor()

        #create parent znode
        if not self.zk.exists(self.topic_root_path):
            self.zk.create(self.topic_root_path)
        else:
            print("Exists: " + self.topic_root_path)
            
        #TODO = create topic under parent /topic/ - trigger children watch above
        if not self.zk.exists(self.path):
            pub_val = {"pubid": self.pubid, "pub_ip": self.ip, "strength":self.strength, "history": self.history, "topic":self.topic}
            b_pub_val = str(pub_val).encode('utf-8')
            self.zk.create(self.path, value=b_pub_val, sequence=True, ephemeral=True)
            print(f'Created Znode: {self.path} : {self.ip}')

        print(f'Publishing - Proxy: {self.proxy}, Topic: {self.topic}')

        if self.proxy:  # PROXY MODE

            #TODO - should I just get registry information from leader LB?
            # would need to ensure pubid exists then get worker/broker ip to connect
            @self.zk.DataWatch(self.leader_path)
            def proxy_watcher(data, stat):
                print(f"Publisher: leader load balancer watcher triggered.")
                if data is not None:
                    try:
                        leader_pub_data = data.decode('utf-8')
                        pub_data = eval(leader_pub_data)
                        print("Registry Data: ")
                        print(pub_data)
                        if self.topic in pub_data:
                            if self.ip in pub_data[self.topic].keys():
                                intf = pub_data[self.topic][self.ip]['broker_ip']
                                conn_str = f'tcp://{intf}:{self.port}'
                                print(f"connecting: {conn_str}")
                                self.socket.connect(conn_str)
                        
                        #socket needs time to connect otherwise will pass over send string
                        time.sleep(5)
                        self.socket.send_string(f'register {self.pubid} {self.topic} {self.strength} {self.history}')
                    except:
                        print("...Waiting on Load Balancer to update publisher registry")
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

    def __init__(self, port=5556, zkserver="10.0.0.1", topic='12345', history = "15", proxy=True):
        self.port = port
        self.topic = topic
        self.history = history
        self.leader_path = "/leader"
        self.path = f"/topic/{topic}"
        self.proxy = proxy
        self.ip = get_ip()
        self.socket = context.socket(zmq.SUB)
        self.zk = start_kazoo_client(zkserver)
        self.pub_data = None
        self.register_port = '5559'
        self.register_socket = None
    
    def start(self):
        print(f"Subscriber: {self.ip}")

        if self.proxy:  # PROXY MODE

            @self.zk.DataWatch(self.leader_path)
            def proxy_watcher(data, stat):
                print(f"Subscriber: proxy watcher triggered. data:{data}")
                if data is not None:
                    try:
                        leader_pub_data = data.decode('utf-8')
                        pub_data = eval(leader_pub_data)
                        print("Registry Data: ")
                        print(pub_data)
                        mytopic = str(self.topic)
                        tmp_strength = {}
                        tmp_history = {}
                        
                        print("before for loops")
                        for k,v in pub_data[mytopic].items():
                            tmp_strength[k] = v['strength']
                            
                        for k,v in pub_data[mytopic].items():
                            tmp_history[k] = v['history']
                        
                        meets_hist_req = dict((k, v) for k, v in tmp_history.items() if v >= int(self.history))   
                        print("History requirement list")
                        print(meets_hist_req)
            
                        max_val = None
                        
                        for p in meets_hist_req:
                            tmp_val = pub_data[mytopic][p]['strength']
                            if max_val == None:
                                max_val = tmp_val 
                                max_ip = pub_data[mytopic][p]['broker_ip']
                            elif max_val < tmp_val:
                                max_val = tmp_val
                                max_ip = pub_data[mytopic][p]['brocker_ip']
                        if not max_ip:
                            print("Missing broker for publisher")
                        intf = max_ip
                        conn_str = f'tcp://{intf}:{self.port}'
                        print(f"connecting: {conn_str}")
                        self.socket.connect(conn_str)
                    except:
                        print("...Waiting on Load Balancer to update publisher registry")

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