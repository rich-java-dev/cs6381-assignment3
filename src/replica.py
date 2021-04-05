import sys
import argparse
from zutils_load import Proxy
from zutils_worker import Worker

parser = argparse.ArgumentParser("replica.py --xin=5555 --xout=5556 --zkserver=10.0.0.1 --type=load")
parser.add_argument("--zkserver","--zkintf", default="10.0.0.1")
parser.add_argument("--xin", "--in_bound", default="5555")
parser.add_argument("--xout", "--out_bound", default="5556")
parser.add_argument("--type", "--type", default="load") 
args = parser.parse_args()

zkserver = args.zkserver
in_bound = args.xin
out_bound = args.xout
worker_type = args.type #either load or worker

if worker_type == "worker":
    Worker(zkserver, in_bound, out_bound).start()
elif worker_type == "load":
    Proxy(zkserver, in_bound, out_bound).start()
else:
    print('Please choose type: \n\--type=load/worker')