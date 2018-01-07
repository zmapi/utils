import zmq
import sys
import argparse
import json
from pprint import pprint, pformat

def parse_args():
    desc = "zmapi MD/AC PUB listener utility"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("pub_addr", help="address to PUB socket")
    parser.add_argument("topics", nargs="*", help="topics to listen")
    args = parser.parse_args()
    return args

def start_listener(args):
    ctx = zmq.Context()
    mdsub = ctx.socket(zmq.SUB)
    mdsub.connect(args.pub_addr)
    if not args.topics:
        mdsub.setsockopt_string(zmq.SUBSCRIBE, "")
    else:
        for topic in args.topics:
            mdsub.setsockopt_string(zmq.SUBSCRIBE, topic)
    while True:
        parts = mdsub.recv_multipart()
        topic = parts[0]
        msg = parts[1]
        if not msg:
            continue
        codec = msg[0]
        if codec == 0x20:
            msg = pformat(json.loads(msg.decode()))
        print("{}: {}".format(topic, msg))

def main():
    args = parse_args()
    start_listener(args)

if __name__ == "__main__":
    main()
