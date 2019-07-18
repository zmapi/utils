import zmq
import sys
import argparse
import json
import struct
from traceback import print_exc
from pprint import pprint, pformat


class GlobalState:
    pass
g = GlobalState()
g.msg_types = set()
g.md_entry_types = None
g.md_price_levels = None
g.md_update_actions = None


def parse_args():
    desc = "zmapi MD/AC PUB listener utility"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("pub_addr", help="address to PUB socket")
    parser.add_argument("topics", nargs="*", help="topics to listen")
    parser.add_argument(
            "--msgtypes",
            help="space separated list of msgtypes to listen to")
    parser.add_argument(
            "--mdentrytypes",
            type=str,
            nargs="+",
            help="accepted MDEntryTypes")
    parser.add_argument(
            "--mdpricelevels",
            type=int,
            nargs="+",
            help="accepted MDPriceLevels")
    parser.add_argument(
            "--mdupdateactions",
            type=str,
            nargs="+",
            help="accepted MDUpdateActions")
    args = parser.parse_args()
    if args.msgtypes:
        g.msg_types = set(args.msgtypes.split())
    if args.mdentrytypes:
        g.md_entry_types = set(args.mdentrytypes)
    if args.mdpricelevels:
        g.md_price_levels = set(args.mdpricelevels)
    if args.mdupdateactions:
        g.md_update_actions = set(args.mdupdateactions)
    return args


def wrangle_msg(msg):
    codec = msg[0]
    if codec == 0x20:
        msg = json.loads(msg.decode())
        msg_type = msg["Header"]["MsgType"]
        if g.msg_types and msg_type not in g.msg_types:
            return
        if msg_type in ("X", "W"):
            group = msg["Body"]["NoMDEntries"]
            if g.md_entry_types:
                for i in range(len(group) - 1, -1, -1):
                    if group[i]["MDEntryType"] not in g.md_entry_types:
                        del group[i]
            if g.md_price_levels:
                for i in range(len(group) - 1, -1, -1):
                    price_level = group[i].get("MDPriceLevel")
                    if price_level is not None and \
                            price_level not in g.md_price_levels:
                                del group[i]
            if g.md_update_actions:
                for i in range(len(group) - 1, -1, -1):
                    ua = group[i].get("MDUpdateAction")
                    if ua is not None and ua not in g.md_update_actions:
                        del group[i]
            if not msg["Body"]["NoMDEntries"]:
                return
            # if len(msg["Body"]["NoMDEntries"]) < 2:
            #     continue
        msg = pformat(msg)
    return msg


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
        msg_parts = mdsub.recv_multipart()
        if len(msg_parts) == 2:
            endpoint = msg_parts[0]
            msg = msg_parts[1]
        else:
            endpoint = None
            msg = msg_parts[0]
        if msg:
            try:
                msg = wrangle_msg(msg)
            except Exception as err:
                print_exc()
                msg = str(msg)
            if not msg:
                continue
        s = ""
        if endpoint:
            s += "{}: ".format(endpoint)
        s += msg
        print(s)
        # topic, session_id, seq_no_bytes, msg = msg_parts
        # seq_no = struct.unpack(">q", seq_no_bytes.rjust(8, b"\x00"))[0]
        # if msg:
        #     codec = msg[0]
        #     if codec == 0x20:
        #         msg = pformat(json.loads(msg.decode()))
        # print("{} ({}/{}): {}".format(topic, seq_no, session_id, msg))


def main():
    args = parse_args()
    start_listener(args)

if __name__ == "__main__":
    main()
