#!/usr/bin/env python
import json
import optparse
import socket
import sys

from msgbox import *

def abort(msg):
    print >>sys.stderr, msg
    sys.exit(1)

class Options(optparse.OptionParser):
    def __init__(self):
        optparse.OptionParser.__init__(self)
        self.add_option('-a', '--address', dest='address', default=None)
        self.add_option('-f', '--config', dest='config_file', default=None)

def main(args):
    def test(cond, msg):
        if not cond: abort(msg + ', quitting')

    parser = Options()
    (options, args) = parser.parse_args(args)
    test(not args, "Unexpected positional arguments")

    address = options.address
    if address:
        address = Address.from_str(address)
    else:
        cfg_file = options.config_file
        if cfg_file is None:
            cfg_file, found = find_msgbox_config_file()
            test(found, "No config file found")
        with open(cfg_file) as f:
            cfg = json.load(f)
        address = Address.from_json(cfg['listen_address'])

    # we read one JSON payload from stdin...
    hello = MessageHelloSender()
    msgdata = json.load(sys.stdin)
    message = MessageGenEvent(msgdata)
    data = hello.serialize() + message.serialize()

    # ... and send it to the address
    sock = socket.socket(address.socket_address_family(), socket.SOCK_STREAM)
    sock.connect(address.socket_address())

    sent = 0
    while sent < len(data):
        sent += sock.send(buffer(data, sent))
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    sys.exit(0)

if __name__ == "__main__": main(sys.argv[1:])
