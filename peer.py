#!/usr/bin/env python
from collections import deque, namedtuple
import asyncore
import bisect
import json
import optparse
import os, os.path
import struct
import sys

# format in which packets are sent, using the struct module's format string.
# this means "big-endian 4-byte unsigned integer".
PACKET_LEN_FMT = '>I'
PACKET_LEN_BYTES = struct.calcsize(PACKET_LEN_FMT)
assert PACKET_LEN_BYTES == 4

def abort(msg):
    print >>sys.stderr, msg
    sys.exit(1)

class Options(optparse.OptionParser):
    # TODO: usage info
    def __init__(self):
        optparse.OptionParser.__init__(self)
        self.add_option('-f', '--config', dest='config_file', default=None)

def parse_config(options):
    def test(cond, msg):
        if not cond: abort(msg + ", quitting")

    cfg_file = options.config_file
    test(cfg_file, "No config file specified")

    with open(cfg_file, 'r') as f:
        cfg = json.load(f)

    # TODO: config verification
    return cfg


# Node types - the types of nodes that can connect to us / we can connect to.
NODE_UNKNOWN = None
# A full peer; sends and receives messages.
NODE_PEER = "peer"
# An event sender; only sends messages.
NODE_SENDER = "sender"

NODE_TYPES = [NODE_PEER, NODE_SENDER]


# The send-multiqueue
#
# Keeps track of data (split into chunks, but this is only a sender-side
# convenience; chunks are sent with no headers or dividers between them) to send
# to a set of destinations. Chunks should be strings. Destinations can be any
# hashable value.
#
# TODO: use buffer() objects instead of copying strings
class MultiQueue(object):
    def __init__(self):
        self.queues = {}

    def connect(self, dest):
        assert dest not in self.queues
        self.queues[dest] = MultiQueue.Queue()

    def disconnect(self, dest):
        del self.queues[dest]

    # schedules a chunk to be sent to each destination in `dests`. Returns a
    # list of exactly those destinations in `dests` which previously had no data
    # scheduled to be sent to them.
    def add(self, chunk, dests):
        empty_dests = [d for d in dests if not self.queues[d]]
        for dest in dests:
            self.queues[dest].add(chunk)
        return empty_dests

    # Tries to send as much data as possible to a given destination. `sender`
    # should be a function that takes a string and returns how much of the
    # string was successfully sent to the destination. send() stops sending once
    # `sender` returns 0, or when it has no more data to send.
    def send(self, dest, sender):
        self.queues[dest].send(sender)

    # a queue for a particular destination
    class Queue(object):
        def __init__(self):
            # self.offset = 0
            self.chunks = deque()

        def __len__(self): return len(self.chunks)

        def add(self, chunk):
            self.chunks.append(chunk)

        def send(self, sender):
            while self.chunks:
                chunk = self.chunks[0]
                sent = sender(chunk)
                if not sent:
                    return
                elif sent == len(chunk):
                    self.chunks.popleft()
                else:
                    assert 0 < sent < len(chunk)
                    self.chunks[0] = chunk[sent:]


# Turning things into JSON reasonably generically
class Jsonable(object):
    def to_json(self):
        raise NotImplemented()

def to_json(obj):
    if isinstance(obj, Jsonable):
        return obj.to_json()
    else:
        return obj


# A vector clock, sort of.
#
# Not exactly like vector clocks as usually described, as we don't necessarily
# increment on send or receive.
class VClock(Jsonable):
    def __init__(self, init=None):
        self.times = {}
        if init is not None:
            self.times.update(init)

    def tick(self, uid):
        self.times[uid] += 1

    def time_for(self, uid):
        return self.times[uid]

    def tick_timestamp(self, uid):
        self.tick(uid)
        return self.time_for(uid)

    def update(self, uid, value):
        assert value >= self.times.get(uid,0)
        self.times[uid] = value

    def merge(self, other):
        for k,v in other.iteritems():
            assert v >= 0
            self[k] = max(self.get(k,0), v)

    def iteritems(self):
        return self.times.iteritems()

    @classmethod
    def from_json(json):
        return VClock(json)

    # Returns a value that can be json.dump()ed, and also passed to VClock() as
    # `init`.
    def to_json(self):
        # inefficient, but referentially transparent
        return dict(self.times)


# Packet types - the types of packets that we can receive.
#
# Note: "packet" does NOT mean a TCP packet. we're using the term for our own
# purposes. I wish I knew a better one.
PACKET_INIT = "init"
PACKET_QUIT = "quit"
PACKET_MESSAGE = "message"
PACKET_TYPES = [PACKET_INIT, PACKET_QUIT, PACKET_MESSAGE]

# A packet.
class Packet(Jsonable):
    def __init__(self, dictionary):
        self._attrs = dictionary.keys()
        for k,v in dictionary.iteritems():
            setattr(self, k, v)
        assert self.type in PACKET_TYPES

    @classmethod
    def from_json(json):
        return Packet(json)

    def to_json(self):
        return {k: to_json(getattr(self, v)) for k in self._attrs}

def PacketMessage(message):
    return Packet(type=PACKET_MESSAGE, message=message)

def PacketInitPeer(peer_id, vclock):
    return Packet(type=PACKET_INIT,
                  node_type=NODE_PEER,
                  peer_id=peer_id,
                  vclock=vclock)

def PacketInitSender():
    return Packet(type=PACKET_INIT, node_type=NODE_SENDER)

def PacketQuit():
    return Packet(type=PACKET_QUIT)

# A message, consisting of a source identifier, a timestamp, and a JSON payload.
class Message(Jsonable):
    def __init__(self, source, timestamp, data):
        assert isinstance(timestamp, int)
        self.source = source
        self.timestamp = timestamp
        self.data = data

    @classmethod
    def from_json(self, json):
        return Message(json['source'], json['timestamp'], json['data'])

    def to_json(self):
        return {'source': self.source,
                'timestamp': self.timestamp,
                'data': self.data}


# The message store
#
# Keeps track of messages received from each source, along with their
# timestamps.
#
# TODO: garbage collection of some sort
class MessageStore(Jsonable):
    def __init__(self):
        self.msgs = {}

    def add(self, msg):
        lst = self.msgs.setdefault(msgs.source,[])
        # check ordering constraint
        assert (not lst) or msgs.timestamp > lst[-1].timestamp
        lst.append(msg)

    def messages_after_vclock(self, vclock):
        msgdict = {}
        for source, msgs in self.msgs.iteritems():
            if source not in vclock:
                idx = 0
            else:
                idx = bisect.bisect(msgs, vclock.time_for(source))
            msgdict[source] = msgs[idx:]
        return msgdict

    @classmethod
    def from_json(json):
        store = MessageStore()
        for source, msgs in json.iteritems():
            msgs = store.msgs[source] = [Message(source, m[0], m[1])
                                         for m in msgs]
            # check message list is strictly sorted by timestamp
            assert all((msgs[i].timestamp < msgs[i+1].timestamp
                        for i in xrange(len(msgs)-1)))
        return store

    def to_json(self):
        json = {}
        for source, msgs in self.msgs.iteritems():
            json[source] = [[m.timestamp, m.data] for m in msgs]
        return json


# Handles an individual socket for PeerController
class SockHandler(asyncore.dispatcher):
    def __init__(self, controller):
        super(SockHandler, self).__init__()
        self.controller = controller
        self.node_type = NODE_UNKNOWN
        self.recvd = []
        # True if we're reading a packet's length header, False if we're reading
        # its body
        self.reading_header = True
        self.expecting = PACKET_LEN_BYTES

    def expect(self, n_bytes):
        assert self.expecting == 0
        self.expecting = n_bytes

    def handle_write(self):
        # FIXME: need to avoid waiting for writes unless we're a full peer.
        # otherwise this assert will trip.
        assert self.node_type == NODE_PEER
        self.controller.handle_write(self)

    def handle_read(self):
        # is self.connected the right thing to loop on?
        while self.connected:
            assert self.expecting > 0
            got = self.read(self.expecting)
            if not got: return  # nothing to read

            self.recvd.append(got)
            self.expecting -= len(got)
            if self.expecting > 0: return # not done yet

            # otherwise, we've received a full chunk
            assert self.expecting == 0
            chunk = ''.join(self.recvd)
            self.recvd = []
            # must call self.expect() with a nonzero argument, or close this
            # socket.
            self.handle_chunk(chunk)

    def handle_chunk(self, chunk):
        if self.reading_header:
            packet_len = struct.unpack(PACKET_LEN_FMT, chunk)
            self.reading_header = False
            self.expect(packet_len)
        else:
            self.handle_packet(chunk)
            self.reading_header = True
            self.expect(PACKET_LEN_BYTES)

    def handle_packet(self, packet):
        p = Packet.from_json(json.loads(packet))
        self.controller.handle_packet(self, p)


# an event handler that manages almost everything else
class PeerController(object):
    def __init__(self, peer_id, message_store):
        self.peer_id = peer_id
        self.message_store = message_store
        self.queue = MultiQueue()
        # maps from socket ids to their handlers
        self.sockets = {}

    # **********************************************************************
    # io_manager must satisfy the following interface:
    #
    # - io.id(sock) -> unique hashable identifier for sock
    #
    # - io.write(sock, msg) -> number of bytes written
    #   Must not block.
    #
    # - io.read(sock) -> string
    #   Must not block. Must return as much data as available.
    #
    # - io.close(sock)
    #   Closes & shuts down sock.

    def handle_packet(self, socket, packet):
        if socket.node_type == NODE_UNKNOWN:
            assert packet.type == PACKET_INIT
            # Add the node in
            self.handle_init(socket, packet)
        elif socket.node_type in [NODE_PEER, NODE_SENDER]:
            if packet.type == PACKET_MESSAGE:
                self.handle_message(socket, packet.message)
            elif packet.type == PACKET_QUIT:
                self.handle_quit(socket)
            else:
                assert False    # malformed packet, TODO: handle error
        else:
            assert False        # unreachable case

    def handle_init(self, socket, packet):
        # TODO: handle malformed packet node types
        assert packet.node_type in NODE_TYPES
        socket.node_type = packet.node_type
        if socket.node_type == NODE_PEER:
            # if they're a full peer, we need to get them up to date
            socket.peer_id = pkt.peer_id
        # FIXME
        raise NotImplemented()

    def handle_quit(self, socket):
        pass                    # FIXME

    def handle_message(self, socket, message):
        pass                    # FIXME

    def handle_write(self, socket):
        assert socket.node_type == NODE_PEER
        pass

    # Handles a new connection.
    def handle_connect(self, sock): pass # FIXME
    def handle_disconnect(self, sock): pass # FIXME

    def shutdown(self): pass
