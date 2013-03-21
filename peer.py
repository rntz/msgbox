#!/usr/bin/env python
from collections import deque, namedtuple
import asyncore
import bisect
import errno
import json
import optparse
import os, os.path
import socket
import struct
import sys
import time

LISTEN_BACKLOG = 5

# We delay approximately this long between connecting to remotes, to allow
# ourselves to sync up with the previous remote we connected to. This is a
# heuristic, not necessary for correctness.
REMOTE_CONNECT_DELAY_S = 2.0

# The timeout to pass to asyncore.loop(). We'll be stuck in a syscall and unable
# to respond to signals for approximately this long, so keep it small to allow
# ctrl-C etc. to work reasonably well.
ASYNCORE_TIMEOUT_S = 1.0

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

    # Schedules a list of chunks to be sent to each destination in `dests`
    # (every chunk gets sent to every destination; first chunk in the list gets
    # sent first). Returns a list of exactly those destinations in `dests` which
    # previously had no data scheduled to be sent to them, but now do.
    def enqueue(self, dests, chunks):
        if not chunks: return []
        empty_dests = [d for d in dests if not self.queues[d]]
        for dest in dests:
            self.queues[dest].extend(chunks)
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

        def extend(self, chunks):
            self.chunks.extend(chunks)

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

    def already_happened(self, uid, time):
        if uid not in self.times: return False
        return time <= self.times[uid]

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


# Node types - the types of nodes that can connect to us / we can connect to.
NODE_UNKNOWN = None
NODE_TYPES = "peer sender".split()
globals().update({'NODE_' + t.upper(): t for t in NODE_TYPES})

# Packet types - the types of packets that we can receive.
#
# Note: "packet" does NOT mean a TCP packet. we're using the term for our own
# purposes. I wish I knew a better one.
#
# TODO: do we need a "bye" message, given that we can call shutdown() on
# sockets?
PACKET_TYPES = "hello welcome uptodate bye message".split()
globals().update({'PACKET_' + t.upper(): t for t in PACKET_TYPES})


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

def PacketHelloPeer(peer_id, vclock):
    return Packet(type=PACKET_HELLO,
                  node_type=NODE_PEER,
                  peer_id=peer_id,
                  vclock=vclock.copy())

def PacketHelloSender():
    return Packet(type=PACKET_INIT, node_type=NODE_SENDER)

def PacketWelcome(peer_id, vclock):
    return Packet(type=PACKET_WELCOME, peer_id=peer_id, vclock=vclock.copy())

def PacketUptodate():
    return Packet(type=PACKET_UPTODATE)

def PacketBye():
    return Packet(type=PACKET_BYE)

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


# Handles the socket we listen on incoming connections for
class ListenHandler(asyncore.dispatcher):
    def __init__(self, sock, map, coro):
        super(ListenHandler, self).__init__(sock=sock, map=map)
        self.coro = coro

    def handle_accept(self):
        # TODO: should we try/catch here for EAGAIN/EWOULDBLOCK
        pair = self.accept()
        # handle_accept() shouldn't be called unless we can accept, I think?
        assert pair is not None
        sock, addr = pair
        print 'Incoming connection from %s' % repr(addr) # TODO: remove
        self.controller.handle_incoming(sock, addr)

    def handle_close(self):
        raise NotImplemented()  # FIXME

    def readable(self): return False
    def writable(self): return False


# Handles splitting things into packets
class PacketDispatcher(asyncore.dispatcher):
    def __init__(self, **kwargs):
        super(ConnHandler, self).__init__(**kwargs)
        self.recvd = []
        # True if we're reading a packet's length header, False if we're reading
        # its body
        self.reading_header = True
        self.expecting = PACKET_LEN_BYTES

    def expect(self, n_bytes):
        assert self.expecting == 0
        self.expecting = n_bytes

    # subclass must implement
    def handle_packet(self, packet): raise NotImplemented()

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
            self.handle_packet(Packet.from_json(json.loads(chunk)))
            self.reading_header = True
            self.expect(PACKET_LEN_BYTES)


# Handles the IO on a connection to another node
class ConnHandler(PacketDispatcher):
    def __init__(self, parent, **kwargs):
        super(Handler, self).__init__(map=parent.socket_map, **kwargs)
        self.parent = parent
        self.reader_coro = None
        self.writable_ = False

    def readable(self): return self.reader_coro is not None
    def writable(self): return self.writable_

    def make_writable(self, writable=True): self.writable_ = writable

    def handle_packet(self, packet):
        self.reader_coro.send(packet)

    def handle_write(self):
        assert self.writable_
        self.parent.handle_write(self)

    def handle_close(self):
        raise NotImplemented()  # FIXME

class IncomingHandler(ConnHandler):
    def __init__(self, parent, sock):
        super(IncomingHandler, self).__init__(parent, sock=sock)
        self.reader_coro = parent.incoming_reader(self)
        self.reader_coro.next()

class OutgoingHandler(ConnHandler):
    def __init__(self, parent, address):
        super(OutgoingHandler, self).__init__(parent)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(address)

    def handle_connect(self):
        self.parent.peers.add(self)
        # start up a reader coro & hook us up to the write-handler
        self.reader_coro = self.parent.outgoing_reader(self)
        self.reader_coro.next()


# deals with IO and shit
# I don't really know what this class' responsibilities are
# it does what it does
class Peer(object):
    def __init__(self, listen_socket, peer_id, vclock, message_store, remotes):
        self.queue = MultiQueue()
        self.sockets = set()
        self.socket_map = {}    # for dispatchers
        self.peers = set()

        self.peer_id = peer_id
        self.vclock = vclock
        self.message_store = message_store
        self.remotes = remotes  # remotes to connect to
        self.listen_socket = ListenHandler(controller=self, sock=listen_socket)

    def run(self):
        # start listening on listening socket
        listen_socket.listen(LISTEN_BACKLOG)

        # start trying to connect to remotes
        for remote in remotes:
            # try to connect to the remote
            address = (remote['host'], remote['port'])
            sock = OutgoingHandler(self, address)
            self.connect(sock)

            # wait for events until the appropriate time delay has passed
            time_begin = time.time()
            elapsed = 0
            while elapsed < REMOTE_CONNECT_DELAY:
                self.loop(timeout = REMOTE_CONNECT_DELAY - elapsed,
                          count = 1)
                elapsed = time.now() - time_begin

        # wait for events
        self.loop()
        # FIXME: deal with shutting down once all open channels have been
        # closed.
        raise NotImplemented()

    def loop(self, **kwargs):
        kwargs['timeout'] = min(ASYNCORE_TIMEOUT_S,
                                kwargs.get('timeout', ASYNCORE_TIMEOUT_S))
        asyncore.loop(map = self.socket_map, **kwargs)

    def connect(self, sock):
        assert sock.connected   # TODO: hackish, relies on dispatcher internals
        self.sockets.add(sock)
        self.queue.connect(sock)

    def disconnect(self, sock):
        # TODO: hackish b/c `sock.connected` internal to dispatcher
        assert not sock.connected
        assert sock not in self.peers
        self.sockets.remove(sock)
        self.queue.disconnect(sock)

    # Schedules packets for sending to a list of sockets.
    def send_many(self, socks, packets):
        def serialize(packet):
            data = json.dumps(packet.to_json())
            header = struct.pack(PACKET_LEN_FMT, len(data))
            return header + data
        newly_active = self.queue.enqueue(socks, map(serialize, packets))
        for sock in newly_active:
            self.try_sending_to(sock)

    def send_one(self, socks, packet):
        return self.send_many(socks, [packet])

    # handling incoming connections
    def handle_incoming(self, sock, addr):
        sock = self.make_handler(sock, self.incoming_reader)
        self.connect(sock)

    def incoming_reader(self, sock):
        # TODO: handle unannounced shutdowns. probably need a try/catch block.

        hello = yield
        # TODO: couldn't the first packet (or ANY packet) be a BYE?
        assert hello.type == PACKET_HELLO   # TODO: error handling
        assert peer.node_type in NODE_TYPES # TODO: error handling
        node_type = hello.node_type

        # this code could be so much cleaner if `yield from` was available.
        if node_type == NODE_PEER:
            peer_id = sock.peer_id = hello.peer_id

            # add a writer for the peer
            sock.writer = lambda: self.handle_write(sock)

            # send welcome response and updates for peer
            msgs = self.updates_for(hello.vclock)
            packets = ([PacketWelcome(self.peer_id, self.vclock)]
                       + [PacketMessage(msg) for msg in msgs]
                       + [PacketUptodate()])
            self.send_many([sock], packets)

            # now, accept messages from our peer
            while True:
                packet = yield
                if packet.type == PACKET_BYE: break
                # TODO: handle bad message types
                assert packet.type == PACKET_MESSAGE
                self.handle_message(packet.message, recvd_from=sock)

            # got a bye from a peer
            self.peers.remove(sock)

        elif node_type == NODE_SENDER:
            # just read messages from them.
            while True:
                packet = yield
                if packet.type == PACKET_BYE: break
                # TODO: handle bad message types
                assert packet.type == PACKET_MESSAGE
                self.handle_message(packet.message, recvd_from=sock)

        else:
            assert False        # unreachable case

        # we got a bye.
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
        self.disconnect(sock)

    def updates_for(self, vclock):
        msgdict = self.message_store.messages_after_vclock(vclock)
        for src, msgs in msgdict.iteritems():
            for m in msgs: yield m

    def handle_message(self, message, recvd_from=None):
        # if we haven't already seen the message...
        if self.vclock.already_happened(message.source, message.timestamp):
            return
        # ... then add it to the store, update our vclock, ...
        self.vclock.update(message.source, message.timestamp)
        self.message_store.add(message)
        # ... and send it on to all our neighboring peers (except the one that
        # sent it to us)
        dests = (x for x in self.peers if x is not recvd_from)
        self.send_one(dests, PacketMessage(message))

    def handle_write(self, sock):
        self.try_sending_to(sock)

    def try_sending_to(self, sock):
        assert sock.node_type == NODE_PEER
        def sender(chunk):
            return sock.send(chunk)
        self.queue.send(sock, sender)

    def shutdown(self): pass
