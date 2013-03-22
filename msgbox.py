from collections import defaultdict, deque, namedtuple
import asyncore
import errno
import json
import socket
import struct
import traceback                # TODO: remove, only used for debugging

def def_enum(name, typestring):
    types = typestring.split()
    globals()[name.upper() + '_TYPES'] = types
    globals().update({name.upper() + '_' + t.upper(): t for t in types})

# Node types - the types of nodes that can connect to us / we can connect to.
def_enum('node', 'peer sender')

# Packet types - the types of packets that we can receive.
#
# Note: "packet" does NOT mean a TCP packet. we're using the term for our own
# purposes. I wish I knew a better one.
#
# TODO: do we need a "bye" message, given that we can call shutdown() on
# sockets?
def_enum('packet', 'hello welcome uptodate bye message')

# Address types
def_enum('address', 'tcp unix')


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
        self.queues = defaultdict(self.Queue)

    # Schedules a list of chunks to be sent to each destination in `dests`
    # (every chunk gets sent to every destination; first chunk in the list gets
    # sent first). Returns a list of exactly those destinations in `dests` which
    # previously had no data scheduled to be sent to them, but now do.
    def enqueue(self, dests, chunks):
        if not chunks: return []
        # chunks needs to not be a generator, otherwise it will be consumed
        chunks = tuple(chunks)
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
        raise NotImplementedError()

    def __str__(self):
        return '%s.from_json(%s)' % (
            type(self).__name__,
            self.to_json())

def to_json(obj):
    if isinstance(obj, Jsonable):
        return obj.to_json()
    else:
        return obj


# A very simple Jsonable type. Useful base type.
class JsonRecord(Jsonable):
    def __init__(self, **kwargs):
        self._attrs = kwargs.keys()
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    @classmethod
    def from_json(klass, json):
        obj = klass(**json)
        obj.post_from_json()
        return obj

    # in case fields need to be validated, etc.
    def post_from_json(self): return

    def to_json(self):
        return {k: to_json(getattr(self, k)) for k in self._attrs}


# A address (usually of a remote node)
class Address(JsonRecord):
    def __init__(self, **kwargs):
        super(Address, self).__init__(**kwargs)
        assert self.type in ADDRESS_TYPES

    def socket_address_family(self):
        if self.type == ADDRESS_TCP: return socket.AF_INET
        elif self.type == ADDRESS_UNIX: return socket.AF_UNIX
        assert False

    def socket_address(self):
        if self.type == ADDRESS_TCP: return (self.address, self.port)
        elif self.type == ADDRESS_UNIX: return self.path
        assert False

    def __str__(self):
        if self.type == ADDRESS_TCP:
            return 'tcp://%s:%s' % (self.address, self.port)
        elif self.type == ADDRESS_UNIX:
            return 'unix://%s' % self.path
        assert False

def AddressTCP(host, port):
    return Address(type=ADDRESS_TCP, host=host, port=port)

def AddressUnix(path):
    return Address(type=ADDRESS_UNIX, path=path)


# A packet
class Packet(JsonRecord):
    # wire format for packets, using the struct module's format string. this
    # means "big-endian 4-byte unsigned integer".
    HEADER_FMT = '>I'
    HEADER_LEN = struct.calcsize(HEADER_FMT)
    assert HEADER_LEN == 4

    def __init__(self, **kwargs):
        super(Packet, self).__init__(**kwargs)
        assert self.type in PACKET_TYPES

    def serialize(self):
        data = json.dumps(self.to_json())
        header = struct.pack(Packet.HEADER_FMT, len(data))
        return header + data

    def post_from_json(self):
        for field, typ in [('vclock', VClock),
                           ('message', Message)]:
            if not hasattr(self, field): continue
            setattr(self, field, typ.from_json(getattr(self, field)))

    @staticmethod
    def parse_header(header):
        (packet_len,) = struct.unpack(Packet.HEADER_FMT, header)
        return packet_len

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

    @staticmethod
    def from_json(json):
        return Message(json['source'], json['timestamp'], json['data'])

    def to_json(self):
        return {'source': to_json(self.source),
                'timestamp': to_json(self.timestamp),
                'data': to_json(self.data)}


# A vector clock, sort of.
#
# Not exactly like vector clocks as usually described, as we don't necessarily
# increment on send or receive.
class VClock(Jsonable):
    def __init__(self, init=None):
        self.times = {}
        if init is not None:
            self.times.update(init)

    def copy(self): return VClock(self.times)

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

    # imitations of methods from dict
    def __contains__(self, x): return x in self.times
    def __len__(self): return len(self.times)
    def __iter__(self): return iter(self.times)
    def keys(self): return self.times.keys()
    def iterkeys(self): return self.times.iterkeys()
    def iteritems(self): return self.times.iteritems()

    @staticmethod
    def from_json(json):
        return VClock(json)

    def to_json(self):
        return dict(self.times)


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
        lst = self.msgs.setdefault(msg.source,[])
        # check ordering constraint
        assert (not lst) or msg.timestamp > lst[-1].timestamp
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

    @staticmethod
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


# Handles splitting things into packets
class PacketDispatcher(asyncore.dispatcher):
    def __init__(self, **kwargs):
        asyncore.dispatcher.__init__(self, **kwargs)
        self.recvd = []
        # True if we're reading a packet's length header, False if we're reading
        # its body
        self.reading_header = True
        self.expecting = Packet.HEADER_LEN

    def expect(self, n_bytes):
        assert self.expecting == 0
        assert isinstance(n_bytes, int)
        self.expecting = n_bytes

    # for some reason asyncore.dispatcher handles EAGAIN/EWOULDBLOCK for send()
    # but not recv(), probably because recv() returning 0 conventionally
    # indicates socket closure.
    def recv(self, *args, **kwargs):
        try:
            return asyncore.dispatcher.recv(self, *args, **kwargs)
        except socket.error as (errnum, msg):
            if errnum in [errno.EAGAIN, errno.EWOULDBLOCK]:
                return ''
            else:
                raise

    # TODO: remove this, it's for debugging purposes.
    # alternatively, make it suck less.
    def handle_error(self):
        print
        traceback.print_exc()
        print
        asyncore.dispatcher.handle_error(self)

    # subclass must implement
    def handle_packet(self, packet): raise NotImplementedError()

    def handle_read(self):
        # TODO: is self.connected the right thing to loop on?
        while self.connected:
            assert self.expecting > 0
            got = self.recv(self.expecting)
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
            packet_len = Packet.parse_header(chunk)
            self.reading_header = False
            self.expect(packet_len)
        else:
            self.handle_packet(Packet.from_json(json.loads(chunk)))
            self.reading_header = True
            self.expect(Packet.HEADER_LEN)


