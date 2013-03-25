from collections import defaultdict, deque, namedtuple
import asyncore
import bisect
import errno
import json
import socket
import struct
import types

def def_enum(name, typestring):
    types = typestring.split()
    globals()[name.upper() + '_TYPES'] = types
    globals().update({name.upper() + '_' + t.upper(): t for t in types})

# Node types - the types of nodes that can connect to us / we can connect to.
def_enum('node', 'peer sender')

# Message types - the types of messages that we can receive.
def_enum('message', 'hello welcome uptodate event gen_event')

# Address types
def_enum('address', 'tcp unix')

# takes a sequence or generator, and returns a sequence. in other words, takes
# something representing a sequence and makes sure it *persistently* represents
# the same sequence (unlike generators, which get consumed when iterated over).
def persist_seq(seq):
    if isinstance(seq, (tuple, list, set)): return seq
    return tuple(seq)


# Dead simple timer class. Is fed all actual information about time by client.
class Timer(object):
    def __init__(self, fire_at, callback):
        self.fire_at = fire_at
        self.callback = callback
        self.fired = False

    def left(self, now):
        return self.fire_at - now

    def try_fire(self, now):
        assert not self.fired
        if now < self.fire_at:
            # we don't fire yet
            return False
        self.callback()
        self.fired = True
        return True


# The send-multiqueue
#
# Keeps track of data (split into chunks, but this is only a sender-side
# convenience; chunks are sent with no headers or dividers between them) to send
# to a set of destinations. Chunks should be strings. Destinations can be any
# hashable value.
class MultiQueue(object):
    def __init__(self):
        self.queues = defaultdict(self.Queue)

    # returns True iff `dest` has data queues for it.
    def has_queued_data(self, dest):
        return bool(self.queues.get(dest))

    # Schedules a list of chunks to be sent to each destination in `dests`
    # (every chunk gets sent to every destination; first chunk in the list gets
    # sent first). Returns a list of exactly those destinations in `dests` which
    # previously had no data scheduled to be sent to them, but now do.
    def enqueue(self, dests, chunks):
        # only keep nonempty chunks
        chunks = tuple(x for x in chunks if len(x))
        # if we have no data to enqueue, we're done.
        if not chunks: return []
        # we'll use this more than once; avoid consuming it if it's a generator.
        dests = persist_seq(dests)
        empty_dests = [d for d in dests if not self.queues[d]]
        for dest in dests:
            self.queues[dest].extend(chunks)
        return empty_dests

    # removes each dest in dests from the queue
    def remove(self, dests):
        for d in dests:
            if d in self.queues:
                del self.queues[d]

    # Tries to send as much data as possible to a given destination. `sender`
    # should be a function that takes a string and returns how much of the
    # string was successfully sent to the destination. send() stops sending once
    # `sender` returns 0, or when it has no more data to send.
    def send(self, dest, sender):
        self.queues[dest].send(sender)

    # A queue for a particular destination.
    class Queue(deque):
        def __init__(self):
            self.offset = 0
            self.chunks = deque()

        def __len__(self):
            return len(self.chunks)

        def extend(self, chunks):
            self.chunks.extend(chunks)

        def send(self, sender):
            while self.chunks:
                chunk = self.chunks[0]
                sent = sender(buffer(chunk, self.offset))
                self.offset += sent
                if not sent: return
                elif self.offset == len(chunk):
                    self.offset = 0
                    self.chunks.popleft()
                else:
                    assert 0 < self.offset < len(chunk)


# Turning things into JSON reasonably generically
class Jsonable(object):
    # subclasses must implement
    def to_json(self): raise NotImplementedError()
    @staticmethod
    def from_json(): raise NotImplementedError()

    def __repr__(self):
        return '%s.from_json(%s)' % (type(self).__name__,
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
        if self.type == ADDRESS_TCP: return (self.host, self.port)
        elif self.type == ADDRESS_UNIX: return self.path
        assert False

    def __str__(self):
        if self.type == ADDRESS_TCP:
            return 'tcp://%s:%s' % (self.host, self.port)
        elif self.type == ADDRESS_UNIX:
            return 'unix://%s' % self.path
        assert False

def AddressTCP(host, port):
    return Address(type=ADDRESS_TCP, host=host, port=port)

def AddressUnix(path):
    return Address(type=ADDRESS_UNIX, path=path)


# A message
class Message(JsonRecord):
    # wire format for messages, using the struct module's format string. this
    # means "big-endian 4-byte unsigned integer".
    HEADER_FMT = '>I'
    HEADER_LEN = struct.calcsize(HEADER_FMT)
    assert HEADER_LEN == 4

    def __init__(self, **kwargs):
        super(Message, self).__init__(**kwargs)
        assert self.type in MESSAGE_TYPES

    def serialize(self):
        data = json.dumps(self.to_json())
        header = struct.pack(Message.HEADER_FMT, len(data))
        return header + data

    def post_from_json(self):
        for field, typ in [('vclock', VClock),
                           ('event', Event)]:
            if not hasattr(self, field): continue
            setattr(self, field, typ.from_json(getattr(self, field)))

    @staticmethod
    def parse_header(header):
        (message_len,) = struct.unpack(Message.HEADER_FMT, header)
        return message_len

def MessageEvent(event):
    return Message(type=MESSAGE_EVENT, event=event)

def MessageGenEvent(source, data):
    return Message(type=MESSAGE_GEN_EVENT, source=source, data=data)

def MessageHelloPeer(peer_id, vclock):
    return Message(type=MESSAGE_HELLO,
                  node_type=NODE_PEER,
                  peer_id=peer_id,
                  vclock=vclock.copy())

def MessageHelloSender():
    return Message(type=MESSAGE_INIT, node_type=NODE_SENDER)

def MessageWelcome(peer_id, vclock):
    return Message(type=MESSAGE_WELCOME, peer_id=peer_id, vclock=vclock.copy())

def MessageUptodate():
    return Message(type=MESSAGE_UPTODATE)


# An event, consisting of a source identifier, a timestamp, and a JSON payload.
class Event(Jsonable):
    def __init__(self, source, timestamp, data):
        assert isinstance(timestamp, int)
        self.source = source
        self.timestamp = timestamp
        self.data = data

    @staticmethod
    def from_json(json):
        return Event(json['source'], json['timestamp'], json['data'])

    def to_json(self):
        return {'source': to_json(self.source),
                'timestamp': to_json(self.timestamp),
                'data': self.data}


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


# The event store
#
# Keeps track of events received from each source, along with their
# timestamps.
#
# TODO: garbage collection of some sort
class EventStore(Jsonable):
    def __init__(self):
        self.events = {}

    def add(self, event):
        lst = self.events.setdefault(event.source,[])
        # check ordering constraint
        assert (not lst) or event.timestamp > lst[-1].timestamp
        lst.append(event)

    def events_after_vclock(self, vclock):
        evdict = {}
        for source, events in self.events.iteritems():
            if source not in vclock:
                idx = 0
            else:
                keys = [e.timestamp for e in events]
                idx = bisect.bisect_right(keys, vclock.time_for(source))
            if idx < len(events):
                evdict[source] = events[idx:]
        return evdict

    @staticmethod
    def from_json(json):
        store = EventStore()
        for source, events in json.iteritems():
            events = store.events[source] = [Event(source, e[0], e[1])
                                             for e in events]
            # check event list is strictly sorted by timestamp
            assert all((events[i].timestamp < events[i+1].timestamp
                        for i in xrange(len(events)-1)))
        return store

    def to_json(self):
        json = {}
        for source, events in self.events.iteritems():
            json[source] = [[to_json(e.timestamp), e.data] for e in events]
        return json


# Handles splitting things into messages
class MessageDispatcher(asyncore.dispatcher):
    def __init__(self, **kwargs):
        asyncore.dispatcher.__init__(self, **kwargs)
        self.recvd = []
        # True if we're reading a message's length header, False if we're reading
        # its body
        self.reading_header = True
        self.expecting = Message.HEADER_LEN

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
        except socket.error as (errnum, errmsg):
            if errnum not in [errno.EAGAIN, errno.EWOULDBLOCK]: raise
            # recv() would block, so we "received" nothing
            return ''

    # subclass must implement
    def handle_message(self, message): raise NotImplementedError()

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
            message_len = Message.parse_header(chunk)
            self.reading_header = False
            self.expect(message_len)
        else:
            self.handle_message(Message.from_json(json.loads(chunk)))
            self.reading_header = True
            self.expect(Message.HEADER_LEN)
