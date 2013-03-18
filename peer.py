#!/usr/bin/env python
from collections import deque
import asyncore
import bisect
import json
import optparse
import os, os.path
import sys

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


# The message multiqueue
#
# Keeps track of messages to send to a set of destinations. Messages should be
# strings. Destinations can be any hashable value.
class MultiQueue(object):
    def __init__(self):
        self.queues = {}

    def connect(self, dest):
        assert dest not in self.queues
        self.queues[dest] = MultiQueue.Queue()

    def disconnect(self, dest):
        del self.queues[dest]

    def add(self, dest, msg):
        self.queues[dest].add(msg)

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
            self.msgs = deque()

        def __len__(self): return len(self.msgs)

        def add(self, msg):
            self.msgs.append(msg)

        def send(self, sender):
            while self.msgs:
                msg = self.msgs[0]
                sent = sender(msg)
                if not sent:
                    return
                elif sent == len(msg):
                    self.msgs.popleft()
                else:
                    assert 0 < sent < len(msg)
                    self.msgs[0] = msg[sent:]


# A vector clock, sort of.
#
# Not exactly like vector clocks as usually described, as we don't necessarily
# increment on send or receive.
class VClock(object):
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


# A message, which is a source identifier, a timestamp, and a JSON payload.
class Message(object):
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
class MessageStore(object):
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


# def start(cfg):
#     rundir = cfg['run_dir']
#     # FIXME: shouldn't be assert
#     assert (os.access(rundir, os.R_OK|os.W_OK|os.X_OK)
#             and os.path.isdir(rundir))

# def connect_to_remotes(cfg):
#     remotes = []
#     for remote in cfg['remotes']:
#         # TODO: handle remote unavailability
#         remotes.append(remote_connect(remote))

# def remote_connect(remote):
#     assert False and "unimplemented"
#     pass
