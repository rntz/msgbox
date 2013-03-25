#!/usr/bin/env python
import asyncore
import errno
import json
import optparse
import signal
import socket
import sys
import time

from msgbox import *

LISTEN_BACKLOG = 5

# We delay approximately this long between connecting to remotes, to allow
# ourselves to sync up with the previous remote we connected to. This is a
# heuristic, not necessary for correctness.
REMOTE_CONNECT_DELAY_S = 2.0

# The timeout to pass to asyncore.loop(). We'll be stuck in a syscall and unable
# to respond to signals for approximately this long, so keep it small to allow
# ctrl-C etc. to work reasonably well.
ASYNCORE_TIMEOUT_S = 1.0

# log levels
LOG_LEVELS = 'debug info warn error'.split()
LOG_LEVEL_NAMES = {i: n for i,n in enumerate(LOG_LEVELS)}
def log_level_name(i): return LOG_LEVEL_NAMES[i]
globals().update({'LOG_' + l.upper(): i for i,l in LOG_LEVEL_NAMES.iteritems()})

# exceptions
# error thrown in a reader coro to indicate connection has closed.
class ClosedError(Exception): pass


# Handles the socket we listen on incoming connections for
class ListenHandler(asyncore.dispatcher):
    def __init__(self, parent, sock=None):
        asyncore.dispatcher.__init__(self, map=parent.socket_map, sock=sock)
        self.parent = parent

    def handle_accept(self):
        # TODO: should we try/catch here for EAGAIN/EWOULDBLOCK
        pair = self.accept()
        # handle_accept() shouldn't be called unless we can accept, I think?
        assert pair is not None
        sock, addr = pair
        self.parent.handle_incoming(sock, addr)

    def handle_close(self):
        print 'FUX: closing listener: %s' % self
        raise NotImplementedError()     # FIXME


# Handles the IO on a connection to another node
class ConnHandler(MessageDispatcher):
    def __init__(self, parent, **kwargs):
        MessageDispatcher.__init__(self, map=parent.socket_map, **kwargs)
        self.parent = parent
        self.reader_coro = None
        self.writable_ = False

    def readable(self): return self.reader_coro is not None
    def writable(self): return self.writable_
    def mark_writable(self, writable=True): self.writable_ = writable

    def handle_message(self, message):
        self.reader_coro.send(message)

    def handle_write(self):
        self.parent.handle_write(self)

    def handle_close(self):
        assert self.reader_coro is not None
        try: self.reader_coro.throw(ClosedError())
        except StopIteration: pass
        else: assert False      # should have raised StopIteration

class IncomingHandler(ConnHandler):
    def __init__(self, parent, sock):
        ConnHandler.__init__(self, parent, sock=sock)
        self.reader_coro = parent.incoming_coro(self)
        self.reader_coro.next()

class OutgoingHandler(ConnHandler):
    def __init__(self, parent):
        ConnHandler.__init__(self, parent)
        # need to be writable to notice that we have connected
        self.mark_writable()

    def handle_connect(self):
        # okay, we've connected, so we go back to not caring whether we're
        # writable. (if messages get queued to us, this will change.)
        self.mark_writable(False)
        # start up a reader coro
        self.reader_coro = self.parent.outgoing_coro(self)
        self.reader_coro.next()

    def handle_error(self):
        if self.reader_coro:
            ConnHandler.handle_error(self)
        # we have no reader_coro; so we were still trying to connect
        if self.reader_coro is None:
            # we're trying to connect
            (typ, value, tb) = sys.exc_info()
            if issubclass(typ, socket.error):
                # connection failed
                self.close()
                self.parent.handle_failed_connect(self, value[0], value[1])
            else:
                ConnHandler.handle_error(self)
        else:
            ConnHandler.handle_error(self)


# The mutable state (as opposed to mostly-static configuration) of a peer node.
class State(Jsonable):
    def __init__(self, vclock, event_store):
        self.vclock = vclock
        self.event_store = event_store

    def to_json(self):
        return {'vclock': self.vclock.to_json(),
                'events': self.event_store.to_json()}

    @staticmethod
    def from_json(json):
        return State(VClock.from_json(json['vclock']),
                     EventStore.from_json(json['events']))


# deals with IO and shit
# I don't really know what this class' responsibilities are
# it does what it does
# TODO: deal with shutting down?
class Peer(object):
    def __init__(self, peer_id, state, listen_address, remote_addresses):
        self.queue = MultiQueue()
        self.socket_map = {}    # for dispatchers
        self.peers = {}
        self.timers = []

        self.peer_id = peer_id
        self.state = state
        self.listen_address = listen_address
        self.remote_addresses = remote_addresses # remotes to connect to

    # ----- Logging -----
    def log(self, level, msg, *args):
        # TODO: real logging
        name = log_level_name(level).upper()
        print >>sys.stderr, '---------- %s: %s' % (name, (msg % args))

    def debug(self, msg, *args): self.log(LOG_DEBUG, msg, *args)
    def info(self, msg, *args): self.log(LOG_INFO, msg, *args)
    def warn(self, msg, *args): self.log(LOG_WARN, msg, *args)

    # ----- Main loop -----
    def run(self):
        # start listening on listening socket
        listen_socket = ListenHandler(parent=self)
        listen_socket.create_socket(self.listen_address.socket_address_family(),
                                    socket.SOCK_STREAM)
        listen_socket.bind(self.listen_address.socket_address())
        listen_socket.listen(LISTEN_BACKLOG)
        self.info('listening on %s', self.listen_address)

        # kickstart the remote-connection process
        if self.remote_addresses:
            self.try_remote_connect()

        # wait for events
        while True: self.loop()

    def loop(self):
        # self.debug('looping')
        assert self.socket_map
        timeout = self.check_timers()
        asyncore.loop(count=1, map = self.socket_map, timeout=timeout)

    def shutdown(self):
        # Shutdown is surprisingly simple: we do nothing. Our open sockets will
        # be closed automatically when the process exits.
        return

    # ----- Timers ----
    def add_timer(self, delay, callback):
        assert delay >= 0
        if delay == 0:
            callback()
        else:
            self.timers.append(Timer(time.time() + delay, callback))

    def check_timers(self):
        now = time.time()
        self.timers = [t for t in self.timers if not t.try_fire(now)]
        return reduce(min, (t.left(now) for t in self.timers),
                      ASYNCORE_TIMEOUT_S)

    # ----- Remote connections ----
    def try_remote_connect(self):
        assert self.remote_addresses
        address = self.remote_addresses.pop()
        self.info('connecting to remote at %r', address)

        sock = OutgoingHandler(self)
        sock.create_socket(address.socket_address_family(), socket.SOCK_STREAM)
        # TODO: error handling (eg. ECONNREFUSED)
        sock.connect(address.socket_address())

        if self.remote_addresses:
            self.add_timer(REMOTE_CONNECT_DELAY_S, self.try_remote_connect)
        else:
            self.info('finished starting connection attempts for remotes')

    # ----- Sending messages -----
    # Schedules messages for sending to a list of sockets.
    def send_many(self, socks, messages):
        newly_active = self.queue.enqueue(socks,
                                          [m.serialize() for m in messages])
        for sock in newly_active:
            self.try_sending_to(sock)

    def send_one(self, socks, message):
        return self.send_many(socks, [message])

    # Sends as much data to a sock as it will accept without blocking
    def try_sending_to(self, sock):
        def sender(chunk):
            return sock.send(chunk)
        self.queue.send(sock, sender)
        # We should wait for write events on `sock` if and only if it has more
        # data queued for it.
        sock.mark_writable(self.queue.has_queued_data(sock))

    # ----- Incoming connections -----
    def handle_incoming(self, sock, addr):
        # TODO: print useful address information
        self.info('accepted incoming connection from %r', addr)
        IncomingHandler(self, sock=sock)

    def incoming_coro(self, sock):
        # TODO: handle unannounced shutdowns. probably need a try/catch block.
        try:
            # TODO: remove debug print
            self.debug('starting incoming coro for %s', sock)

            hello = yield
            assert hello.type == MESSAGE_HELLO    # TODO: error handling
            assert hello.node_type in NODE_TYPES # TODO: error handling
            node_type = hello.node_type

            # this code could be so much cleaner if `yield from` was available.
            if node_type == NODE_PEER:
                self.add_peer(sock, hello.peer_id)

                # send welcome response and updates for peer
                events = self.updates_for(hello.vclock)
                messages = ([MessageWelcome(self.peer_id, self.state.vclock)]
                            + [MessageEvent(e) for e in events]
                            + [MessageUptodate()])
                self.send_many([sock], messages)

                # now, accept events from our peer
                while True:
                    message = yield
                    # TODO: handle bad message types
                    assert message.type == MESSAGE_EVENT
                    self.handle_event(message.event, recvd_from=sock)

            elif node_type == NODE_SENDER:
                # just read events from them.
                while True:
                    message = yield
                    if message.type == MESSAGE_EVENT:
                        self.handle_event(message.event, recvd_from=sock)
                    elif message.type == MESSAGE_GEN_EVENT:
                        self.gen_event(message.data)
                    else:
                        # TODO: handle bad event types
                        assert False # bad message type

            else:
                assert False        # unreachable case
            assert False        # unreachable

        except ClosedError:
            # TODO: better log formatting for sockets
            self.info('incoming socket closed from other side: %s', sock)
            self.disconnect(sock)

    # ----- Outgoing connections -----
    def handle_failed_connect(self, sock, errnum, errmsg):
        # TODO: useful printing of sockets in error messages
        self.warn('outgoing connection on %s failed: %s', sock, errmsg)

    def outgoing_coro(self, sock):
        try:
            # TODO: remove debug print
            self.debug('starting outgoing coro for %s', sock)

            # send a hello message
            self.send_one([sock], MessageHelloPeer(self.peer_id,
                                                   self.state.vclock))

            # wait for a welcome
            welcome = yield
            assert welcome.type == MESSAGE_WELCOME # TODO: error handling
            self.add_peer(sock, welcome.peer_id)

            # queue up updates for peer according to its vclock
            events = self.updates_for(welcome.vclock)
            self.send_many([sock], (MessageEvent(e) for e in events))

            # accept events from peer until we're up-to-date
            while True:
                message = yield
                if message.type == MESSAGE_UPTODATE:
                    # TODO: remove debug print
                    self.debug('up-to-date on outgoing socket')
                    # We don't actually treat the uptodate message specially. If
                    # we were smart, we might wait to connect to the next remote
                    # until we got this message. but this isn't strictly
                    # necessary, so for now we don't do it.
                    continue
                # TODO: handle bad message types
                assert message.type == MESSAGE_EVENT
                self.handle_event(message.event, recvd_from=sock)

        except ClosedError:
            # TODO: better log formatting for sockets
            self.info('outgoing socket closed from other side: %s', sock)
            self.disconnect(sock)

    # ----- Peer utility methods -----
    def add_peer(self, sock, peer_id):
        assert peer_id not in self.peers
        assert getattr(sock, 'peer_id', None) is None
        sock.peer_id = peer_id
        self.peers[peer_id] = sock

    def disconnect(self, sock):
        if hasattr(sock, 'peer_id'):
            assert sock is self.peers[sock.peer_id]
            del self.peers[sock.peer_id]
        sock.close()

    def updates_for(self, vclock):
        evdict = self.state.event_store.events_after_vclock(vclock)
        for src, events in evdict.iteritems():
            for e in events: yield e

    # ----- Event handling -----
    def handle_write(self, sock):
        self.try_sending_to(sock)

    def handle_event(self, event, recvd_from):
        # TODO: remove debug print
        self.debug('handling event: %s', event)
        # if we haven't already seen the event...
        if self.state.vclock.already_happened(event.source, event.timestamp):
            return
        # we should never receive events whose source is _us_ from another peer.
        assert event.source != self.peer_id or recvd_from is None
        # ... then add it to the store, update our vclock, ...
        # TODO: schedule state-save?
        self.state.vclock.update(event.source, event.timestamp)
        self.state.event_store.add(event)
        # ... and send it on to all our neighboring peers (except the one that
        # sent it to us)
        dests = (x for x in self.peers.values() if x is not recvd_from)
        self.send_one(dests, MessageEvent(event))

    def gen_event(self, data):
        source = self.peer_id
        timestamp = 1 + self.state.vclock.time_for(source)
        event = Event(source, timestamp, data)
        self.handle_event(event, recvd_from=None)


# Startup stuff
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


# The main program
def main(args):
    parser = Options()
    (options, args) = parser.parse_args(args)
    assert not args        # we don't take positional args. TODO: error message.
    cfg = parse_config(options)

    # Parse the config into arguments for Peer() constructor.
    def parse_state(filename):
        with open(filename) as f:
            return State.from_json(json.load(f))
    ctors = {'peer_id': lambda x: x,
             'state': parse_state,
             'listen_address': Address.from_json,
             'remote_addresses': lambda x: map(Address.from_json, x)}
    kwargs = {name: ctor(cfg[name]) for name, ctor in ctors.iteritems()}

    peer = Peer(**kwargs)

    # set up signal handlers to shut down the peer and exit main loop
    quitting = [False]
    def quit_handler(signo, frame):
        # TODO?: ideally, should use an atomic-test-and-set on `quitting`, to
        # make our signal handler re-entrant.
        if not quitting[0]:
            # first signal. try to die gracefully.
            quitting[0] = True
            peer.shutdown()
            sys.exit(0)
        else:
            # second signal. die NOW.
            sys.exit(1)

    signal.signal(signal.SIGTERM, quit_handler)
    signal.signal(signal.SIGINT, quit_handler)

    # run main loop
    peer.run()

if __name__ == "__main__":
    try: main(sys.argv[1:])
    except KeyboardInterrupt: sys.exit(1)
