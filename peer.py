#!/usr/bin/env python
import asyncore
import bisect
import errno
import json
import optparse
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


# Handles the socket we listen on incoming connections for
class ListenHandler(asyncore.dispatcher):
    def __init__(self, parent, sock=None):
        asyncore.dispatcher.__init__(self, map=parent.socket_map, sock=sock)
        self.parent = parent

    def handle_accept(self):
        print 'handle_accept() called' # TODO: remove debug print
        # TODO: should we try/catch here for EAGAIN/EWOULDBLOCK
        pair = self.accept()
        # handle_accept() shouldn't be called unless we can accept, I think?
        assert pair is not None
        sock, addr = pair
        self.parent.handle_incoming(sock, addr)

    def handle_close(self):
        raise NotImplementedError()  # FIXME


# Handles the IO on a connection to another node
class ConnHandler(PacketDispatcher):
    def __init__(self, parent, **kwargs):
        PacketDispatcher.__init__(self, map=parent.socket_map, **kwargs)
        self.parent = parent
        self.reader_coro = None
        self.writable_ = False

    def readable(self): return self.reader_coro is not None
    def writable(self): return self.writable_
    def mark_writable(self, writable=True): self.writable_ = writable

    def handle_packet(self, packet):
        self.reader_coro.send(packet)

    def handle_write(self):
        self.parent.handle_write(self)

    def handle_close(self):
        raise NotImplementedError()  # FIXME

# TODO: we should be marking these unwritable when we have no messages for them,
# otherwise we'll spin the CPU by waking up all the time in the select() loop.
class IncomingHandler(ConnHandler):
    def __init__(self, parent, sock):
        ConnHandler.__init__(self, parent, sock=sock)
        self.reader_coro = parent.incoming_coro(self)
        self.reader_coro.next()

class OutgoingHandler(ConnHandler):
    def __init__(self, parent):
        ConnHandler.__init__(self, parent)

    def handle_connect(self):
        # start up a reader coro & hook us up to the write-handler
        self.reader_coro = self.parent.outgoing_coro(self)
        self.reader_coro.next()


# The mutable state (as opposed to mostly-static configuration) of a peer node.
class State(Jsonable):
    def __init__(self, vclock, message_store):
        self.vclock = vclock
        self.message_store = message_store

    def to_json(self):
        return {'vclock': self.vclock.to_json(),
                'messages': self.message_store.to_json()}

    @staticmethod
    def from_json(json):
        return State(VClock.from_json(json['vclock']),
                     MessageStore.from_json(json['messages']))


# Dead simple timer class.
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


# deals with IO and shit
# I don't really know what this class' responsibilities are
# it does what it does
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

    def run(self):
        # start listening on listening socket
        listen_socket = ListenHandler(parent=self)
        listen_socket.create_socket(self.listen_address.socket_address_family(),
                                    socket.SOCK_STREAM)
        listen_socket.bind(self.listen_address.socket_address())
        listen_socket.listen(LISTEN_BACKLOG)
        # TODO: remove debug print
        print 'listening on %s' % self.listen_address

        # kickstart the remote-connection process
        self.schedule_remote_connect()

        # wait for events
        while True:
            self.loop()
        # FIXME: deal with shutting down once all open channels have been
        # closed.
        raise NotImplementedError()

    def loop(self):
        print 'loop'
        # TODO: remove debug print
        #print 'socket_map: %s' % self.socket_map
        # print 'going around the loop (%s)' % kwargs
        now = time.time()
        self.timers = [t for t in self.timers if not t.try_fire(now)]
        timeout = reduce(min, (t.left(now) for t in self.timers),
                         ASYNCORE_TIMEOUT_S)
        asyncore.loop(map = self.socket_map, timeout=timeout, count=1)

    def add_timer(self, delay, callback):
        assert delay >= 0
        if delay == 0:
            callback()
        else:
            self.timers.append(Timer(time.time() + delay, callback))

    def try_remote_connect(self):
        assert self.remote_addresses
        address = self.remote_addresses.pop()

        # TODO: remove debug print
        print 'connecting to remote at %s' % address
        sock = OutgoingHandler(self)
        sock.create_socket(address.socket_address_family(), socket.SOCK_STREAM)
        sock.connect(address.socket_address())

        self.schedule_remote_connect()

    def schedule_remote_connect(self):
        # schedule a connection to the next remote
        if self.remote_addresses:
            self.add_timer(REMOTE_CONNECT_DELAY_S, self.try_remote_connect)
        else:
            # TODO: remove debug print
            print 'finished starting connection attempts for remotes'

    # Schedules packets for sending to a list of sockets.
    def send_many(self, socks, packets):
        newly_active = self.queue.enqueue(socks,
                                          [p.serialize() for p in packets])
        for sock in newly_active:
            self.try_sending_to(sock)

    def send_one(self, socks, packet):
        return self.send_many(socks, [packet])

    # handling incoming connections
    def handle_incoming(self, sock, addr):
        # TODO: remove debug print
        print 'accepted incoming connection'
        IncomingHandler(self, sock=sock)

    def incoming_coro(self, sock):
        # TODO: remove debug print
        print 'starting incoming coro for %s' % sock

        # TODO: handle unannounced shutdowns. probably need a try/catch block.
        hello = yield
        # TODO: couldn't the first packet (or ANY packet) be a BYE?
        assert hello.type == PACKET_HELLO    # TODO: error handling
        assert hello.node_type in NODE_TYPES # TODO: error handling
        node_type = hello.node_type

        # this code could be so much cleaner if `yield from` was available.
        if node_type == NODE_PEER:
            peer_id = sock.peer_id = hello.peer_id
            assert peer_id not in self.peers
            self.peers[peer_id] = sock

            # send welcome response and updates for peer
            msgs = self.updates_for(hello.vclock)
            packets = ([PacketWelcome(self.peer_id, self.state.vclock)]
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
            assert sock is self.peers[sock.peer_id]
            del self.peers[sock.peer_id]

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

    def outgoing_coro(self, sock):
        # TODO: remove debug print
        print 'starting outgoing coro for %s' % sock

        # send a hello message
        self.send_one([sock], PacketHelloPeer(self.peer_id, self.state.vclock))

        # wait for a welcome
        welcome = yield
        assert welcome.type == PACKET_WELCOME # TODO: error handling
        sock.peer_id = welcome.peer_id
        self.peers[sock.peer_id] = sock

        # queue up updates for peer according to its vclock
        msgs = self.updates_for(welcome.vclock)
        self.send_many([sock], (PacketMessage(msg) for msg in msgs))

        # accept messages from peer until we're up-to-date
        while True:
            packet = yield
            if packet.type == PACKET_BYE: break
            if packet.type == PACKET_UPTODATE:
                # We don't actually treat the uptodate packet specially. If we
                # were smart, we might wait to connect to the next remote until
                # we got this packet. but this isn't strictly necessary, so for
                # now we don't do it.
                continue
            # TODO: handle bad message types
            assert packet.type == PACKET_MESSAGE
            self.handle_message(packet.message, recvd_from=sock)

        # got bye
        assert sock is self.peers[sock.peer_id]
        del self.peers[sock.peer_id]
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()

    def updates_for(self, vclock):
        msgdict = self.state.message_store.messages_after_vclock(vclock)
        for src, msgs in msgdict.iteritems():
            for m in msgs: yield m

    def handle_message(self, message, recvd_from=None):
        # TODO: remove debug print
        print 'handling message: %s' % message
        # if we haven't already seen the message...
        if self.state.vclock.already_happened(message.source,
                                              message.timestamp):
            return
        # ... then add it to the store, update our vclock, ...
        self.state.vclock.update(message.source, message.timestamp)
        self.state.message_store.add(message)
        # ... and send it on to all our neighboring peers (except the one that
        # sent it to us)
        dests = (x for x in self.peers.values() if x is not recvd_from)
        self.send_one(dests, PacketMessage(message))

    def handle_write(self, sock):
        self.try_sending_to(sock)

    def try_sending_to(self, sock):
        def sender(chunk):
            return sock.send(chunk)
        self.queue.send(sock, sender)
        # we should wait for write events on `sock` if and only if it has more
        # data queued for it.
        sock.mark_writable(self.queue.has_queued_data(sock))

    def shutdown(self): pass


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
    peer.run()

if __name__ == "__main__":
    main(sys.argv[1:])
