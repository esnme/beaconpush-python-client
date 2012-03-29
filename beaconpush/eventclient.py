# coding=UTF-8
import gevent
import gevent.pool
from gevent import socket
from gevent.greenlet import Greenlet
import logging
import random

class Event(object):
    def __init__(self, operator_id, name, user_id, channel):
        self.operator_id = operator_id
        self.name = name
        self.user_id = user_id
        self.channel = channel

    def __str__(self):
        return "%s\t%s\t%s\t%s\r\n" % (self.operator_id, self.name, self.user_id, self.channel)

    def __repr__(self):
        return "Event(operator_id=%s, name=%s, user_id=%s, channel=%s)" % (self.operator_id, self.name, self.user_id, self.channel)

class EventClient():
    host = None
    port = 6051
    max_event_size = 1000
    _spawn = Greenlet.spawn

    def __init__(self, host, port=6051, handle=None, spawn=20):
        if handle is None:
            raise TypeError("'handle' must be provided")

        self.host = host
        self.port = port
        self.handle = handle
        self.set_spawn(spawn)
        self.sock = None
        self.logger = logging.getLogger("beaconpush.eventclient.%s:%d" % (host, port))
        self.connect_timeout = 5
        self._event_receiver_task = None
        self.pool = None # Pool used to execute event handlers in

    def set_spawn(self, spawn):
        if spawn == 'default':
            self.pool = None
            self._spawn = self._spawn
        elif hasattr(spawn, 'spawn'):
            self.pool = spawn
            self._spawn = spawn.spawn
        elif isinstance(spawn, (int, long)):
            from gevent.pool import Pool
            self.pool = Pool(spawn)
            self._spawn = self.pool.spawn
        else:
            self.pool = None
            self._spawn = spawn
        if hasattr(self.pool, 'full'):
            self.full = self.pool.full

    def connect(self):
        self.logger.debug("Connecting...")
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
            self.logger.debug("Connected!")
            self.backoff = 0

            if not self._event_receiver_task:
                self._event_receiver_task = gevent.spawn(self._event_receiver)
            gevent.sleep() # Yield for our event receiver
        except socket.error as e:
            self.logger.error("Unable to connect. Reason: '%s'" % e)
            self.reconnect()

    def disconnect(self):
        if not self.sock:
            return

        self.logger.debug("Disconnecting...")
        self.sock.close()
        self.sock = None
        self.logger.debug("Disconnected!")

    def reconnect(self):
        if not self.sock:
            self.logger.debug("Skipping reconnect because disconnect() was explicitly called.")
            return

        self.backoff += random.randint(3, 6) # Add some jitter to timeout to prevent thundering herd
        self.backoff = min(self.backoff, 60)
        self.logger.warn("Reconnecting in %d seconds..." % self.backoff)
        gevent.sleep(self.backoff)
        self.connect()

    def _event_receiver(self):
        self.logger.debug("Event receiver started.")

        buffer = ""
        while self.sock:
            try:
                data = self.sock.recv(1024)
                if not data:
                    self.logger.warn("Unexpected disconnect.")
                    self.reconnect()
                    continue
            except socket.timeout as e:
                # A read timeout occurred, just try reading again
                continue

            buffer += data
            events = buffer.split("\r\n") # Unframe events from buffer
            buffer = events.pop() # Put back any incomplete event, often an empty string

            for raw_event in events:
                args = raw_event.split('\t')
                if not len(args) == 4:
                    self.logger.error("Received event has not 4 arguments. Event '%s'" % line)

                e = Event(*args)
                #self.logger.debug("Received %s" % e)
                self._dispatch_event(e)

        self.logger.debug("Event receiver finished.")
        self._event_receiver_task = None

    def _dispatch_event(self, *args):
        spawn = self._spawn
        if spawn is None:
            self.handle(*args)
        else:
            spawn(self.handle, *args)