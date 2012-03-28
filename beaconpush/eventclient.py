# coding=UTF-8
import logging
import gevent
import gevent.pool
import random
from gevent import socket

class Event(object):
    def __init__(self, operator_id, name, user_id, channel):
        self.operator_id = operator_id
        self.name = name
        self.user_id = user_id
        self.channel = channel

    def __repr__(self):
        return "Event(operator_id=%s, name=%s, user_id=%s, channel=%s)" % (self.operator_id, self.name, self.user_id, self.channel)

class EventClient():
    host = None
    port = 6051

    def __init__(self, host, event_handler, port=6051):
        self.host = host
        self.port = port
        self.event_handler = event_handler
        self.logger = logging.getLogger("beaconpush.eventclient.%s:%d" % (host, port))
        self.connect_timeout = 5
        self._event_receiver_task = None
        self.events_received = gevent.queue.Queue()
        gevent.spawn(self._event_dispatcher)

    def connect(self):
        self.logger.debug("Connecting...")
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
            self.logger.debug("Connected!")
            self.backoff = 0

            if not self._event_receiver_task:
                self._event_receiver_task = gevent.spawn(self._event_receiver)
        except socket.error as e:
            self.logger.error("Unable to connect. Reason: '%s'" % e)
            self.reconnect()

    def disconnect(self):
        self.logger.debug("Disconnecting...")
        self.sock.close()
        self.sock = None
        self.logger.debug("Disconnected!")

    def reconnect(self):
        self.backoff += random.randint(3, 6) # Add some jitter to timeout to prevent thundering herd
        self.backoff = min(self.backoff, 60)
        self.logger.warn("Reconnecting in %d seconds..." % self.backoff)
        gevent.sleep(self.backoff)
        self.connect()

    def _event_receiver(self):
        self.logger.debug("Event receiver started.")

        buffer = ""
        while True:
            # FIXME: What if we receive a read timeout?
            data = self.sock.recv(1024)
            if not data:
                self.logger.warn("Unexpected disconnect.")
                self.reconnect()
                continue

            buffer += data
            events = buffer.split("\r\n") # Unframe events from buffer
            buffer = events.pop() # Put back any incomplete event, often an empty string

            for raw_event in events:
                args = raw_event.split('\t')
                if not len(args) == 4:
                    self.logger.error("Received event has not 4 arguments. Event '%s'" % line)

                e = Event(*args)
                self.logger.debug("Received %s" % e)
                self.events_received.put_nowait(e)

        self.logger.debug("Event receiver finished.")

    def _event_dispatcher(self):
        # Do we really this? Can't we dispatch inside _event_receiver?
        while True:
            e = self.events_received.get()
            self.event_handler(e)