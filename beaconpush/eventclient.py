# coding=UTF-8
from beaconpush.socket import BeaconpushSocket
import logging
import gevent
import gevent.pool
logging = logging.getLogger("beaconpush.eventclient")


class EventReader(BeaconpushSocket):
    def run(self, callback):
        pool = gevent.pool.Pool(500)
        i = 0
        while True:
            i += 1
            if i % 100 == 0: logging.debug("Beaconpush Events: %d" % i)
            try:
                event = gevent.with_timeout(30, self.getResponse)
            except gevent.Timeout:
                event = True
                continue

            if event is None: # Something is wrong, reconnect
                logging.warn("Event client got None, breaking out of read loop")
                break
            if pool.full():
                logging.warn("Discarding event since the event pool is full")
            else:
                pool.spawn(lambda : callback(event))

class EventClient():
    host = None
    port = 6051

    def __init__(self, host, port=6051):
        self.host = host
        self.port = port

    def connect(self, event_handler):
        backoff = 1
        while True:
            logging.info("Beaconpush Event Client connecting to %s:%d" % (self.host, self.port))
            event_reader = EventReader(self.host, self.port)
            try:
                event_reader.connect()
            except Exception:
                logging.warn("Beaconpush Event Client failed to connect, make sure Beaconpush server is up and running at %s:%d" % (self.host, self.port))
            else:
                logging.info("Beaconpush Event Client connected to %s:%d" % (self.host, self.port))
                backoff = 1 # Reset backoff upon successful connection
                event_reader.run(event_handler)

            # If we get here, the event client has failed/disconnected
            try:
                event_reader.disconnect()
            except Exception:
                logging.warn("Disconnected failed.")

            logging.warn("Beaconpush Event Client disconnected. Waiting %s seconds before reconnecting" % (backoff, ))
            gevent.sleep(backoff)
            backoff += 5
            if backoff > 60:
                backoff = 60