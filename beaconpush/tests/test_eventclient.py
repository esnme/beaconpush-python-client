import socket; socket.setdefaulttimeout(1)
import logging
import unittest
import gevent

from beaconpush import EventClient
from beaconpush.eventclient import Event
from beaconpush.tests import MockedEventServer

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s')

EVENT_1 = Event('my-web-site', 'USER_CONNECTED_TO_CHANNEL', 'hector', '@hector')
EVENT_2 = Event('planet', 'USER_DISCONNECTED_TO_CHANNEL', 'elvis', '@elvis')
EVENT_3 = Event('default', 'USER_DISCONNECTED_TO_CHANNEL', 'frank', '@frank')

class EventClientTest(unittest.TestCase):
    def setUp(self):
        self.event_server = MockedEventServer()
        self.port = self.event_server.start()
        self.events = gevent.queue.Queue()
        self.c = EventClient('127.0.0.1', port=self.port, handle=lambda e: self.events.put_nowait(e))
        self.delay = 0

    def slow_handler(self, event):
        # A small delay to guarantee execution order when the greenlet is woken up.
        # Having multiple greenlets sleeping on the same delay makes wake up order a bit random.
        self.delay += 0.05
        gevent.sleep(0.5 + self.delay)
        self.events.put_nowait(event)

    def send(self, event):
        self.event_server.send(str(event))

    def assertReceived(self, event, timeout):
        self.assertEqual(str(self.events.get(timeout=timeout)), str(event))

    def test_connect(self):
        self.c.connect()
        self.send(EVENT_1)
        self.send(EVENT_2)
        self.assertReceived(EVENT_1, 0.2)
        self.assertReceived(EVENT_2, 0.2)

    def test_send_lots_of_messages(self):
        self.c.connect()
        for i in xrange(8000):
            self.event_server.send(str(EVENT_1))

        for i in xrange(5000):
            self.assertEqual(str(self.events.get(timeout=10)), str(EVENT_1))

    def test_disconnect(self):
        self.c.connect()
        self.c.disconnect()

    def test_disconnect_without_connect(self):
        c = EventClient('127.0.0.1', port=12345, handle=lambda x: x)
        c.disconnect()
        c.disconnect() # Test double disconnect as well

    def test_spawn_inline(self):
        """Tests that only a single, slow greenlet is processing events."""
        self.c = EventClient('127.0.0.1', port=self.port, handle=self.slow_handler, spawn=None)
        self.c.connect()
        self.send(EVENT_1)
        self.send(EVENT_2)
        self.send(EVENT_3)
        self.assertReceived(EVENT_1, 1)
        self.assertReceived(EVENT_2, 1)
        def timeout():
            # 0.2 sec is not enough for the slow handler to process, so it should fail.
            self.assertReceived(EVENT_3, 0.2)
        self.assertRaises(gevent.queue.Empty, timeout)

        # Pickup the last event given that we wait enough.
        self.assertReceived(EVENT_3, 1)

    def test_spawn_pool(self):
        """Tests that only a pool of greenlets are processing events."""
        self.c = EventClient('127.0.0.1', port=self.port, handle=self.slow_handler, spawn=2)
        self.c.connect()
        self.send(EVENT_1)
        self.send(EVENT_2)
        self.send(EVENT_3)
        self.assertReceived(EVENT_1, 1)
        self.assertReceived(EVENT_2, 0.1) # Since the pool process in parallel, this should be immediately available.
        def timeout():
            # Since we use a pool of 2, it will need to wait for the third message. Timeout of 0 should fail.
            self.assertReceived(EVENT_3, 0)
        self.assertRaises(gevent.queue.Empty, timeout)

        # Pickup the last event given that we wait enough.
        self.assertReceived(EVENT_3, 1)

    def test_spawn_default(self):
        """Tests that a new greenlet is spawned for each event to be processed."""
        self.c = EventClient('127.0.0.1', port=self.port, handle=self.slow_handler, spawn='default')
        self.c.connect()
        self.send(EVENT_1)
        self.send(EVENT_2)
        self.send(EVENT_3)
        self.assertReceived(EVENT_1, 1)
        self.assertReceived(EVENT_2, 0.1)
        self.assertReceived(EVENT_3, 0.1)

if __name__ == '__main__':
    unittest.main()