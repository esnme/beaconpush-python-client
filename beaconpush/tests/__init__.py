import sys
import gevent
from gevent.server import StreamServer
import socket

def find_unused_port(family=socket.AF_INET, socktype=socket.SOCK_STREAM):
        """Copied from the gevent tests"""
        tempsock = socket.socket(family, socktype)
        port = bind_port(tempsock)
        tempsock.close()
        del tempsock
        return port

def bind_port(sock, host='', preferred_port=54321):
    """Try to bind the sock to a port.  If we are running multiple
    tests and we don't try multiple ports, the test can fails.  This
    makes the test more robust."""

    import socket, errno

    # Find some random ports that hopefully no one is listening on.
    # Ideally each test would clean up after itself and not continue listening
    # on any ports.  However, this isn't the case.  The last port (0) is
    # a stop-gap that asks the O/S to assign a port.  Whenever the warning
    # message below is printed, the test that is listening on the port should
    # be fixed to close the socket at the end of the test.
    # Another reason why we can't use a port is another process (possibly
    # another instance of the test suite) is using the same port.
    for port in [preferred_port, 9907, 10243, 32999, 0]:
        try:
            sock.bind((host, port))
            if port == 0:
                port = sock.getsockname()[1]
            return port
        except socket.error, (err, msg):
            if err != errno.EADDRINUSE:
                raise
    raise Exception('unable to find port to listen on')


class MockedEventServer(object):
    def start(self):
        port = find_unused_port()
        self.running = True
        self.server = StreamServer(('0.0.0.0', port), self._event_sender)
        self.server.start()
        self.events_to_send = gevent.queue.Queue()
        return port

    def _event_sender(self, sock, address):
        while self.running:
            e = self.events_to_send.get()
            sock.sendall(e)

    def send(self, raw_event):
        self.events_to_send.put_nowait(raw_event)
