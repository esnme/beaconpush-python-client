# coding=UTF-8
from gevent import socket
from gevent import queue
import time, gevent, logging

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport.TTransport import TFramedTransport

from beaconpush.generated_thrift import BackendService

logger = logging.getLogger("beaconpush.socketpool")

class SocketPool(object):
    def __init__(self, addr, max_sockets=40, idle_timeout=5.0, connect_timeout=10.0):
        self.addr = addr
        self.max_sockets = max_sockets
        self.idle_timeout = idle_timeout
        self.connected_sockets = 0
        self.free_sockets = queue.Queue()
        self.timeout = connect_timeout
        self.last_release = {}
        self.failed_state = False
        gevent.spawn(self.disconnect_idle_sockets_task)
        gevent.spawn(self.check_failed_connections)

    def check_failed_connections(self):
        while True:
            # We only process if we are in the failed state
            gevent.sleep(5)
            if not self.failed_state:
                continue

            # If we happen to be in faled stats, try making a connection to see if the service is up
            try:
                sock = socket.create_connection(self.addr, timeout=1)
                sock.close()
                self.failed_state = False
            except:
                pass

    def disconnect_idle_sockets_task(self):
        while 1:
            gevent.sleep(2)

            try:
                sock = self.free_sockets.get_nowait()
            except:
                continue

            released_at = self.last_release.get(sock, None)
            if released_at and (time.time() - released_at) > self.idle_timeout:
                self.release_socket(sock, idle=True)
            else:
                self.free_sockets.put_nowait(sock)

    def _connect_socket(self, timeout=None):
        if self.failed_state:
            raise Exception("Unable to establish connection since server %s:%s is in failed state." % self.addr)

        self.connected_sockets += 1 # This must be done before creating connection to avoid races
        try:
            host, port = self.addr

            sock = TSocket.TSocket(host, port)
            protocol = TBinaryProtocol(TFramedTransport(sock))
            client = BackendService.Client(protocol)
            sock.open()

            return client
        except:
            self.connected_sockets -= 1
            self.failed_state = True
            raise

    def acquire_socket(self, timeout=None):
        if self.free_sockets.empty() and self.connected_sockets < self.max_sockets:
            sock = self._connect_socket(timeout)
        else:
            try:
                sock = self.free_sockets.get(timeout=self.timeout)
            except Exception, e:
                host, port = self.addr
                logging.warn("Could not aquire socket for %s:%s, i waited for %s. Qsize: %s, Sockets: %s/%s." % (host, port, self.timeout, self.free_sockets.qsize(), self.connected_sockets, self.max_sockets))
                raise

        return sock

    def release_socket(self, sock, failed=False, idle=False):
        if failed or idle:
            try:
                del self.last_release[sock]
            except:
                pass

            self.connected_sockets -= 1
            try:
                sock.close()
            except:
                pass

            if failed:
                logger.critical("Exception occurred, disposing connection to %r" % (self.addr, ))
            if idle:
                logger.debug("Disconnected idle connection to %r, %d sockets still connected" % (self.addr, self.connected_sockets))
        else:
            self.last_release[sock] = time.time()
            self.free_sockets.put_nowait(sock)

class MultiHostSocketPool(SocketPool):
    def __init__(self, *args, **kwargs):
        self.create_pool = lambda addr: SocketPool(addr, *args, **kwargs)
        self.pools = {}

    def acquire_socket(self, addr, timeout=10.0):
        if not addr in self.pools:
            self.pools[addr] = self.create_pool(addr)

        return self.pools[addr].acquire_socket(timeout)

    def release_socket(self, addr, sock, failed=False):
        self.pools[addr].release_socket(sock, failed)
