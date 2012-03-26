# coding=UTF-8
import gevent
from gevent import socket
from beaconpush.response import createEvent

verbose = False

class BeaconpushSocket(object):
    def __init__(self, host, port):
        self.addr = host, port
        self.sock = None
        self.buffer = ""
        self._readTask = None
        self._responseQueue = gevent.queue.Queue()

    def log(self, msg):
        if verbose:
            print self.addr, msg

    def connect(self):
        self.sock = socket.create_connection(self.addr)
        return self

    def disconnect(self):
        try:
            self.sock.close()
        except Exception:
            pass
        self.sock = None

    def _reader(self):
        buffer = ""
        while 1:
            try:
                data = self.sock.recv(1024)
                if not data:
                    break

                buffer += data
            except:
                # Usually exception is thrown when the socket is in a error state. Wipe it and start over.
                break

            lines = buffer.split("\r\n")
            buffer = lines.pop() # Last line is always incomplete (in most cases an empty string)

            for line in lines:
                try:
                    bp_event = createEvent(line.split('\t'))
                    self.log(bp_event)
                    self._responseQueue.put_nowait(bp_event)
                except Exception, e:
                    continue

    def getResponse(self):
        if self._readTask is None:
            self._readTask = gevent.spawn(self._reader)
        return self._responseQueue.get()





