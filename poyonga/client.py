import urllib
import urllib2
import socket
from ctypes.util import find_library
from ctypes import Structure, pointer, c_long, CDLL
from poyonga.result import GroongaResult


class Groonga(object):

    LIBRT = CDLL(find_library("rt"))

    class CTimeSpec(Structure):
        _fields_ = [("tv_sec", c_long), ("tv_nsec", c_long)]

    def __init__(self, host='localhost', port=10041, protocol='http'):
        self.host = host
        self.port = port
        self.protocol = protocol

    def _call_gqtp(self, cmd, **kwargs):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        _cmd = cmd
        _start = self.CTimeSpec()
        _end = self.CTimeSpec()
        for d in kwargs:
            _cmd += " --%s %s" % (d, kwargs[d])
        exec "_cmd_id = \"\\x%02x\"" % len(_cmd)
        _header = "".join(["\xc7", "\x00" * 10, _cmd_id, "\x00" * 12])
        self.LIBRT.clock_gettime(0, pointer(_start))     # 0: CLOCK_REALTIME
        s.send(_header + _cmd)
        raw_data = s.recv(8192)
        self.LIBRT.clock_gettime(0, pointer(_end))
        s.close()

        diff_time = (_end.tv_sec + _end.tv_nsec / 1000000000.) - \
                    (_start.tv_sec + _start.tv_nsec / 1000000000.)
        status = ord(raw_data[6]) + (ord(raw_data[7]) << 8)
        if status != 0:
            status -= 65536
            body = "\"\",[[\"\",\"\",0]]"
            _data = "[[%d,%d.%d,%lf,%s]]" % (
                    status, _start.tv_sec, _start.tv_nsec, diff_time, body)
        else:
            body = raw_data[24:]
            _data = "[[%d,%d.%d,%lf],%s]" % (
                    status, _start.tv_sec, _start.tv_nsec, diff_time, body)
        return GroongaResult(_data)

    def _call_http(self, cmd, **kwargs):
        domain = [self.protocol, "://", self.host, ":", str(self.port), "/d/"]
        if kwargs:
            cmd = cmd + "?"
        url = "".join(domain) + cmd + urllib.urlencode(kwargs)
        try:
            _data = urllib2.urlopen(url).read()
        except urllib2.HTTPError, msg:
            _data = msg.read()
        return GroongaResult(_data)

    def call(self, cmd, **kwargs):
        if self.protocol == "http":
            return self._call_http(cmd, **kwargs)
        else:
            return self._call_gqtp(cmd, **kwargs)
