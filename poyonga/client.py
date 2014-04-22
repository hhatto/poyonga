import sys
if sys.version_info[0] == 3:
    from urllib.request import urlopen
    from urllib.error import HTTPError
    from urllib.parse import urlencode
else:
    from urllib2 import urlopen, HTTPError
    from urllib import urlencode
import socket
import struct
from ctypes.util import find_library
from ctypes import Structure, pointer, c_long, CDLL
from poyonga.result import GroongaResult, GroongaSelectResult


GQTP_HEADER_SIZE = 24


class Groonga(object):

    LIBRT = CDLL(find_library("rt"))
    LIBC = CDLL(find_library("c"))

    class CTimeSpec(Structure):
        _fields_ = [("tv_sec", c_long), ("tv_nsec", c_long)]

    class CTimeval(Structure):
        _fields_ = [("tv_sec", c_long), ("tv_usec", c_long)]

    class _TimeSpec(object):
        tv_sec = 0.
        tv_nsec = 0.

    def __init__(self, host='localhost', port=10041, protocol='http',
                 encoding='utf-8'):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.encoding = encoding

    def _usec2nsec(self, nsec):
        return nsec * (1000000000 / 1000000)

    def _clock_gettime(self):
        ret = self._TimeSpec()
        if hasattr(self.LIBRT, 'clock_gettime'):
            timespec = self.CTimeSpec()
            self.LIBRT.clock_gettime(0, pointer(timespec))     # 0: CLOCK_REALTIME
            ret.tv_sec = timespec.tv_sec
            ret.tv_nsec = timespec.tv_nsec
        else:   # MacOSX and other environment
            timespec = self.CTimeval()
            self.LIBC.gettimeofday(pointer(timespec), None)
            ret.tv_sec = timespec.tv_sec
            ret.tv_nsec = self._usec2nsec(timespec.tv_usec)
        return ret

    def _call_gqtp(self, cmd, **kwargs):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        _cmd = cmd
        _cmd_arg = "".join([" --%s '%s'" % (d, str(kwargs[d]).replace("'", r"\'")) for d in kwargs])
        _cmd = _cmd + _cmd_arg
        _cmd_str = "%08x" % len(_cmd)
        exec("_cmd_len = \"\\x%02s\\x%02s\\x%02s\\x%02s\"" % (
                _cmd_str[:2], _cmd_str[2:4], _cmd_str[4:6], _cmd_str[6:]))
        _header = "".join(["\xc7", "\x00" * 7, _cmd_len, "\x00" * 12])
        _start = self._clock_gettime()
        s.send(_header + _cmd)
        raw_data = s.recv(8192)
        proto, qtype, keylen, level, flags, status, size, opaque, cas \
                = struct.unpack("!BBHBBHIIQ", raw_data[:GQTP_HEADER_SIZE])
        while len(raw_data) < size + GQTP_HEADER_SIZE:
            raw_data += s.recv(8192)
        _end = self._clock_gettime()
        s.close()

        diff_time = (_end.tv_sec + _end.tv_nsec / 1000000000.) - \
                    (_start.tv_sec + _start.tv_nsec / 1000000000.)
        if status != 0:
            status -= 65536
            body = "\"\",[[\"\",\"\",0]]"
            _data = "[[%d,%d.%d,%lf,%s]]" % (
                    status, _start.tv_sec, _start.tv_nsec, diff_time, body)
        else:
            body = raw_data[24:]
            _data = "[[%d,%d.%d,%lf],%s]" % (
                    status, _start.tv_sec, _start.tv_nsec, diff_time, body)
        return _data

    def _call_http(self, cmd, **kwargs):
        domain = [self.protocol, "://", self.host, ":", str(self.port), "/d/"]
        if kwargs:
            url = "".join(domain) + cmd + "?" + urlencode(kwargs)
        else:
            url = "".join(domain) + cmd
        try:
            _data = urlopen(url).read()
        except HTTPError as msg:
            _data = msg.read()
        return _data

    def call(self, cmd, **kwargs):
        output_type = kwargs.get("output_type")
        if not output_type:
            output_type = "json"
        if self.protocol == "http":
            ret = self._call_http(cmd, **kwargs)
        else:
            ret = self._call_gqtp(cmd, **kwargs)
        if cmd == 'select':
            return GroongaSelectResult(ret, output_type, self.encoding)
        else:
            return GroongaResult(ret, output_type, self.encoding)
