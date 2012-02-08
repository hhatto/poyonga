import sys
if sys.version_info.major == 3:
    from urllib.request import urlopen
    from urllib.error import HTTPError
    from urllib.parse import urlencode
else:
    from urllib2 import urlopen, HTTPError
    from urllib import urlencode
import socket
from ctypes.util import find_library
from ctypes import Structure, pointer, c_long, CDLL
from poyonga.result import GroongaResult, GroongaSelectResult


class Groonga(object):

    LIBRT = CDLL(find_library("rt"))

    class CTimeSpec(Structure):
        _fields_ = [("tv_sec", c_long), ("tv_nsec", c_long)]

    def __init__(self, host='localhost', port=10041, protocol='http',
                 encoding='utf-8'):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.encoding = encoding

    def _call_gqtp(self, cmd, **kwargs):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        _cmd = cmd
        _start = self.CTimeSpec()
        _end = self.CTimeSpec()
        _cmd_arg = "".join([" --%s '%s'" % (d, kwargs[d]) for d in kwargs])
        _cmd = _cmd + _cmd_arg
        _cmd_str = "%08x" % len(_cmd)
        exec("_cmd_len = \"\\x%02s\\x%02s\\x%02s\\x%02s\"" % (
                _cmd_str[:2], _cmd_str[2:4], _cmd_str[4:6], _cmd_str[6:]))
        _header = "".join(["\xc7", "\x00" * 7, _cmd_len, "\x00" * 12])
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
