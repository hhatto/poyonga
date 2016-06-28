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
                 encoding='utf-8', prefix_path='/d/'):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.encoding = encoding
        self.prefix_path = prefix_path

    def _usec2nsec(self, nsec):
        return nsec * (1000000000 / 1000000)

    def _clock_gettime(self):
        ret = self._TimeSpec()
        if hasattr(self.LIBRT, 'clock_gettime'):
            timespec = self.CTimeSpec()
            # 0: CLOCK_REALTIME
            self.LIBRT.clock_gettime(0, pointer(timespec))
            ret.tv_sec = timespec.tv_sec
            ret.tv_nsec = timespec.tv_nsec
        else:   # MacOSX and other environment
            timespec = self.CTimeval()
            self.LIBC.gettimeofday(pointer(timespec), None)
            ret.tv_sec = timespec.tv_sec
            ret.tv_nsec = self._usec2nsec(timespec.tv_usec)
        return ret

    def _get_send_data_for_gqtp(self, cmd, **kwargs):
        """create cmd & send data to groonga"""
        _cmd = cmd
        _cmd_arg = "".join(
            [" --%s '%s'" % (d, str(kwargs[d]).replace("'", r"\'")) for d in kwargs])
        _cmd = _cmd + _cmd_arg
        _cmd_str = "%08x" % len(_cmd)
        if sys.version_info[0] == 3:
            exec("_cmd_len = b\"\\x%02s\\x%02s\\x%02s\\x%02s\"" % (
                _cmd_str[:2], _cmd_str[2:4], _cmd_str[4:6], _cmd_str[6:]), globals())
            _header = b"".join([b"\xc7", b"\x00" * 7, _cmd_len, b"\x00" * 12])
        else:
            exec("_cmd_len = \"\\x%02s\\x%02s\\x%02s\\x%02s\"" % (
                _cmd_str[:2], _cmd_str[2:4], _cmd_str[4:6], _cmd_str[6:]))
            _header = "".join(["\xc7", "\x00" * 7, _cmd_len, "\x00" * 12])
        if sys.version_info[0] == 3:
            _send_data = _header + _cmd.encode()
        else:
            _send_data = _header + _cmd
        return _send_data

    def _convert_gqtp_result_data(self, _start, _end, status, raw_data):
        # struct result data
        diff_time = (_end.tv_sec + _end.tv_nsec / 1000000000.) - \
                    (_start.tv_sec + _start.tv_nsec / 1000000000.)
        if status != 0:
            status -= 65536
            body = "\"\",[[\"\",\"\",0]]"
            _data = "[[%d,%d.%d,%lf,%s]]" % (
                    status, _start.tv_sec, _start.tv_nsec, diff_time, body)
        else:
            body = raw_data[GQTP_HEADER_SIZE:]
            if sys.version_info[0] == 3:
                body = body.decode()
            _data = "[[%d,%d.%d,%lf],%s]" % (
                    status, _start.tv_sec, _start.tv_nsec, diff_time, body)
        return _data

    def _call_gqtp(self, cmd, **kwargs):
        # create socket & send data
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        _start = self._clock_gettime()
        s.send(self._get_send_data_for_gqtp(cmd, **kwargs))
        # recv groonga data
        raw_data = s.recv(8192)
        proto, qtype, keylen, level, flags, status, size, opaque, cas \
            = struct.unpack("!BBHBBHIIQ", raw_data[:GQTP_HEADER_SIZE])
        while len(raw_data) < size + GQTP_HEADER_SIZE:
            raw_data += s.recv(8192)
        _end = self._clock_gettime()
        s.close()
        return self._convert_gqtp_result_data(_start, _end, status, raw_data)

    def _call_http(self, cmd, **kwargs):
        domain = [self.protocol, "://", self.host, ":", str(self.port), self.prefix_path]
        url = "".join(domain) + cmd
        if kwargs:
            url = "".join([url, "?", urlencode(kwargs)])
        try:
            _data = urlopen(url).read().decode(self.encoding)
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
