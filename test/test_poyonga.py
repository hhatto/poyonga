# coding: utf-8
import json
import sys
import struct
import unittest

try:
    from mock import patch, Mock
except ImportError:
    from unittest.mock import patch, Mock
from poyonga import Groonga, GroongaResult
from poyonga.client import get_send_data_for_gqtp, convert_gqtp_result_data, GQTP_HEADER_SIZE
from poyonga.const import GRN_STATUS_UNSUPPORTED_COMMAND_VERSION


class PoyongaHTTPTestCase(unittest.TestCase):
    def setUp(self):
        self.g = Groonga()

    @patch("poyonga.client.urlopen")
    def test_json_result_with_http(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], {}]"]
        mock_urlopen.return_value = m
        ret = self.g.call("status")
        self.assertEqual(type(ret), GroongaResult)
        self.assertEqual(ret.status, 0)


class PoyongaGQTPTestCase(unittest.TestCase):
    def setUp(self):
        self.g = Groonga(protocol="gqtp")

    @patch("poyonga.client.socket.socket")
    def test_json_result_with_gqtp(self, mock_socket):
        m = Mock()
        if sys.version_info[0] == 3:
            _data = b"{}"
        else:
            _data = "{}"
        _proto, _qtype, _keylen, _level, _flags, _status, _size, _opaque, _cas = (
            0xC7,
            0x02,
            0,
            0,
            0,
            0,
            2,
            0,
            0,
        )
        packdata = struct.pack(
            "!BBHBBHIIQ2s",
            _proto,
            _qtype,
            _keylen,
            _level,
            _flags,
            _status,
            _size,
            _opaque,
            _cas,
            _data,
        )
        m.recv.return_value = packdata
        mock_socket.return_value = m
        ret = self.g.call("status")
        self.assertEqual(type(ret), GroongaResult)
        self.assertEqual(ret.status, 0)


class PoyongaFunctions(unittest.TestCase):
    def test_get_send_data(self):
        d = get_send_data_for_gqtp("status")
        self.assertEqual(30, len(d))
        if sys.version_info[0] == 3:
            self.assertEqual(ord("\xc7"), d[0])
        else:
            self.assertEqual("\xc7", d[0])
        (size,) = struct.unpack("!I", d[8:12])
        self.assertEqual(6, size)

    def test_get_send_data_with_args(self):
        kwargs = {"table": "Site"}
        d = get_send_data_for_gqtp("select", **kwargs)
        self.assertEqual(45, len(d))
        if sys.version_info[0] == 3:
            self.assertEqual(ord("\xc7"), d[0])
            # check body length
            self.assertEqual(21, d[11])
        else:
            self.assertEqual("\xc7", d[0])
            # check body length
            self.assertEqual("\x15", d[11])

        body = d[GQTP_HEADER_SIZE:]
        (size,) = struct.unpack("!I", d[8:12])
        self.assertEqual(len(body), size)

    def test_get_send_data_with_args_and_two_bytes(self):
        kwargs = {"table": "Sitは"}
        d = get_send_data_for_gqtp("select", **kwargs)
        self.assertEqual(47, len(d))
        if sys.version_info[0] == 3:
            self.assertEqual(ord("\xc7"), d[0])
            # check body length
            self.assertEqual(23, d[11])
        else:
            self.assertEqual("\xc7", d[0])
            # check body length
            self.assertEqual("\x17", d[11])

        body = d[GQTP_HEADER_SIZE:]
        (size,) = struct.unpack("!I", d[8:12])
        self.assertEqual(len(body), size)

    def test_convert_gqtp_result(self):
        grn = Groonga()
        s = grn._clock_gettime()
        e = grn._clock_gettime()
        senddata = get_send_data_for_gqtp("status")
        rawdata = senddata[:GQTP_HEADER_SIZE] + b'{"test": 0}'
        d = convert_gqtp_result_data(s, e, 0, rawdata)
        d = json.loads(d)
        self.assertEqual(d[0][0], 0)

    def test_convert_gqtp_result_fail(self):
        grn = Groonga()
        s = grn._clock_gettime()
        e = grn._clock_gettime()
        senddata = get_send_data_for_gqtp("status")
        rawdata = senddata[:GQTP_HEADER_SIZE] + b'{"test": 0}'
        d = convert_gqtp_result_data(s, e, GRN_STATUS_UNSUPPORTED_COMMAND_VERSION, rawdata)
        d = json.loads(d)
        self.assertEqual(d[0][0], -71)


class PoyongaResult(unittest.TestCase):
    def test_str(self):
        status = 0
        start_time = 0.0
        elapsed = 1.0
        raw_result = [[status, start_time, elapsed], False]
        result = GroongaResult(json.dumps(raw_result))
        self.assertEqual('<GroongaResult ' +
                         f'status={status} ' +
                         f'start_time={start_time} ' +
                         f'elapsed={elapsed}>',
                         str(result))


if __name__ == "__main__":
    unittest.main()
