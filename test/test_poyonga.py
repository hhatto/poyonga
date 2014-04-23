import sys
import struct
import unittest
try:
    from mock import patch, Mock
except ImportError:
    from unittest.mock import patch, Mock
from poyonga import Groonga, GroongaResult


class PoyongaHTTPTestCase(unittest.TestCase):

    def setUp(self):
        self.g = Groonga()

    @patch('poyonga.client.urlopen')
    def test_json_result_with_http(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ['[[0, 1337566253.89858, 0.000354], {}]']
        mock_urlopen.return_value = m
        ret = self.g.call('status')
        self.assertEqual(type(ret), GroongaResult)
        self.assertEqual(ret.status, 0)


class PoyongaGQTPTestCase(unittest.TestCase):

    def setUp(self):
        self.g = Groonga(protocol='gqtp')

    @patch('poyonga.client.socket.socket')
    def test_json_result_with_gqtp(self, mock_socket):
        m = Mock()
        if sys.version_info[0] == 3:
            _data = b"{}"
        else:
            _data = "{}"
        _proto, _qtype, _keylen, _level, _flags, _status, _size, _opaque, _cas = \
                0xc7, 0x02, 0, 0, 0, 0, 2, 0, 0
        packdata = struct.pack("!BBHBBHIIQ2s",
                _proto, _qtype, _keylen, _level, _flags, _status, _size, _opaque, _cas, _data)
        m.recv.return_value = packdata
        mock_socket.return_value = m
        ret = self.g.call('status')
        self.assertEqual(type(ret), GroongaResult)
        self.assertEqual(ret.status, 0)


if __name__ == '__main__':
    unittest.main()
