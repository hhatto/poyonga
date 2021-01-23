import io
import json
import struct
import unittest

try:
    from mock import patch, Mock
except ImportError:
    from unittest.mock import patch, Mock
from poyonga import Groonga, GroongaResult
from poyonga.client import get_send_data_for_gqtp, convert_gqtp_result_data, GQTP_HEADER_SIZE
from poyonga.const import GRN_STATUS_UNSUPPORTED_COMMAND_VERSION

try:
    import pyarrow as pa
except ImportError:
    pa = None


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

    @patch("poyonga.client.urlopen")
    def test_load_list(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], 1]"]
        mock_urlopen.return_value = m
        ret = self.g.call("load",
                          table="Site",
                          values=[{"_key": "groonga.org"}])
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]
        self.assertEqual([{"_key": "groonga.org"}],
                         json.loads(request.data))
        self.assertEqual({"Content-type": "application/json"},
                         request.headers)

    @patch("poyonga.client.urlopen")
    def test_load_json(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], 1]"]
        mock_urlopen.return_value = m
        ret = self.g.call("load",
                          table="Site",
                          values=json.dumps([{"_key": "groonga.org"}]))
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]
        self.assertEqual([{"_key": "groonga.org"}],
                         json.loads(request.data))
        self.assertEqual({"Content-type": "application/json"},
                         request.headers)

    @unittest.skipUnless(pa, "require pyarrow")
    @patch("poyonga.client.urlopen")
    def test_load_apache_arrow(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], 1]"]
        mock_urlopen.return_value = m
        data = [pa.array(["groonga.org"])]
        batch = pa.record_batch(data, names=["_key"])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        ret = self.g.call(
            "load",
            table="Site",
            values=sink.getvalue().to_pybytes(),
            input_type="apache-arrow",
        )
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]

        reader = pa.ipc.open_stream(request.data)
        batches = [b for b in reader]
        self.assertEqual({"_key": ["groonga.org"]}, batches[0].to_pydict())
        self.assertEqual({"Content-type": "application/x-apache-arrow-streaming"},
                         request.headers)

    @patch("poyonga.client.urlopen")
    def test_load_io(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], 1]"]
        mock_urlopen.return_value = m
        json_values = json.dumps([{"_key": "groonga.org"}])
        ret = self.g.call("load",
                          table="Site",
                          values=io.BytesIO(json_values.encode()))
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]
        self.assertEqual([{"_key": "groonga.org"}],
                         json.loads(request.data.read()))
        self.assertEqual({"Content-type": "application/json"},
                         request.headers)

    @unittest.skipUnless(pa, "require pyarrow")
    @patch("poyonga.client.urlopen")
    def test_select_apache_arrow(self, mock_urlopen):
        m = Mock()
        metadata_fields = [
            pa.field("return_code", pa.int32()),
            pa.field("start_time", pa.timestamp("ns")),
            pa.field("elapsed_time", pa.float64()),
        ]
        metadata_metadata = {
            "GROONGA:data_type": "metadata",
        }
        metadata_schema = pa.schema(metadata_fields, metadata_metadata)
        sec_to_ns = 1_000_000_000
        metadata = [
            [0],
            [int(1337566253.89858 * sec_to_ns)],
            [0.000354],
        ]
        metadata_record_batch = pa.record_batch(metadata,
                                                schema=metadata_schema)
        result_set_fields = [
            pa.field("name", pa.string()),
        ]
        result_set_metadata = {
            "GROONGA:n_hits": "29"
        }
        result_set_schema = pa.schema(result_set_fields, result_set_metadata)
        result_set = [
            ["Groonga", "poyonga"],
        ]
        result_set_record_batch = pa.record_batch(result_set,
                                                  schema=result_set_schema)
        output = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(output, metadata_schema) as writer:
            writer.write(metadata_record_batch)
        with pa.RecordBatchStreamWriter(output, result_set_schema) as writer:
            writer.write(result_set_record_batch)
        m.read.side_effect = [output.getvalue().to_pybytes()]
        m.headers = {
            "content-type": "application/x-apache-arrow-streaming",
        }
        mock_urlopen.return_value = m
        ret = self.g.call("select",
                          command_version="3",
                          table="Site",
                          output_type="apache-arrow")
        self.assertEqual(ret.status, 0)
        self.assertEqual(ret.start_time, 1337566253.89858)
        self.assertEqual(ret.elapsed, 0.000354)
        self.assertEqual(ret.hit_num, 29)
        self.assertEqual(ret.items,
                         pa.Table.from_batches([result_set_record_batch]))


class PoyongaGQTPTestCase(unittest.TestCase):
    def setUp(self):
        self.g = Groonga(protocol="gqtp")

    @patch("poyonga.client.socket.socket")
    def test_json_result_with_gqtp(self, mock_socket):
        m = Mock()
        _data = b"{}"
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
        self.assertEqual(ord("\xc7"), d[0])
        (size,) = struct.unpack("!I", d[8:12])
        self.assertEqual(6, size)

    def test_get_send_data_with_args(self):
        kwargs = {"table": "Site"}
        d = get_send_data_for_gqtp("select", **kwargs)
        self.assertEqual(45, len(d))
        self.assertEqual(ord("\xc7"), d[0])
        # check body length
        self.assertEqual(21, d[11])

        body = d[GQTP_HEADER_SIZE:]
        (size,) = struct.unpack("!I", d[8:12])
        self.assertEqual(len(body), size)

    def test_get_send_data_with_args_and_two_bytes(self):
        kwargs = {"table": "Sitは"}
        d = get_send_data_for_gqtp("select", **kwargs)
        self.assertEqual(47, len(d))
        self.assertEqual(ord("\xc7"), d[0])
        # check body length
        self.assertEqual(23, d[11])

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
