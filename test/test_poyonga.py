import io
import json
import struct
import unittest
from unittest.mock import Mock, patch

from poyonga import Groonga, GroongaResult
from poyonga.client import (
    GQTP_HEADER_SIZE,
    convert_gqtp_result_data,
    get_send_data_for_gqtp,
)
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
        ret = self.g.call("load", table="Site", values=[{"_key": "groonga.org"}])
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]
        self.assertEqual([{"_key": "groonga.org"}], json.loads(request.data))
        self.assertEqual({"Content-type": "application/json"}, request.headers)

    @patch("poyonga.client.urlopen")
    def test_load_json(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], 1]"]
        mock_urlopen.return_value = m
        ret = self.g.call("load", table="Site", values=json.dumps([{"_key": "groonga.org"}]))
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]
        self.assertEqual([{"_key": "groonga.org"}], json.loads(request.data))
        self.assertEqual({"Content-type": "application/json"}, request.headers)

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
        batches = list(reader)
        self.assertEqual({"_key": ["groonga.org"]}, batches[0].to_pydict())
        self.assertEqual({"Content-type": "application/x-apache-arrow-streaming"}, request.headers)

    @patch("poyonga.client.urlopen")
    def test_load_io(self, mock_urlopen):
        m = Mock()
        m.read.side_effect = ["[[0, 1337566253.89858, 0.000354], 1]"]
        mock_urlopen.return_value = m
        json_values = json.dumps([{"_key": "groonga.org"}])
        ret = self.g.call("load", table="Site", values=io.BytesIO(json_values.encode()))
        self.assertEqual(ret.body, 1)
        request = mock_urlopen.call_args[0][0]
        self.assertEqual([{"_key": "groonga.org"}], json.loads(request.data.read()))
        self.assertEqual({"Content-type": "application/json"}, request.headers)

    @unittest.skipUnless(pa, "require pyarrow")
    @patch("poyonga.client.urlopen")
    def test_error_apache_arrow(self, mock_urlopen):
        m = Mock()
        metadata_fields = [
            pa.field("return_code", pa.int32()),
            pa.field("start_time", pa.timestamp("ns")),
            pa.field("elapsed_time", pa.float64()),
            pa.field("error_message", pa.string()),
            pa.field("error_file", pa.string()),
            pa.field("error_line", pa.uint32()),
            pa.field("error_function", pa.string()),
            pa.field("error_input_file", pa.string()),
            pa.field("error_input_line", pa.int32()),
            pa.field("error_input_command", pa.string()),
        ]
        metadata_metadata = {
            "GROONGA:data_type": "metadata",
        }
        metadata_schema = pa.schema(metadata_fields, metadata_metadata)
        sec_to_ns = 1_000_000_000
        metadata = [
            [-63],
            [int(1337566253.89858 * sec_to_ns)],
            [0.000354],
            ["Syntax error: <nonexistent||>"],
            ["grn_ecmascript.lemon"],
            [29],
            ["yy_syntax_error"],
            [None],
            [None],
            [None],
        ]
        metadata_record_batch = pa.record_batch(metadata, schema=metadata_schema)
        output = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(output, metadata_schema) as writer:
            writer.write(metadata_record_batch)
        m.read.side_effect = [output.getvalue().to_pybytes()]
        m.headers = {
            "content-type": "application/x-apache-arrow-streaming",
        }
        mock_urlopen.return_value = m
        ret = self.g.call("select", command_version="3", table="Site", filter="nonexistent", output_type="apache-arrow")
        self.assertEqual(ret.status, -63)
        self.assertEqual(ret.start_time, 1337566253.89858)
        self.assertEqual(ret.elapsed, 0.000354)
        self.assertEqual(ret.body, "Syntax error: <nonexistent||>")
        self.assertEqual(ret.hit_num, -1)
        self.assertEqual(ret.items, None)

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
        metadata_record_batch = pa.record_batch(metadata, schema=metadata_schema)
        result_set_fields = [
            pa.field("name", pa.string()),
        ]
        result_set_metadata = {"GROONGA:n_hits": "29"}
        result_set_schema = pa.schema(result_set_fields, result_set_metadata)
        result_set = [
            ["Groonga", "poyonga"],
        ]
        result_set_record_batch = pa.record_batch(result_set, schema=result_set_schema)
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
        ret = self.g.call("select", command_version="3", table="Site", output_type="apache-arrow")
        self.assertEqual(ret.status, 0)
        self.assertEqual(ret.start_time, 1337566253.89858)
        self.assertEqual(ret.elapsed, 0.000354)
        self.assertEqual(ret.hit_num, 29)
        self.assertEqual(ret.items, pa.Table.from_batches([result_set_record_batch]))

    @patch("poyonga.client.urlopen")
    def test_select_json_trace_log(self, mock_urlopen):
        m = Mock()
        response = {
            "header": {
                "return_code": 0,
                "start_time": 1337566253.89858,
                "elapsed_time": 0.000354,
            },
            "trace_log": {
                "columns": [
                    {"name": "depth"},
                    {"name": "sequence"},
                    {"name": "name"},
                    {"name": "value"},
                    {"name": "elapsed_time"},
                ],
                "logs": [
                    [1, 0, "ii.select.input", "Thas", 0],
                    [1, 1, "ii.select.operator", "or", 1],
                    [2, 0, "ii.select.exact.n_hits", 0, 2],
                    [2, 0, "ii.select.fuzzy.input", "Thas", 3],
                    [2, 1, "ii.select.fuzzy.input.actual", "that", 4],
                    [2, 2, "ii.select.fuzzy.input.actual", "this", 5],
                    [2, 3, "ii.select.fuzzy.n_hits", 2, 6],
                    [1, 2, "ii.select.n_hits", 2, 7],
                    [1, 0, "ii.select.input", "ere", 8],
                    [1, 1, "ii.select.operator", "or", 9],
                    [2, 0, "ii.select.exact.n_hits", 2, 10],
                    [1, 2, "ii.select.n_hits", 2, 11],
                ],
            },
            "body": {
                "n_hits": 2,
                "columns": [{"name": "content", "type": "ShortText"}, {"name": "_score", "type": "Float"}],
                "records": [["This is a pen", 1.0], ["That is a pen", 1.0]],
            },
        }
        m.read.side_effect = [json.dumps(response)]
        mock_urlopen.return_value = m
        ret = self.g.call("select", command_version="3", table="Site", output_trace_log="yes")
        self.assertEqual(ret.status, 0)
        self.assertEqual(ret.start_time, 1337566253.89858)
        self.assertEqual(ret.elapsed, 0.000354)
        self.assertEqual(ret.hit_num, 2)
        trace_log_column_names = ["depth", "sequence", "name", "value", "elapsed_time"]
        self.assertEqual(
            ret.trace_logs, [dict(zip(trace_log_column_names, log)) for log in response["trace_log"]["logs"]]
        )
        record_column_names = ["content", "_score"]
        self.assertEqual(ret.items, [dict(zip(record_column_names, record)) for record in response["body"]["records"]])

    @unittest.skipUnless(pa, "require pyarrow")
    @patch("poyonga.client.urlopen")
    def test_select_apache_arrow_trace_log(self, mock_urlopen):
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
        metadata_record_batch = pa.record_batch(metadata, schema=metadata_schema)

        value_type = pa.dense_union([pa.field("0", pa.uint32()), pa.field("1", pa.string())])
        value_type_dictionary = {
            int: 0,
            str: 1,
        }
        trace_log_fields = [
            pa.field("depth", pa.uint16()),
            pa.field("sequence", pa.uint16()),
            pa.field("name", pa.string()),
            pa.field("value", value_type),
            pa.field("elapsed_time", pa.uint64()),
        ]
        trace_log_metadata = {
            "GROONGA:data_type": "trace_log",
        }
        trace_log_schema = pa.schema(trace_log_fields, trace_log_metadata)
        # Row-based for easy to maintain
        trace_logs = [
            [1, 0, "ii.select.input", "Thas", 0],
            [1, 1, "ii.select.operator", "or", 1],
            [2, 0, "ii.select.exact.n_hits", 0, 2],
            [2, 0, "ii.select.fuzzy.input", "Thas", 3],
            [2, 1, "ii.select.fuzzy.input.actual", "that", 4],
            [2, 2, "ii.select.fuzzy.input.actual", "this", 5],
            [2, 3, "ii.select.fuzzy.n_hits", 2, 6],
            [1, 2, "ii.select.n_hits", 2, 7],
            [1, 0, "ii.select.input", "ere", 8],
            [1, 1, "ii.select.operator", "or", 9],
            [2, 0, "ii.select.exact.n_hits", 2, 10],
            [1, 2, "ii.select.n_hits", 2, 11],
        ]
        # Column-based for PyArrow
        trace_logs = list(zip(*trace_logs))
        # value: Row Python values to union array
        values = trace_logs[3]
        value_types = []
        value_offsets = []
        value_offset_dictionary = {
            int: 0,
            str: 0,
        }
        value_child_dictionary = {
            int: [],
            str: [],
        }
        for value in values:
            value_types.append(value_type_dictionary[type(value)])
            value_offsets.append(value_offset_dictionary[type(value)])
            value_offset_dictionary[type(value)] += 1
            value_child_dictionary[type(value)].append(value)
        value_children = [
            pa.array(child, type=value_type[i].type) for i, child in enumerate(value_child_dictionary.values())
        ]
        value_type_field_names = [value_type.field(i).name for i in range(value_type.num_fields)]
        trace_logs[3] = pa.UnionArray.from_dense(
            pa.array(value_types, type=pa.int8()),
            pa.array(value_offsets, type=pa.int32()),
            value_children,
            value_type_field_names,
            value_type.type_codes,
        )
        trace_log_record_batch = pa.record_batch(trace_logs, schema=trace_log_schema)

        result_set_fields = [
            pa.field("content", pa.string()),
        ]
        result_set_metadata = {"GROONGA:n_hits": "2"}
        result_set_schema = pa.schema(result_set_fields, result_set_metadata)
        result_set = [
            ["This is a pen", "That is a pen"],
        ]
        result_set_record_batch = pa.record_batch(result_set, schema=result_set_schema)
        output = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(output, metadata_schema) as writer:
            writer.write(metadata_record_batch)
        with pa.RecordBatchStreamWriter(output, trace_log_schema) as writer:
            writer.write(trace_log_record_batch)
        with pa.RecordBatchStreamWriter(output, result_set_schema) as writer:
            writer.write(result_set_record_batch)
        m.read.side_effect = [output.getvalue().to_pybytes()]
        m.headers = {
            "content-type": "application/x-apache-arrow-streaming",
        }
        mock_urlopen.return_value = m
        ret = self.g.call(
            "select", command_version="3", table="Site", output_type="apache-arrow", output_trace_log="yes"
        )
        self.assertEqual(ret.status, 0)
        self.assertEqual(ret.start_time, 1337566253.89858)
        self.assertEqual(ret.elapsed, 0.000354)
        self.assertEqual(ret.hit_num, 2)
        self.assertEqual(ret.trace_logs, pa.Table.from_batches([trace_log_record_batch]))
        self.assertEqual(ret.items, pa.Table.from_batches([result_set_record_batch]))


class PoyongaHTTPSTestCase(unittest.TestCase):
    def setUp(self):
        self.g = Groonga(protocol="https")

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
        self.assertEqual(f"<GroongaResult status={status} start_time={start_time} elapsed={elapsed}>", str(result))


if __name__ == "__main__":
    unittest.main()
