import csv

try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO
try:
    import orjson as json
except ImportError:
    import json
try:
    import msgpack
except ImportError:
    msgpack = None
try:
    import pyarrow as pa
except ImportError:
    pa = None

from poyonga.const import OutputType


class GroongaResult:
    def __init__(self, data, output_type: OutputType = OutputType.JSON, encoding="utf-8", content_type=None):
        self.raw_result = data
        if output_type == OutputType.TSV or content_type == "text/tab-separated-values":
            # TODO: not implement
            csv.reader(StringIO(data), delimiter="\t")
            raise NotImplementedError(f"not implement output_type: {output_type}")
        elif output_type == OutputType.MSGPACK or content_type == "application/x-msgpack":
            if msgpack:
                _result = msgpack.unpackb(data)
            else:
                raise Exception("msgpack is not supported")
        elif output_type == OutputType.JSON or content_type == "application/json":
            if encoding == "utf-8":
                _result = json.loads(data)
            else:
                _result = json.loads(data, encoding=encoding)
        elif output_type == OutputType.APACHE_ARROW:
            if self._is_apache_arrow(content_type):
                self._parse_apache_arrow(data)
                return
            else:
                raise Exception("groonga is not supported Apache Arrow output type")
        else:  # xml or other types...
            # TODO: not implement
            raise NotImplementedError(f"not implement output_type: {output_type}")
        if isinstance(_result, dict):
            # command_version=3 or later
            header = _result["header"]
            self.status = header["return_code"]
            self.start_time = header["start_time"]
            self.elapsed = header["elapsed_time"]
            self.trace_logs = None
            trace_log = _result.get("trace_log")
            if trace_log:
                names = [column["name"] for column in trace_log["columns"]]
                self.trace_logs = [dict(zip(names, log)) for log in trace_log["logs"]]
            self.body = _result["body"]
        else:
            self.status = _result[0][0]
            self.start_time = _result[0][1]
            self.elapsed = _result[0][2]
            self.trace_logs = None
            if len(_result) == 1 and self.status != 0:
                self.body = _result[0][3]
            elif len(_result) == 1 and self.status == 0:
                self.body = ""  # handling invalid result
            else:
                self.body = _result[1]

    def _is_apache_arrow(self, content_type):
        return content_type == "application/x-apache-arrow-streaming"

    def _parse_apache_arrow(self, data):
        if not pa:
            raise Exception("Apache Arrow is not supported")

        def is_metadata(schema):
            if not schema.metadata:
                return False
            return schema.metadata.get(b"GROONGA:data_type") == b"metadata"

        def is_trace_log(schema):
            if not schema.metadata:
                return False
            return schema.metadata.get(b"GROONGA:data_type") == b"trace_log"

        self.hit_num = -1
        self.items = None
        self.body = None
        self.trace_logs = None
        source = pa.BufferReader(data)
        while source.tell() < source.size():
            with pa.RecordBatchStreamReader(source) as reader:
                schema = reader.schema
                table = reader.read_all()
                if is_metadata(schema):
                    self.status = table["return_code"][0].as_py()
                    start_time_ns = table["start_time"][0].value
                    start_time_s = start_time_ns / 1_000_000_000
                    self.start_time = start_time_s
                    self.elapsed = table["elapsed_time"][0].as_py()
                    if self.status != 0:
                        try:
                            table.schema.field("error_message")
                            self.body = table["error_message"][0].as_py()
                        except KeyError:
                            # For Groonga < 13.0.9.
                            # Groonga < 13.0.9 doesn't provide the "error_message"
                            # column.
                            pass
                elif is_trace_log(schema):
                    self.trace_logs = table
                else:
                    self._parse_apache_arrow_body(table)

    def _parse_apache_arrow_body(self, table):
        raise NotImplementedError("Apache Arrow data is not supported")

    def __str__(self):
        return (
            f"<{type(self).__name__} "
            + f"status={self.status} "
            + f"start_time={self.start_time} "
            + f"elapsed={self.elapsed}>"
        )


class GroongaSelectResult(GroongaResult):
    def __init__(self, data, output_type: OutputType = OutputType.JSON, encoding="utf-8", content_type=None):
        super(GroongaSelectResult, self).__init__(data, output_type, encoding, content_type)
        if self._is_apache_arrow(content_type):
            return
        self.items = []
        self.hit_num = -1  # default is -1 (Error)
        if self.body is None:
            pass
        elif isinstance(self.body, dict):
            # command_version=3 or later
            if "n_hits" in self.body:
                self.hit_num = self.body["n_hits"]
            if "records" in self.body:
                keys = [column["name"] for column in self.body["columns"]]
                self.items = [dict(zip(keys, record)) for record in self.body["records"]]
        else:
            if len(self.body) != 0:
                self.hit_num = self.body[0][0][0]
            if self.status == 0:
                keys = [k[0] for k in self.body[0][1]]
                self.items = [dict(zip(keys, item)) for item in self.body[0][2:]]

    def _parse_apache_arrow_body(self, table):
        self.body = table
        if self.status != 0:
            return
        metadata = table.schema.metadata
        if metadata:
            n_hits_raw = metadata.get(b"GROONGA:n_hits")
            if n_hits_raw:
                self.hit_num = int(n_hits_raw)
        self.items = table
