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


class GroongaResult:
    def __init__(self, data, output_type="json", encoding="utf-8", content_type=None):
        self.raw_result = data
        if output_type == "tsv" or content_type == "text/tab-separated-values":
            # TODO: not implement
            csv.reader(StringIO(data), delimiter="\t")
        elif output_type == "msgpack" or content_type == "application/x-msgpack":
            if msgpack:
                _result = msgpack.unpackb(data)
            else:
                raise Exception("msgpack is not supported")
        elif output_type == "json" or content_type == "application/json":
            if encoding == "utf-8":
                _result = json.loads(data)
            else:
                _result = json.loads(data, encoding=encoding)
        elif self._is_apache_arrow(content_type):
            self._parse_apache_arrow(data)
            return
        else:  # xml or other types...
            # TODO: not implement
            pass
        self.status = _result[0][0]
        self.start_time = _result[0][1]
        self.elapsed = _result[0][2]
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
                else:
                    self._parse_apache_arrow_body(table)

    def _parse_apache_arrow_body(self, table):
        raise NotImplementedError("Apache Arrow data is not supported")

    def __str__(self):
        return f"<{type(self).__name__} " + \
            f"status={self.status} " + \
            f"start_time={self.start_time} " + \
            f"elapsed={self.elapsed}>"


class GroongaSelectResult(GroongaResult):
    def __init__(self, data, output_type="json", encoding="utf-8", content_type=None):
        super(GroongaSelectResult, self).__init__(data, output_type, encoding, content_type)
        if self._is_apache_arrow(content_type):
            return
        self.items = []
        self.hit_num = -1  # default is -1 (Error)
        if len(self.body) != 0:
            self.hit_num = self.body[0][0][0]
        if self.status == 0:
            keys = [k[0] for k in self.body[0][1]]
            self.items = [dict(list(zip(keys, item))) for item in self.body[0][2:]]

    def _parse_apache_arrow_body(self, table):
        self.body = table
        self.hit_num = -1
        if self.status == 0:
            metadata = table.schema.metadata
            if metadata:
                n_hits_raw = metadata.get(b"GROONGA:n_hits")
                if n_hits_raw:
                    self.hit_num = int(n_hits_raw)
            self.items = table
        else:
            self.items = None
