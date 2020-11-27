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


class GroongaResult:
    def __init__(self, data, output_type="json", encoding="utf-8"):
        self.raw_result = data
        if output_type == "tsv":
            # TODO: not implement
            csv.reader(StringIO(data), delimiter="\t")
        elif output_type == "msgpack":
            if msgpack:
                _result = msgpack.unpackb(data)
            else:
                raise Exception("msgpack is not support")
        elif output_type == "json":
            if encoding == "utf-8":
                _result = json.loads(data)
            else:
                _result = json.loads(data, encoding=encoding)
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

    def __str__(self):
        return f"<{type(self).__name__} " + \
            f"status={self.status} " + \
            f"start_time={self.start_time} " + \
            f"elapsed={self.elapsed}>"


class GroongaSelectResult(GroongaResult):
    def __init__(self, data, output_type="json", encoding="utf-8"):
        super(GroongaSelectResult, self).__init__(data, output_type, encoding)
        self.items = []
        self.hit_num = -1  # default is -1 (Error)
        if len(self.body) != 0:
            self.hit_num = self.body[0][0][0]
        if self.status == 0:
            keys = [k[0] for k in self.body[0][1]]
            self.items = [dict(list(zip(keys, item))) for item in self.body[0][2:]]
