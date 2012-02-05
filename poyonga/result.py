import json


class GroongaResult(object):

    def __init__(self, data, output_type="json"):
        self.raw_result = data
        if output_type == "json":
            _result = json.loads(data)
        self.status = _result[0][0]
        self.start_time = _result[0][1]
        self.elapsed = _result[0][2]
        if len(_result) == 1 and self.status != 0:
            self.body = _result[0][3]
        elif len(_result) == 1 and self.status == 0:
            self.body = ""  # handling invalid result
        else:
            self.body = _result[1]


class GroongaSelectResult(GroongaResult):

    def __init__(self, data, output_type="json"):
        super(GroongaSelectResult, self).__init__(data, output_type)
        self.items = []
        self.hit_num = self.body[0][0][0]
        if self.status == 0:
            keys = [k[0] for k in self.body[0][1]]
            self.items = [dict(zip(keys, item)) for item in self.body[0][2:]]
