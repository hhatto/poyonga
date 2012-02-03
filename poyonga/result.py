import json


class GroongaResult(object):

    def __init__(self, data, output_type="json"):
        self.raw_result = data
        if output_type == "json":
            _result = json.loads(data)
        self.status = _result[0][0]
        self.start_time = _result[0][1]
        self.elapsed = _result[0][2]
        if len(_result) == 1:
            self.body = _result[0][3]
        else:
            self.body = _result[1]
