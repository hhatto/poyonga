# coding: utf-8
from poyonga import Groonga


def _call_with_apachearrow(g, cmd, **kwargs):
    print("[http with apache arrow response]")
    ret = g.call(cmd, **kwargs)
    print("status:", ret.status)
    if cmd == "select":
        print(type(ret.items))
        if ret.items:
            print("item:", len(ret.items))
            for item in ret.items:
                print(item)
        else:
            print(ret.body)
        print("=*=" * 30)
    else:
        print(ret.body)


g = Groonga()

_call_with_apachearrow(
    g,
    "select",
    table="Comment",  # NOTE: invalid table name
    filter="last_modified<=1268802000",
    output_columns="posted_by.name,comment,last_modified",
    output_type="apache-arrow",
    drilldown="hash_tags,posted_by",
    drilldown_output_column="_id",
)
