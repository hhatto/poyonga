# coding: utf-8
from poyonga import Groonga


def _call_with_apachearrow(g, cmd, **kwargs):
    print("[http with apache arrow response]")
    ret = g.call(cmd, **kwargs)
    print("status:", ret.status)
    if cmd == "select":
        print("item:", len(ret.items))
        print(type(ret.items))
        print("pydict:", ret.items.to_pydict())
        for item in ret.items:
            print(item)
        print("=*=" * 30)
    else:
        print(ret.body)


def _call(g, cmd, **kwargs):
    print("[http with json response]")
    ret = g.call(cmd, **kwargs)
    print("status:", ret.status)
    if cmd == "select":
        print("item:", len(ret.items))
        print(type(ret.items))
        for item in ret.items:
            print(item)
        print("=*=" * 30)
    else:
        print(ret.body)


g = Groonga()

_call(
    g,
    "select",
    table="Users",
    match_columns="name,location_str,description",
    query="東京",
    output_columns="_key,name",
)
_call_with_apachearrow(
    g,
    "select",
    table="Users",
    match_columns="name,location_str,description",
    query="東京",
    output_type="apache-arrow",
    output_columns="_key,name",
)
_call(
    g,
    "select",
    table="Comments",
    filter="last_modified<=1268802000",
    output_columns="posted_by.name,comment,last_modified",
    drilldown="hash_tags,posted_by",
)
_call_with_apachearrow(
    g,
    "select",
    table="Comments",
    filter="last_modified<=1268802000",
    output_columns="posted_by.name,comment,last_modified",
    output_type="apache-arrow",
    drilldown="hash_tags,posted_by",
)
