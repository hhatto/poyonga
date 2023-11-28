from poyonga import Groonga


def _call(g, cmd, **kwargs):
    ret = g.call(cmd, **kwargs)
    print("status:", ret.status)
    if cmd == "select":
        print("item:", len(ret.items))
        for item in ret.items:
            print(item)
        print(f"[trace_log] {ret.trace_logs}")
        print("=*=" * 30)


g = Groonga()

_call(
    g,
    "select",
    table="Users",
    match_columns="name,location_str,description",
    query="東京",
    output_columns="_key,name",
    output_trace_log="yes",
    command_version="3",
)
_call(
    g,
    "select",
    table="Users",
    filter='geo_in_circle(location,"128484216x502919856",5000)',
    output_columns="_key,name",
    output_trace_log="yes",
    command_version="3",
)
_call(
    g,
    "select",
    table="Comments",
    filter="last_modified<=1268802000",
    output_columns="posted_by.name,comment,last_modified",
    drilldown="hash_tags,posted_by",
    output_trace_log="yes",
    command_version="3",
)
