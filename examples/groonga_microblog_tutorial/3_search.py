# coding: utf-8
from poyonga import Groonga


def _call(g, cmd, **kwargs):
    ret = g.call(cmd, **kwargs)
    print("status:", ret.status)
    if cmd == "select":
        print("item:", len(ret.items))
        for item in ret.items:
            print(item)
        print("=*=" * 30)


g = Groonga()

_call(
    g,
    "select",
    table="Users",
    match_columns="name,location_str,description",
    query="東京",
    output_columns="_key,name",
)
_call(
    g,
    "select",
    table="Users",
    filter='geo_in_circle(location,"128484216x502919856",5000)',
    output_columns="_key,name",
)
_call(g, "select", table="Users", query="follower:@tasukuchan", output_columns="_key,name")
_call(
    g,
    "select",
    table="Comments",
    filter='geo_in_circle(location,"127975798x502919856",20000)',
    output_columns="posted_by.name,comment",
    drilldown="hash_tags,posted_by",
)

_call(g, "select", table="Comments", query="comment:@なう", output_columns="comment,_score")
_call(
    g,
    "select",
    table="Comments",
    query="comment:@羽田",
    filter='geo_in_circle(location,"127975798x502919856",20000)',
    output_columns="posted_by.name,comment",
    drilldown="hash_tags,posted_by",
)
_call(
    g,
    "select",
    table="Comments",
    query="hash_tags:@groonga",
    output_columns="posted_by.name,comment",
    drilldown="posted_by",
)
_call(
    g,
    "select",
    table="Comments",
    query="posted_by:tasukuchan",
    output_columns="comment",
    drilldown="hash_tags",
)
_call(
    g,
    "select",
    table="Users",
    query="_key:tasukuchan",
    output_columns="favorites.posted_by,favorites.comment",
)
_call(
    g,
    "select",
    table="Comments",
    filter="last_modified<=1268802000",
    output_columns="posted_by.name,comment,last_modified",
    drilldown="hash_tags,posted_by",
)
