import sys
from poyonga import Groonga

query = "content:@%s" % sys.argv[1]
grn = Groonga()
ret = grn.call(
    "select", table="Files", limit=1000, sortby="-_score", output_columns="_score,_key", query=query
)
for item in ret.items:
    print("[score:%3d]%s" % (item["_score"], item["_key"]))
print("hit:", ret.hit_num)
