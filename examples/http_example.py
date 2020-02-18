from poyonga.client import Groonga

g = Groonga()

cmds = [("status", {}),
        ("log_level", {"level": "warning"}),
        # ("table_create", {"name": "Site", "flags": "TABLE_HASH_KEY"}),
        ("select", {"table": "Site"})]
for cmd, kwargs in cmds:
    ret = g.call(cmd, **kwargs)
    print(ret.status)
    print(ret.body)
    print("*" * 40)
