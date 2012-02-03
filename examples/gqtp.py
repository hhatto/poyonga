from poyonga import Groonga

g = Groonga(protocol="gqtp")

cmds = [("status", {}),
        ("stat", {}),   # invalid
        #("log_level", {"level": "warning"}),
        #("table_create", {"name": "Site", "flags": "TABLE_HASH_KEY"}),
        ("table_list", {}),
        ("select", {"table": "Site"})
        ]
for cmd, kwargs in cmds:
    ret = g.call(cmd, **kwargs)
    print ret.status
    print ret.body
    print "*" * 40
