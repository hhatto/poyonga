import eventlet

from poyonga import Groonga

eventlet.monkey_patch()


def fetch(cmd, **kwargs):
    g = Groonga()
    ret = g.call(cmd, **kwargs)
    print(ret.status)
    print(ret.body)
    print("*" * 40)


cmds = [
    ("status", {}),
    ("log_level", {"level": "warning"}),
    # ("table_create", {"name": "Site", "flags": "TABLE_HASH_KEY"}),
    ("select", {"table": "Site"}),
]
pool = eventlet.GreenPool()
for cmd, kwargs in cmds:
    pool.spawn_n(fetch, cmd, **kwargs)
pool.waitall()
