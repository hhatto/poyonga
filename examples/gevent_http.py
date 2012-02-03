from poyonga import Groonga
import gevent
from gevent import monkey

monkey.patch_all()


def fetch(cmd, **kwargs):
    g = Groonga()
    ret = g.call(cmd, **kwargs)
    print ret.status
    print ret.body
    print "*" * 40
    return ret.body


cmds = [("status", {}),
        ("log_level", {"level": "warning"}),
        #("table_create", {"name": "Site", "flags": "TABLE_HASH_KEY"}),
        ("select", {"table": "Site"})]
jobs = [gevent.spawn(fetch, cmd, **kwargs) for cmd, kwargs in cmds]
gevent.joinall(jobs)
results = [job.value for job in jobs]
print results
