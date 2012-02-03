About
=====
Python Groonga_ Client.
poyonga support to HTTP and GQTP protocol.

.. _Groonga: http://groonga.org/


Installation
============
from pip::

    pip install --upgrade poyonga

from easy_install::

    easy_install -ZU poyonga


Usage
=====

Basic Usage
-----------
::

    >>> from poyonga import Groonga
    >>> g = Groonga()
    >>> g.protocol
    'http'
    >>> ret = g.call("status")
    >>> ret
    <poyonga.result.GroongaResult object at 0x8505ccc>
    >>> ret.status
    0
    >>> ret.body
    {u'uptime': 427, u'max_command_version': 2, u'n_queries': 3,
    u'cache_hit_rate': 66.6666666666667, u'version': u'1.2.8', u
    'alloc_count': 156, u'command_version': 1, u'starttime': 132
    8286909, u'default_command_version': 1}
    >>>

with eventlet
-------------
::

    from poyonga import Groonga
    import eventlet

    eventlet.monkey_patch()

    def fetch(cmd, **kwargs):
        g = Groonga()
        ret = g.call(cmd, **kwargs)
        print ret.status
        print ret.body
        print "*" * 40

    cmds = [("status", {}),
            ("log_level", {"level": "warning"}),
            ("table_list", {})
            ("select", {"table": "Site"})]
    pool = eventlet.GreenPool()
    for cmd, kwargs in cmds:
        pool.spawn_n(fetch, cmd, **kwargs)
    pool.waitall()
