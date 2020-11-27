=======
poyonga
=======

.. image:: https://img.shields.io/pypi/v/poyonga.svg
    :target: https://pypi.org/project/poyonga/
    :alt: PyPI Version

.. image:: https://github.com/hhatto/poyonga/workflows/Python%20package/badge.svg
    :target: https://github.com/hhatto/poyonga/actions
    :alt: Build status

Python Groonga_ Client.
poyonga support to HTTP and GQTP protocol.

.. _Groonga: http://groonga.org/


Requrements
===========
* Python 3.6+


Installation
============
from pip::

    pip install --upgrade poyonga


Usage
=====

Setup Groonga Server
--------------------
::

    $ groonga -n grn.db     # create groonga database file
    $ groonga -s grn.db     # start groonga server with GQTP


Basic Usage
-----------

.. code-block:: python

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
.. code-block:: python

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

Custom prefix path
------------------
If you use the `Custom prefix path`_ and `Multi databases`_ , specify `prefix_path` .

.. _`Custom prefix path`: http://groonga.org/docs/server/http/comparison.html#custom-prefix-path
.. _`Multi databases`: http://groonga.org/docs/server/http/comparison.html#multi-databases

.. code-block:: python

    # default is '/d/'
    g = Groonga(prefix_path='/db2/')


example code
------------
see `examples directory`_

.. _`examples directory`: https://github.com/hhatto/poyonga/tree/master/examples


Links
=====
* PyPI_
* GitHub_

.. _PyPI: https://pypi.python.org/pypi/poyonga
.. _GitHub: https://github.com/hhatto/poyonga
