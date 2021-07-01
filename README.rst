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

with Apache Arrow
-----------------
Groonga supports `Apache Arrow`_, use it with ``load`` and ``select`` commands.

use poyonga with Apache Arrow, you need pyarrow_ .

.. _`Apache Arrow`: https://arrow.apache.org/
.. _pyarrow: https://pypi.org/project/pyarrow/

requrie pyarrow::

    $ pip install pyarrow

and call with ``output_type="apache-arrow"`` option:

.. code-block:: python

    from poyonga import Groonga

    g = Groonga()
    g.call(
        "select",
        table="Users",
        match_columns="name,location_str,description",
        query="東京",
        output_type="apache-arrow",
        output_columns="_key,name",
    )

load with ``input_type="apache-arrow"``:

.. code-block:: python

    import pyarrow as pa
    from poyonga import Groonga

    # use Apache Arrow IPC Streaming Format
    data = [pa.array(["groonga.org"])]
    batch = pa.record_batch(data, names=["_key"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    buf = sink.getvalue()
    values = buf.to_pybytes()

    g = Groonga()
    g.call("load", table="Site", values=values, input_type="apache-arrow")


more information:

- https://groonga.org/docs/reference/commands/load.html


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
