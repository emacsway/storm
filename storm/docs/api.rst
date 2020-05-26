API
===
.. contents:: :local:


Locals
------

The following names are re-exported from ``storm.locals`` for convenience:

* :py:class:`storm.base.Storm`
* :py:func:`storm.database.create_database`
* :py:class:`storm.exceptions.StormError`
* :py:class:`storm.expr.And`
* :py:class:`storm.expr.Asc`
* :py:class:`storm.expr.Count`
* :py:class:`storm.expr.Delete`
* :py:class:`storm.expr.Desc`
* :py:class:`storm.expr.In`
* :py:class:`storm.expr.Insert`
* :py:class:`storm.expr.Join`
* :py:class:`storm.expr.Like`
* :py:class:`storm.expr.Max`
* :py:class:`storm.expr.Min`
* :py:class:`storm.expr.Not`
* :py:class:`storm.expr.Or`
* :py:class:`storm.expr.SQL`
* :py:class:`storm.expr.Select`
* :py:class:`storm.expr.Update`
* :py:class:`storm.info.ClassAlias`
* :py:class:`storm.properties.Bool`
* :py:class:`storm.properties.Bytes`
* :py:class:`storm.properties.Date`
* :py:class:`storm.properties.DateTime`
* :py:class:`storm.properties.Decimal`
* :py:class:`storm.properties.Enum`
* :py:class:`storm.properties.Float`
* :py:class:`storm.properties.Int`
* :py:class:`storm.properties.JSON`
* :py:class:`storm.properties.List`
* :py:class:`storm.properties.Pickle`
* :py:class:`storm.properties.Time`
* :py:class:`storm.properties.TimeDelta`
* :py:class:`storm.properties.UUID`
* :py:class:`storm.properties.Unicode`
* :py:class:`storm.references.Proxy`
* :py:class:`storm.references.Reference`
* :py:class:`storm.references.ReferenceSet`
* :py:data:`storm.store.AutoReload`
* :py:class:`storm.store.Store`
* :py:class:`storm.xid.Xid`



Store
-----
.. automodule:: storm.store
.. autoclass:: storm.store.ResultSet
.. autoclass:: storm.store.TableSet
.. autodata:: storm.store.AutoReload
   :annotation:

Defining tables and columns
---------------------------

Base
~~~~
.. automodule:: storm.base

Properties
~~~~~~~~~~
.. automodule:: storm.properties

References
~~~~~~~~~~
.. automodule:: storm.references

Variables
~~~~~~~~~
.. automodule:: storm.variables

SQLObject emulation
~~~~~~~~~~~~~~~~~~~
.. automodule:: storm.sqlobject


Expressions
-----------
.. automodule:: storm.expr


Databases
---------
.. automodule:: storm.database

PostgreSQL
~~~~~~~~~~
.. automodule:: storm.databases.postgres

SQLite
~~~~~~
.. automodule:: storm.databases.sqlite

Transaction identifiers
~~~~~~~~~~~~~~~~~~~~~~~
.. automodule:: storm.xid


Hooks and events
----------------

Event
~~~~~
.. automodule:: storm.event

Tracer
~~~~~~
.. automodule:: storm.tracer


Miscellaneous
-------------

Cache
~~~~~
.. automodule:: storm.cache

Exceptions
~~~~~~~~~~
.. automodule:: storm.exceptions

Info
~~~~~
.. automodule:: storm.info

Testing
~~~~~~~
.. automodule:: storm.testing

Timezone
~~~~~~~~
.. automodule:: storm.tz

URIs
~~~~
.. automodule:: storm.uri

WSGI
~~~~
.. automodule:: storm.wsgi
