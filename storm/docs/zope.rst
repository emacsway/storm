..
    Copyright (c) 2006, 2007 Canonical

    Written by Jamshed Kakar <jkakar@kakar.ca>

    This file is part of Storm Object Relational Mapper.

    Storm is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as
    published by the Free Software Foundation; either version 2.1 of
    the License, or (at your option) any later version.

    Storm is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.


Zope integration
================

The ``storm.zope`` package contains the ZStorm utility which provides
seamless integration between Storm and Zope 3's transaction system.
Setting up ZStorm is quite easy.  In most cases, you want to include
``storm/zope/configure.zcml`` in your application, which you would normally
do in ZCML as follows:

.. code-block:: xml

  <include package="storm.zope" />

For the purposes of this doctest we'll register ZStorm manually.

  >>> from zope.component import provideUtility, getUtility
  >>> import transaction
  >>> from storm.zope.interfaces import IZStorm
  >>> from storm.zope.zstorm import global_zstorm

  >>> provideUtility(global_zstorm, IZStorm)
  >>> zstorm = getUtility(IZStorm)
  >>> zstorm
  <storm.zope.zstorm.ZStorm object at ...>

Awesome, now that the utility is in place we can start to use it!


Getting stores
--------------

The ZStorm utility allows us work with named stores.

  >>> zstorm.set_default_uri("test", "sqlite:")

Setting a default URI for stores isn't strictly required.  We could
pass it as the second argument to ``zstorm.get``.  Providing a default URI
makes it possible to use ``zstorm.get`` more easily; this is especially
handy when multiple threads are used as we'll see further on.

  >>> store = zstorm.get("test")
  >>> store
  <storm.store.Store object at ...>

ZStorm has automatically created a store instance for us.  If we ask
for a store by name again, we should get the same instance.

  >>> same_store = zstorm.get("test")
  >>> same_store is store
  True

The stores provided by ZStorm are per-thread.  If we ask for the named
store in a different thread we should get a different instance.

  >>> import threading

  >>> thread_store = []
  >>> def get_thread_store():
  ...     thread_store.append(zstorm.get("test"))

  >>> thread = threading.Thread(target=get_thread_store)
  >>> thread.start()
  >>> thread.join()
  >>> thread_store != [store]
  True

Great!  ZStorm abstracts away the process of creating and managing
named stores.  Let's move on and use the stores with Zope's
transaction system.


Committing transactions
-----------------------

The primary purpose of ZStorm is to integrate with Zope's transaction
system.  Let's create a schema so we can play with some real data and
see how it works.

  >>> result = store.execute("""
  ...     CREATE TABLE person (
  ...         id INTEGER PRIMARY KEY,
  ...         name TEXT)
  ... """)
  >>> store.commit()

We'll need a ``Person`` class to use with this database.

  >>> from storm.locals import Storm, Int, Unicode

  >>> class Person(Storm):
  ...
  ...     __storm_table__ = "person"
  ...
  ...     id = Int(primary=True)
  ...     name = Unicode()
  ...
  ...     def __init__(self, name):
  ...         self.name = name

Great!  Let's try it out.

  >>> person = Person(u"John Doe")
  >>> store.add(person)
  <...Person object at ...>
  >>> transaction.commit()

Notice that we're not using ``store.commit`` directly; we're using Zope's
transaction system.  Let's make sure it worked.

  >>> store.rollback()
  >>> same_person = store.find(Person).one()
  >>> same_person is person
  True

Awesome!


Aborting transactions
---------------------

Let's make sure aborting transactions works, too.

  >>> store.add(Person(u"Imposter!"))
  <...Person object at ...>

At this point a ``store.find`` should return the new object.

  >>> for name in sorted(person.name for person in store.find(Person)):
  ...     print(name)
  Imposter!
  John Doe

All this means is that the data has been flushed to the database; it's
still not committed.  If we abort the transaction the new ``Person``
object should disappear.

  >>> transaction.abort()
  >>> for person in store.find(Person):
  ...     print(person.name)
  John Doe

Excellent!  As you can see, ZStorm makes working with SQL databases
and Zope 3 very natural.


ZCML
----

In the examples above we setup our stores manually.  In many cases,
setting up named stores via ZCML directives is more desirable.  Add a
stanza similar to the following to your ZCML configuration to setup a
named store.

.. code-block:: xml

  <store name="test" uri="sqlite:" />

With that in place ``getUtility(IZStorm).get("test")`` will return the
store named "test".


Security Wrappers
-----------------

Storm knows how to deal with "wrapped" objects -- the identity of any
Storm-managed object does not need to be the same as the original
object, by way of the "object info" system. As long as the object info
can be retrieved from the wrapped objects, things work fine.

To interoperate with the Zope security wrapper system, storm.zope
tells Zope to exposes certain Storm-internal attributes which appear
on Storm-managed objects.

  >>> from storm.info import get_obj_info, ObjectInfo
  >>> from zope.security.checker import ProxyFactory
  >>> from pprint import pprint

  >>> person = store.find(Person).one()
  >>> type(get_obj_info(person)) is ObjectInfo
  True
  >>> type(get_obj_info(ProxyFactory(person))) is ObjectInfo
  True

Security-wrapped result sets can be used in the same way as unwrapped ones.

  >>> from zope.component.testing import (
  ...     setUp,
  ...     tearDown,
  ...     )
  >>> from zope.configuration import xmlconfig
  >>> from zope.security.protectclass import protectName
  >>> import storm.zope

  >>> setUp()
  >>> _ = xmlconfig.file("configure.zcml", package=storm.zope)
  >>> protectName(Person, "name", "zope.Public")

  >>> another_person = Person(u"Jane Doe")
  >>> store.add(another_person)
  <...Person object at ...>
  >>> result = ProxyFactory(store.find(Person).order_by(Person.name))
  >>> for person in result:
  ...     print(person.name)
  Jane Doe
  John Doe
  >>> print(result[0].name)
  Jane Doe
  >>> for person in result[:1]:
  ...     print(person.name)
  Jane Doe
  >>> another_person in result
  True
  >>> result.is_empty()
  False
  >>> result.any()
  <...Person object at ...>
  >>> print(result.first().name)
  Jane Doe
  >>> print(result.last().name)
  John Doe
  >>> print(result.count())
  2

Check ``list()`` as well as ordinary iteration: on Python 3, this tries to
call ``__len__`` first (which doesn't exist, but is nevertheless allowed by
the security wrapper).

  >>> for person in list(result):
  ...     print(person.name)
  Jane Doe
  John Doe

  >>> result = ProxyFactory(
  ...     store.find(Person, Person.name.startswith(u"John")))
  >>> print(result.one().name)
  John Doe

Security-wrapped reference sets work too.

  >>> _ = store.execute("""
  ...     CREATE TABLE team (
  ...         id INTEGER PRIMARY KEY,
  ...         name TEXT)
  ... """)
  >>> _ = store.execute("""
  ...     CREATE TABLE teammembership (
  ...         id INTEGER PRIMARY KEY,
  ...         person INTEGER NOT NULL REFERENCES person,
  ...         team INTEGER NOT NULL REFERENCES team)
  ... """)
  >>> store.commit()

  >>> from storm.locals import Reference, ReferenceSet, Store

  >>> class TeamMembership(Storm):
  ...
  ...     __storm_table__ = "teammembership"
  ...
  ...     id = Int(primary=True)
  ...
  ...     person_id = Int(name="person", allow_none=False)
  ...     person = Reference(person_id, "Person.id")
  ...
  ...     team_id = Int(name="team", allow_none=False)
  ...     team = Reference(team_id, "Team.id")
  ...
  ...     def __init__(self, person, team):
  ...         self.person = person
  ...         self.team = team

  >>> class Team(Storm):
  ...
  ...     __storm_table__ = "team"
  ...
  ...     id = Int(primary=True)
  ...     name = Unicode()
  ...
  ...     def __init__(self, name):
  ...         self.name = name
  ...
  ...     members = ReferenceSet(
  ...         "id", "TeamMembership.team_id",
  ...         "TeamMembership.person_id", "Person.id",
  ...         order_by="Person.name")
  ...
  ...     def addMember(self, person):
  ...         Store.of(self).add(TeamMembership(person, self))

  >>> protectName(Team, "members", "zope.Public")
  >>> protectName(Team, "addMember", "zope.Public")

  >>> doe_family = Team(U"does")
  >>> store.add(doe_family)
  <...Team object at ...>
  >>> doe_family = ProxyFactory(doe_family)
  >>> doe_family.addMember(person)
  >>> doe_family.addMember(another_person)

  >>> for member in doe_family.members:
  ...     print(member.name)
  Jane Doe
  John Doe
  >>> for person in doe_family.members[:1]:
  ...     print(person.name)
  Jane Doe
  >>> print(doe_family.members[0].name)
  Jane Doe

  >>> tearDown()


ResultSet interfaces
--------------------

Query results provide ``IResultSet`` (or ``ISQLObjectResultSet`` if
SQLObject's compatibility layer is used).

  >>> from storm.zope.interfaces import IResultSet, ISQLObjectResultSet
  >>> from storm.store import EmptyResultSet, ResultSet
  >>> from storm.sqlobject import SQLObjectResultSet
  >>> IResultSet.implementedBy(ResultSet)
  True
  >>> IResultSet.implementedBy(EmptyResultSet)
  True

  >>> ISQLObjectResultSet.implementedBy(SQLObjectResultSet)
  True


..
  >>> Team._storm_property_registry.clear()
  >>> TeamMembership._storm_property_registry.clear()
  >>> Person._storm_property_registry.clear()
  >>> transaction.abort()
  >>> zstorm._reset()
