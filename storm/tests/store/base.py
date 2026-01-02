#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import decimal
import gc
from io import StringIO
import operator
import pickle
from uuid import uuid4
import weakref

from storm.references import Reference, ReferenceSet, Proxy
from storm.database import Result, STATE_DISCONNECTED
from storm.properties import (
    Int, Float, Bytes, Unicode, Property, Pickle, UUID)
from storm.properties import PropertyPublisherMeta, Decimal
from storm.variables import PickleVariable
from storm.expr import (
    Asc, Desc, Select, LeftJoin, SQL, Count, Sum, Avg, And, Or, Eq, Lower)
from storm.variables import Variable, UnicodeVariable, IntVariable
from storm.info import get_obj_info, ClassAlias
from storm.exceptions import (
    ClosedError, ConnectionBlockedError, FeatureError, LostObjectError,
    NoStoreError, NotFlushedError, NotOneError, OrderLoopError, UnorderedError,
    WrongStoreError, DisconnectionError)
from storm.cache import Cache
from storm.store import AutoReload, EmptyResultSet, Store, ResultSet
from storm.tracer import debug
from storm.tests.info import Wrapper
from storm.tests.helper import AsyncTestHelper


class Foo:
    __storm_table__ = "foo"
    id = Int(primary=True)
    title = Unicode()

class Bar:
    __storm_table__ = "bar"
    id = Int(primary=True)
    title = Unicode()
    foo_id = Int()
    foo = Reference(foo_id, Foo.id)

class UniqueID:
    __storm_table__ = "unique_id"
    id = UUID(primary=True)
    def __init__(self, id):
        self.id = id

class Blob:
    __storm_table__ = "bin"
    id = Int(primary=True)
    bin = Bytes()

class Link:
    __storm_table__ = "link"
    __storm_primary__ = "foo_id", "bar_id"
    foo_id = Int()
    bar_id = Int()

class SelfRef:
    __storm_table__ = "selfref"
    id = Int(primary=True)
    title = Unicode()
    selfref_id = Int()
    selfref = Reference(selfref_id, id)
    selfref_on_remote = Reference(id, selfref_id, on_remote=True)

class FooRef(Foo):
    bar = Reference(Foo.id, Bar.foo_id)

class FooRefSet(Foo):
    bars = ReferenceSet(Foo.id, Bar.foo_id)

class FooRefSetOrderID(Foo):
    bars = ReferenceSet(Foo.id, Bar.foo_id, order_by=Bar.id)

class FooRefSetOrderTitle(Foo):
    bars = ReferenceSet(Foo.id, Bar.foo_id, order_by=Bar.title)

class FooIndRefSet(Foo):
    bars = ReferenceSet(Foo.id, Link.foo_id, Link.bar_id, Bar.id)

class FooIndRefSetOrderID(Foo):
    bars = ReferenceSet(Foo.id, Link.foo_id, Link.bar_id, Bar.id,
                        order_by=Bar.id)

class FooIndRefSetOrderTitle(Foo):
    bars = ReferenceSet(Foo.id, Link.foo_id, Link.bar_id, Bar.id,
                        order_by=Bar.title)


class FooValue:
    __storm_table__ = "foovalue"
    id = Int(primary=True)
    foo_id = Int()
    value1 = Int()
    value2 = Int()

class BarProxy:
    __storm_table__ = "bar"
    id = Int(primary=True)
    title = Unicode()
    foo_id = Int()
    foo = Reference(foo_id, Foo.id)
    foo_title = Proxy(foo, Foo.title)

class Money:
    __storm_table__ = "money"
    id = Int(primary=True)
    value = Decimal()


class DecorateVariable(Variable):

    def parse_get(self, value, to_db):
        return "to_%s(%s)" % (to_db and "db" or "py", value)

    def parse_set(self, value, from_db):
        return "from_%s(%s)" % (from_db and "db" or "py", value)


class FooVariable(Foo):
    title = Property(variable_class=DecorateVariable)


class DummyDatabase:

    def connect(self, event=None):
        return None


class StoreCacheTest(AsyncTestHelper):

    async def test_wb_custom_cache(self):
        cache = Cache(25)
        store = Store(DummyDatabase(), cache=cache)
        self.assertEqual(store._cache, cache)

    async def test_wb_default_cache_size(self):
        store = Store(DummyDatabase())
        self.assertEqual(store._cache._size, 1000)


class StoreDatabaseTest(AsyncTestHelper):

    async def test_store_has_reference_to_its_database(self):
        database = DummyDatabase()
        store = Store(database)
        self.assertIdentical(store.get_database(), database)


class StoreTest:

    async def asyncSetUp(self):
        self.store = None
        self.stores = []
        self.create_database()
        self.connection = self.database.connect()
        await self.drop_tables()
        await self.create_tables()
        await self.create_sample_data()
        await self.create_store()

    async def asyncTearDown(self):
        await self.drop_store()
        await self.drop_sample_data()
        await self.drop_tables()
        self.drop_database()
        await self.connection.close()

    def create_database(self):
        raise NotImplementedError

    async def create_tables(self):
        raise NotImplementedError

    async def create_sample_data(self):
        connection = self.connection
        await connection.execute("INSERT INTO foo (id, title)"
                           " VALUES (10, 'Title 30')")
        await connection.execute("INSERT INTO foo (id, title)"
                           " VALUES (20, 'Title 20')")
        await connection.execute("INSERT INTO foo (id, title)"
                           " VALUES (30, 'Title 10')")
        await connection.execute("INSERT INTO bar (id, foo_id, title)"
                           " VALUES (100, 10, 'Title 300')")
        await connection.execute("INSERT INTO bar (id, foo_id, title)"
                           " VALUES (200, 20, 'Title 200')")
        await connection.execute("INSERT INTO bar (id, foo_id, title)"
                           " VALUES (300, 30, 'Title 100')")
        await connection.execute("INSERT INTO bin (id, bin) VALUES (10, 'Blob 30')")
        await connection.execute("INSERT INTO bin (id, bin) VALUES (20, 'Blob 20')")
        await connection.execute("INSERT INTO bin (id, bin) VALUES (30, 'Blob 10')")
        await connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (10, 100)")
        await connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (10, 200)")
        await connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (10, 300)")
        await connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (20, 100)")
        await connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (20, 200)")
        await connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (30, 300)")
        await connection.execute("INSERT INTO money (id, value)"
                           " VALUES (10, '12.3455')")
        await connection.execute("INSERT INTO selfref (id, title, selfref_id)"
                           " VALUES (15, 'SelfRef 15', NULL)")
        await connection.execute("INSERT INTO selfref (id, title, selfref_id)"
                           " VALUES (25, 'SelfRef 25', NULL)")
        await connection.execute("INSERT INTO selfref (id, title, selfref_id)"
                           " VALUES (35, 'SelfRef 35', 15)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (1, 10, 2, 1)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (2, 10, 2, 1)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (3, 10, 2, 1)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (4, 10, 2, 2)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (5, 20, 1, 3)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (6, 20, 1, 3)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (7, 20, 1, 4)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (8, 20, 1, 4)")
        await connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (9, 20, 1, 2)")

        await connection.commit()

    async def create_store(self):
        store = Store(self.database)
        self.stores.append(store)
        if self.store is None:
            self.store = store
        return store

    async def drop_store(self):
        for store in self.stores:
            await store.rollback()

            # Closing the store is needed because testcase objects are all
            # instantiated at once, and thus connections are kept open.
            store.close()

    async def drop_sample_data(self):
        pass

    async def drop_tables(self):
        for table in ["foo", "bar", "bin", "link", "money", "selfref",
                      "foovalue", "unique_id"]:
            try:
                await self.connection.execute("DROP TABLE %s" % table)
                await self.connection.commit()
            except:
                await self.connection.rollback()

    def drop_database(self):
        pass

    async def get_items(self):
        # Bypass the store to avoid flushing.
        connection = self.store._connection
        result = await connection.execute("SELECT * FROM foo ORDER BY id")
        return [item async for item in result]

    async def get_committed_items(self):
        connection = self.database.connect()
        result = await connection.execute("SELECT * FROM foo ORDER BY id")
        return [item async for item in result]

    def get_cache(self, store):
        # We don't offer a public API for this just yet.
        return store._cache

    async def test_execute(self):
        result = await self.store.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))
        self.assertEqual(await result.get_one(), (1,))

        result = await self.store.execute("SELECT 1", noresult=True)
        self.assertEqual(result, None)

    async def test_execute_params(self):
        result = await self.store.execute("SELECT ?", [1])
        self.assertTrue(isinstance(result, Result))
        self.assertEqual(await result.get_one(), (1,))

    async def test_execute_flushes(self):
        foo = await self.store.get(Foo, 10)
        foo.title = "New Title"

        result = await self.store.execute("SELECT title FROM foo WHERE id=10")
        self.assertEqual(await result.get_one(), ("New Title",))

    async def test_close(self):
        store = Store(self.database)
        store.close()
        self.assertRaises(ClosedError, store.execute, "SELECT 1")

    async def test_get(self):
        foo = await self.store.get(Foo, 10)
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.id, 20)
        self.assertEqual(foo.title, "Title 20")

        foo = await self.store.get(Foo, 40)
        self.assertEqual(foo, None)

    async def test_get_cached(self):
        foo = await self.store.get(Foo, 10)
        self.assertTrue(await self.store.get(Foo, 10) is foo)

    async def test_wb_get_cached_doesnt_need_connection(self):
        foo = await self.store.get(Foo, 10)
        connection = self.store._connection
        self.store._connection = None
        await self.store.get(Foo, 10)
        self.store._connection = connection

    async def test_cache_cleanup(self):
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = await self.store.get(Foo, 10)
        foo.taint = True

        del foo
        gc.collect()

        foo = await self.store.get(Foo, 10)
        self.assertFalse(getattr(foo, "taint", False))

    async def test_add_returns_object(self):
        """
        Store.add() returns the object passed to it.  This allows this
        kind of code:

        thing = Thing()
        await store.add(thing)
        return thing

        to be simplified as:

        return await store.add(Thing())
        """
        foo = Foo()
        self.assertEqual(await self.store.add(foo), foo)

    async def test_add_and_stop_referencing(self):
        # After adding an object, no references should be needed in
        # python for it still to be added to the database.
        foo = Foo()
        foo.title = "live"
        await self.store.add(foo)

        del foo
        gc.collect()

        self.assertTrue(await self.store.find(Foo, title="live").one())

    async def test_obj_info_with_deleted_object(self):
        # Let's try to put Storm in trouble by killing the object
        # while still holding a reference to the obj_info.

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        class MyFoo(Foo):
            loaded = False
            def __storm_loaded__(self):
                self.loaded = True

        foo = await self.store.get(MyFoo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertEqual(obj_info.get_obj(), None)

        foo = await self.store.find(MyFoo, id=20).one()
        self.assertTrue(foo)
        self.assertFalse(getattr(foo, "tainted", False))

        # The object was rebuilt, so the loaded hook must have run.
        self.assertTrue(foo.loaded)

    async def test_obj_info_with_deleted_object_and_changed_event(self):
        """
        When an object is collected, the variables disable change notification
        to not create a leak. If we're holding a reference to the obj_info and
        rebuild the object, it should re-enable change notication.
        """
        class PickleBlob(Blob):
            bin = Pickle()

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()
        del blob
        gc.collect()

        pickle_blob = await self.store.get(PickleBlob, 20)
        obj_info = get_obj_info(pickle_blob)
        del pickle_blob
        gc.collect()
        self.assertEqual(obj_info.get_obj(), None)

        pickle_blob = await self.store.get(PickleBlob, 20)
        pickle_blob.bin = "foobin"
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))

        await self.store.flush()
        self.assertEqual(len(events), 1)

    async def test_wb_flush_event_with_deleted_object_before_flush(self):
        """
        When an object is deleted before flush and it contains mutable
        variables, those variables unhook from the global event system to
        prevent a leak.
        """
        class PickleBlob(Blob):
            bin = Pickle()

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()
        del blob
        gc.collect()

        pickle_blob = await self.store.get(PickleBlob, 20)
        pickle_blob.bin = "foobin"
        del pickle_blob

        await self.store.flush()
        self.assertEqual(self.store._event._hooks["flush"], set())

    async def test_mutable_variable_detect_change_from_alive(self):
        """
        Changes in a mutable variable like a L{PickleVariable} are correctly
        detected, even if the object comes from the alive cache.
        """
        class PickleBlob(Blob):
            bin = Pickle()

        blob = PickleBlob()
        blob.bin = {"k": "v"}
        blob.id = 4000
        await self.store.add(blob)
        await self.store.commit()

        blob = await self.store.find(PickleBlob, PickleBlob.id == 4000).one()
        blob.bin["k1"] = "v1"

        await self.store.commit()

        blob = await self.store.find(PickleBlob, PickleBlob.id == 4000).one()
        self.assertEqual(blob.bin, {"k1": "v1", "k": "v"})

    async def test_mutable_variable_no_reference_cycle(self):
        """
        Mutable variables only hold weak refs to EventSystem, to prevent
        leaks.
        """
        class PickleBlob(Blob):
            bin = Pickle()

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()
        del blob

        # Get an existing object and make an unflushed change to it so that
        # a flush hook for the variable is registered with the event system.
        pickle_blob = await self.store.get(PickleBlob, 20)
        pickle_blob.bin = "foobin"
        pickle_blob_ref = weakref.ref(pickle_blob)
        del pickle_blob

        for store in self.stores:
            store.close()
        del store
        self.store = None
        self.stores = []
        gc.collect()

        self.assertIsNone(pickle_blob_ref())

    async def test_wb_checkpoint_doesnt_override_changed(self):
        """
        This test ensures that we don't uselessly checkpoint when getting back
        objects from the alive cache, which would hide changed values from the
        store.
        """
        foo = await self.store.get(Foo, 20)
        foo.title = "changed"
        await self.store.block_implicit_flushes()
        foo2 = await self.store.find(Foo, Foo.id == 20).one()
        await self.store.unblock_implicit_flushes()
        await self.store.commit()

        foo3 = await self.store.find(Foo, Foo.id == 20).one()
        self.assertEqual(foo3.title, "changed")

    async def test_obj_info_with_deleted_object_with_get(self):
        # Same thing, but using get rather than find.

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = await self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertEqual(obj_info.get_obj(), None)

        foo = await self.store.get(Foo, 20)
        self.assertTrue(foo)
        self.assertFalse(getattr(foo, "tainted", False))

    async def test_delete_object_when_obj_info_is_dirty(self):
        """Object should stay in memory if dirty."""

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = await self.store.get(Foo, 20)
        foo.title = "Changed"
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertTrue(obj_info.get_obj())

    async def test_get_tuple(self):
        class MyFoo(Foo):
            __storm_primary__ = "title", "id"
        foo = await self.store.get(MyFoo, ("Title 30", 10))
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.get(MyFoo, ("Title 20", 10))
        self.assertEqual(foo, None)

    async def test_of(self):
        foo = await self.store.get(Foo, 10)
        self.assertEqual(Store.of(foo), self.store)
        self.assertEqual(Store.of(Foo()), None)
        self.assertEqual(Store.of(object()), None)

    async def test_is_empty(self):
        result = await self.store.find(Foo, id=300)
        self.assertEqual(result.is_empty(), True)
        result = await self.store.find(Foo, id=30)
        self.assertEqual(result.is_empty(), False)

    async def test_is_empty_strips_order_by(self):
        """
        L{ResultSet.is_empty} strips the C{ORDER BY} clause, if one is
        present, since it isn't required to actually determine if a result set
        has any matching rows.  This should provide a performance improvement
        when the ordered result set would be large.
        """
        stream = StringIO()
        self.addCleanup(debug, False)
        debug(True, stream)

        result = await self.store.find(Foo, Foo.id == 300)
        result.order_by(Foo.id)
        self.assertEqual(True, result.is_empty())
        self.assertNotIn("ORDER BY", stream.getvalue())

    async def test_is_empty_with_composed_key(self):
        result = await self.store.find(Link, foo_id=300, bar_id=3000)
        self.assertEqual(result.is_empty(), True)
        result = await self.store.find(Link, foo_id=30, bar_id=300)
        self.assertEqual(result.is_empty(), False)

    async def test_is_empty_with_expression_find(self):
        result = await self.store.find(Foo.title, Foo.id == 300)
        self.assertEqual(result.is_empty(), True)
        result = await self.store.find(Foo.title, Foo.id == 30)
        self.assertEqual(result.is_empty(), False)

    async def test_find_iter(self):
        result = await self.store.find(Foo)

        lst = [(foo.id, foo.title) async for foo in result]
        lst.sort()
        self.assertEqual(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_find_from_cache(self):
        foo = await self.store.get(Foo, 10)
        self.assertTrue(await self.store.find(Foo, id=10).one() is foo)

    async def test_find_expr(self):
        result = await self.store.find(Foo, Foo.id == 20,
                                 Foo.title == "Title 20")
        self.assertEqual([(foo.id, foo.title) async for foo in result], [
                          (20, "Title 20"),
                         ])

        result = await self.store.find(Foo, Foo.id == 10,
                                 Foo.title == "Title 20")
        self.assertEqual([(foo.id, foo.title) async for foo in result], [
                         ])

    async def test_find_sql(self):
        foo = await self.store.find(Foo, SQL("foo.id = 20")).one()
        self.assertEqual(foo.title, "Title 20")

    async def test_find_str(self):
        foo = await self.store.find(Foo, "foo.id = 20").one()
        self.assertEqual(foo.title, "Title 20")

    async def test_find_keywords(self):
        result = await self.store.find(Foo, id=20, title="Title 20")
        self.assertEqual([(foo.id, foo.title) async for foo in result], [
                          (20, "Title 20")
                         ])

        result = await self.store.find(Foo, id=10, title="Title 20")
        self.assertEqual([(foo.id, foo.title) async for foo in result], [
                         ])

    async def test_find_order_by(self, *args):
        result = await self.store.find(Foo).order_by(Foo.title)
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    async def test_find_order_asc(self, *args):
        result = await self.store.find(Foo).order_by(Asc(Foo.title))
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    async def test_find_order_desc(self, *args):
        result = await self.store.find(Foo).order_by(Desc(Foo.title))
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_find_default_order_asc(self):
        class MyFoo(Foo):
            __storm_order__ = "title"

        result = await self.store.find(MyFoo)
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    async def test_find_default_order_desc(self):
        class MyFoo(Foo):
            __storm_order__ = "-title"

        result = await self.store.find(MyFoo)
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_find_default_order_with_tuple(self):
        class MyLink(Link):
            __storm_order__ = ("foo_id", "-bar_id")

        result = await self.store.find(MyLink)
        lst = [(link.foo_id, link.bar_id) async for link in result]
        self.assertEqual(lst, [
                          (10, 300),
                          (10, 200),
                          (10, 100),
                          (20, 200),
                          (20, 100),
                          (30, 300),
                         ])

    async def test_find_default_order_with_tuple_and_expr(self):
        class MyLink(Link):
            __storm_order__ = ("foo_id", Desc(Link.bar_id))

        result = await self.store.find(MyLink)
        lst = [(link.foo_id, link.bar_id) async for link in result]
        self.assertEqual(lst, [
                          (10, 300),
                          (10, 200),
                          (10, 100),
                          (20, 200),
                          (20, 100),
                          (30, 300),
                         ])

    async def test_find_index(self):
        """
        L{ResultSet.__getitem__} returns the object at the specified index.
        if a slice is used, a new L{ResultSet} is returned configured with the
        appropriate offset and limit.
        """
        foo = await self.store.find(Foo).order_by(Foo.title)[0]
        self.assertEqual(foo.id, 30)
        self.assertEqual(foo.title, "Title 10")

        foo = await self.store.find(Foo).order_by(Foo.title)[1]
        self.assertEqual(foo.id, 20)
        self.assertEqual(foo.title, "Title 20")

        foo = await self.store.find(Foo).order_by(Foo.title)[2]
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.find(Foo).order_by(Foo.title)[1:][1]
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        result = await self.store.find(Foo).order_by(Foo.title)
        self.assertRaises(IndexError, result.__getitem__, 3)

    async def test_find_slice(self):
        result = await self.store.find(Foo).order_by(Foo.title)[1:2]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(20, "Title 20")])

    async def test_find_slice_offset(self):
        result = await self.store.find(Foo).order_by(Foo.title)[1:]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(20, "Title 20"),
                          (10, "Title 30")])

    async def test_find_slice_offset_any(self):
        foo = await self.store.find(Foo).order_by(Foo.title)[1:].any()
        self.assertEqual(foo.id, 20)
        self.assertEqual(foo.title, "Title 20")

    async def test_find_slice_offset_one(self):
        foo = await self.store.find(Foo).order_by(Foo.title)[1:2].one()
        self.assertEqual(foo.id, 20)
        self.assertEqual(foo.title, "Title 20")

    async def test_find_slice_offset_first(self):
        foo = await self.store.find(Foo).order_by(Foo.title)[1:].first()
        self.assertEqual(foo.id, 20)
        self.assertEqual(foo.title, "Title 20")

    async def test_find_slice_offset_last(self):
        foo = await self.store.find(Foo).order_by(Foo.title)[1:].last()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

    async def test_find_slice_limit(self):
        result = await self.store.find(Foo).order_by(Foo.title)[:2]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(30, "Title 10"),
                          (20, "Title 20")])

    async def test_find_slice_limit_last(self):
        result = await self.store.find(Foo).order_by(Foo.title)[:2]
        self.assertRaises(FeatureError, result.last)

    async def test_find_slice_slice(self):
        result = await self.store.find(Foo).order_by(Foo.title)[0:2][1:3]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(20, "Title 20")])

        result = await self.store.find(Foo).order_by(Foo.title)[:2][1:3]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(20, "Title 20")])

        result = await self.store.find(Foo).order_by(Foo.title)[1:3][0:1]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(20, "Title 20")])

        result = await self.store.find(Foo).order_by(Foo.title)[1:3][:1]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst,
                         [(20, "Title 20")])

        result = await self.store.find(Foo).order_by(Foo.title)[5:5][1:1]
        lst = [(foo.id, foo.title) async for foo in result]
        self.assertEqual(lst, [])

    async def test_find_slice_order_by(self):
        result = await self.store.find(Foo)[2:]
        self.assertRaises(FeatureError, result.order_by, None)

        result = await self.store.find(Foo)[:2]
        self.assertRaises(FeatureError, result.order_by, None)

    async def test_find_slice_remove(self):
        result = await self.store.find(Foo)[2:]
        self.assertRaises(FeatureError, result.remove)

        result = await self.store.find(Foo)[:2]
        self.assertRaises(FeatureError, result.remove)

    async def test_find_contains(self):
        foo = await self.store.get(Foo, 10)
        result = await self.store.find(Foo)
        self.assertEqual(foo in result, True)
        result = await self.store.find(Foo, Foo.id == 20)
        self.assertEqual(foo in result, False)
        result = await self.store.find(Foo, "foo.id = 20")
        self.assertEqual(foo in result, False)

    async def test_find_contains_wrong_type(self):
        foo = await self.store.get(Foo, 10)
        bar = await self.store.get(Bar, 200)
        self.assertRaises(TypeError, operator.contains,
                          await self.store.find(Foo), bar)
        self.assertRaises(TypeError, operator.contains,
                          await self.store.find((Foo,)), foo)
        self.assertRaises(TypeError, operator.contains,
                          await self.store.find(Foo), (foo,))
        self.assertRaises(TypeError, operator.contains,
                          await self.store.find((Foo, Bar)), (bar, foo))

    async def test_find_contains_does_not_use_iter(self):
        def no_iter(self):
            raise RuntimeError()
        orig_iter = ResultSet.__iter__
        ResultSet.__iter__ = no_iter
        try:
            foo = await self.store.get(Foo, 10)
            result = await self.store.find(Foo)
            self.assertEqual(foo in result, True)
        finally:
            ResultSet.__iter__ = orig_iter

    async def test_find_contains_with_composed_key(self):
        link = await self.store.get(Link, (10, 100))
        result = await self.store.find(Link, Link.foo_id == 10)
        self.assertEqual(link in result, True)
        result = await self.store.find(Link, Link.bar_id == 200)
        self.assertEqual(link in result, False)

    async def test_find_contains_with_set_expression(self):
        foo = await self.store.get(Foo, 10)
        result1 = await self.store.find(Foo, Foo.id == 10)
        result2 = await self.store.find(Foo, Foo.id != 10)
        self.assertEqual(foo in result1.union(result2), True)

        if self.__class__.__name__.startswith("MySQL"):
            return
        self.assertEqual(foo in result1.intersection(result2), False)
        self.assertEqual(foo in result1.intersection(result1), True)
        self.assertEqual(foo in result1.difference(result2), True)
        self.assertEqual(foo in result1.difference(result1), False)

    async def test_find_any(self, *args):
        """
        L{ResultSet.any} returns an arbitrary objects from the result set.
        """
        self.assertNotEqual(None, await self.store.find(Foo).any())
        self.assertEqual(None, await self.store.find(Foo, id=40).any())

    async def test_find_any_strips_order_by(self):
        """
        L{ResultSet.any} strips the C{ORDER BY} clause, if one is present,
        since it isn't required.  This should provide a performance
        improvement when the ordered result set would be large.
        """
        stream = StringIO()
        self.addCleanup(debug, False)
        debug(True, stream)

        result = await self.store.find(Foo, Foo.id == 300)
        result.order_by(Foo.id)
        await result.any()
        self.assertNotIn("ORDER BY", stream.getvalue())

    async def test_find_first(self, *args):
        with self.assertRaises(UnorderedError):
            await self.store.find(Foo).first()

        foo = await self.store.find(Foo).order_by(Foo.title).first()
        self.assertEqual(foo.id, 30)
        self.assertEqual(foo.title, "Title 10")

        foo = await self.store.find(Foo).order_by(Foo.id).first()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.find(Foo, id=40).order_by(Foo.id).first()
        self.assertEqual(foo, None)

    async def test_find_last(self, *args):
        with self.assertRaises(UnorderedError):
            await self.store.find(Foo).last()

        foo = await self.store.find(Foo).order_by(Foo.title).last()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.find(Foo).order_by(Foo.id).last()
        self.assertEqual(foo.id, 30)
        self.assertEqual(foo.title, "Title 10")

        foo = await self.store.find(Foo, id=40).order_by(Foo.id).last()
        self.assertEqual(foo, None)

    async def test_find_last_desc(self, *args):
        foo = await self.store.find(Foo).order_by(Desc(Foo.title)).last()
        self.assertEqual(foo.id, 30)
        self.assertEqual(foo.title, "Title 10")

        foo = await self.store.find(Foo).order_by(Asc(Foo.id)).last()
        self.assertEqual(foo.id, 30)
        self.assertEqual(foo.title, "Title 10")

    async def test_find_one(self, *args):
        with self.assertRaises(NotOneError):
            await self.store.find(Foo).one()

        foo = await self.store.find(Foo, id=10).one()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.find(Foo, id=40).one()
        self.assertEqual(foo, None)

    async def test_find_count(self):
        self.assertEqual(await self.store.find(Foo).count(), 3)

    async def test_find_count_after_slice(self):
        """
        When we slice a ResultSet obtained after a set operation (like union),
        we get a fresh select that doesn't modify the limit and offset
        attribute of the original ResultSet.
        """
        result1 = await self.store.find(Foo, Foo.id == 10)
        result2 = await self.store.find(Foo, Foo.id == 20)
        result3 = result1.union(result2)
        result3.order_by(Foo.id)
        self.assertEqual(await result3.count(), 2)

        result_slice = [item async for item in result3[:2]]
        self.assertEqual(await result3.count(), 2)

    async def test_find_count_column(self):
        self.assertEqual(await self.store.find(Link).count(Link.foo_id), 6)

    async def test_find_count_column_distinct(self):
        count = await self.store.find(Link).count(Link.foo_id, distinct=True)
        self.assertEqual(count, 3)

    async def test_find_limit_count(self):
        result = await self.store.find(Link.foo_id)
        result.config(limit=2)
        count = await result.count()
        self.assertEqual(count, 2)

    async def test_find_offset_count(self):
        result = await self.store.find(Link.foo_id)
        result.config(offset=3)
        count = await result.count()
        self.assertEqual(count, 3)

    async def test_find_sliced_count(self):
        result = await self.store.find(Link.foo_id)
        count = await result[2:4].count()
        self.assertEqual(count, 2)

    async def test_find_distinct_count(self):
        result = await self.store.find(Link.foo_id)
        result.config(distinct=True)
        count = await result.count()
        self.assertEqual(count, 3)

    async def test_find_distinct_order_by_limit_count(self):
        result = await self.store.find(Foo)
        result.order_by(Foo.title)
        result.config(distinct=True, limit=3)
        count = await result.count()
        self.assertEqual(count, 3)

    async def test_find_distinct_count_multiple_columns(self):
        result = await self.store.find((Link.foo_id, Link.bar_id))
        result.config(distinct=True)
        count = await result.count()
        self.assertEqual(count, 6)

    async def test_find_count_column_with_implicit_distinct(self):
        result = await self.store.find(Link)
        result.config(distinct=True)
        count = await result.count(Link.foo_id)
        self.assertEqual(count, 6)

    async def test_find_max(self):
        self.assertEqual(await self.store.find(Foo).max(Foo.id), 30)

    async def test_find_max_expr(self):
        self.assertEqual(await self.store.find(Foo).max(Foo.id + 1), 31)

    async def test_find_max_unicode(self):
        title = await self.store.find(Foo).max(Foo.title)
        self.assertEqual(title, "Title 30")
        self.assertTrue(isinstance(title, str))

    async def test_find_max_with_empty_result_and_disallow_none(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int(allow_none=False)

        result = await self.store.find(Bar, Bar.id > 1000)
        self.assertTrue(result.is_empty())
        self.assertEqual(await result.max(Bar.foo_id), None)

    async def test_find_min(self):
        self.assertEqual(await self.store.find(Foo).min(Foo.id), 10)

    async def test_find_min_expr(self):
        self.assertEqual(await self.store.find(Foo).min(Foo.id - 1), 9)

    async def test_find_min_unicode(self):
        title = await self.store.find(Foo).min(Foo.title)
        self.assertEqual(title, "Title 10")
        self.assertTrue(isinstance(title, str))

    async def test_find_min_with_empty_result_and_disallow_none(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int(allow_none=False)

        result = await self.store.find(Bar, Bar.id > 1000)
        self.assertTrue(result.is_empty())
        self.assertEqual(await result.min(Bar.foo_id), None)

    async def test_find_avg(self):
        self.assertEqual(await self.store.find(Foo).avg(Foo.id), 20)

    async def test_find_avg_expr(self):
        self.assertEqual(await self.store.find(Foo).avg(Foo.id + 10), 30)

    async def test_find_avg_float(self):
        foo = Foo()
        foo.id = 15
        foo.title = "Title 15"
        await self.store.add(foo)
        self.assertEqual(await self.store.find(Foo).avg(Foo.id), 18.75)

    async def test_find_sum(self):
        self.assertEqual(await self.store.find(Foo).sum(Foo.id), 60)

    async def test_find_sum_expr(self):
        self.assertEqual(await self.store.find(Foo).sum(Foo.id * 2), 120)

    async def test_find_sum_with_empty_result_and_disallow_none(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int(allow_none=False)

        result = await self.store.find(Bar, Bar.id > 1000)
        self.assertTrue(result.is_empty())
        self.assertEqual(await result.sum(Bar.foo_id), None)

    async def test_find_max_order_by(self):
        """Interaction between order by and aggregation shouldn't break."""
        result = await self.store.find(Foo)
        self.assertEqual(result.order_by(Foo.id).max(Foo.id), 30)

    async def test_find_get_select_expr_without_columns(self):
        """
        A L{FeatureError} is raised if L{ResultSet.get_select_expr} is called
        without a list of L{Column}s.
        """
        result = await self.store.find(Foo)
        self.assertRaises(FeatureError, result.get_select_expr)

    async def test_find_get_select_expr(self):
        """
        Only the specified L{Column}s are included in the L{Select} expression
        provided by L{ResultSet.get_select_expr}.
        """
        foo = await self.store.get(Foo, 10)
        result1 = await self.store.find(Foo, Foo.id <= 10)
        subselect = result1.get_select_expr(Foo.id)
        self.assertEqual((Foo.id,), subselect.columns)
        result2 = await self.store.find(Foo, Foo.id.is_in(subselect))
        self.assertEqual([foo], list(result2))

    async def test_find_get_select_expr_with_set_expression(self):
        """
        A L{FeatureError} is raised if L{ResultSet.get_select_expr} is used
        with a L{ResultSet} that represents a set expression, such as a union.
        """
        result1 = await self.store.find(Foo, Foo.id == 10)
        result2 = await self.store.find(Foo, Foo.id == 20)
        result3 = result1.union(result2)
        self.assertRaises(FeatureError, result3.get_select_expr, Foo.id)

    async def test_find_values(self):
        values = await self.store.find(Foo).order_by(Foo.id).values(Foo.id)
        self.assertEqual(list(values), [10, 20, 30])

        values = await self.store.find(Foo).order_by(Foo.id).values(Foo.title)
        values = list(values)
        self.assertEqual(values, ["Title 30", "Title 20", "Title 10"])
        self.assertEqual([type(value) for value in values], [str, str, str])

    async def test_find_multiple_values(self):
        result = await self.store.find(Foo).order_by(Foo.id)
        values = result.values(Foo.id, Foo.title)
        self.assertEqual(list(values),
                         [(10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10")])

    async def test_find_values_with_no_arguments(self):
        result = await self.store.find(Foo).order_by(Foo.id)
        self.assertRaises(FeatureError, next, result.values())

    async def test_find_slice_values(self):
        values = await self.store.find(Foo).order_by(Foo.id)[1:2].values(Foo.id)
        self.assertEqual(list(values), [20])

    async def test_find_values_with_set_expression(self):
        """
        A L{FeatureError} is raised if L{ResultSet.values} is used with a
        L{ResultSet} that represents a set expression, such as a union.
        """
        result1 = await self.store.find(Foo, Foo.id == 10)
        result2 = await self.store.find(Foo, Foo.id == 20)
        result3 = result1.union(result2)
        self.assertRaises(FeatureError, list, result3.values(Foo.id))

    async def test_find_remove(self):
        await self.store.find(Foo, Foo.id == 20).remove()
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    async def test_find_cached(self):
        foo = await self.store.get(Foo, 20)
        bar = await self.store.get(Bar, 200)
        self.assertTrue(foo)
        self.assertTrue(bar)
        self.assertEqual(await self.store.find(Foo).cached(), [foo])

    async def test_find_cached_where(self):
        foo1 = await self.store.get(Foo, 10)
        foo2 = await self.store.get(Foo, 20)
        bar = await self.store.get(Bar, 200)
        self.assertTrue(foo1)
        self.assertTrue(foo2)
        self.assertTrue(bar)
        self.assertEqual(await self.store.find(Foo, title="Title 20").cached(),
                         [foo2])

    async def test_find_cached_invalidated(self):
        foo = await self.store.get(Foo, 20)
        await self.store.invalidate(foo)
        self.assertEqual(await self.store.find(Foo).cached(), [foo])

    async def test_find_cached_invalidated_and_deleted(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("DELETE FROM foo WHERE id=20")
        await self.store.invalidate(foo)
        # Do not look for the primary key (id), since it's able to get
        # it without touching the database. Use the title instead.
        self.assertEqual(await self.store.find(Foo, title="Title 20").cached(), [])

    async def test_find_cached_with_info_alive_and_object_dead(self):
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = await self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        cached = await self.store.find(Foo).cached()
        self.assertEqual(len(cached), 1)
        foo = await self.store.get(Foo, 20)
        self.assertFalse(hasattr(foo, "tainted"))

    async def test_using_find_join(self):
        bar = await self.store.get(Bar, 100)
        bar.foo_id = None

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find(Bar).order_by(Foo.id, Bar.id)
        lst = [bar and (bar.id, bar.title) async for bar in result]
        self.assertEqual(lst, [
                          None,
                          (200, "Title 200"),
                          (300, "Title 100"),
                         ])

    async def test_using_find_with_strings(self):
        foo = await self.store.using("foo").find(Foo, id=10).one()
        self.assertEqual(foo.title, "Title 30")

        foo = await self.store.using("foo", "bar").find(Foo, id=10).any()
        self.assertEqual(foo.title, "Title 30")

    async def test_using_find_join_with_strings(self):
        bar = await self.store.get(Bar, 100)
        bar.foo_id = None

        tables = self.store.using(LeftJoin("foo", "bar",
                                           "bar.foo_id = foo.id"))
        result = tables.find(Bar).order_by(Foo.id, Bar.id)
        lst = [bar and (bar.id, bar.title) async for bar in result]
        self.assertEqual(lst, [
                          None,
                          (200, "Title 200"),
                          (300, "Title 100"),
                         ])

    async def test_find_tuple(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        result = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        result = result.order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        self.assertEqual(lst, [
                          ((10, "Title 30"), (100, "Title 300")),
                          ((30, "Title 10"), (300, "Title 100")),
                         ])

    async def test_find_tuple_using(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find((Foo, Bar)).order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        self.assertEqual(lst, [
                          ((10, "Title 30"), (100, "Title 300")),
                          ((20, "Title 20"), None),
                          ((30, "Title 10"), (300, "Title 100")),
                         ])

    async def test_find_tuple_using_with_disallow_none(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True, allow_none=False)
            title = Unicode()
            foo_id = Int()
            foo = Reference(foo_id, Foo.id)

        bar = await self.store.get(Bar, 200)
        await self.store.remove(bar)

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find((Foo, Bar)).order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        self.assertEqual(lst, [
                          ((10, "Title 30"), (100, "Title 300")),
                          ((20, "Title 20"), None),
                          ((30, "Title 10"), (300, "Title 100")),
                         ])

    async def test_find_tuple_using_skip_when_none(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        tables = self.store.using(Foo,
                                  LeftJoin(Bar, Bar.foo_id == Foo.id),
                                  LeftJoin(Link, Link.bar_id == Bar.id))
        result = tables.find((Bar, Link)).order_by(Foo.id, Bar.id, Link.foo_id)
        lst = [(bar and (bar.id, bar.title),
                link and (link.bar_id, link.foo_id))
               for (bar, link) in result]
        self.assertEqual(lst, [
                          ((100, "Title 300"), (100, 10)),
                          ((100, "Title 300"), (100, 20)),
                          (None, None),
                          ((300, "Title 100"), (300, 10)),
                          ((300, "Title 100"), (300, 30)),
                         ])

    async def test_find_tuple_contains(self):
        foo = await self.store.get(Foo, 10)
        bar = await self.store.get(Bar, 100)
        bar200 = await self.store.get(Bar, 200)
        result = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        self.assertEqual((foo, bar) in result, True)
        self.assertEqual((foo, bar200) in result, False)

    async def test_find_tuple_contains_with_set_expression(self):
        foo = await self.store.get(Foo, 10)
        bar = await self.store.get(Bar, 100)
        bar200 = await self.store.get(Bar, 200)
        result1 = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        result2 = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        self.assertEqual((foo, bar) in result1.union(result2), True)

        if self.__class__.__name__.startswith("MySQL"):
            return
        self.assertEqual((foo, bar) in result1.intersection(result2), True)
        self.assertEqual((foo, bar) in result1.difference(result2), False)

    async def test_find_tuple_any(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        result = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = await result.order_by(Foo.id).any()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")
        self.assertEqual(bar.id, 100)
        self.assertEqual(bar.title, "Title 300")

    async def test_find_tuple_first(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        result = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = await result.order_by(Foo.id).first()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")
        self.assertEqual(bar.id, 100)
        self.assertEqual(bar.title, "Title 300")

    async def test_find_tuple_last(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        result = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = await result.order_by(Foo.id).last()
        self.assertEqual(foo.id, 30)
        self.assertEqual(foo.title, "Title 10")
        self.assertEqual(bar.id, 300)
        self.assertEqual(bar.title, "Title 100")

    async def test_find_tuple_one(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None

        result = await self.store.find((Foo, Bar),
                                 Bar.foo_id == Foo.id, Foo.id == 10)
        foo, bar = await result.order_by(Foo.id).one()
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")
        self.assertEqual(bar.id, 100)
        self.assertEqual(bar.title, "Title 300")

    async def test_find_tuple_count(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None
        result = await self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        self.assertEqual(await result.count(), 2)

    async def test_find_tuple_remove(self):
        result = await self.store.find((Foo, Bar))
        self.assertRaises(FeatureError, result.remove)

    async def test_find_tuple_set(self):
        result = await self.store.find((Foo, Bar))
        self.assertRaises(FeatureError, result.set, title="Title 40")

    async def test_find_tuple_kwargs(self):
        self.assertRaises(FeatureError,
                          self.store.find, (Foo, Bar), title="Title 10")

    async def test_find_tuple_cached(self):
        result = await self.store.find((Foo, Bar))
        self.assertRaises(FeatureError, result.cached)

    async def test_find_using_cached(self):
        result = self.store.using(Foo, Bar).find(Foo)
        self.assertRaises(FeatureError, result.cached)

    async def test_find_with_expr(self):
        result = await self.store.find(Foo.title)
        self.assertEqual(sorted(result),
                         ["Title 10", "Title 20", "Title 30"])

    async def test_find_with_expr_uses_variable_set(self):
        result = await self.store.find(FooVariable.title,
                                 FooVariable.id == 10)
        self.assertEqual([item async for item in result], ["to_py(from_db(Title 30))"])

    async def test_find_tuple_with_expr(self):
        result = await self.store.find((Foo, Bar.id, Bar.title),
                                 Bar.foo_id == Foo.id)
        result.order_by(Foo.id)
        self.assertEqual([(foo.id, foo.title, bar_id, bar_title)
                          for foo, bar_id, bar_title in result],
                          [(10, "Title 30", 100, "Title 300"),
                           (20, "Title 20", 200, "Title 200"),
                           (30, "Title 10", 300, "Title 100")])

    async def test_find_using_with_expr(self):
        result = self.store.using(Foo).find(Foo.title)
        self.assertEqual(sorted(result),
                         ["Title 10", "Title 20", "Title 30"])

    async def test_find_with_expr_contains(self):
        result = await self.store.find(Foo.title)
        self.assertEqual("Title 10" in result, True)
        self.assertEqual("Title 42" in result, False)

    async def test_find_tuple_with_expr_contains(self):
        foo = await self.store.get(Foo, 10)
        result = await self.store.find((Foo, Bar.title),
                                 Bar.foo_id == Foo.id)
        self.assertEqual((foo, "Title 300") in result, True)
        self.assertEqual((foo, "Title 100") in result, False)

    async def test_find_with_expr_contains_with_set_expression(self):
        result1 = await self.store.find(Foo.title)
        result2 = await self.store.find(Foo.title)
        self.assertEqual("Title 10" in result1.union(result2), True)

        if self.__class__.__name__.startswith("MySQL"):
            return
        self.assertEqual("Title 10" in result1.intersection(result2), True)
        self.assertEqual("Title 10" in result1.difference(result2), False)

    async def test_find_with_expr_remove_unsupported(self):
        result = await self.store.find(Foo.title)
        self.assertRaises(FeatureError, result.remove)

    async def test_find_tuple_with_expr_remove_unsupported(self):
        result = await self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        self.assertRaises(FeatureError, result.remove)

    async def test_find_with_expr_count(self):
        result = await self.store.find(Foo.title)
        self.assertEqual(await result.count(), 3)

    async def test_find_tuple_with_expr_count(self):
        result = await self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        self.assertEqual(await result.count(), 3)

    async def test_find_with_expr_values(self):
        result = await self.store.find(Foo.title)
        self.assertEqual(sorted(result.values(Foo.title)),
                         ["Title 10", "Title 20", "Title 30"])

    async def test_find_tuple_with_expr_values(self):
        result = await self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        self.assertEqual(sorted(result.values(Foo.title)),
                         ["Title 10", "Title 20", "Title 30"])

    async def test_find_with_expr_set_unsupported(self):
        result = await self.store.find(Foo.title)
        self.assertRaises(FeatureError, result.set)

    async def test_find_tuple_with_expr_set_unsupported(self):
        result = await self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        self.assertRaises(FeatureError, result.set)

    async def test_find_with_expr_cached_unsupported(self):
        result = await self.store.find(Foo.title)
        self.assertRaises(FeatureError, result.cached)

    async def test_find_tuple_with_expr_cached_unsupported(self):
        result = await self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        self.assertRaises(FeatureError, result.cached)

    async def test_find_with_expr_union(self):
        result1 = await self.store.find(Foo.title, Foo.id == 10)
        result2 = await self.store.find(Foo.title, Foo.id != 10)
        result = result1.union(result2)
        self.assertEqual(sorted(result),
                         ["Title 10", "Title 20", "Title 30",])

    async def test_find_with_expr_union_mismatch(self):
        result1 = await self.store.find(Foo.title)
        result2 = await self.store.find(Bar.foo_id)
        self.assertRaises(FeatureError, result1.union, result2)

    async def test_find_tuple_with_expr_union(self):
        result1 = await self.store.find(
            (Foo, Bar.title), Bar.foo_id == Foo.id, Bar.title == "Title 100")
        result2 = await self.store.find(
            (Foo, Bar.title), Bar.foo_id == Foo.id, Bar.title == "Title 200")
        result = result1.union(result2)
        self.assertEqual(sorted((foo.id, title) for (foo, title) in result),
                         [(20, "Title 200"), (30, "Title 100")])

    async def test_get_does_not_validate(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo:
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator)

        foo = await self.store.get(Foo, 10)
        self.assertEqual(foo.title, "Title 30")

    async def test_get_does_not_validate_default_value(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo:
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator, default="default value")

        foo = await self.store.get(Foo, 10)
        self.assertEqual(foo.title, "Title 30")

    async def test_find_does_not_validate(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo:
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator)

        foo = await self.store.find(Foo, Foo.id == 10).one()
        self.assertEqual(foo.title, "Title 30")

    async def test_find_group_by(self):
        result = await self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        result.order_by(Count(FooValue.id), Sum(FooValue.value1))
        result = [item async for item in result]
        self.assertEqual(result, [(2, 2), (2, 2), (2, 3), (3, 6)])

    async def test_find_group_by_table(self):
        result = await self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        foo1 = await self.store.get(Foo, 10)
        foo2 = await self.store.get(Foo, 20)
        self.assertEqual([item async for item in result], [(5, foo1), (16, foo2)])

    async def test_find_group_by_table_contains(self):
        result = await self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        foo1 = await self.store.get(Foo, 10)
        self.assertEqual((5, foo1) in result, True)

    async def test_find_group_by_multiple_tables(self):
        result = await self.store.find(
            Sum(FooValue.value2), Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.order_by(Sum(FooValue.value2))
        result = [item async for item in result]
        self.assertEqual(result, [5, 16])

        result = await self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        result.order_by(Sum(FooValue.value2))
        result = [item async for item in result]
        foo1 = await self.store.get(Foo, 10)
        foo2 = await self.store.get(Foo, 20)
        self.assertEqual(result, [(5, foo1), (16, foo2)])

        result = await self.store.find(
            (Foo.id, Sum(FooValue.value2), Avg(FooValue.value1)),
            Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.order_by(Foo.id)
        result = [item async for item in result]
        self.assertEqual(result, [(10, 5, 2),
                                  (20, 16, 1)])

    async def test_find_group_by_having(self):
        result = await self.store.find(
            Sum(FooValue.value2), Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.having(Sum(FooValue.value2) == 5)
        self.assertEqual([item async for item in result], [5])
        result = await self.store.find(
            Sum(FooValue.value2), Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.having(Count() == 5)
        self.assertEqual([item async for item in result], [16])

    async def test_find_having_without_group_by(self):
        result = await self.store.find(FooValue)
        self.assertRaises(FeatureError, result.having, FooValue.value1 == 1)

    async def test_find_group_by_multiple_having(self):
        result = await self.store.find((Count(), FooValue.value2))
        result.group_by(FooValue.value2)
        result.having(Count() == 2, FooValue.value2 >= 3)
        result.order_by(Count(), FooValue.value2)
        list_result = [item async for item in result]
        self.assertEqual(list_result, [(2, 3), (2, 4)])

    async def test_find_successive_group_by(self):
        result = await self.store.find(Count())
        result.group_by(FooValue.value2)
        result.order_by(Count())
        list_result = [item async for item in result]
        self.assertEqual(list_result, [2, 2, 2, 3])
        result.group_by(FooValue.value1)
        list_result = [item async for item in result]
        self.assertEqual(list_result, [4, 5])

    async def test_find_multiple_group_by(self):
        result = await self.store.find(Count())
        result.group_by(FooValue.value2, FooValue.value1)
        result.order_by(Count())
        list_result = [item async for item in result]
        self.assertEqual(list_result, [1, 1, 2, 2, 3])

    async def test_find_multiple_group_by_with_having(self):
        result = await self.store.find((Count(), FooValue.value2))
        result.group_by(FooValue.value2, FooValue.value1).having(Count() == 2)
        result.order_by(Count(), FooValue.value2)
        list_result = [item async for item in result]
        self.assertEqual(list_result, [(2, 3), (2, 4)])

    async def test_find_group_by_avg(self):
        result = await self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        self.assertRaises(FeatureError, result.avg, FooValue.value2)

    async def test_find_group_by_values(self):
        result = await self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        result.order_by(Foo.title)
        result = list(result.values(Foo.title))
        self.assertEqual(result, ['Title 20', 'Title 30'])

    async def test_find_group_by_union(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=10)
        result3 = result1.union(result2)
        self.assertRaises(FeatureError, result3.group_by, Foo.title)

    async def test_find_group_by_remove(self):
        result = await self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        self.assertRaises(FeatureError, result.remove)

    async def test_find_group_by_set(self):
        result = await self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        self.assertRaises(FeatureError, result.set, FooValue.value1 == 1)

    async def test_add_commit(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "Title 40"),
                         ])

    async def test_add_rollback_commit(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)
        await self.store.rollback()

        self.assertEqual(await self.store.get(Foo, 3), None)

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_add_get(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)

        old_foo = foo

        foo = await self.store.get(Foo, 40)

        self.assertEqual(foo.id, 40)
        self.assertEqual(foo.title, "Title 40")

        self.assertTrue(foo is old_foo)

    async def test_add_find(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)

        old_foo = foo

        foo = await self.store.find(Foo, Foo.id == 40).one()

        self.assertEqual(foo.id, 40)
        self.assertEqual(foo.title, "Title 40")

        self.assertTrue(foo is old_foo)

    async def test_add_twice(self):
        foo = Foo()
        await self.store.add(foo)
        await self.store.add(foo)
        self.assertEqual(Store.of(foo), self.store)

    async def test_add_loaded(self):
        foo = await self.store.get(Foo, 10)
        await self.store.add(foo)
        self.assertEqual(Store.of(foo), self.store)

    async def test_add_twice_to_wrong_store(self):
        foo = Foo()
        await self.store.add(foo)
        self.assertRaises(WrongStoreError, Store(self.database).add, foo)

    async def test_add_checkpoints(self):
        bar = Bar()
        await self.store.add(bar)

        bar.id = 400
        bar.title = "Title 400"
        bar.foo_id = 40

        await self.store.flush()
        await self.store.execute("UPDATE bar SET title='Title 500' "
                           "WHERE id=400")
        bar.foo_id = 400

        # When not checkpointing, this flush will set title again.
        await self.store.flush()
        await self.store.reload(bar)
        self.assertEqual(bar.title, "Title 500")

    async def test_add_completely_undefined(self):
        foo = Foo()
        await self.store.add(foo)
        await self.store.flush()
        self.assertEqual(type(foo.id), int)
        self.assertEqual(foo.title, "Default Title")

    async def test_add_uuid(self):
        unique_id = await self.store.add(UniqueID(uuid4()))
        self.assertEqual(unique_id, await self.store.find(UniqueID).one())

    async def test_remove_commit(self):
        foo = await self.store.get(Foo, 20)
        await self.store.remove(foo)
        self.assertEqual(Store.of(foo), self.store)
        await self.store.flush()
        self.assertEqual(Store.of(foo), None)

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])
        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        await self.store.commit()
        self.assertEqual(Store.of(foo), None)

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    async def test_remove_rollback_update(self):
        foo = await self.store.get(Foo, 20)

        await self.store.remove(foo)
        await self.store.rollback()

        foo.title = "Title 200"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    async def test_remove_flush_rollback_update(self):
        foo = await self.store.get(Foo, 20)

        await self.store.remove(foo)
        await self.store.flush()
        await self.store.rollback()

        foo.title = "Title 200"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_remove_add_update(self):
        foo = await self.store.get(Foo, 20)

        await self.store.remove(foo)
        await self.store.add(foo)

        foo.title = "Title 200"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    async def test_remove_flush_add_update(self):
        foo = await self.store.get(Foo, 20)

        await self.store.remove(foo)
        await self.store.flush()
        await self.store.add(foo)

        foo.title = "Title 200"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    async def test_remove_twice(self):
        foo = await self.store.get(Foo, 10)
        await self.store.remove(foo)
        await self.store.remove(foo)

    async def test_remove_unknown(self):
        foo = Foo()
        self.assertRaises(WrongStoreError, self.store.remove, foo)

    async def test_remove_from_wrong_store(self):
        foo = await self.store.get(Foo, 20)
        self.assertRaises(WrongStoreError, Store(self.database).remove, foo)

    async def test_wb_remove_flush_update_isnt_dirty(self):
        foo = await self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        await self.store.remove(foo)
        await self.store.flush()

        foo.title = "Title 200"

        self.assertTrue(obj_info not in self.store._dirty)

    async def test_wb_remove_rollback_isnt_dirty(self):
        foo = await self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        await self.store.remove(foo)
        await self.store.rollback()

        self.assertTrue(obj_info not in self.store._dirty)

    async def test_wb_remove_flush_rollback_isnt_dirty(self):
        foo = await self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        await self.store.remove(foo)
        await self.store.flush()
        await self.store.rollback()

        self.assertTrue(obj_info not in self.store._dirty)

    async def test_add_rollback_not_in_store(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)
        await self.store.rollback()

        self.assertEqual(Store.of(foo), None)

    async def test_update_flush_commit(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])
        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])
        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    async def test_update_flush_reload_rollback(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"
        await self.store.flush()
        await self.store.reload(foo)
        await self.store.rollback()
        self.assertEqual(foo.title, "Title 20")

    async def test_update_commit(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    async def test_update_commit_twice(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"
        await self.store.commit()
        foo.title = "Title 2000"
        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 2000"),
                          (30, "Title 10"),
                         ])

    async def test_update_checkpoints(self):
        bar = await self.store.get(Bar, 200)
        bar.title = "Title 400"
        await self.store.flush()
        await self.store.execute("UPDATE bar SET title='Title 500' "
                           "WHERE id=200")
        bar.foo_id = 40
        # When not checkpointing, this flush will set title again.
        await self.store.flush()
        await self.store.reload(bar)
        self.assertEqual(bar.title, "Title 500")

    async def test_update_primary_key(self):
        foo = await self.store.get(Foo, 20)
        foo.id = 25

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (25, "Title 20"),
                          (30, "Title 10"),
                         ])

        # Update twice to see if the notion of primary key for the
        # existent object was updated as well.

        foo.id = 27

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 30"),
                          (27, "Title 20"),
                          (30, "Title 10"),
                         ])

        # Ensure only the right ones are there.

        self.assertTrue(await self.store.get(Foo, 27) is foo)
        self.assertTrue(await self.store.get(Foo, 25) is None)
        self.assertTrue(await self.store.get(Foo, 20) is None)

    async def test_update_primary_key_exchange(self):
        foo1 = await self.store.get(Foo, 10)
        foo2 = await self.store.get(Foo, 30)

        foo1.id = 40
        await self.store.flush()
        foo2.id = 10
        await self.store.flush()
        foo1.id = 30

        self.assertTrue(await self.store.get(Foo, 30) is foo1)
        self.assertTrue(await self.store.get(Foo, 10) is foo2)

        await self.store.commit()

        self.assertEqual(await self.get_committed_items(), [
                          (10, "Title 10"),
                          (20, "Title 20"),
                          (30, "Title 30"),
                         ])

    async def test_wb_update_not_dirty_after_flush(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"

        await self.store.flush()

        # If changes get committed even with the notification disabled,
        # it means the dirty flag isn't being cleared.

        self.store._disable_change_notification(get_obj_info(foo))

        foo.title = "Title 2000"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    async def test_update_find(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"

        result = await self.store.find(Foo, Foo.title == "Title 200")

        self.assertTrue(await result.one() is foo)

    async def test_update_get(self):
        foo = await self.store.get(Foo, 20)
        foo.id = 200

        self.assertTrue(await self.store.get(Foo, 200) is foo)

    async def test_add_update(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)

        foo.title = "Title 400"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "Title 400"),
                         ])

    async def test_add_remove_add(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)
        await self.store.remove(foo)

        self.assertEqual(Store.of(foo), None)

        foo.title = "Title 400"

        await self.store.add(foo)

        foo.id = 400

        await self.store.commit()

        self.assertEqual(Store.of(foo), self.store)

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (400, "Title 400"),
                         ])

        self.assertTrue(await self.store.get(Foo, 400) is foo)

    async def test_wb_add_remove_add(self):
        foo = Foo()
        obj_info = get_obj_info(foo)
        await self.store.add(foo)
        self.assertTrue(obj_info in self.store._dirty)
        await self.store.remove(foo)
        self.assertTrue(obj_info not in self.store._dirty)
        await self.store.add(foo)
        self.assertTrue(obj_info in self.store._dirty)
        self.assertTrue(Store.of(foo) is self.store)

    async def test_wb_update_remove_add(self):
        foo = await self.store.get(Foo, 20)
        foo.title = "Title 200"

        obj_info = get_obj_info(foo)

        await self.store.remove(foo)
        await self.store.add(foo)

        self.assertTrue(obj_info in self.store._dirty)

    async def test_commit_autoreloads(self):
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "Title 20")
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEqual(foo.title, "Title 20")
        await self.store.commit()
        self.assertEqual(foo.title, "New Title")

    async def test_commit_invalidates(self):
        foo = await self.store.get(Foo, 20)
        self.assertTrue(foo)
        await self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEqual(await self.store.get(Foo, 20), foo)
        await self.store.commit()
        self.assertEqual(await self.store.get(Foo, 20), None)

    async def test_rollback_autoreloads(self):
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "Title 20")
        await self.store.rollback()
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEqual(foo.title, "New Title")

    async def test_rollback_invalidates(self):
        foo = await self.store.get(Foo, 20)
        self.assertTrue(foo)
        self.assertEqual(await self.store.get(Foo, 20), foo)
        await self.store.rollback()
        await self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEqual(await self.store.get(Foo, 20), None)

    async def test_sub_class(self):
        class SubFoo(Foo):
            id = Float(primary=True)

        foo1 = await self.store.get(Foo, 20)
        foo2 = await self.store.get(SubFoo, 20)

        self.assertEqual(foo1.id, 20)
        self.assertEqual(foo2.id, 20)
        self.assertEqual(type(foo1.id), int)
        self.assertEqual(type(foo2.id), float)

    async def test_join(self):

        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        bar = Bar()
        bar.id = 40
        bar.title = "Title 20"

        await self.store.add(bar)

        # Add anbar object with the same title to ensure DISTINCT
        # is in place.

        bar = Bar()
        bar.id = 400
        bar.title = "Title 20"

        await self.store.add(bar)

        result = await self.store.find(Foo, Foo.title == Bar.title)

        self.assertEqual([(foo.id, foo.title) async for foo in result], [
                          (20, "Title 20"),
                          (20, "Title 20"),
                         ])

    async def test_join_distinct(self):

        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        bar = Bar()
        bar.id = 40
        bar.title = "Title 20"

        await self.store.add(bar)

        # Add a bar object with the same title to ensure DISTINCT
        # is in place.

        bar = Bar()
        bar.id = 400
        bar.title = "Title 20"

        await self.store.add(bar)

        result = await self.store.find(Foo, Foo.title == Bar.title)
        result.config(distinct=True)

        # Make sure that it won't unset it, and that it's returning itself.
        config = result.config()

        self.assertEqual([(foo.id, foo.title) async for foo in result], [
                          (20, "Title 20"),
                         ])

    async def test_sub_select(self):
        foo = await self.store.find(Foo, Foo.id == Select(SQL("20"))).one()
        self.assertTrue(foo)
        self.assertEqual(foo.id, 20)
        self.assertEqual(foo.title, "Title 20")

    async def test_cache_has_improper_object(self):
        foo = await self.store.get(Foo, 20)
        await self.store.remove(foo)
        await self.store.commit()

        await self.store.execute("INSERT INTO foo VALUES (20, 'Title 20')")

        self.assertTrue(await self.store.get(Foo, 20) is not foo)

    async def test_cache_has_improper_object_readded(self):
        foo = await self.store.get(Foo, 20)
        await self.store.remove(foo)

        await self.store.flush()

        old_foo = foo # Keep a reference.

        foo = Foo()
        foo.id = 20
        foo.title = "Readded"
        await self.store.add(foo)

        await self.store.commit()

        self.assertTrue(await self.store.get(Foo, 20) is foo)

    async def test_loaded_hook(self):

        loaded = []

        class MyFoo(Foo):
            def __init__(self):
                loaded.append("NO!")
            def __storm_loaded__(self):
                loaded.append((self.id, self.title))
                self.title = "Title 200"
                self.some_attribute = 1

        foo = await self.store.get(MyFoo, 20)

        self.assertEqual(loaded, [(20, "Title 20")])
        self.assertEqual(foo.title, "Title 200")
        self.assertEqual(foo.some_attribute, 1)

        foo.some_attribute = 2

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

        await self.store.rollback()

        self.assertEqual(foo.title, "Title 20")
        self.assertEqual(foo.some_attribute, 2)

    async def test_flush_hook(self):

        class MyFoo(Foo):
            counter = 0
            def __storm_pre_flush__(self):
                if self.counter == 0:
                    self.title = "Flushing: %s" % self.title
                self.counter += 1

        foo = await self.store.get(MyFoo, 20)

        self.assertEqual(foo.title, "Title 20")
        await self.store.flush()
        self.assertEqual(foo.title, "Title 20") # It wasn't dirty.
        foo.title = "Something"
        await self.store.flush()
        self.assertEqual(foo.title, "Flushing: Something")

        # It got in the database, because it was flushed *twice* (the
        # title was changed after flushed, and thus the object got dirty
        # again).
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Flushing: Something"),
                          (30, "Title 10"),
                         ])

        # This shouldn't do anything, because the object is clean again.
        foo.counter = 0
        await self.store.flush()
        self.assertEqual(foo.title, "Flushing: Something")

    async def test_flush_hook_all(self):

        class MyFoo(Foo):
            def __storm_pre_flush__(self):
                other = [foo1, foo2][foo1 is self]
                other.title = "Changed in hook: " + other.title

        foo1 = await self.store.get(MyFoo, 10)
        foo2 = await self.store.get(MyFoo, 20)
        foo1.title = "Changed"
        await self.store.flush()

        self.assertEqual(foo1.title, "Changed in hook: Changed")
        self.assertEqual(foo2.title, "Changed in hook: Title 20")

    async def test_flushed_hook(self):

        class MyFoo(Foo):
            done = False
            def __storm_flushed__(self):
                if not self.done:
                    self.done = True
                    self.title = "Flushed: %s" % self.title

        foo = await self.store.get(MyFoo, 20)

        self.assertEqual(foo.title, "Title 20")
        await self.store.flush()
        self.assertEqual(foo.title, "Title 20") # It wasn't dirty.
        foo.title = "Something"
        await self.store.flush()
        self.assertEqual(foo.title, "Flushed: Something")

        # It got in the database, because it was flushed *twice* (the
        # title was changed after flushed, and thus the object got dirty
        # again).
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Flushed: Something"),
                          (30, "Title 10"),
                         ])

        # This shouldn't do anything, because the object is clean again.
        foo.done = False
        await self.store.flush()
        self.assertEqual(foo.title, "Flushed: Something")

    async def test_retrieve_default_primary_key(self):
        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)
        await self.store.flush()
        self.assertNotEqual(foo.id, None)
        self.assertTrue(await self.store.get(Foo, foo.id) is foo)

    async def test_retrieve_default_value(self):
        foo = Foo()
        foo.id = 40
        await self.store.add(foo)
        await self.store.flush()
        self.assertEqual(foo.title, "Default Title")

    async def test_retrieve_null_when_no_default(self):
        bar = Bar()
        bar.id = 400
        await self.store.add(bar)
        await self.store.flush()
        self.assertEqual(bar.title, None)

    async def test_wb_remove_prop_not_dirty(self):
        foo = await self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        del foo.title
        self.assertTrue(obj_info not in self.store._dirty)

    async def test_flush_with_removed_prop(self):
        foo = await self.store.get(Foo, 20)
        del foo.title
        await self.store.flush()
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_flush_with_removed_prop_forced_dirty(self):
        foo = await self.store.get(Foo, 20)
        del foo.title
        foo.id = 40
        foo.id = 20
        await self.store.flush()
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_flush_with_removed_prop_really_dirty(self):
        foo = await self.store.get(Foo, 20)
        del foo.title
        foo.id = 25
        await self.store.flush()
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (25, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_wb_block_implicit_flushes(self):
        # Make sure calling await store.flush(await store.flush() will fail.
        def flush():
            raise RuntimeError("Flush called")
        self.store.flush = flush

        # The following operations do not call flush.
        await self.store.block_implicit_flushes()
        foo = await self.store.get(Foo, 20)
        foo = await self.store.find(Foo, Foo.id == 20).one()
        await self.store.execute("SELECT title FROM foo WHERE id = 20")

        await self.store.unblock_implicit_flushes()
        self.assertRaises(RuntimeError, self.store.get, Foo, 20)

    async def test_wb_block_implicit_flushes_is_recursive(self):
        # Make sure calling await store.flush(await store.flush() will fail.
        def flush():
            raise RuntimeError("Flush called")
        self.store.flush = flush

        await self.store.block_implicit_flushes()
        await self.store.block_implicit_flushes()
        await self.store.unblock_implicit_flushes()
        # implicit flushes are still blocked, until unblock() is called again.
        foo = await self.store.get(Foo, 20)
        await self.store.unblock_implicit_flushes()
        self.assertRaises(RuntimeError, self.store.get, Foo, 20)

    async def test_block_access(self):
        """Access to the store is blocked by block_access()."""
        # The set_blocked() method blocks access to the connection.
        self.store.block_access()
        self.assertRaises(ConnectionBlockedError,
                          self.store.execute, "SELECT 1")
        self.assertRaises(ConnectionBlockedError, self.store.commit)
        # The rollback method is not blocked.
        await self.store.rollback()
        self.store.unblock_access()
        await self.store.execute("SELECT 1")

    async def test_reload(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='Title 40' WHERE id=20")
        self.assertEqual(foo.title, "Title 20")
        await self.store.reload(foo)
        self.assertEqual(foo.title, "Title 40")

    async def test_reload_not_changed(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='Title 40' WHERE id=20")
        await self.store.reload(foo)
        for variable in get_obj_info(foo).variables.values():
            self.assertFalse(variable.has_changed())

    async def test_reload_new(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"
        self.assertRaises(WrongStoreError, self.store.reload, foo)

    async def test_reload_new_unflushed(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"
        await self.store.add(foo)
        self.assertRaises(NotFlushedError, self.store.reload, foo)

    async def test_reload_removed(self):
        foo = await self.store.get(Foo, 20)
        await self.store.remove(foo)
        await self.store.flush()
        self.assertRaises(WrongStoreError, self.store.reload, foo)

    async def test_reload_unknown(self):
        foo = await self.store.get(Foo, 20)
        store = await self.create_store()
        self.assertRaises(WrongStoreError, store.reload, foo)

    async def test_wb_reload_not_dirty(self):
        foo = await self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        foo.title = "Title 40"
        await self.store.reload(foo)
        self.assertTrue(obj_info not in self.store._dirty)

    async def test_find_set_empty(self):
        await self.store.find(Foo, title="Title 20").set()
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "Title 20")

    async def test_find_set(self):
        await self.store.find(Foo, title="Title 20").set(title="Title 40")
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "Title 40")

    async def test_find_set_with_func_expr(self):
        await self.store.find(Foo, title="Title 20").set(title=Lower("Title 40"))
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "title 40")

    async def test_find_set_equality_with_func_expr(self):
        await self.store.find(Foo, title="Title 20").set(
            Foo.title == Lower("Title 40"))
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "title 40")

    async def test_find_set_column(self):
        await self.store.find(Bar, title="Title 200").set(foo_id=Bar.id)
        bar = await self.store.get(Bar, 200)
        self.assertEqual(bar.foo_id, 200)

    async def test_find_set_expr(self):
        await self.store.find(Foo, title="Title 20").set(Foo.title == "Title 40")
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "Title 40")

    async def test_find_set_none(self):
        await self.store.find(Foo, title="Title 20").set(title=None)
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, None)

    async def test_find_set_expr_column(self):
        await self.store.find(Bar, id=200).set(Bar.foo_id == Bar.id)
        bar = await self.store.get(Bar, 200)
        self.assertEqual(bar.id, 200)
        self.assertEqual(bar.foo_id, 200)

    async def test_find_set_on_cached(self):
        foo1 = await self.store.get(Foo, 20)
        foo2 = await self.store.get(Foo, 30)
        await self.store.find(Foo, id=20).set(id=40)
        self.assertEqual(foo1.id, 40)
        self.assertEqual(foo2.id, 30)

    async def test_find_set_expr_on_cached(self):
        bar = await self.store.get(Bar, 200)
        await self.store.find(Bar, id=200).set(Bar.foo_id == Bar.id)
        self.assertEqual(bar.id, 200)
        self.assertEqual(bar.foo_id, 200)

    async def test_find_set_none_on_cached(self):
        foo = await self.store.get(Foo, 20)
        await self.store.find(Foo, title="Title 20").set(title=None)
        self.assertEqual(foo.title, None)

    async def test_find_set_on_cached_but_removed(self):
        foo1 = await self.store.get(Foo, 20)
        foo2 = await self.store.get(Foo, 30)
        await self.store.remove(foo1)
        await self.store.find(Foo, id=20).set(id=40)
        self.assertEqual(foo1.id, 20)
        self.assertEqual(foo2.id, 30)

    async def test_find_set_on_cached_unsupported_python_expr(self):
        foo1 = await self.store.get(Foo, 20)
        foo2 = await self.store.get(Foo, 30)
        await self.store.find(
            Foo, Foo.id == Select(SQL("20"))).set(title="Title 40")
        self.assertEqual(foo1.title, "Title 40")
        self.assertEqual(foo2.title, "Title 10")

    async def test_find_set_expr_unsupported(self):
        result = await self.store.find(Foo, title="Title 20")
        self.assertRaises(FeatureError, result.set, Foo.title > "Title 40")

    async def test_find_set_expr_unsupported_without_column(self):
        result = await self.store.find(Foo, title="Title 20")
        self.assertRaises(FeatureError, result.set,
                          Eq(object(), IntVariable(1)))

    async def test_find_set_expr_unsupported_without_expr_or_variable(self):
        result = await self.store.find(Foo, title="Title 20")
        self.assertRaises(FeatureError, result.set, Eq(Foo.id, object()))

    async def test_find_set_expr_unsupported_autoreloads(self):
        bar1 = await self.store.get(Bar, 200)
        bar2 = await self.store.get(Bar, 300)
        await self.store.find(Bar, id=Select(SQL("200"))).set(title="Title 400")
        bar1_vars = get_obj_info(bar1).variables
        bar2_vars = get_obj_info(bar2).variables
        self.assertEqual(bar1_vars[Bar.title].get_lazy(), AutoReload)
        self.assertEqual(bar2_vars[Bar.title].get_lazy(), AutoReload)
        self.assertEqual(bar1_vars[Bar.foo_id].get_lazy(), None)
        self.assertEqual(bar2_vars[Bar.foo_id].get_lazy(), None)
        self.assertEqual(bar1.title, "Title 400")
        self.assertEqual(bar2.title, "Title 100")

    async def test_find_set_expr_unsupported_mixed_autoreloads(self):
        # For an expression that does not compile (eg:
        # ResultSet.cached() raises a CompileError), while setting
        # cached entries' columns to AutoReload, if objects of
        # different types could be found in the cache then a KeyError
        # would happen if some object did not have a matching
        # column. See Bug #328603 for more info.
        foo1 = await self.store.get(Foo, 20)
        bar1 = await self.store.get(Bar, 200)
        await self.store.find(Bar, id=Select(SQL("200"))).set(title="Title 400")
        foo1_vars = get_obj_info(foo1).variables
        bar1_vars = get_obj_info(bar1).variables
        self.assertNotEqual(foo1_vars[Foo.title].get_lazy(), AutoReload)
        self.assertEqual(bar1_vars[Bar.title].get_lazy(), AutoReload)
        self.assertEqual(bar1_vars[Bar.foo_id].get_lazy(), None)
        self.assertEqual(foo1.title, "Title 20")
        self.assertEqual(bar1.title, "Title 400")

    async def test_find_set_autoreloads_with_func_expr(self):
        # In the process of fixing this bug, we've temporarily
        # introduced another bug: the expression would be called
        # twice. We've used an expression that increments the value by
        # one here to see if that case is triggered. In the buggy
        # bugfix, the value would end up being incremented by two due
        # to misfiring two updates.
        foo1 = await self.store.get(FooValue, 1)
        self.assertEqual(foo1.value1, 2)
        await self.store.find(FooValue, id=1).set(value1=SQL("value1 + 1"))
        foo1_vars = get_obj_info(foo1).variables
        self.assertEqual(foo1_vars[FooValue.value1].get_lazy(), AutoReload)
        self.assertEqual(foo1.value1, 3)

    async def test_find_set_equality_autoreloads_with_func_expr(self):
        foo1 = await self.store.get(FooValue, 1)
        self.assertEqual(foo1.value1, 2)
        await self.store.find(FooValue, id=1).set(
            FooValue.value1 == SQL("value1 + 1"))
        foo1_vars = get_obj_info(foo1).variables
        self.assertEqual(foo1_vars[FooValue.value1].get_lazy(), AutoReload)
        self.assertEqual(foo1.value1, 3)

    async def test_wb_find_set_checkpoints(self):
        bar = await self.store.get(Bar, 200)
        await self.store.find(Bar, id=200).set(title="Title 400")
        self.store._connection.execute("UPDATE bar SET "
                                       "title='Title 500' "
                                       "WHERE id=200")
        # When not checkpointing, this flush will set title again.
        await self.store.flush()
        await self.store.reload(bar)
        self.assertEqual(bar.title, "Title 500")

    async def test_find_set_with_info_alive_and_object_dead(self):
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = await self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        await self.store.find(Foo, title="Title 20").set(title="Title 40")
        foo = await self.store.get(Foo, 20)
        self.assertFalse(hasattr(foo, "tainted"))
        self.assertEqual(foo.title, "Title 40")

    async def test_reference(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        self.assertTrue(foo)
        self.assertEqual(foo.title, "Title 30")

    async def test_reference_explicitly_with_wrapper(self):
        bar = await self.store.get(Bar, 100)
        foo = await Bar.foo.__get__(Wrapper(bar))
        self.assertTrue(foo)
        self.assertEqual(foo.title, "Title 30")

    async def test_reference_break_on_local_diverged(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        self.assertTrue(foo)
        bar.foo_id = 40
        foo = await bar.foo
        self.assertEqual(foo, None)

    async def test_reference_break_on_remote_diverged(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        foo.id = 40
        foo = await bar.foo
        self.assertEqual(foo, None)

    async def test_reference_break_on_local_diverged_by_lazy(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        self.assertEqual(foo.id, 10)
        bar.foo_id = SQL("20")
        foo = await bar.foo
        self.assertEqual(foo.id, 20)

    async def test_reference_remote_leak_on_flush_with_changed(self):
        """
        "changed" events only hold weak references to remote infos object, thus
        not creating a leak when unhooked.
        """
        self.get_cache(self.store).set_size(0)
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        foo.title = "Changed title"
        bar_ref = weakref.ref(get_obj_info(bar))
        foo = await bar.foo
        del bar
        await self.store.flush()
        gc.collect()
        self.assertEqual(bar_ref(), None)

    async def test_reference_remote_leak_on_flush_with_removed(self):
        """
        "removed" events only hold weak references to remote infos objects,
        thus not creating a leak when unhooked.
        """
        self.get_cache(self.store).set_size(0)
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = await self.store.get(MyFoo, 10)
        foo.bar.title = "Changed title"
        foo_ref = weakref.ref(get_obj_info(foo))
        bar = foo.bar
        del foo
        await self.store.flush()
        gc.collect()
        self.assertEqual(foo_ref(), None)

    async def test_reference_break_on_remote_diverged_by_lazy(self):
        class MyBar(Bar):
            pass
        MyBar.foo = Reference(MyBar.title, Foo.title)
        bar = await self.store.get(MyBar, 100)
        bar.title = "Title 30"
        await self.store.flush()
        foo = await bar.foo
        self.assertEqual(foo.id, 10)
        foo.title = SQL("'Title 40'")
        foo = await bar.foo
        self.assertEqual(foo, None)
        self.assertEqual(await self.store.find(Foo, title="Title 30").one(), None)
        self.assertEqual(await self.store.get(Foo, 10).title, "Title 40")

    async def test_reference_on_non_primary_key(self):
        await self.store.execute("INSERT INTO bar VALUES (400, 40, 'Title 30')")
        class MyBar(Bar):
            foo = Reference(Bar.title, Foo.title)

        bar = await self.store.get(Bar, 400)
        self.assertEqual(bar.title, "Title 30")
        foo = await bar.foo
        self.assertEqual(foo, None)

        mybar = await self.store.get(MyBar, 400)
        self.assertEqual(mybar.title, "Title 30")
        foo = await mybar.foo
        self.assertNotEqual(foo, None)
        self.assertEqual(foo.id, 10)
        self.assertEqual(foo.title, "Title 30")

    async def test_new_reference(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo_id = 10

        foo = await bar.foo
        self.assertEqual(foo, None)

        await self.store.add(bar)

        foo = await bar.foo
        self.assertTrue(foo)
        self.assertEqual(foo.title, "Title 30")

    async def test_set_reference(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        self.assertEqual(foo.id, 10)
        foo = await self.store.get(Foo, 30)
        bar.foo = foo
        foo = await bar.foo
        self.assertEqual(foo.id, 30)
        result = await self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        self.assertEqual(await result.get_one(), (30,))

    async def test_set_reference_explicitly_with_wrapper(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        self.assertEqual(foo.id, 10)
        foo = await self.store.get(Foo, 30)
        Bar.foo.__set__(Wrapper(bar), Wrapper(foo))
        foo = await bar.foo
        self.assertEqual(foo.id, 30)
        result = await self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        self.assertEqual(await result.get_one(), (30,))

    async def test_reference_assign_remote_key(self):
        bar = await self.store.get(Bar, 100)
        foo = await bar.foo
        self.assertEqual(foo.id, 10)
        bar.foo = 30
        self.assertEqual(bar.foo_id, 30)
        foo = await bar.foo
        self.assertEqual(foo.id, 30)
        result = await self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        self.assertEqual(await result.get_one(), (30,))

    async def test_reference_on_added(self):
        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        await self.store.add(bar)

        foo_ref = await bar.foo
        self.assertEqual(foo_ref.id, None)
        self.assertEqual(foo_ref.title, "Title 40")


        await self.store.flush()

        foo_ref = await bar.foo
        self.assertTrue(foo_ref.id)
        self.assertEqual(foo_ref.title, "Title 40")

        result = await self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        self.assertEqual(await result.get_one(), ("Title 40",))

    async def test_reference_on_added_with_autoreload_key(self):
        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        await self.store.add(bar)

        foo_ref = await bar.foo
        self.assertEqual(foo_ref.id, None)
        self.assertEqual(foo_ref.title, "Title 40")

        foo.id = AutoReload

        # Variable shouldn't be autoreloaded yet.
        obj_info = get_obj_info(foo)
        self.assertEqual(obj_info.variables[Foo.id].get_lazy(), AutoReload)

        self.assertEqual(type(foo.id), int)

        await self.store.flush()

        foo_ref = await bar.foo
        self.assertTrue(foo_ref.id)
        self.assertEqual(foo_ref.title, "Title 40")

        result = await self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        self.assertEqual(await result.get_one(), ("Title 40",))

    async def test_reference_assign_none(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        bar.foo = None
        bar.foo = None # Twice to make sure it doesn't blow up.
        await self.store.add(bar)

        await self.store.flush()

        self.assertEqual(type(bar.id), int)
        self.assertEqual(foo.id, None)

    async def test_reference_assign_none_with_unseen(self):
        bar = await self.store.get(Bar, 200)
        bar.foo = None
        foo = await bar.foo
        self.assertEqual(foo, None)

    async def test_reference_on_added_composed_key(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        await self.store.add(bar)

        foo_ref = await bar.foo
        self.assertEqual(foo_ref.id, None)
        self.assertEqual(foo_ref.title, "Title 40")
        self.assertEqual(bar.title, "Title 40")

        await self.store.flush()

        foo_ref = await bar.foo
        self.assertTrue(foo_ref.id)
        self.assertEqual(foo_ref.title, "Title 40")

        result = await self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        self.assertEqual(await result.get_one(), ("Title 40",))

    async def test_reference_assign_composed_remote_key(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        bar = Bar()
        bar.id = 400
        bar.foo = (20, "Title 20")
        await self.store.add(bar)

        self.assertEqual(bar.foo_id, 20)
        foo = await bar.foo
        self.assertEqual(foo.id, 20)
        self.assertEqual(bar.title, "Title 20")
        self.assertEqual(foo.title, "Title 20")

    async def test_reference_on_added_unlink_on_flush(self):
        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        bar.title = "Title 400"
        await self.store.add(bar)

        foo.id = 40
        self.assertEqual(bar.foo_id, 40)
        foo.id = 50
        self.assertEqual(bar.foo_id, 50)
        foo.id = 60
        self.assertEqual(bar.foo_id, 60)

        await self.store.flush()

        foo.id = 70
        self.assertEqual(bar.foo_id, 60)

    async def test_reference_on_added_unsets_original_key(self):
        foo = Foo()
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo_id = 40
        bar.foo = foo

        self.assertEqual(bar.foo_id, None)

    async def test_reference_on_two_added(self):
        foo1 = Foo()
        foo1.title = "Title 40"
        foo2 = Foo()
        foo2.title = "Title 40"
        await self.store.add(foo1)
        await self.store.add(foo2)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo1
        bar.foo = foo2
        await self.store.add(bar)

        foo1.id = 40
        self.assertEqual(bar.foo_id, None)
        foo2.id = 50
        self.assertEqual(bar.foo_id, 50)

    async def test_reference_on_added_and_changed_manually(self):
        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        await self.store.add(bar)

        bar.foo_id = 40
        foo.id = 50
        self.assertEqual(bar.foo_id, 40)

    async def test_reference_on_added_composed_key_changed_manually(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        await self.store.add(bar)

        bar.title = "Title 50"

        foo_ref = await bar.foo
        self.assertEqual(foo_ref, None)

        foo.id = 40

        self.assertEqual(bar.foo_id, None)

    async def test_reference_on_added_no_local_store(self):
        foo = Foo()
        foo.title = "Title 40"
        await self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo), self.store)

    async def test_reference_on_added_no_remote_store(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        await self.store.add(bar)

        bar.foo = foo

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo), self.store)

    async def test_reference_on_added_no_store(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo

        await self.store.add(bar)

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo), self.store)

        await self.store.flush()

        self.assertEqual(type(bar.foo_id), int)

    async def test_reference_on_added_no_store_2(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo

        await self.store.add(foo)

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo), self.store)

        await self.store.flush()

        self.assertEqual(type(bar.foo_id), int)

    async def test_reference_on_added_wrong_store(self):
        store = await self.create_store()

        foo = Foo()
        foo.title = "Title 40"
        await store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        await self.store.add(bar)

        self.assertRaises(WrongStoreError, setattr, bar, "foo", foo)

    async def test_reference_on_added_no_store_unlink_before_adding(self):
        foo1 = Foo()
        foo1.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo1
        bar.foo = None

        await self.store.add(bar)

        store = await self.create_store()
        await store.add(foo1)

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo1), store)

    async def test_reference_on_removed_wont_add_back(self):
        bar = await self.store.get(Bar, 200)
        foo = await self.store.get(Foo, bar.foo_id)

        await self.store.remove(bar)

        foo_ref = await bar.foo
        self.assertEqual(foo_ref, foo)
        await self.store.flush()

        self.assertEqual(Store.of(bar), None)
        self.assertEqual(Store.of(foo), self.store)

    async def test_reference_equals(self):
        foo = await self.store.get(Foo, 10)

        bar = await self.store.find(Bar, foo=foo).one()
        self.assertTrue(bar)
        foo_ref = await bar.foo
        self.assertEqual(foo_ref, foo)

        bar = await self.store.find(Bar, foo=foo.id).one()
        self.assertTrue(bar)
        foo_ref = await bar.foo
        self.assertEqual(foo_ref, foo)

    async def test_reference_equals_none(self):
        result = list(await self.store.find(SelfRef, selfref=None))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].selfref, None)
        self.assertEqual(result[1].selfref, None)

    async def test_reference_equals_with_composed_key(self):
        # Interesting case of self-reference.
        class LinkWithRef(Link):
            myself = Reference((Link.foo_id, Link.bar_id),
                               (Link.foo_id, Link.bar_id))

        link = await self.store.find(LinkWithRef, foo_id=10, bar_id=100).one()
        myself = await self.store.find(LinkWithRef, myself=link).one()
        self.assertEqual(link, myself)

        myself = await self.store.find(LinkWithRef,
                                 myself=(link.foo_id, link.bar_id)).one()
        self.assertEqual(link, myself)

    async def test_reference_equals_with_wrapped(self):
        foo = await self.store.get(Foo, 10)

        bar = await self.store.find(Bar, foo=Wrapper(foo)).one()
        self.assertTrue(bar)
        foo_ref = await bar.foo
        self.assertEqual(foo_ref, foo)

    async def test_reference_not_equals(self):
        foo = await self.store.get(Foo, 10)

        result = await self.store.find(Bar, Bar.foo != foo)
        self.assertEqual([200, 300], sorted(bar.id for bar in result))

    async def test_reference_not_equals_none(self):
        obj = await self.store.find(SelfRef, SelfRef.selfref != None).one()
        self.assertTrue(obj)
        self.assertNotEqual(obj.selfref, None)

    async def test_reference_not_equals_with_composed_key(self):
        class LinkWithRef(Link):
            myself = Reference((Link.foo_id, Link.bar_id),
                               (Link.foo_id, Link.bar_id))

        link = await self.store.find(LinkWithRef, foo_id=10, bar_id=100).one()
        result = list(await self.store.find(LinkWithRef, LinkWithRef.myself != link))
        self.assertTrue(link not in result, "%r not in %r" % (link, result))

        result = list(await self.store.find(
            LinkWithRef, LinkWithRef.myself != (link.foo_id, link.bar_id)))
        self.assertTrue(link not in result, "%r not in %r" % (link, result))

    async def test_reference_self(self):
        selfref = await self.store.add(SelfRef())
        selfref.id = 400
        selfref.title = "Title 400"
        selfref.selfref_id = 25
        self.assertEqual(selfref.selfref.id, 25)
        self.assertEqual(selfref.selfref.title, "SelfRef 25")

    async def get_bar_200_title(self):
        connection = self.store._connection
        result = await connection.execute("SELECT title FROM bar WHERE id=200")
        return await result.get_one()[0]

    async def test_reference_wont_touch_store_when_key_is_none(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = None
        bar.title = "Don't flush this!"

        foo = await bar.foo
        self.assertEqual(foo, None)

        # Bypass the store to prevent flushing.
        self.assertEqual(await self.get_bar_200_title(), "Title 200")

    async def test_reference_wont_touch_store_when_key_is_unset(self):
        bar = await self.store.get(Bar, 200)
        del bar.foo_id
        bar.title = "Don't flush this!"

        foo = await bar.foo
        self.assertEqual(foo, None)

        # Bypass the store to prevent flushing.
        connection = self.store._connection
        result = await connection.execute("SELECT title FROM bar WHERE id=200")
        self.assertEqual(await result.get_one()[0], "Title 200")

    async def test_reference_wont_touch_store_with_composed_key_none(self):
        class Bar:
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        bar = await self.store.get(Bar, 200)
        bar.foo_id = None
        bar.title = None

        foo = await bar.foo
        self.assertEqual(foo, None)

        # Bypass the store to prevent flushing.
        self.assertEqual(await self.get_bar_200_title(), "Title 200")

    async def test_reference_will_resolve_auto_reload(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = AutoReload
        foo = await bar.foo
        self.assertTrue(foo)

    async def test_back_reference(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = await self.store.get(MyFoo, 10)
        self.assertTrue(foo.bar)
        self.assertEqual(foo.bar.title, "Title 300")

    async def test_back_reference_setting(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"
        await self.store.add(bar)

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"
        await self.store.add(foo)

        await self.store.flush()

        self.assertTrue(foo.id)
        self.assertEqual(bar.foo_id, foo.id)

        result = await self.store.execute("SELECT bar.title "
                                    "FROM foo, bar "
                                    "WHERE foo.id = bar.foo_id AND "
                                    "foo.title = 'Title 40'")
        self.assertEqual(await result.get_one(), ("Title 400",))

    async def test_back_reference_setting_changed_manually(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"
        await self.store.add(bar)

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"
        await self.store.add(foo)

        foo.id = 40

        self.assertEqual(foo.bar, bar)

        await self.store.flush()

        self.assertEqual(foo.id, 40)
        self.assertEqual(bar.foo_id, 40)

        result = await self.store.execute("SELECT bar.title "
                                    "FROM foo, bar "
                                    "WHERE foo.id = bar.foo_id AND "
                                    "foo.title = 'Title 40'")
        self.assertEqual(await result.get_one(), ("Title 400",))

    async def test_back_reference_assign_none_with_unseen(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)
        foo = await self.store.get(MyFoo, 20)
        foo.bar = None
        self.assertEqual(foo.bar, None)

    async def test_back_reference_assign_none_from_none(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)
        await self.store.execute("INSERT INTO foo (id, title)"
                           " VALUES (40, 'Title 40')")
        await self.store.commit()
        foo = await self.store.get(MyFoo, 40)
        foo.bar = None
        self.assertEqual(foo.bar, None)

    async def test_back_reference_on_added_unsets_original_key(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = MyFoo()

        bar = Bar()
        bar.id = 400
        bar.foo_id = 40

        foo.bar = bar

        self.assertEqual(bar.foo_id, None)

    async def test_back_reference_on_added_no_store(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"

        await self.store.add(bar)

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo), self.store)

        await self.store.flush()

        self.assertEqual(type(bar.foo_id), int)

    async def test_back_reference_on_added_no_store_2(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"

        await self.store.add(foo)

        self.assertEqual(Store.of(bar), self.store)
        self.assertEqual(Store.of(foo), self.store)

        await self.store.flush()

        self.assertEqual(type(bar.foo_id), int)

    async def test_back_reference_remove_remote(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"

        foo = MyFoo()
        foo.title = "Title 40"
        foo.bar = bar

        await self.store.add(foo)
        await self.store.flush()

        self.assertEqual(foo.bar, bar)
        await self.store.remove(bar)
        self.assertEqual(foo.bar, None)

    async def test_back_reference_remove_remote_pending_add(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"

        foo = MyFoo()
        foo.title = "Title 40"
        foo.bar = bar

        await self.store.add(foo)

        self.assertEqual(foo.bar, bar)
        await self.store.remove(bar)
        self.assertEqual(foo.bar, None)

    async def test_reference_loop_with_undefined_keys_fails(self):
        """A loop of references with undefined keys raises OrderLoopError."""
        ref1 = SelfRef()
        await self.store.add(ref1)
        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_with_dirty_keys_fails(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        ref1.id = 42
        ref2 = SelfRef()
        ref2.id = 43
        ref2.selfref = ref1
        ref1.selfref = ref2

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_with_dirty_keys_changed_later_fails(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        await self.store.add(ref1)
        await self.store.add(ref2)
        await self.store.flush()
        ref2.selfref = ref1
        ref1.selfref = ref2
        ref1.id = 42
        ref2.id = 43

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_with_dirty_keys_on_remote_fails(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        ref1.id = 42
        ref2 = SelfRef()
        ref2.id = 43
        ref2.selfref_on_remote = ref1
        ref1.selfref_on_remote = ref2

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_with_dirty_keys_on_remote_changed_later_fails(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        await self.store.add(ref1)
        await self.store.flush()
        ref2.selfref_on_remote = ref1
        ref1.selfref_on_remote = ref2
        ref1.id = 42
        ref2.id = 43

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_with_unchanged_keys_succeeds(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        ref1.id = 42
        ref2 = SelfRef()
        await self.store.add(ref2)
        ref1.id = 43

        await self.store.flush()

        # As ref1 and ref2 have been flushed to the database, so these
        # changes can be flushed.
        ref2.selfref = ref1
        ref1.selfref = ref2
        await self.store.flush()

    async def test_reference_loop_with_one_unchanged_key_succeeds(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        await self.store.flush()

        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2

        # As ref1 and ref2 have been flushed to the database, so these
        # changes can be flushed.
        await self.store.flush()

    async def test_reference_loop_with_key_changed_later_succeeds(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        await self.store.flush()

        ref2 = SelfRef()
        ref1.selfref = ref2
        ref2.id = 42

        await self.store.flush()

    async def test_reference_loop_with_key_changed_later_on_remote_succeeds(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        await self.store.flush()

        ref2 = SelfRef()
        ref2.selfref_on_remote = ref1
        ref2.id = 42

        await self.store.flush()

    async def test_reference_loop_with_undefined_and_changed_keys_fails(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        await self.store.flush()

        ref1.id = 400
        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_with_undefined_and_changed_keys_fails2(self):
        ref1 = SelfRef()
        await self.store.add(ref1)
        await self.store.flush()

        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2
        ref1.id = 400

        self.assertRaises(OrderLoopError, self.store.flush)

    async def test_reference_loop_broken_by_set(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        ref1.selfref = ref2
        ref2.selfref = ref1
        await self.store.add(ref1)

        ref1.selfref = None
        await self.store.flush()

    async def test_reference_loop_set_only_removes_own_flush_order(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        await self.store.add(ref2)
        await self.store.flush()

        # The following does not create a loop since the keys are
        # dirty (as shown in another test).
        ref1.selfref = ref2
        ref2.selfref = ref1

        # Now add a flush order loop.
        self.store.add_flush_order(ref1, ref2)
        self.store.add_flush_order(ref2, ref1)

        # Now break the reference.  This should leave the flush
        # ordering loop we previously created in place..
        ref1.selfref = None
        self.assertRaises(OrderLoopError, self.store.flush)

    async def add_reference_set_bar_400(self):
        bar = Bar()
        bar.id = 400
        bar.foo_id = 20
        bar.title = "Title 100"
        await self.store.add(bar)

    async def test_reference_set(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

    async def test_reference_set_assign_fails(self):
        foo = await self.store.get(FooRefSet, 20)
        try:
            foo.bars = []
        except FeatureError:
            pass
        else:
            self.fail("FeatureError not raised")

    async def test_reference_set_explicitly_with_wrapper(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)

        items = []
        for bar in FooRefSet.bars.__get__(Wrapper(foo)):
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

    async def test_reference_set_with_added(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)

        await self.store.add(foo)

        self.assertEqual(foo.id, None)
        self.assertEqual(bar1.foo_id, None)
        self.assertEqual(bar2.foo_id, None)
        self.assertEqual(list(foo.bars.order_by(Bar.id)),
                         [bar1, bar2])
        self.assertEqual(type(foo.id), int)
        self.assertEqual(foo.id, bar1.foo_id)
        self.assertEqual(foo.id, bar2.foo_id)

    async def test_reference_set_composed(self):
        await self.add_reference_set_bar_400()

        bar = await self.store.get(Bar, 400)
        bar.title = "Title 20"

        class FooRefSetComposed(Foo):
            bars = ReferenceSet((Foo.id, Foo.title),
                                (Bar.foo_id, Bar.title))

        foo = await self.store.get(FooRefSetComposed, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))

        self.assertEqual(items, [
                          (400, 20, "Title 20"),
                         ])

        bar = await self.store.get(Bar, 200)
        bar.title = "Title 20"

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (200, 20, "Title 20"),
                          (400, 20, "Title 20"),
                         ])

    async def test_reference_set_contains(self):
        def no_iter(self):
            raise RuntimeError()
        from storm.references import BoundReferenceSetBase
        orig_iter = BoundReferenceSetBase.__iter__
        BoundReferenceSetBase.__iter__ = no_iter
        try:
            foo = await self.store.get(FooRefSet, 20)
            bar = await self.store.get(Bar, 200)
            self.assertEqual(bar in foo.bars, True)
        finally:
            BoundReferenceSetBase.__iter__ = orig_iter

    async def test_reference_set_find(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        # Notice that there's another item with this title in the base,
        # which isn't part of the reference.

        objects = list(foo.bars.find(Bar.title == "Title 100"))
        self.assertEqual(len(objects), 1)
        self.assertTrue(objects[0] is bar)

        objects = list(foo.bars.find(title="Title 100"))
        self.assertEqual(len(objects), 1)
        self.assertTrue(objects[0] is bar)

    async def test_reference_set_clear(self):
        foo = await self.store.get(FooRefSet, 20)
        foo.bars.clear()
        self.assertEqual(list(foo.bars), [])

        # Object wasn't removed.
        self.assertTrue(await self.store.get(Bar, 200))

    async def test_reference_set_clear_cached(self):
        foo = await self.store.get(FooRefSet, 20)
        bar = await self.store.get(Bar, 200)
        self.assertEqual(bar.foo_id, 20)
        foo.bars.clear()
        self.assertEqual(bar.foo_id, None)

    async def test_reference_set_clear_where(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)
        foo.bars.clear(Bar.id > 200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                         ])

        bar = await self.store.get(Bar, 400)
        bar.foo_id = 20

        foo.bars.clear(id=200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEqual(items, [
                          (400, 20, "Title 100"),
                         ])

    async def test_reference_set_is_empty(self):
        foo = await self.store.get(FooRefSet, 20)
        self.assertFalse(foo.bars.is_empty())

        foo.bars.clear()
        self.assertTrue(foo.bars.is_empty())

    async def test_reference_set_count(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)

        self.assertEqual(await foo.bars.count(), 2)

    async def test_reference_set_order_by(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars.order_by(Bar.id):
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        del items[:]
        for bar in foo.bars.order_by(Bar.title):
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (400, 20, "Title 100"),
                          (200, 20, "Title 200"),
                         ])

    async def test_reference_set_default_order_by(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSetOrderID, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        items = []
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        foo = await self.store.get(FooRefSetOrderTitle, 20)

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (400, 20, "Title 100"),
                          (200, 20, "Title 200"),
                         ])

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (400, 20, "Title 100"),
                          (200, 20, "Title 200"),
                         ])

    async def test_reference_set_getitem(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSetOrderID, 20)

        self.assertEqual(foo.bars[0].id, 200)
        self.assertEqual(foo.bars[1].id, 400)
        self.assertRaises(IndexError, foo.bars.__getitem__, 2)

        items = []
        for bar in foo.bars[:1]:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                         ])

        del items[:]
        for bar in foo.bars[1:]:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (400, 20, "Title 100"),
                         ])

        del items[:]
        for bar in foo.bars[:2]:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

    async def test_reference_set_first_last(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSetOrderID, 20)
        self.assertEqual(await foo.bars.first().id, 200)
        self.assertEqual(await foo.bars.last().id, 400)

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first().id, 400)
        self.assertEqual(await foo.bars.last().id, 200)

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first(Bar.id > 400), None)
        self.assertEqual(await foo.bars.last(Bar.id > 400), None)

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first(Bar.id < 400).id, 200)
        self.assertEqual(await foo.bars.last(Bar.id < 400).id, 200)

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first(id=200).id, 200)
        self.assertEqual(await foo.bars.last(id=200).id, 200)

        foo = await self.store.get(FooRefSet, 20)
        self.assertRaises(UnorderedError, foo.bars.first)
        self.assertRaises(UnorderedError, foo.bars.last)

    async def test_indirect_reference_set_any(self):
        """
        L{BoundReferenceSet.any} returns an arbitrary object from the set of
        referenced objects.
        """
        foo = await self.store.get(FooRefSet, 20)
        self.assertNotEqual(None, await foo.bars.any())

    async def test_indirect_reference_set_any_filtered(self):
        """
        L{BoundReferenceSet.any} optionally takes a list of filtering criteria
        to narrow the set of objects to search.  When provided, the criteria
        are used to filter the set before returning an arbitrary object.
        """
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.any(Bar.id > 400), None)

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.any(Bar.id < 400).id, 200)

        foo = await self.store.get(FooRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.any(id=200).id, 200)

    async def test_reference_set_one(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSetOrderID, 20)
        self.assertRaises(NotOneError, foo.bars.one)

        foo = await self.store.get(FooRefSetOrderID, 30)
        self.assertEqual(await foo.bars.one().id, 300)

        foo = await self.store.get(FooRefSetOrderID, 20)
        self.assertEqual(await foo.bars.one(Bar.id > 400), None)

        foo = await self.store.get(FooRefSetOrderID, 20)
        self.assertEqual(await foo.bars.one(Bar.id < 400).id, 200)

        foo = await self.store.get(FooRefSetOrderID, 20)
        self.assertEqual(await foo.bars.one(id=200).id, 200)

    async def test_reference_set_remove(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSet, 20)
        for bar in foo.bars:
            foo.bars.remove(bar)

        self.assertEqual(bar.foo_id, None)
        self.assertEqual(list(foo.bars), [])

    async def test_reference_set_after_object_removed(self):
        class MyBar(Bar):
            # Make sure that this works even with allow_none=False.
            foo_id = Int(allow_none=False)

        class MyFoo(Foo):
            bars = ReferenceSet(Foo.id, MyBar.foo_id)

        foo = await self.store.get(MyFoo, 20)
        bar = await foo.bars.any()
        await self.store.remove(bar)
        self.assertTrue(bar not in list(foo.bars))

    async def test_reference_set_add(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 100"

        foo = await self.store.get(FooRefSet, 20)
        foo.bars.add(bar)

        self.assertEqual(bar.foo_id, 20)
        self.assertEqual(Store.of(bar), self.store)

    async def test_reference_set_add_no_store(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar)

        await self.store.add(foo)

        self.assertEqual(Store.of(foo), self.store)
        self.assertEqual(Store.of(bar), self.store)

        await self.store.flush()

        self.assertEqual(type(bar.foo_id), int)

    async def test_reference_set_add_no_store_2(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar)

        await self.store.add(bar)

        self.assertEqual(Store.of(foo), self.store)
        self.assertEqual(Store.of(bar), self.store)

        await self.store.flush()

        self.assertEqual(type(bar.foo_id), int)

    async def test_reference_set_add_no_store_unlink_after_adding(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)
        foo.bars.remove(bar1)

        await self.store.add(foo)

        store = await self.create_store()
        await store.add(bar1)

        self.assertEqual(Store.of(foo), self.store)
        self.assertEqual(Store.of(bar1), store)
        self.assertEqual(Store.of(bar2), self.store)

    async def test_reference_set_values(self):
        await self.add_reference_set_bar_400()

        foo = await self.store.get(FooRefSetOrderID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        self.assertEqual(values,
                         [(200, 20, "Title 200"), (400, 20, "Title 100")])

    async def test_reference_set_order_by_desc_id(self):
        await self.add_reference_set_bar_400()

        class FooRefSetOrderByDescID(Foo):
            bars = ReferenceSet(Foo.id, Bar.foo_id, order_by=Desc(Bar.id))

        foo = await self.store.get(FooRefSetOrderByDescID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        self.assertEqual(values,
                         [(400, 20, "Title 100"), (200, 20, "Title 200")])

        self.assertEqual(await foo.bars.first().id, 400)
        self.assertEqual(await foo.bars.last().id, 200)

    async def test_indirect_reference_set(self):
        foo = await self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [(100, "Title 300"), (200, "Title 200")])

    async def test_indirect_reference_set_with_added(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"
        await self.store.add(bar1)
        await self.store.add(bar2)

        foo = FooIndRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)

        self.assertEqual(foo.id, None)

        await self.store.add(foo)

        self.assertEqual(foo.id, None)
        self.assertEqual(bar1.foo_id, None)
        self.assertEqual(bar2.foo_id, None)
        self.assertEqual(list(foo.bars.order_by(Bar.id)),
                         [bar1, bar2])
        self.assertEqual(type(foo.id), int)
        self.assertEqual(type(bar1.id), int)
        self.assertEqual(type(bar2.id), int)

    async def test_indirect_reference_set_find(self):
        foo = await self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars.find(Bar.title == "Title 300"):
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (100, "Title 300"),
                         ])

    async def test_indirect_reference_set_clear(self):
        foo = await self.store.get(FooIndRefSet, 20)
        foo.bars.clear()
        self.assertEqual(list(foo.bars), [])

    async def test_indirect_reference_set_clear_where(self):
        foo = await self.store.get(FooIndRefSet, 20)
        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEqual(items, [
                          (100, 10, "Title 300"),
                          (200, 20, "Title 200"),
                         ])

        foo = await self.store.get(FooIndRefSet, 30)
        foo.bars.clear(Bar.id < 300)
        foo.bars.clear(id=200)

        foo = await self.store.get(FooIndRefSet, 20)
        foo.bars.clear(Bar.id < 200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEqual(items, [
                          (200, 20, "Title 200"),
                         ])

        foo.bars.clear(id=200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEqual(items, [])

    async def test_indirect_reference_set_is_empty(self):
        foo = await self.store.get(FooIndRefSet, 20)
        self.assertFalse(foo.bars.is_empty())

        foo.bars.clear()
        self.assertTrue(foo.bars.is_empty())

    async def test_indirect_reference_set_count(self):
        foo = await self.store.get(FooIndRefSet, 20)
        self.assertEqual(await foo.bars.count(), 2)

    async def test_indirect_reference_set_order_by(self):
        foo = await self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars.order_by(Bar.title):
            items.append((bar.id, bar.title))

        self.assertEqual(items, [
                          (200, "Title 200"),
                          (100, "Title 300"),
                         ])

        del items[:]
        for bar in foo.bars.order_by(Bar.id):
            items.append((bar.id, bar.title))

        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    async def test_indirect_reference_set_default_order_by(self):
        foo = await self.store.get(FooIndRefSetOrderTitle, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (200, "Title 200"),
                          (100, "Title 300"),
                         ])

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (200, "Title 200"),
                          (100, "Title 300"),
                         ])

        foo = await self.store.get(FooIndRefSetOrderID, 20)

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    async def test_indirect_reference_set_getitem(self):
        foo = await self.store.get(FooIndRefSetOrderID, 20)

        self.assertEqual(foo.bars[0].id, 100)
        self.assertEqual(foo.bars[1].id, 200)
        self.assertRaises(IndexError, foo.bars.__getitem__, 2)

        items = []
        for bar in foo.bars[:1]:
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (100, "Title 300"),
                         ])

        del items[:]
        for bar in foo.bars[1:]:
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (200, "Title 200"),
                         ])

        del items[:]
        for bar in foo.bars[:2]:
            items.append((bar.id, bar.title))
        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    async def test_indirect_reference_set_first_last(self):
        foo = await self.store.get(FooIndRefSetOrderID, 20)
        self.assertEqual(await foo.bars.first().id, 100)
        self.assertEqual(await foo.bars.last().id, 200)

        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first().id, 200)
        self.assertEqual(await foo.bars.last().id, 100)

        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first(Bar.id > 200), None)
        self.assertEqual(await foo.bars.last(Bar.id > 200), None)

        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first(Bar.id < 200).id, 100)
        self.assertEqual(await foo.bars.last(Bar.id < 200).id, 100)

        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.first(id=200).id, 200)
        self.assertEqual(await foo.bars.last(id=200).id, 200)

        foo = await self.store.get(FooIndRefSet, 20)
        self.assertRaises(UnorderedError, foo.bars.first)
        self.assertRaises(UnorderedError, foo.bars.last)

    async def test_indirect_reference_set_any(self):
        """
        L{BoundIndirectReferenceSet.any} returns an arbitrary object from the
        set of referenced objects.
        """
        foo = await self.store.get(FooIndRefSet, 20)
        self.assertNotEqual(None, await foo.bars.any())

    async def test_indirect_reference_set_any_filtered(self):
        """
        L{BoundIndirectReferenceSet.any} optionally takes a list of filtering
        criteria to narrow the set of objects to search.  When provided, the
        criteria are used to filter the set before returning an arbitrary
        object.
        """
        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.any(Bar.id > 200), None)

        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.any(Bar.id < 200).id, 100)

        foo = await self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEqual(await foo.bars.any(id=200).id, 200)

    async def test_indirect_reference_set_one(self):
        foo = await self.store.get(FooIndRefSetOrderID, 20)
        self.assertRaises(NotOneError, foo.bars.one)

        foo = await self.store.get(FooIndRefSetOrderID, 30)
        self.assertEqual(await foo.bars.one().id, 300)

        foo = await self.store.get(FooIndRefSetOrderID, 20)
        self.assertEqual(await foo.bars.one(Bar.id > 200), None)

        foo = await self.store.get(FooIndRefSetOrderID, 20)
        self.assertEqual(await foo.bars.one(Bar.id < 200).id, 100)

        foo = await self.store.get(FooIndRefSetOrderID, 20)
        self.assertEqual(await foo.bars.one(id=200).id, 200)

    async def test_indirect_reference_set_add(self):
        foo = await self.store.get(FooIndRefSet, 20)
        bar = await self.store.get(Bar, 300)

        foo.bars.add(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                          (300, "Title 100"),
                         ])

    async def test_indirect_reference_set_remove(self):
        foo = await self.store.get(FooIndRefSet, 20)
        bar = await self.store.get(Bar, 200)

        foo.bars.remove(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (100, "Title 300"),
                         ])

    async def test_indirect_reference_set_add_remove(self):
        foo = await self.store.get(FooIndRefSet, 20)
        bar = await self.store.get(Bar, 300)

        foo.bars.add(bar)
        foo.bars.remove(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    async def test_indirect_reference_set_add_remove_with_wrapper(self):
        foo = await self.store.get(FooIndRefSet, 20)
        bar300 = await self.store.get(Bar, 300)
        bar200 = await self.store.get(Bar, 200)

        foo.bars.add(Wrapper(bar300))
        foo.bars.remove(Wrapper(bar200))

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (100, "Title 300"),
                          (300, "Title 100"),
                         ])

    async def test_indirect_reference_set_add_remove_with_added(self):
        foo = FooIndRefSet()
        foo.id = 40
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"
        await self.store.add(foo)
        await self.store.add(bar1)
        await self.store.add(bar2)

        foo.bars.add(bar1)
        foo.bars.add(bar2)
        foo.bars.remove(bar1)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (500, "Title 500"),
                         ])

    async def test_indirect_reference_set_with_added_no_store(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"

        foo = FooIndRefSet()
        foo.title = "Title 40"

        foo.bars.add(bar1)
        foo.bars.add(bar2)

        await self.store.add(bar1)

        self.assertEqual(Store.of(foo), self.store)
        self.assertEqual(Store.of(bar1), self.store)
        self.assertEqual(Store.of(bar2), self.store)

        self.assertEqual(foo.id, None)
        self.assertEqual(bar1.foo_id, None)
        self.assertEqual(bar2.foo_id, None)

        self.assertEqual(list(foo.bars.order_by(Bar.id)),
                         [bar1, bar2])

    async def test_indirect_reference_set_values(self):
        foo = await self.store.get(FooIndRefSetOrderID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        self.assertEqual(values, [
                          (100, 10, "Title 300"),
                          (200, 20, "Title 200"),
                         ])

    async def test_references_raise_nostore(self):
        foo1 = FooRefSet()
        foo2 = FooIndRefSet()

        self.assertRaises(NoStoreError, foo1.bars.__iter__)
        self.assertRaises(NoStoreError, foo2.bars.__iter__)
        self.assertRaises(NoStoreError, foo1.bars.find)
        self.assertRaises(NoStoreError, foo2.bars.find)
        self.assertRaises(NoStoreError, foo1.bars.order_by)
        self.assertRaises(NoStoreError, foo2.bars.order_by)
        self.assertRaises(NoStoreError, foo1.bars.count)
        self.assertRaises(NoStoreError, foo2.bars.count)
        self.assertRaises(NoStoreError, foo1.bars.clear)
        self.assertRaises(NoStoreError, foo2.bars.clear)
        self.assertRaises(NoStoreError, foo2.bars.remove, object())

    async def test_string_reference(self):
        class Base(metaclass=PropertyPublisherMeta):
            pass

        class MyBar(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()
            foo_id = Int()
            foo = Reference("foo_id", "MyFoo.id")

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()

        bar = await self.store.get(MyBar, 100)
        foo = await bar.foo
        self.assertTrue(foo)
        self.assertEqual(foo.title, "Title 30")
        self.assertEqual(type(foo), MyFoo)

    async def test_string_indirect_reference_set(self):
        """
        A L{ReferenceSet} can have its reference keys specified as strings
        when the class its a member of uses the L{PropertyPublisherMeta}
        metaclass.  This makes it possible to work around problems with
        circular dependencies by delaying property resolution.
        """
        class Base(metaclass=PropertyPublisherMeta):
            pass

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()
            bars = ReferenceSet("id", "MyLink.foo_id",
                                "MyLink.bar_id", "MyBar.id")

        class MyBar(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        class MyLink(Base):
            __storm_table__ = "link"
            __storm_primary__ = "foo_id", "bar_id"
            foo_id = Int()
            bar_id = Int()

        foo = await self.store.get(MyFoo, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEqual(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    async def test_string_reference_set_order_by(self):
        """
        A L{ReferenceSet} can have its default order by specified as a string
        when the class its a member of uses the L{PropertyPublisherMeta}
        metaclass.  This makes it possible to work around problems with
        circular dependencies by delaying resolution of the order by column.
        """
        class Base(metaclass=PropertyPublisherMeta):
            pass

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()
            bars = ReferenceSet("id", "MyLink.foo_id",
                                "MyLink.bar_id", "MyBar.id",
                                order_by="MyBar.title")

        class MyBar(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        class MyLink(Base):
            __storm_table__ = "link"
            __storm_primary__ = "foo_id", "bar_id"
            foo_id = Int()
            bar_id = Int()

        foo = await self.store.get(MyFoo, 20)
        items = [(bar.id, bar.title) for bar in foo.bars]
        self.assertEqual(items, [(200, "Title 200"), (100, "Title 300")])

    async def test_flush_order(self):
        foo1 = Foo()
        foo2 = Foo()
        foo3 = Foo()
        foo4 = Foo()
        foo5 = Foo()

        for i, foo in enumerate([foo1, foo2, foo3, foo4, foo5]):
            foo.title = "Object %d" % (i+1)
            await self.store.add(foo)

        self.store.add_flush_order(foo2, foo4)
        self.store.add_flush_order(foo4, foo1)
        self.store.add_flush_order(foo1, foo3)
        self.store.add_flush_order(foo3, foo5)
        self.store.add_flush_order(foo5, foo2)
        self.store.add_flush_order(foo5, foo2)

        self.assertRaises(OrderLoopError, self.store.flush)

        self.store.remove_flush_order(foo5, foo2)

        self.assertRaises(OrderLoopError, self.store.flush)

        self.store.remove_flush_order(foo5, foo2)

        await self.store.flush()

        self.assertTrue(foo2.id < foo4.id)
        self.assertTrue(foo4.id < foo1.id)
        self.assertTrue(foo1.id < foo3.id)
        self.assertTrue(foo3.id < foo5.id)

    async def test_variable_filter_on_load(self):
        foo = await self.store.get(FooVariable, 20)
        self.assertEqual(foo.title, "to_py(from_db(Title 20))")

    async def test_variable_filter_on_update(self):
        foo = await self.store.get(FooVariable, 20)
        foo.title = "Title 20"

        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    async def test_variable_filter_on_update_unchanged(self):
        foo = await self.store.get(FooVariable, 20)
        await self.store.flush()
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_variable_filter_on_insert(self):
        foo = FooVariable()
        foo.id = 40
        foo.title = "Title 40"

        await self.store.add(foo)
        await self.store.flush()

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "to_db(from_py(Title 40))"),
                         ])

    async def test_variable_filter_on_missing_values(self):
        foo = FooVariable()
        foo.id = 40

        await self.store.add(foo)
        await self.store.flush()

        self.assertEqual(foo.title, "to_py(from_db(Default Title))")

    async def test_variable_filter_on_set(self):
        foo = FooVariable()
        await self.store.find(FooVariable, id=20).set(title="Title 20")

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    async def test_variable_filter_on_set_expr(self):
        foo = FooVariable()
        result = await self.store.find(FooVariable, id=20)
        await result.set(FooVariable.title == "Title 20")

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    async def test_wb_result_set_variable(self):
        Result = self.store._connection.result_factory

        class MyResult(Result):
            def set_variable(self, variable, value):
                if variable.__class__ is UnicodeVariable:
                    variable.set("set_variable(%s)" % value)
                elif variable.__class__ is IntVariable:
                    variable.set(value+1)
                else:
                    variable.set(value)

        self.store._connection.result_factory = MyResult
        try:
            foo = await self.store.get(Foo, 20)
        finally:
            self.store._connection.result_factory = Result

        self.assertEqual(foo.id, 21)
        self.assertEqual(foo.title, "set_variable(Title 20)")

    async def test_default(self):
        class MyFoo(Foo):
            title = Unicode(default="Some default value")

        foo = MyFoo()
        await self.store.add(foo)
        await self.store.flush()

        result = await self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        self.assertEqual(await result.get_one(), ("Some default value",))

        self.assertEqual(foo.title, "Some default value")

    async def test_default_factory(self):
        class MyFoo(Foo):
            title = Unicode(default_factory=lambda:"Some default value")

        foo = MyFoo()
        await self.store.add(foo)
        await self.store.flush()

        result = await self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        self.assertEqual(await result.get_one(), ("Some default value",))

        self.assertEqual(foo.title, "Some default value")

    async def test_pickle_variable(self):
        class PickleBlob(Blob):
            bin = Pickle()

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()

        pickle_blob = await self.store.get(PickleBlob, 20)
        self.assertEqual(pickle_blob.bin["a"], 1)

        pickle_blob.bin["b"] = 2

        await self.store.flush()
        await self.store.reload(blob)
        self.assertEqual(pickle.loads(blob.bin), {"a": 1, "b": 2})

    async def test_pickle_variable_remove(self):
        """
        When an object is removed from a store, it should unhook from the
        "flush" event emitted by the store, and thus not emit a "changed" event
        if its content change and that the store is flushed.
        """
        class PickleBlob(Blob):
            bin = Pickle()

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()

        pickle_blob = await self.store.get(PickleBlob, 20)
        await self.store.remove(pickle_blob)
        await self.store.flush()

        #  Let's change the object
        pickle_blob.bin = "foobin"

        # And subscribe to its changed event
        obj_info = get_obj_info(pickle_blob)
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))

        await self.store.flush()
        self.assertEqual(events, [])

    async def test_pickle_variable_unhook(self):
        """
        A variable instance must unhook itself from the store event system when
        the store invalidates its objects.
        """
        # I create a custom PickleVariable, with no __slots__ definition, to be
        # able to get a weakref of it, thing that I can't do with
        # PickleVariable that defines __slots__ *AND* those parent is the C
        # implementation of Variable
        class CustomPickleVariable(PickleVariable):
            pass

        class CustomPickle(Pickle):
            variable_class = CustomPickleVariable

        class PickleBlob(Blob):
            bin = CustomPickle()

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()

        pickle_blob = await self.store.get(PickleBlob, 20)
        await self.store.flush()
        await self.store.invalidate()

        obj_info = get_obj_info(pickle_blob)
        variable = obj_info.variables[PickleBlob.bin]
        var_ref = weakref.ref(variable)
        del variable, blob, pickle_blob, obj_info
        gc.collect()
        self.assertTrue(var_ref() is None)

    async def test_pickle_variable_referenceset(self):
        """
        A variable instance must unhook itself from the store event system
        explcitely when the store invalidates its objects: it's particulary
        important when a ReferenceSet is used, because it keeps strong
        references to objects involved.
        """
        class CustomPickleVariable(PickleVariable):
            pass

        class CustomPickle(Pickle):
            variable_class = CustomPickleVariable

        class PickleBlob(Blob):
            bin = CustomPickle()
            foo_id = Int()

        class FooBlobRefSet(Foo):
            blobs = ReferenceSet(Foo.id, PickleBlob.foo_id)

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()

        pickle_blob = await self.store.get(PickleBlob, 20)

        foo = await self.store.get(FooBlobRefSet, 10)
        foo.blobs.add(pickle_blob)

        await self.store.flush()
        await self.store.invalidate()

        obj_info = get_obj_info(pickle_blob)
        variable = obj_info.variables[PickleBlob.bin]
        var_ref = weakref.ref(variable)
        del variable, blob, pickle_blob, obj_info, foo
        gc.collect()
        self.assertTrue(var_ref() is None)

    async def test_pickle_variable_referenceset_several_transactions(self):
        """
        Check that a pickle variable fires the changed event when used among
        several transactions.
        """
        class PickleBlob(Blob):
            bin = Pickle()
            foo_id = Int()

        class FooBlobRefSet(Foo):
            blobs = ReferenceSet(Foo.id, PickleBlob.foo_id)
        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()

        pickle_blob = await self.store.get(PickleBlob, 20)

        foo = await self.store.get(FooBlobRefSet, 10)
        foo.blobs.add(pickle_blob)

        await self.store.flush()
        await self.store.invalidate()
        await self.store.reload(pickle_blob)

        pickle_blob.bin = "foo"
        obj_info = get_obj_info(pickle_blob)
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))
        await self.store.flush()
        self.assertEqual(len(events), 1)

    async def test_undefined_variables_filled_on_find(self):
        """
        Check that when data is fetched from the database on a find,
        it is used to fill up any undefined variables.
        """
        # We do a first find to get the object_infos into the cache.
        foos = list(await self.store.find(Foo, title="Title 20"))

        # Commit so that all foos are invalidated and variables are
        # set back to AutoReload.
        await self.store.commit()

        # Another find which should reuse in-memory foos.
        for foo in await self.store.find(Foo, title="Title 20"):
            # Make sure we have all variables defined, because
            # values were already retrieved by the find's select.
            obj_info = get_obj_info(foo)
            for column in obj_info.variables:
                self.assertTrue(obj_info.variables[column].is_defined())

    async def test_storm_loaded_after_define(self):
        """
        C{__storm_loaded__} is only called once all the variables are correctly
        defined in the object. If the object is in the alive cache but
        disappeared, it used to be called without its variables defined.
        """
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)
        loaded = []
        class MyFoo(Foo):
            def __storm_loaded__(oself):
                loaded.append(None)
                obj_info = get_obj_info(oself)
                for column in obj_info.variables:
                    self.assertTrue(obj_info.variables[column].is_defined())

        foo = await self.store.get(MyFoo, 20)
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertEqual(obj_info.get_obj(), None)

        # Commit so that all foos are invalidated and variables are
        # set back to AutoReload.
        await self.store.commit()

        foo = await self.store.find(MyFoo, title="Title 20").one()
        self.assertEqual(foo.id, 20)
        self.assertEqual(len(loaded), 2)

    async def test_defined_variables_not_overridden_on_find(self):
        """
        Check that the keep_defined=True setting in _load_object()
        is in place.  In practice, it ensures that already defined
        values aren't replaced during a find, when new data comes
        from the database and is used whenever possible.
        """
        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        class PickleBlob:
            __storm_table__ = "bin"
            id = Int(primary=True)
            pickle = Pickle("bin")
        blob = await self.store.get(PickleBlob, 20)
        value = blob.pickle
        # Now the find should not destroy our value pointer.
        blob = await self.store.find(PickleBlob, id=20).one()
        self.assertTrue(value is blob.pickle)

    async def test_pickle_variable_with_deleted_object(self):
        class PickleBlob(Blob):
            bin = Pickle()

        blob = await self.store.get(Blob, 20)
        blob.bin = b"\x80\x02}q\x01U\x01aK\x01s."
        await self.store.flush()

        pickle_blob = await self.store.get(PickleBlob, 20)
        self.assertEqual(pickle_blob.bin["a"], 1)

        pickle_blob.bin["b"] = 2

        del pickle_blob
        gc.collect()

        await self.store.flush()
        await self.store.reload(blob)
        self.assertEqual(pickle.loads(blob.bin), {"a": 1, "b": 2})

    async def test_unhashable_object(self):

        class DictFoo(Foo, dict):
            pass

        foo = await self.store.get(DictFoo, 20)
        foo["a"] = 1

        self.assertEqual(list(foo.items()), [("a", 1)])

        new_obj = DictFoo()
        new_obj.id = 40
        new_obj.title = "My Title"

        await self.store.add(new_obj)
        await self.store.commit()

        self.assertTrue(await self.store.get(DictFoo, 40) is new_obj)

    async def test_wrapper(self):
        foo = await self.store.get(Foo, 20)
        wrapper = Wrapper(foo)
        await self.store.remove(wrapper)
        await self.store.flush()
        self.assertEqual(await self.store.get(Foo, 20), None)

    async def test_rollback_loaded_and_still_in_cached(self):
        # Explore problem found on interaction between caching, commits,
        # and rollbacks, when they still existed.
        foo1 = await self.store.get(Foo, 20)
        await self.store.commit()
        await self.store.rollback()
        foo2 = await self.store.get(Foo, 20)
        self.assertTrue(foo1 is foo2)

    async def test_class_alias(self):
        FooAlias = ClassAlias(Foo)
        result = await self.store.find(FooAlias, FooAlias.id < Foo.id)
        self.assertEqual([(foo.id, foo.title) for foo in result
                          if type(foo) is Foo], [
                          (10, "Title 30"),
                          (10, "Title 30"),
                          (20, "Title 20"),
                         ])

    async def test_expr_values(self):
        foo = await self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        # No commits yet.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        await self.store.flush()

        # Now it should be there.

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, "New title")

    async def test_expr_values_flush_on_demand(self):
        foo = await self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        # No commits yet.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, "New title")

        # Now it should be there.

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

    async def test_expr_values_flush_and_load_in_separate_steps(self):
        foo = await self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        await self.store.flush()

        # It's already in the database.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

        # But our value is now an AutoReload.
        lazy_value = get_obj_info(foo).variables[Foo.title].get_lazy()
        self.assertTrue(lazy_value is AutoReload)

        # Which gets resolved once touched.
        self.assertEqual(foo.title, "New title")

    async def test_expr_values_flush_on_demand_with_added(self):
        foo = Foo()
        foo.id = 40
        foo.title = SQL("'New title'")

        await self.store.add(foo)

        # No commits yet.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, "New title")

        # Now it should be there.

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "New title"),
                         ])

    async def test_expr_values_flush_on_demand_with_removed_and_added(self):
        foo = await self.store.get(Foo, 20)
        foo.title = SQL("'New title'")

        await self.store.remove(foo)
        await self.store.add(foo)

        # No commits yet.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, "New title")

        # Now it should be there.

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

    async def test_expr_values_flush_on_demand_with_removed_and_rollbacked(self):
        foo = await self.store.get(Foo, 20)

        await self.store.remove(foo)
        await self.store.rollback()

        foo.title = SQL("'New title'")

        # No commits yet.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, "New title")

        # Now it should be there.

        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

    async def test_expr_values_flush_on_demand_with_added_and_removed(self):

        # This test tries to trigger a problem in a few different ways.
        # It uses the same id of an existing object, and add and remove
        # the object. This object should never get in the database, nor
        # update the object that is already there, nor flush any other
        # pending changes when the lazy value is accessed.

        foo = Foo()
        foo.id = 20

        foo_dep = Foo()
        foo_dep.id = 50

        await self.store.add(foo)
        await self.store.add(foo_dep)

        foo.title = SQL("'New title'")

        # Add ordering to see if it helps triggering a bug of
        # incorrect flushing.
        self.store.add_flush_order(foo_dep, foo)

        await self.store.remove(foo)

        # No changes.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, None)

        # Still no changes. There's no reason why foo_dep would be flushed.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_expr_values_flush_on_demand_with_removed(self):

        # Similar case, but removing an existing object instead.

        foo = await self.store.get(Foo, 20)

        foo_dep = Foo()
        foo_dep.id = 50

        await self.store.add(foo_dep)

        foo.title = SQL("'New title'")

        # Add ordering to see if it helps triggering a bug of
        # incorrect flushing.
        self.store.add_flush_order(foo_dep, foo)

        await self.store.remove(foo)

        # No changes.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEqual(foo.title, None)

        # Still no changes. There's no reason why foo_dep would be flushed.
        self.assertEqual(await self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    async def test_lazy_value_preserved_with_subsequent_object_initialization(self):
        """
        If a lazy value has been modified on an object that is subsequently
        initialized from the database the lazy value is correctly preserved
        and the object is initialized properly.  This tests the fix for the
        problem reported in bug #620615.
        """
        # Retrieve an object, fully loaded.
        foo = await self.store.get(Foo, 20)

        # Build and retrieve a result set ahead of time, so that
        # flushes won't happen when actually loading the object.
        result = await self.store.find(Foo, Foo.id == 20)

        # Now, set an unflushed lazy value on an attribute.
        foo.title = SQL("'New title'")

        # Finally, get the existing object.
        foo = await result.one()

        # We don't really have to test anything here, since the
        # explosion happened above, but here it is anyway.
        self.assertEqual(foo.title, "New title")

    async def test_lazy_value_discarded_on_reload(self):
        """
        A counter-test to the above logic, also related to bug #620615. On
        an explicit reload, the lazy value must be discarded.
        """
        # Retrieve an object, fully loaded.
        foo = await self.store.get(Foo, 20)

        # Build and retrieve a result set ahead of time, so that
        # flushes won't happen when actually loading the object.
        result = await self.store.find(Foo, Foo.id == 20)

        # Now, set an unflushed lazy value on an attribute.
        foo.title = SQL("'New title'")

        # Give up on this and reload the original object.
        await self.store.reload(foo)

        # We don't really have to test anything here, since the
        # explosion happened above, but here it is anyway.
        self.assertEqual(foo.title, "Title 20")

    async def test_expr_values_with_columns(self):
        bar = await self.store.get(Bar, 200)
        bar.foo_id = Bar.id+1
        self.assertEqual(bar.foo_id, 201)

    async def test_autoreload_attribute(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEqual(foo.title, "Title 20")
        foo.title = AutoReload
        self.assertEqual(foo.title, "New Title")
        self.assertFalse(get_obj_info(foo).variables[Foo.title].has_changed())

    async def test_autoreload_attribute_with_changed_primary_key(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEqual(foo.title, "Title 20")
        foo.id = 40
        foo.title = AutoReload
        self.assertEqual(foo.title, "New Title")
        self.assertEqual(foo.id, 40)

    async def test_autoreload_object(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEqual(foo.title, "Title 20")
        self.store.autoreload(foo)
        self.assertEqual(foo.title, "New Title")

    async def test_autoreload_primary_key_of_unflushed_object(self):
        foo = Foo()
        await self.store.add(foo)
        foo.id = AutoReload
        foo.title = "New Title"
        self.assertTrue(isinstance(foo.id, int))
        self.assertEqual(foo.title, "New Title")

    async def test_autoreload_primary_key_doesnt_reload_everything_else(self):
        foo = await self.store.get(Foo, 20)
        self.store.autoreload(foo)

        obj_info = get_obj_info(foo)

        self.assertEqual(obj_info.variables[Foo.id].get_lazy(), None)
        self.assertEqual(obj_info.variables[Foo.title].get_lazy(), AutoReload)

        self.assertEqual(foo.id, 20)

        self.assertEqual(obj_info.variables[Foo.id].get_lazy(), None)
        self.assertEqual(obj_info.variables[Foo.title].get_lazy(), AutoReload)

    async def test_autoreload_all_objects(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEqual(foo.title, "Title 20")
        self.store.autoreload()
        self.assertEqual(foo.title, "New Title")

    async def test_autoreload_and_get_will_not_reload(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.store.autoreload(foo)

        obj_info = get_obj_info(foo)

        self.assertEqual(obj_info.variables[Foo.title].get_lazy(), AutoReload)
        await self.store.get(Foo, 20)
        self.assertEqual(obj_info.variables[Foo.title].get_lazy(), AutoReload)
        self.assertEqual(foo.title, "New Title")

    async def test_autoreload_object_doesnt_tag_as_dirty(self):
        foo = await self.store.get(Foo, 20)
        self.store.autoreload(foo)
        self.assertTrue(get_obj_info(foo) not in self.store._dirty)

    async def test_autoreload_missing_columns_on_insertion(self):
        foo = Foo()
        await self.store.add(foo)
        await self.store.flush()
        lazy_value = get_obj_info(foo).variables[Foo.title].get_lazy()
        self.assertEqual(lazy_value, AutoReload)
        self.assertEqual(foo.title, "Default Title")

    async def test_reference_break_on_local_diverged_doesnt_autoreload(self):
        foo = await self.store.get(Foo, 10)
        self.store.autoreload(foo)

        bar = await self.store.get(Bar, 100)
        foo_ref = await bar.foo
        self.assertTrue(foo_ref)
        bar.foo_id = 40
        foo_ref = await bar.foo
        self.assertEqual(foo_ref, None)

        obj_info = get_obj_info(foo)
        self.assertEqual(obj_info.variables[Foo.title].get_lazy(), AutoReload)

    async def test_primary_key_reference(self):
        """
        When an object references another one using its primary key, it
        correctly checks for the invalidated state after the store has been
        committed, detecting if the referenced object has been removed behind
        its back.
        """
        class BarOnRemote:
            __storm_table__ = "bar"
            foo_id = Int(primary=True)
            foo = Reference(foo_id, Foo.id, on_remote=True)
        foo = await self.store.get(Foo, 10)
        bar = await self.store.get(BarOnRemote, 10)
        foo_ref = await bar.foo
        self.assertEqual(foo_ref, foo)
        await self.store.execute("DELETE FROM foo WHERE id = 10")
        await self.store.commit()
        foo_ref = await bar.foo
        self.assertEqual(foo_ref, None)

    async def test_invalidate_and_get_object(self):
        foo = await self.store.get(Foo, 20)
        await self.store.invalidate(foo)
        self.assertEqual(await self.store.get(Foo, 20), foo)
        self.assertEqual(await self.store.find(Foo, id=20).one(), foo)

    async def test_invalidate_and_get_removed_object(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("DELETE FROM foo WHERE id=20")
        await self.store.invalidate(foo)
        self.assertEqual(await self.store.get(Foo, 20), None)
        self.assertEqual(await self.store.find(Foo, id=20).one(), None)

    async def test_invalidate_and_validate_with_find(self):
        foo = await self.store.get(Foo, 20)
        await self.store.invalidate(foo)
        self.assertEqual(await self.store.find(Foo, id=20).one(), foo)

        # Cache should be considered valid again at this point.
        await self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEqual(await self.store.get(Foo, 20), foo)

    async def test_invalidate_object_gets_validated(self):
        foo = await self.store.get(Foo, 20)
        await self.store.invalidate(foo)
        self.assertEqual(await self.store.get(Foo, 20), foo)

        # At this point the object is valid again, so deleting it
        # from the database directly shouldn't affect caching.
        await self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEqual(await self.store.get(Foo, 20), foo)

    async def test_invalidate_object_with_only_primary_key(self):
        link = await self.store.get(Link, (20, 200))
        await self.store.execute("DELETE FROM link WHERE foo_id=20 AND bar_id=200")
        await self.store.invalidate(link)
        self.assertEqual(await self.store.get(Link, (20, 200)), None)

    async def test_invalidate_added_object(self):
        foo = Foo()
        await self.store.add(foo)
        await self.store.invalidate(foo)
        foo.id = 40
        foo.title = "Title 40"
        await self.store.flush()

        # Object must have a valid cache at this point, since it was
        # just added.
        await self.store.execute("DELETE FROM foo WHERE id=40")
        self.assertEqual(await self.store.get(Foo, 40), foo)

    async def test_invalidate_and_update(self):
        foo = await self.store.get(Foo, 20)
        await self.store.execute("DELETE FROM foo WHERE id=20")
        await self.store.invalidate(foo)
        self.assertRaises(LostObjectError, setattr, foo, "title", "Title 40")

    async def test_invalidated_objects_reloaded_by_get(self):
        foo = await self.store.get(Foo, 20)
        await self.store.invalidate(foo)
        foo = await self.store.get(Foo, 20)
        title_variable = get_obj_info(foo).variables[Foo.title]
        self.assertEqual(title_variable.get_lazy(), None)
        self.assertEqual(title_variable.get(), "Title 20")
        self.assertEqual(foo.title, "Title 20")

    async def test_invalidated_hook(self):
        called = []
        class MyFoo(Foo):
            def __storm_invalidated__(self):
                called.append(True)
        foo = await self.store.get(MyFoo, 20)
        self.assertEqual(called, [])
        self.store.autoreload(foo)
        self.assertEqual(called, [])
        await self.store.invalidate(foo)
        self.assertEqual(called, [True])

    async def test_invalidated_hook_called_after_all_invalidated(self):
        """
        Ensure that invalidated hooks are called only when all objects have
        already been marked as invalidated. See comment in
        store.py:_mark_autoreload.
        """
        called = []
        class MyFoo(Foo):
            def __storm_invalidated__(self):
                if not called:
                    called.append(get_obj_info(foo1).get("invalidated"))
                    called.append(get_obj_info(foo2).get("invalidated"))
        foo1 = await self.store.get(MyFoo, 10)
        foo2 = await self.store.get(MyFoo, 20)
        await self.store.invalidate()
        self.assertEqual(called, [True, True])

    async def test_reset_recreates_objects(self):
        """
        After resetting the store, all queries return fresh objects, even if
        there are other objects representing the same database rows still in
        memory.
        """
        foo1 = await self.store.get(Foo, 10)
        foo1.dirty = True
        self.store.reset()
        new_foo1 = await self.store.get(Foo, 10)
        self.assertFalse(hasattr(new_foo1, "dirty"))
        self.assertNotIdentical(new_foo1, foo1)

    async def test_reset_unmarks_dirty(self):
        """
        If an object was dirty when store.reset() is called, its changes will
        not be affected.
        """
        foo1 = await self.store.get(Foo, 10)
        foo1_title = foo1.title
        foo1.title = "radix wuz here"
        self.store.reset()
        await self.store.flush()
        new_foo1 = await self.store.get(Foo, 10)
        self.assertEqual(new_foo1.title, foo1_title)

    async def test_reset_clears_cache(self):
        cache = self.get_cache(self.store)
        foo1 = await self.store.get(Foo, 10)
        self.assertTrue(get_obj_info(foo1) in cache.get_cached())
        self.store.reset()
        self.assertEqual(cache.get_cached(), [])

    async def test_reset_breaks_store_reference(self):
        """
        After resetting the store, all objects that were associated with that
        store will no longer be.
        """
        foo1 = await self.store.get(Foo, 10)
        self.store.reset()
        self.assertIdentical(Store.of(foo1), None)

    async def test_result_find(self):
        result1 = await self.store.find(Foo, Foo.id <= 20)
        result2 = result1.find(Foo.id > 10)
        foo = await result2.one()
        self.assertTrue(foo)
        self.assertEqual(foo.id, 20)

    async def test_result_find_kwargs(self):
        result1 = await self.store.find(Foo, Foo.id <= 20)
        result2 = result1.find(id=20)
        foo = await result2.one()
        self.assertTrue(foo)
        self.assertEqual(foo.id, 20)

    async def test_result_find_introduce_join(self):
        result1 = await self.store.find(Foo, Foo.id <= 20)
        result2 = result1.find(Foo.id == Bar.foo_id,
                               Bar.title == "Title 300")
        foo = await result2.one()
        self.assertTrue(foo)
        self.assertEqual(foo.id, 10)

    async def test_result_find_tuple(self):
        result1 = await self.store.find((Foo, Bar), Foo.id == Bar.foo_id)
        result2 = result1.find(Bar.title == "Title 100")
        foo_bar = await result2.one()
        self.assertTrue(foo_bar)
        foo, bar = foo_bar
        self.assertEqual(foo.id, 30)
        self.assertEqual(bar.id, 300)

    async def test_result_find_undef_where(self):
        result = await self.store.find(Foo, Foo.id == 20).find()
        foo = await result.one()
        self.assertTrue(foo)
        self.assertEqual(foo.id, 20)
        result = await self.store.find(Foo).find(Foo.id == 20)
        foo = await result.one()
        self.assertTrue(foo)
        self.assertEqual(foo.id, 20)

    async def test_result_find_fails_on_set_expr(self):
        result1 = await self.store.find(Foo)
        result2 = await self.store.find(Foo)
        result = result1.union(result2)
        self.assertRaises(FeatureError, result.find, Foo.id == 20)

    async def test_result_find_fails_on_slice(self):
        result = await self.store.find(Foo)[1:2]
        self.assertRaises(FeatureError, result.find, Foo.id == 20)

    async def test_result_find_fails_on_group_by(self):
        result = await self.store.find(Foo)
        result.group_by(Foo)
        self.assertRaises(FeatureError, result.find, Foo.id == 20)

    async def test_result_union(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=10)
        result3 = result1.union(result2)

        result3.order_by(Foo.title)
        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (10, "Title 30"),
                         ])

        result3.order_by(Desc(Foo.title))
        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    async def test_result_union_duplicated(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=30)

        result3 = result1.union(result2)

        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                         ])

    async def test_result_union_duplicated_with_all(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)

        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (30, "Title 10"),
                         ])

    async def test_result_union_with_empty(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = EmptyResultSet()

        result3 = result1.union(result2)

        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                         ])

    async def test_result_union_class_columns(self):
        """
        It's possible to do a union of two result sets on columns on
        different classes, as long as their variable classes are the
        same (e.g. both are IntVariables).
        """
        result1 = await self.store.find(Foo.id, Foo.id == 10)
        result2 = await self.store.find(Bar.foo_id, Bar.id == 200)
        self.assertEqual([10, 20], sorted(result1.union(result2)))

    async def test_result_union_incompatible(self):
        result1 = await self.store.find(Foo, id=10)
        result2 = await self.store.find(Bar, id=100)
        self.assertRaises(FeatureError, result1.union, result2)

    async def test_result_union_unsupported_methods(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=10)
        result3 = result1.union(result2)

        self.assertRaises(FeatureError, result3.set, title="Title 40")
        self.assertRaises(FeatureError, result3.remove)

    async def test_result_union_count(self):
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)

        self.assertEqual(await result3.count(), 2)

    async def test_result_union_limit_count(self):
        """
        It's possible to count the result of a union that is limited.
        """
        result1 = await self.store.find(Foo, id=30)
        result2 = await self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)
        result3.order_by(Foo.id)
        result3.config(limit=1)

        self.assertEqual(await result3.count(), 1)
        self.assertEqual(result3.count(Foo.id), 1)

    async def test_result_union_limit_avg(self):
        """
        It's possible to average the result of a union that is limited.
        """
        result1 = await self.store.find(Foo, id=10)
        result2 = await self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)
        result3.order_by(Foo.id)
        result3.config(limit=1)

        # Since 30 was left off because of the limit, the only result will be
        # 10, and the average of that is 10.
        self.assertEqual(result3.avg(Foo.id), 10)

    async def test_result_difference(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.difference tests on MySQL")

        result1 = await self.store.find(Foo)
        result2 = await self.store.find(Foo, id=20)
        result3 = result1.difference(result2)

        result3.order_by(Foo.title)
        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (10, "Title 30"),
                         ])

        result3.order_by(Desc(Foo.title))
        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    async def test_result_difference_with_empty(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.difference tests on MySQL")

        result1 = await self.store.find(Foo, id=30)
        result2 = EmptyResultSet()

        result3 = result1.difference(result2)

        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                         ])

    async def test_result_difference_incompatible(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.difference tests on MySQL")

        result1 = await self.store.find(Foo, id=10)
        result2 = await self.store.find(Bar, id=100)
        self.assertRaises(FeatureError, result1.difference, result2)

    async def test_result_difference_count(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.difference tests on MySQL")

        result1 = await self.store.find(Foo)
        result2 = await self.store.find(Foo, id=20)

        result3 = result1.difference(result2)

        self.assertEqual(await result3.count(), 2)

    async def test_is_in_empty_result_set(self):
        result1 = await self.store.find(Foo, Foo.id < 10)
        result2 = await self.store.find(Foo, Or(Foo.id > 20, Foo.id.is_in(result1)))
        self.assertEqual(await result2.count(), 1)

    async def test_is_in_empty_list(self):
        result2 = await self.store.find(Foo, Eq(False, And(True, Foo.id.is_in([]))))
        self.assertEqual(await result2.count(), 3)

    async def test_result_intersection(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.intersection tests on MySQL")

        result1 = await self.store.find(Foo)
        result2 = await self.store.find(Foo, Foo.id.is_in((10, 30)))
        result3 = result1.intersection(result2)

        result3.order_by(Foo.title)
        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (10, "Title 30"),
                         ])

        result3.order_by(Desc(Foo.title))
        self.assertEqual([(foo.id, foo.title) for foo in result3], [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    async def test_result_intersection_with_empty(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.intersection tests on MySQL")

        result1 = await self.store.find(Foo, id=30)
        result2 = EmptyResultSet()
        result3 = result1.intersection(result2)

        self.assertEqual(len(list(result3)), 0)

    async def test_result_intersection_incompatible(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.intersection tests on MySQL")

        result1 = await self.store.find(Foo, id=10)
        result2 = await self.store.find(Bar, id=100)
        self.assertRaises(FeatureError, result1.intersection, result2)

    async def test_result_intersection_count(self):
        if self.__class__.__name__.startswith("MySQL"):
            self.skipTest("Skipping ResultSet.intersection tests on MySQL")

        result1 = await self.store.find(Foo, Foo.id.is_in((10, 20)))
        result2 = await self.store.find(Foo, Foo.id.is_in((10, 30)))
        result3 = result1.intersection(result2)

        self.assertEqual(await result3.count(), 1)

    async def test_proxy(self):
        bar = await self.store.get(BarProxy, 200)
        self.assertEqual(bar.foo_title, "Title 20")

    async def test_proxy_equals(self):
        bar = await self.store.find(BarProxy, BarProxy.foo_title == "Title 20").one()
        self.assertTrue(bar)
        self.assertEqual(bar.id, 200)

    async def test_proxy_as_column(self):
        result = await self.store.find(BarProxy, BarProxy.id == 200)
        self.assertEqual(list(result.values(BarProxy.foo_title)),
                         ["Title 20"])

    async def test_proxy_set(self):
        bar = await self.store.get(BarProxy, 200)
        bar.foo_title = "New Title"
        foo = await self.store.get(Foo, 20)
        self.assertEqual(foo.title, "New Title")

    def get_bar_proxy_with_string(self):
        class Base(metaclass=PropertyPublisherMeta):
            pass

        class MyBarProxy(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            foo = Reference("foo_id", "MyFoo.id")
            foo_title = Proxy(foo, "MyFoo.title")

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()

        return MyBarProxy, MyFoo

    async def test_proxy_with_string(self):
        MyBarProxy, MyFoo = self.get_bar_proxy_with_string()
        bar = await self.store.get(MyBarProxy, 200)
        self.assertEqual(bar.foo_title, "Title 20")

    async def test_proxy_with_string_variable_factory_attribute(self):
        MyBarProxy, MyFoo = self.get_bar_proxy_with_string()
        variable = MyBarProxy.foo_title.variable_factory(value="Hello")
        self.assertTrue(isinstance(variable, UnicodeVariable))

    async def test_proxy_with_extra_table(self):
        """
        Proxies use a join on auto_tables. It should work even if we have
        more tables in the query.
        """
        result = await self.store.find((BarProxy, Link),
                                 BarProxy.foo_title == "Title 20",
                                 BarProxy.foo_id == Link.foo_id)
        results = [item async for item in result]
        self.assertEqual(len(results), 2)
        for bar, link in results:
            self.assertEqual(bar.id, 200)
            self.assertEqual(bar.foo_title, "Title 20")
            self.assertEqual(bar.foo_id, 20)
            self.assertEqual(link.foo_id, 20)

    async def test_get_decimal_property(self):
        money = await self.store.get(Money, 10)
        self.assertEqual(money.value, decimal.Decimal("12.3455"))

    async def test_set_decimal_property(self):
        money = await self.store.get(Money, 10)
        money.value = decimal.Decimal("12.3456")
        await self.store.flush()
        result = await self.store.find(Money, value=decimal.Decimal("12.3456"))
        self.assertEqual(await result.one(), money)

    async def test_fill_missing_primary_key_with_lazy_value(self):
        foo = await self.store.get(Foo, 10)
        foo.id = SQL("40")
        await self.store.flush()
        self.assertEqual(foo.id, 40)
        self.assertEqual(await self.store.get(Foo, 10), None)
        self.assertEqual(await self.store.get(Foo, 40), foo)

    async def test_fill_missing_primary_key_with_lazy_value_on_creation(self):
        foo = Foo()
        foo.id = SQL("40")
        await self.store.add(foo)
        await self.store.flush()
        self.assertEqual(foo.id, 40)
        self.assertEqual(await self.store.get(Foo, 40), foo)

    async def test_preset_primary_key(self):
        check = []
        def preset_primary_key(primary_columns, primary_variables):
            check.append([(variable.is_defined(), variable.get_lazy())
                          for variable in primary_variables])
            check.append([column.name for column in primary_columns])
            primary_variables[0].set(SQL("40"))

        class DatabaseWrapper:
            """Wrapper to inject our custom preset_primary_key hook."""

            def __init__(self, database):
                self.database = database

            def connect(self, event=None):
                connection = self.database.connect(event)
                connection.preset_primary_key = preset_primary_key
                return connection

        store = Store(DatabaseWrapper(self.database))

        foo = await store.add(Foo())

        await store.flush()
        try:
            self.assertEqual(check, [[(False, None)], ["id"]])
            self.assertEqual(foo.id, 40)
        finally:
            store.close()

    async def test_strong_cache_used(self):
        """
        Objects should be referenced in the cache if not referenced
        in application code.
        """
        foo = await self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        cached = await self.store.find(Foo).cached()
        self.assertEqual(len(cached), 1)
        foo = await self.store.get(Foo, 20)
        self.assertEqual(cached, [foo])
        self.assertTrue(hasattr(foo, "tainted"))

    async def test_strong_cache_cleared_on_invalidate_all(self):
        cache = self.get_cache(self.store)
        foo = await self.store.get(Foo, 20)
        self.assertEqual(cache.get_cached(), [get_obj_info(foo)])
        await self.store.invalidate()
        self.assertEqual(cache.get_cached(), [])

    async def test_strong_cache_loses_object_on_invalidate(self):
        cache = self.get_cache(self.store)
        foo = await self.store.get(Foo, 20)
        self.assertEqual(cache.get_cached(), [get_obj_info(foo)])
        await self.store.invalidate(foo)
        self.assertEqual(cache.get_cached(), [])

    async def test_strong_cache_loses_object_on_remove(self):
        """
        Make sure an object gets removed from the strong reference
        cache when removed from the store.
        """
        cache = self.get_cache(self.store)
        foo = await self.store.get(Foo, 20)
        self.assertEqual(cache.get_cached(), [get_obj_info(foo)])
        await self.store.remove(foo)
        await self.store.flush()
        self.assertEqual(cache.get_cached(), [])

    async def test_strong_cache_renews_object_on_get(self):
        cache = self.get_cache(self.store)
        foo1 = await self.store.get(Foo, 10)
        foo2 = await self.store.get(Foo, 20)
        foo1 = await self.store.get(Foo, 10)
        self.assertEqual(cache.get_cached(),
                         [get_obj_info(foo1), get_obj_info(foo2)])

    async def test_strong_cache_renews_object_on_find(self):
        cache = self.get_cache(self.store)
        foo1 = await self.store.find(Foo, id=10).one()
        foo2 = await self.store.find(Foo, id=20).one()
        foo1 = await self.store.find(Foo, id=10).one()
        self.assertEqual(cache.get_cached(),
                         [get_obj_info(foo1), get_obj_info(foo2)])

    async def test_unicode(self):
        class MyFoo(Foo):
            pass
        foo = await self.store.get(Foo, 20)
        myfoo = await self.store.get(MyFoo, 20)
        for title in ['Cừơng', 'Đức', 'Hạnh']:
            foo.title = title
            await self.store.commit()
            try:
                self.assertEqual(myfoo.title, title)
            except AssertionError as e:
                raise AssertionError(str(e, 'replace') +
                    " (ensure your database was created with CREATE DATABASE"
                    " ... CHARACTER SET utf8mb3)")

    async def test_creation_order_is_preserved_when_possible(self):
        foos = [await self.store.add(Foo()) for i in range(10)]
        await self.store.flush()
        for i in range(len(foos)-1):
            self.assertTrue(foos[i].id < foos[i+1].id)

    async def test_update_order_is_preserved_when_possible(self):
        class MyFoo(Foo):
            sequence = 0
            def __storm_flushed__(self):
                self.flush_order = MyFoo.sequence
                MyFoo.sequence += 1

        foos = [await self.store.add(MyFoo()) for i in range(10)]
        await self.store.flush()

        MyFoo.sequence = 0
        for foo in foos:
            foo.title = "Changed Title"
        await self.store.flush()

        for i, foo in enumerate(foos):
            self.assertEqual(foo.flush_order, i)

    async def test_removal_order_is_preserved_when_possible(self):
        class MyFoo(Foo):
            sequence = 0
            def __storm_flushed__(self):
                self.flush_order = MyFoo.sequence
                MyFoo.sequence += 1

        foos = [await self.store.add(MyFoo()) for i in range(10)]
        await self.store.flush()

        MyFoo.sequence = 0
        for foo in foos:
            await self.store.remove(foo)
        await self.store.flush()

        for i, foo in enumerate(foos):
            self.assertEqual(foo.flush_order, i)

    async def test_cache_poisoning(self):
        """
        When a object update a field value to the previous value, which is in
        the cache, it correctly updates the value in the database.

        Because of change detection, this has been broken in the past, see bug
        #277095 in launchpad.
        """
        store = await self.create_store()
        foo2 = await store.get(Foo, 10)
        self.assertEqual(foo2.title, "Title 30")
        await store.commit()

        foo1 = await self.store.get(Foo, 10)
        foo1.title = "Title 40"
        await self.store.commit()

        foo2.title = "Title 30"
        await store.commit()
        self.assertEqual(foo2.title, "Title 30")

    async def test_execute_sends_event(self):
        """Statement execution emits the register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        await self.store.execute("SELECT 1")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], self.store)

    async def test_wb_event_before_check_connection(self):
        """
        The register-transaction event is emitted before checking the state of
        the connection.
        """
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        self.store._connection._state = STATE_DISCONNECTED
        self.assertRaises(DisconnectionError, self.store.execute, "SELECT 1")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], self.store)

    async def test_add_sends_event(self):
        """Adding an object emits the register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        foo = Foo()
        foo.title = "Foo"
        await self.store.add(foo)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], self.store)

    async def test_remove_sends_event(self):
        """Adding an object emits the register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        foo = await self.store.get(Foo, 10)
        del calls[:]

        await self.store.remove(foo)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], self.store)

    async def test_change_invalidated_object_sends_event(self):
        """Modifying an object retrieved in a previous transaction emits the
        register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        foo = await self.store.get(Foo, 10)
        await self.store.rollback()
        del calls[:]

        foo.title = "New title"
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], self.store)

    async def test_rowcount_remove(self):
        # All supported backends support rowcount, so far.
        result_to_remove = await self.store.find(Foo, Foo.id <= 30)
        self.assertEqual(await result_to_remove.remove(), 3)


class EmptyResultSetTest:

    async def asyncSetUp(self):
        self.create_database()
        self.connection = self.database.connect()
        await self.drop_tables()
        await self.create_tables()
        await self.create_store()
        # Most of the tests here exercise the same functionality using
        # self.empty and self.result to ensure that EmptyResultSet and
        # ResultSet behave the same way, in the same situations.
        self.empty = EmptyResultSet()
        self.result = await self.store.find(Foo)

    async def asyncTearDown(self):
        await self.drop_store()
        await self.drop_tables()
        self.drop_database()
        await self.connection.close()

    def create_database(self):
        raise NotImplementedError

    async def create_tables(self):
        raise NotImplementedError

    async def create_store(self):
        self.store = Store(self.database)

    def drop_database(self):
        pass

    async def drop_tables(self):
        for table in ["foo", "bar", "bin", "link"]:
            try:
                await self.connection.execute("DROP TABLE %s" % table)
                await self.connection.commit()
            except:
                await self.connection.rollback()

    async def drop_store(self):
        await self.store.rollback()
        # Closing the store is needed because testcase objects are all
        # instantiated at once, and thus connections are kept open.
        await self.store.close()

    async def test_iter(self):
        self.assertEqual([x async for x in self.result], [x async for x in self.empty])

    async def test_copy(self):
        self.assertNotEqual(self.result.copy(), self.result)
        self.assertNotEqual(self.empty.copy(), self.empty)
        self.assertEqual([x async for x in self.result.copy()], [x async for x in self.empty.copy()])

    async def test_config(self):
        self.result.config(distinct=True, offset=1, limit=1)
        self.empty.config(distinct=True, offset=1, limit=1)
        self.assertEqual([x async for x in self.result], [x async for x in self.empty])

    async def test_config_returns_self(self):
        self.assertIs(self.result, self.result.config())
        self.assertIs(self.empty, self.empty.config())

    async def test_slice(self):
        self.assertEqual([x async for x in self.result[:]], [])
        self.assertEqual([x async for x in self.empty[:]], [])

    async def test_contains(self):
        self.assertEqual(await self.empty.__contains__(Foo()), False)

    async def test_is_empty(self):
        self.assertEqual(await self.result.is_empty(), True)
        self.assertEqual(await self.empty.is_empty(), True)

    async def test_any(self):
        self.assertEqual(await self.result.any(), None)
        self.assertEqual(await self.empty.any(), None)

    async def test_first_unordered(self):
        with self.assertRaises(UnorderedError):
            await self.result.first()
        with self.assertRaises(UnorderedError):
            await self.empty.first()

    async def test_first_ordered(self):
        self.result.order_by(Foo.title)
        self.assertEqual(await self.result.first(), None)
        self.empty.order_by(Foo.title)
        self.assertEqual(await self.empty.first(), None)

    async def test_last_unordered(self):
        with self.assertRaises(UnorderedError):
            await self.result.last()
        with self.assertRaises(UnorderedError):
            await self.empty.last()

    async def test_last_ordered(self):
        self.result.order_by(Foo.title)
        self.assertEqual(await self.result.last(), None)
        self.empty.order_by(Foo.title)
        self.assertEqual(await self.empty.last(), None)

    async def test_one(self):
        self.assertEqual(await self.result.one(), None)
        self.assertEqual(await self.empty.one(), None)

    async def test_order_by(self):
        self.assertEqual(self.result.order_by(Foo.title), self.result)
        self.assertEqual(self.empty.order_by(Foo.title), self.empty)

    async def test_group_by(self):
        self.assertEqual(self.result.group_by(Foo.title), self.result)
        self.assertEqual(self.empty.group_by(Foo.title), self.empty)

    async def test_remove(self):
        self.assertEqual(await self.result.remove(), 0)
        self.assertEqual(await self.empty.remove(), 0)

    async def test_count(self):
        self.assertEqual(await self.result.count(), 0)
        self.assertEqual(await self.empty.count(), 0)
        self.assertEqual(await self.empty.count(expr="abc"), 0)
        self.assertEqual(await self.empty.count(distinct=True), 0)

    async def test_max(self):
        self.assertEqual(await self.result.max(Foo.id), None)
        self.assertEqual(await self.empty.max(Foo.id), None)

    async def test_min(self):
        self.assertEqual(await self.result.min(Foo.id), None)
        self.assertEqual(await self.empty.min(Foo.id), None)

    async def test_avg(self):
        self.assertEqual(await self.result.avg(Foo.id), None)
        self.assertEqual(await self.empty.avg(Foo.id), None)

    async def test_sum(self):
        self.assertEqual(await self.result.sum(Foo.id), None)
        self.assertEqual(await self.empty.sum(Foo.id), None)

    async def test_get_select_expr_without_columns(self):
        """
        A L{FeatureError} is raised if L{EmptyResultSet.get_select_expr} is
        called without a list of L{Column}s.
        """
        self.assertRaises(FeatureError, self.result.get_select_expr)
        self.assertRaises(FeatureError, self.empty.get_select_expr)

    async def test_get_select_expr_(self):
        """
        A L{FeatureError} is raised if L{EmptyResultSet.get_select_expr} is
        called without a list of L{Column}s.
        """
        subselect = self.result.get_select_expr(Foo.id)
        self.assertEqual((Foo.id,), subselect.columns)
        result = await self.store.find(Foo, Foo.id.is_in(subselect))
        self.assertEqual([item async for item in result], [])

        subselect = self.empty.get_select_expr(Foo.id)
        self.assertEqual((Foo.id,), subselect.columns)
        result = await self.store.find(Foo, Foo.id.is_in(subselect))
        self.assertEqual([item async for item in result], [])

    async def test_values_no_columns(self):
        self.assertRaises(FeatureError, list, self.result.values())
        self.assertRaises(FeatureError, list, self.empty.values())

    async def test_values(self):
        self.assertEqual(list(self.result.values(Foo.title)), [])
        self.assertEqual(list(self.empty.values(Foo.title)), [])

    async def test_set_no_args(self):
        self.assertEqual(await self.result.set(), None)
        self.assertEqual(self.empty.set(), None)

    async def test_cached(self):
        self.assertEqual(self.result.cached(), [])
        self.assertEqual(self.empty.cached(), [])

    async def test_find(self):
        self.assertEqual([item async for item in self.result.find(Foo.title == "foo")], [])
        self.assertEqual([item async for item in self.empty.find(Foo.title == "foo")], [])

    async def test_union(self):
        self.assertEqual(self.empty.union(self.empty), self.empty)
        self.assertEqual(type(self.empty.union(self.result)),
                         type(self.result))
        self.assertEqual(type(self.result.union(self.empty)),
                         type(self.result))

    async def test_difference(self):
        self.assertEqual(self.empty.difference(self.empty), self.empty)
        self.assertEqual(self.empty.difference(self.result), self.empty)
        self.assertEqual(self.result.difference(self.empty), self.result)

    async def test_intersection(self):
        self.assertEqual(self.empty.intersection(self.empty), self.empty)
        self.assertEqual(self.empty.intersection(self.result), self.empty)
        self.assertEqual(self.result.intersection(self.empty), self.empty)
