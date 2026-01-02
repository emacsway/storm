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
import os
import gc

from storm.database import create_database
from storm.properties import Enum, Int, List
from storm.info import get_obj_info
from storm.tests.store.base import StoreTest, EmptyResultSetTest, Foo
from storm.tests.helper import AsyncTestHelper


class Lst1:
    __storm_table__ = "lst1"
    id = Int(primary=True)
    ints = List(type=Int())

class LstEnum:
    __storm_table__ = "lst1"
    id = Int(primary=True)
    ints = List(type=Enum(map={"one": 1, "two": 2, "three": 3}))

class Lst2:
    __storm_table__ = "lst2"
    id = Int(primary=True)
    ints = List(type=List(type=Int()))

class FooWithSchema(Foo):
    __storm_table__ = "public.foo"


class PostgresStoreTest(AsyncTestHelper, StoreTest):

    async def asyncSetUp(self):
        await AsyncTestHelper.asyncSetUp(self)
        await StoreTest.asyncSetUp(self)

    async def asyncTearDown(self):
        await AsyncTestHelper.asyncTearDown(self)
        await StoreTest.asyncTearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    async def create_tables(self):
        connection = self.connection
        await connection.execute("CREATE TABLE foo "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        # Prevent dynamically created Foos from having conflicting ids.
        await connection.execute("SELECT setval('foo_id_seq', 1000)")
        await connection.execute("CREATE TABLE bar "
                           "(id SERIAL PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
        await connection.execute("CREATE TABLE bin "
                           "(id SERIAL PRIMARY KEY, bin BYTEA, foo_id INTEGER)")
        await connection.execute("CREATE TABLE link "
                           "(foo_id INTEGER, bar_id INTEGER,"
                           " PRIMARY KEY (foo_id, bar_id))")
        await connection.execute("CREATE TABLE money "
                           "(id SERIAL PRIMARY KEY, value NUMERIC(6,4))")
        await connection.execute("CREATE TABLE selfref "
                           "(id SERIAL PRIMARY KEY, title VARCHAR,"
                           " selfref_id INTEGER REFERENCES selfref(id))")
        await connection.execute("CREATE TABLE lst1 "
                           "(id SERIAL PRIMARY KEY, ints INTEGER[])")
        await connection.execute("CREATE TABLE lst2 "
                           "(id SERIAL PRIMARY KEY, ints INTEGER[][])")
        await connection.execute("CREATE TABLE foovalue "
                           "(id SERIAL PRIMARY KEY, foo_id INTEGER,"
                           " value1 INTEGER, value2 INTEGER)")
        await connection.execute("CREATE TABLE unique_id "
                           "(id UUID PRIMARY KEY)")
        await connection.commit()

    async def drop_tables(self):
        await StoreTest.drop_tables(self)
        for table in ["lst1", "lst2"]:
            try:
                await self.connection.execute("DROP TABLE %s" % table)
                await self.connection.commit()
            except:
                await self.connection.rollback()

    async def test_list_variable(self):

        lst = Lst1()
        lst.id = 1
        lst.ints = [1,2,3,4]

        self.store.add(lst)

        result = await self.store.execute("SELECT ints FROM lst1 WHERE id=1")
        self.assertEqual(await result.get_one(), ([1,2,3,4],))

        del lst
        gc.collect()

        lst = await (await self.store.find(Lst1, Lst1.ints == [1,2,3,4])).one()
        self.assertTrue(lst)

        lst.ints.append(5)

        result = await self.store.execute("SELECT ints FROM lst1 WHERE id=1")
        self.assertEqual(await result.get_one(), ([1,2,3,4,5],))

    async def test_list_enum_variable(self):

        lst = LstEnum()
        lst.id = 1
        lst.ints = ["one", "two"]
        self.store.add(lst)

        result = await self.store.execute("SELECT ints FROM lst1 WHERE id=1")
        self.assertEqual(await result.get_one(), ([1,2],))

        del lst
        gc.collect()

        lst = await (await self.store.find(LstEnum, LstEnum.ints == ["one", "two"])).one()
        self.assertTrue(lst)

        lst.ints.append("three")

        result = await self.store.execute("SELECT ints FROM lst1 WHERE id=1")
        self.assertEqual(await result.get_one(), ([1,2,3],))

    async def test_list_variable_nested(self):

        lst = Lst2()
        lst.id = 1
        lst.ints = [[1, 2], [3, 4]]

        self.store.add(lst)

        result = await self.store.execute("SELECT ints FROM lst2 WHERE id=1")
        self.assertEqual(await result.get_one(), ([[1,2],[3,4]],))

        del lst
        gc.collect()

        lst = await (await self.store.find(Lst2, Lst2.ints == [[1,2],[3,4]])).one()
        self.assertTrue(lst)

        lst.ints.append([5, 6])

        result = await self.store.execute("SELECT ints FROM lst2 WHERE id=1")
        self.assertEqual(await result.get_one(), ([[1,2],[3,4],[5,6]],))

    async def test_add_find_with_schema(self):
        foo = FooWithSchema()
        foo.title = "Title"
        self.store.add(foo)
        await self.store.flush()
        # We use find() here to actually exercise the backend code.
        # get() would just pick the object from the cache.
        self.assertEqual(await (await self.store.find(FooWithSchema, id=foo.id)).one(), foo)

    async def test_wb_currval_based_identity(self):
        """
        Ensure that the currval()-based identity retrieval continues
        to work, even if we're currently running on a 8.2+ database.
        """
        self.database._version = 80109
        foo1 = self.store.add(Foo())
        await self.store.flush()
        foo2 = self.store.add(Foo())
        await self.store.flush()
        self.assertEqual(foo2.id-foo1.id, 1)

    async def test_list_unnecessary_update(self):
        """
        Flushing an object with a list variable doesn't create an unnecessary
        UPDATE statement.
        """
        await self.store.execute("INSERT INTO lst1 VALUES (1, '{}')", noresult=True)

        lst = await (await self.store.find(Lst1, id=1)).one()
        self.assertTrue(lst)
        self.store.invalidate()

        lst2 = await (await self.store.find(Lst1, id=1)).one()
        self.assertTrue(lst2)
        obj_info = get_obj_info(lst2)
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))
        await self.store.flush()
        self.assertEqual(events, [])


class PostgresEmptyResultSetTest(AsyncTestHelper, EmptyResultSetTest):

    async def asyncSetUp(self):
        await AsyncTestHelper.asyncSetUp(self)
        await EmptyResultSetTest.asyncSetUp(self)

    async def asyncTearDown(self):
        await AsyncTestHelper.asyncTearDown(self)
        await EmptyResultSetTest.asyncTearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    async def create_tables(self):
        await self.connection.execute("CREATE TABLE foo "
                                "(id SERIAL PRIMARY KEY,"
                                " title VARCHAR DEFAULT 'Default Title')")
        await self.connection.commit()
