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
from storm.databases.sqlite import SQLite
from storm.uri import URI
from storm.tests.store.base import StoreTest, EmptyResultSetTest
from storm.tests.helper import AsyncTestHelper, MakePath


class SQLiteStoreTest(AsyncTestHelper, StoreTest):

    helpers = [MakePath]

    async def asyncSetUp(self):
        await AsyncTestHelper.asyncSetUp(self)
        await StoreTest.asyncSetUp(self)

    async def asyncTearDown(self):
        await AsyncTestHelper.asyncTearDown(self)
        await StoreTest.asyncTearDown(self)

    def create_database(self):
        self.database = SQLite(URI("sqlite:%s?synchronous=OFF" %
                                   self.make_path()))

    async def create_tables(self):
        connection = self.connection
        await connection.execute("CREATE TABLE foo "
                           "(id INTEGER PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        await connection.execute("CREATE TABLE bar "
                           "(id INTEGER PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
        await connection.execute("CREATE TABLE bin "
                           "(id INTEGER PRIMARY KEY, bin BLOB, foo_id INTEGER)")
        await connection.execute("CREATE TABLE link "
                           "(foo_id INTEGER, bar_id INTEGER)")
        # We have to use TEXT here, since NUMERIC would cause SQLite
        # to interpret values as float, and thus lose precision.
        await connection.execute("CREATE TABLE money "
                           "(id INTEGER PRIMARY KEY, value TEXT)")
        await connection.execute("CREATE TABLE selfref "
                           "(id INTEGER PRIMARY KEY, title VARCHAR,"
                           " selfref_id INTEGER)")
        await connection.execute("CREATE TABLE foovalue "
                           "(id INTEGER PRIMARY KEY, foo_id INTEGER,"
                           " value1 INTEGER, value2 INTEGER)")
        await connection.execute("CREATE TABLE unique_id "
                           "(id VARCHAR PRIMARY KEY)")
        await connection.commit()

    def drop_tables(self):
        pass


class SQLiteEmptyResultSetTest(AsyncTestHelper, EmptyResultSetTest):

    helpers = [MakePath]

    async def asyncSetUp(self):
        await AsyncTestHelper.asyncSetUp(self)
        await EmptyResultSetTest.asyncSetUp(self)

    async def asyncTearDown(self):
        await AsyncTestHelper.asyncTearDown(self)
        await EmptyResultSetTest.asyncTearDown(self)

    def create_database(self):
        self.database = SQLite(URI("sqlite:%s?synchronous=OFF" %
                                   self.make_path()))

    async def create_tables(self):
        await self.connection.execute("CREATE TABLE foo "
                                "(id INTEGER PRIMARY KEY,"
                                " title VARCHAR DEFAULT 'Default Title')")
        await self.connection.commit()

    def drop_tables(self):
        pass
