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

from storm.database import create_database

from storm.tests.store.base import StoreTest, EmptyResultSetTest
from storm.tests.helper import AsyncTestHelper


class MySQLStoreTest(AsyncTestHelper, StoreTest):

    async def asyncSetUp(self):
        await AsyncTestHelper.asyncSetUp(self)
        await StoreTest.asyncSetUp(self)

    async def asyncTearDown(self):
        await AsyncTestHelper.asyncTearDown(self)
        await StoreTest.asyncTearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_MYSQL_URI"])

    async def create_tables(self):
        connection = self.connection
        await connection.execute("CREATE TABLE foo "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " title VARCHAR(50) DEFAULT 'Default Title') "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE bar "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " foo_id INTEGER, title VARCHAR(50)) "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE bin "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " bin BLOB, foo_id INTEGER) "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE link "
                           "(foo_id INTEGER, bar_id INTEGER,"
                           " PRIMARY KEY (foo_id, bar_id)) "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE money "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " value NUMERIC(6,4)) "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE selfref "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " title VARCHAR(50),"
                           " selfref_id INTEGER,"
                           " INDEX (selfref_id),"
                           " FOREIGN KEY (selfref_id) REFERENCES selfref(id)) "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE foovalue "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " foo_id INTEGER,"
                           " value1 INTEGER, value2 INTEGER) "
                           "ENGINE=InnoDB")
        await connection.execute("CREATE TABLE unique_id "
                           "(id VARCHAR(36) PRIMARY KEY) "
                           "ENGINE=InnoDB")
        await connection.commit()


class MySQLEmptyResultSetTest(AsyncTestHelper, EmptyResultSetTest):

    async def asyncSetUp(self):
        await AsyncTestHelper.asyncSetUp(self)
        await EmptyResultSetTest.asyncSetUp(self)

    async def asyncTearDown(self):
        await AsyncTestHelper.asyncTearDown(self)
        await EmptyResultSetTest.asyncTearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_MYSQL_URI"])

    async def create_tables(self):
        await self.connection.execute("CREATE TABLE foo "
                                "(id INT PRIMARY KEY AUTO_INCREMENT,"
                                " title VARCHAR(50) DEFAULT 'Default Title') "
                                "ENGINE=InnoDB")
        await self.connection.commit()
