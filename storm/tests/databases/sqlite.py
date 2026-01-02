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
from datetime import timedelta
import time
import os

from storm.exceptions import OperationalError
from storm.databases.sqlite import SQLite
from storm.database import create_database
from storm.uri import URI
from storm.tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from storm.tests.helper import AsyncTestHelper, MakePath


class SQLiteMemoryTest(DatabaseTest, AsyncTestHelper):

    helpers = [MakePath]
    
    def get_path(self):
        return ""

    def create_database(self):
        self.database = SQLite(URI("sqlite:%s?synchronous=OFF&timeout=0" %
                                   self.get_path()))

    async def create_tables(self):
        await self.connection.execute("CREATE TABLE number "
                                "(one INTEGER, two INTEGER, three INTEGER)")
        await self.connection.execute("CREATE TABLE test "
                                "(id INTEGER PRIMARY KEY, title VARCHAR)")
        await self.connection.execute("CREATE TABLE datetime_test "
                                "(id INTEGER PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME, td INTERVAL)")
        await self.connection.execute("CREATE TABLE bin_test "
                                "(id INTEGER PRIMARY KEY, b BLOB)")

    async def drop_tables(self):
        pass

    async def test_wb_create_database(self):
        database = create_database("sqlite:")
        self.assertTrue(isinstance(database, SQLite))
        self.assertEqual(database._filename, ":memory:")

    async def test_concurrent_behavior(self):
        pass # We can't connect to the in-memory database twice, so we can't
             # exercise the concurrency behavior (nor it makes sense).

    async def test_synchronous(self):
        synchronous_values = {"OFF": 0, "NORMAL": 1, "FULL": 2}
        for value in synchronous_values:
            database = SQLite(URI("sqlite:%s?synchronous=%s" %
                                  (self.get_path(), value)))
            connection = database.connect()
            result = await connection.execute("PRAGMA synchronous")
            self.assertEqual((await result.get_one())[0],
                             synchronous_values[value])
            await connection.close()

    def test_sqlite_specific_reserved_words(self):
        """Check sqlite-specific reserved words are recognized.

        This uses a list copied from http://www.sqlite.org/lang_keywords.html
        with the reserved words from SQL1992 removed.
        """
        reserved_words = """
            abort after analyze attach autoincrement before conflict
            database detach each exclusive explain fail glob if ignore
            index indexed instead isnull limit notnull offset plan
            pragma query raise regexp reindex release rename replace
            row savepoint temp trigger vacuum virtual
            """.split()
        for word in reserved_words:
            self.assertTrue(self.connection.compile.is_reserved_word(word),
                            "Word missing: %s" % (word,))


class SQLiteFileTest(SQLiteMemoryTest):

    def get_path(self):
        return self.make_path()

    def test_wb_create_database(self):
        filename = self.make_path()
        database = create_database("sqlite:%s" % filename)
        self.assertTrue(isinstance(database, SQLite))
        self.assertEqual(database._filename, filename)

    async def test_timeout(self):
        database = create_database("sqlite:%s?timeout=0.3" % self.get_path())
        connection1 = database.connect()
        connection2 = database.connect()
        try:
            await connection1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            await connection1.commit()
            await connection1.execute("INSERT INTO test VALUES (1)")
            started = time.time()
            try:
                await connection2.execute("INSERT INTO test VALUES (2)")
            except OperationalError as exception:
                self.assertEqual(str(exception), "database is locked")
                self.assertTrue(time.time()-started >= 0.3)
            else:
                self.fail("OperationalError not raised")
        finally:
            await connection1.close()
            await connection2.close()

    async def test_commit_timeout(self):
        """Regression test for commit observing the timeout.

        In 0.10, the timeout wasn't observed for connection.commit().

        """
        # Create a database with a table.
        database = create_database("sqlite:%s?timeout=0.3" % self.get_path())
        connection1 = database.connect()
        connection2 = None
        try:
            await connection1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            await connection1.commit()

            # Put some data in, but also make a second connection to the database,
            # which will prevent a commit until it is closed.
            await connection1.execute("INSERT INTO test VALUES (1)")
            connection2 = database.connect()
            await connection2.execute("SELECT id FROM test")

            started = time.time()
            try:
                await connection1.commit()
            except OperationalError as exception:
                self.assertEqual(str(exception), "database is locked")
                # In 0.10, the next assertion failed because the timeout wasn't
                # enforced for the "COMMIT" statement.
                self.assertTrue(time.time()-started >= 0.3)
            else:
                self.fail("OperationalError not raised")
        finally:
            await connection1.close()
            if connection2 is not None:
                await connection2.close()

    async def test_recover_after_timeout(self):
        """Regression test for recovering from database locked exception.

        In 0.10, connection.commit() would forget that a transaction was in
        progress if an exception was raised, such as an OperationalError due to
        another connection being open.  As a result, a subsequent modification
        to the database would cause BEGIN to be issued to the database, which
        would complain that a transaction was already in progress.

        """
        # Create a database with a table.
        database = create_database("sqlite:%s?timeout=0.3" % self.get_path())
        connection1 = database.connect()
        await connection1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        await connection1.commit()

        # Put some data in, but also make a second connection to the database,
        # which will prevent a commit until it is closed.
        await connection1.execute("INSERT INTO test VALUES (1)")
        connection2 = database.connect()
        await connection2.execute("SELECT id FROM test")
        with self.assertRaises(OperationalError):
            await connection1.commit()

        # Close the second connection - it should now be possible to commit.
        await connection2.close()

        # In 0.10, the next statement raised OperationalError: cannot start a
        # transaction within a transaction
        await connection1.execute("INSERT INTO test VALUES (2)")
        await connection1.commit()

        # Check that the correct data is present
        self.assertEqual(await (await connection1.execute("SELECT id FROM test")).get_all(),
                         [(1,), (2,)])

        await connection1.close()

    async def test_journal(self):
        journal_values = {"DELETE": 'delete', "TRUNCATE": 'truncate',
                          "PERSIST": 'persist', "MEMORY": 'memory',
                          "WAL": 'wal', "OFF": 'off'}
        for value in journal_values:
            database = SQLite(URI("sqlite:%s?journal_mode=%s" %
                                  (self.make_path(), value)))
            connection = database.connect()
            result = (await (await connection.execute("PRAGMA journal_mode")).get_one())[0]
            self.assertEqual(result,
                             journal_values[value])
            await connection.close()

    async def test_journal_persistency_to_rollback(self):
        journal_values = {"DELETE": 'delete', "TRUNCATE": 'truncate',
                          "PERSIST": 'persist', "MEMORY": 'memory',
                          "WAL": 'wal', "OFF": 'off'}
        for value in journal_values:
            path = self.make_path()
            database = SQLite(URI("sqlite:%s?journal_mode=%s" %
                                  (path, value)))
            connection = database.connect()
            await connection.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            await connection.rollback()
            result = (await (await connection.execute("PRAGMA journal_mode")).get_one())[0]
            self.assertEqual(result, journal_values[value],
                           f"Expected {journal_values[value]} for {value} at {path}, got {result}")
            await connection.close()

    async def test_foreign_keys(self):
        foreign_keys_values = {"ON": 1, "OFF": 0}
        for value in foreign_keys_values:
            database = SQLite(URI("sqlite:%s?foreign_keys=%s" %
                                  (self.make_path(), value)))
            connection = database.connect()
            result = (await (await connection.execute("PRAGMA foreign_keys")).get_one())[0]
            self.assertEqual(result,
                             foreign_keys_values[value])
            await connection.close()

    async def test_foreign_keys_persistency_to_rollback(self):
        foreign_keys_values = {"ON": 1, "OFF": 0}
        for value in foreign_keys_values:
            database = SQLite(URI("sqlite:%s?foreign_keys=%s" %
                                  (self.make_path(), value)))
            connection = database.connect()
            await connection.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            await connection.rollback()
            result = (await (await connection.execute("PRAGMA foreign_keys")).get_one())[0]
            self.assertEqual(result,
                             foreign_keys_values[value])
            await connection.close()

class SQLiteUnsupportedTest(UnsupportedDatabaseTest, AsyncTestHelper):
 
    dbapi_module_names = ["aiosqlite"]
    db_module_name = "sqlite"
