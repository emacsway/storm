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
from datetime import datetime, date, time, timedelta
import pickle
import shutil
import sys
import os

from storm.uri import URI
from storm.expr import Select, Column, SQLToken, SQLRaw, Count, Alias
from storm.variables import (Variable, PickleVariable, BytesVariable,
                             DecimalVariable, DateTimeVariable, DateVariable,
                             TimeVariable, TimeDeltaVariable)
from storm.database import *
from storm.xid import Xid
from storm.event import EventSystem
from storm.exceptions import (
    DatabaseError, DatabaseModuleError, ConnectionBlockedError,
    DisconnectionError, Error, OperationalError, ProgrammingError)
from storm.tests.databases.proxy import ProxyTCPServer
from storm.tests.helper import MakePath


class Marker:
    pass

marker = Marker()


class DatabaseTest:

    supports_microseconds = True

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.create_database()
        await self.create_connection()
        await self.drop_tables()
        await self.create_tables()
        await self.create_sample_data()

    async def asyncTearDown(self):
        await self.drop_sample_data()
        await self.drop_tables()
        await self.drop_connection()
        self.drop_database()
        await super().asyncTearDown()

    def create_database(self):
        raise NotImplementedError

    async def create_connection(self):
        self.connection = self.database.connect()

    async def create_tables(self):
        raise NotImplementedError

    async def create_sample_data(self):
        await self.connection.execute("INSERT INTO number VALUES (1, 2, 3)")
        await self.connection.execute("INSERT INTO test VALUES (10, 'Title 10')")
        await self.connection.execute("INSERT INTO test VALUES (20, 'Title 20')")
        await self.connection.commit()

    async def drop_sample_data(self):
        pass

    async def drop_tables(self):
        for table in ["number", "test", "datetime_test", "bin_test"]:
            try:
                await self.connection.execute("DROP TABLE " + table)
                await self.connection.commit()
            except:
                await self.connection.rollback()

    async def drop_connection(self):
        await self.connection.close()

    def drop_database(self):
        pass

    async def test_create(self):
        self.assertTrue(isinstance(self.database, Database))

    async def test_get_uri(self):
        """
        The get_uri() method returns the URI the database with created with.
        """
        uri = self.database.get_uri()
        self.assertIsNotNone(uri.scheme)

    async def test_connection(self):
        self.assertTrue(isinstance(self.connection, Connection))

    async def test_rollback(self):
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.rollback()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertFalse(await result.get_one())

    async def test_rollback_twice(self):
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.rollback()
        await self.connection.rollback()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertFalse(await result.get_one())

    async def test_commit(self):
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.commit()
        await self.connection.rollback()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertTrue(await result.get_one())

    async def test_commit_twice(self):
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.commit()
        await self.connection.commit()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertTrue(await result.get_one())

    async def test_execute_result(self):
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))
        self.assertTrue(await result.get_one())

    async def test_execute_unicode_result(self):
        result = await self.connection.execute("SELECT title FROM test")
        self.assertTrue(isinstance(result, Result))
        row = await result.get_one()
        self.assertEqual(row, ("Title 10",))
        self.assertTrue(isinstance(row[0], str))

    async def test_execute_params(self):
        result = await self.connection.execute("SELECT one FROM number "
                                         "WHERE 1=?", (1,))
        self.assertTrue(await result.get_one())
        result = await self.connection.execute("SELECT one FROM number "
                                         "WHERE 1=?", (2,))
        self.assertFalse(await result.get_one())

    async def test_execute_empty_params(self):
        result = await self.connection.execute("SELECT one FROM number", ())
        self.assertTrue(await result.get_one())

    async def test_execute_expression(self):
        result = await self.connection.execute(Select(1))
        self.assertTrue(await result.get_one(), (1,))

    async def test_execute_expression_empty_params(self):
        result = await self.connection.execute(Select(SQLRaw("1")))
        self.assertTrue(await result.get_one(), (1,))

    async def test_get_one(self):
        result = await self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEqual(await result.get_one(), (10, "Title 10"))

    async def test_get_all(self):
        result = await self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEqual(await result.get_all(),
                         [(10, "Title 10"), (20, "Title 20")])

    async def test_iter(self):
        result = await self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEqual([item async for item in result],
                         [(10, "Title 10"), (20, "Title 20")])

    async def test_simultaneous_iter(self):
        result1 = await self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id ASC")
        result2 = await self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id DESC")
        iter1 = aiter(result1)
        iter2 = aiter(result2)
        self.assertEqual(await anext(iter1), (10, "Title 10"))
        self.assertEqual(await anext(iter2), (20, "Title 20"))
        self.assertEqual(await anext(iter1), (20, "Title 20"))
        self.assertEqual(await anext(iter2), (10, "Title 10"))
        with self.assertRaises(StopAsyncIteration):
            await anext(iter1)
        with self.assertRaises(StopAsyncIteration):
            await anext(iter2)

    async def test_get_insert_identity(self):
        result = await self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        primary_key = (Column("id", SQLToken("test")),)
        primary_variables = (Variable(),)
        expr = result.get_insert_identity(primary_key, primary_variables)
        select = Select(Column("title", SQLToken("test")), expr)
        result = await self.connection.execute(select)
        self.assertEqual(await result.get_one(), ("Title 30",))

    async def test_get_insert_identity_composed(self):
        result = await self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        primary_key = (Column("id", SQLToken("test")),
                       Column("title", SQLToken("test")))
        primary_variables = (Variable(), Variable("Title 30"))
        expr = result.get_insert_identity(primary_key, primary_variables)
        select = Select(Column("title", SQLToken("test")), expr)
        result = await self.connection.execute(select)
        self.assertEqual(await result.get_one(), ("Title 30",))

    async def test_datetime(self):
        value = datetime(1977, 4, 5, 12, 34, 56, 78)
        await self.connection.execute("INSERT INTO datetime_test (dt) VALUES (?)",
                                (value,))
        result = await self.connection.execute("SELECT dt FROM datetime_test")
        variable = DateTimeVariable()
        result.set_variable(variable, (await result.get_one())[0])
        if not self.supports_microseconds:
            value = value.replace(microsecond=0)
        self.assertEqual(variable.get(), value)

    async def test_date(self):
        value = date(1977, 4, 5)
        await self.connection.execute("INSERT INTO datetime_test (d) VALUES (?)",
                                (value,))
        result = await self.connection.execute("SELECT d FROM datetime_test")
        variable = DateVariable()
        result.set_variable(variable, (await result.get_one())[0])
        self.assertEqual(variable.get(), value)

    async def test_time(self):
        value = time(12, 34, 56, 78)
        await self.connection.execute("INSERT INTO datetime_test (t) VALUES (?)",
                                (value,))
        result = await self.connection.execute("SELECT t FROM datetime_test")
        variable = TimeVariable()
        result.set_variable(variable, (await result.get_one())[0])
        if not self.supports_microseconds:
            value = value.replace(microsecond=0)
        self.assertEqual(variable.get(), value)

    async def test_timedelta(self):
        value = timedelta(12, 34, 56)
        await self.connection.execute("INSERT INTO datetime_test (td) VALUES (?)",
                                (value,))
        result = await self.connection.execute("SELECT td FROM datetime_test")
        variable = TimeDeltaVariable()
        result.set_variable(variable, (await result.get_one())[0])
        self.assertEqual(variable.get(), value)

    async def test_pickle(self):
        value = {"a": 1, "b": 2}
        value_dump = pickle.dumps(value, -1)
        await self.connection.execute("INSERT INTO bin_test (b) VALUES (?)",
                                (value_dump,))
        result = await self.connection.execute("SELECT b FROM bin_test")
        variable = PickleVariable()
        result.set_variable(variable, (await result.get_one())[0])
        self.assertEqual(variable.get(), value)

    async def test_binary(self):
        """Ensure database works with high bits and embedded zeros."""
        value = b"\xff\x00\xff\x00"
        await self.connection.execute("INSERT INTO bin_test (b) VALUES (?)",
                                (value,))
        result = await self.connection.execute("SELECT b FROM bin_test")
        variable = BytesVariable()
        result.set_variable(variable, (await result.get_one())[0])
        self.assertEqual(variable.get(), value)

    async def test_binary_ascii(self):
        """Some databases like pysqlite2 may return unicode for strings."""
        await self.connection.execute("INSERT INTO bin_test VALUES (10, 'Value')")
        result = await self.connection.execute("SELECT b FROM bin_test")
        variable = BytesVariable()
        # If the following doesn't raise a TypeError we're good.
        result.set_variable(variable, (await result.get_one())[0])
        self.assertEqual(variable.get(), b"Value")

    async def test_order_by_group_by(self):
        await self.connection.execute("INSERT INTO test VALUES (100, 'Title 10')")
        await self.connection.execute("INSERT INTO test VALUES (101, 'Title 10')")
        id = Column("id", "test")
        title = Column("title", "test")
        expr = Select(Count(id), group_by=title, order_by=Count(id))
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(1,), (3,)])

    async def test_set_decimal_variable_from_str_column(self):
        await self.connection.execute("INSERT INTO test VALUES (40, '40.5')")
        variable = DecimalVariable()
        result = await self.connection.execute("SELECT title FROM test WHERE id=40")
        result.set_variable(variable, (await result.get_one())[0])

    async def test_get_decimal_variable_to_str_column(self):
        variable = DecimalVariable()
        variable.set("40.5", from_db=True)
        await self.connection.execute("INSERT INTO test VALUES (40, ?)", (variable,))
        result = await self.connection.execute("SELECT title FROM test WHERE id=40")
        self.assertEqual((await result.get_one())[0], "40.5")

    async def test_quoting(self):
        # FIXME "with'quote" should be in the list below, but it doesn't
        #       work because it breaks the parameter mark translation.
        for reserved_name in ["with space", 'with`"escape', "SELECT"]:
            reserved_name = SQLToken(reserved_name)
            expr = Select(reserved_name,
                          tables=Alias(Select(Alias(1, reserved_name))))
            result = await self.connection.execute(expr)
            self.assertEqual(await result.get_one(), (1,))

    async def test_concurrent_behavior(self):
        """The default behavior should be to handle transactions in isolation.

        Data committed in one transaction shouldn't be visible to another
        running transaction before the later is committed or aborted.  If
        this isn't the case, the caching made by Storm (or by anything
        that works with data in memory, in fact) becomes a dangerous thing.

        For PostgreSQL, isolation level must be SERIALIZABLE.
        For MySQL, isolation level must be REPEATABLE READ (the default),
        and the InnoDB engine must be in use.
        For SQLite, the isolation level already is SERIALIZABLE when not
        in autocommit mode.  OTOH, PySQLite is nuts regarding transactional
        behavior, and will easily offer READ COMMITTED behavior inside a
        "transaction" (it didn't tell SQLite to open a transaction, in fact).
        """
        connection1 = self.connection
        connection2 = self.database.connect()
        try:
            result = await connection1.execute("SELECT title FROM test WHERE id=10")
            self.assertEqual(await result.get_one(), ("Title 10",))
            try:
                await connection2.execute("UPDATE test SET title='Title 100' "
                                    "WHERE id=10")
                await connection2.commit()
            except OperationalError as e:
                self.assertEqual(str(e), "database is locked") # SQLite blocks
            result = await connection1.execute("SELECT title FROM test WHERE id=10")
            self.assertEqual(await result.get_one(), ("Title 10",))
        finally:
            await connection1.rollback()

    async def test_wb_connect_sets_event_system(self):
        connection = self.database.connect(marker)
        self.assertEqual(connection._event, marker)

    async def test_execute_sends_event(self):
        event = EventSystem(marker)
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        event.hook("register-transaction", register_transaction)

        connection = self.database.connect(event)
        await connection.execute("SELECT 1")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], marker)

    def from_database(self, row):
        return [int(item)+1 for item in row]

    async def test_wb_result_get_one_goes_through_from_database(self):
        result = await self.connection.execute("SELECT one, two FROM number")
        result.from_database = self.from_database
        self.assertEqual(await result.get_one(), (2, 3))

    async def test_wb_result_get_all_goes_through_from_database(self):
        result = await self.connection.execute("SELECT one, two FROM number")
        result.from_database = self.from_database
        self.assertEqual(await result.get_all(), [(2, 3)])

    async def test_wb_result_iter_goes_through_from_database(self):
        result = await self.connection.execute("SELECT one, two FROM number")
        result.from_database = self.from_database
        self.assertEqual(await anext(aiter(result)), (2, 3))

    async def test_rowcount_insert(self):
        # All supported backends support rowcount, so far.
        result = await self.connection.execute(
            "INSERT INTO test VALUES (999, '999')")
        self.assertEqual(result.rowcount, 1)

    async def test_rowcount_delete(self):
        # All supported backends support rowcount, so far.
        result = await self.connection.execute("DELETE FROM test")
        self.assertEqual(result.rowcount, 2)

    async def test_rowcount_update(self):
        # All supported backends support rowcount, so far.
        result = await self.connection.execute(
            "UPDATE test SET title='whatever'")
        self.assertEqual(result.rowcount, 2)

    async def test_expr_startswith(self):
        await self.connection.execute("INSERT INTO test VALUES (30, '!!_%blah')")
        await self.connection.execute("INSERT INTO test VALUES (40, '!!blah')")
        id = Column("id", SQLToken("test"))
        title = Column("title", SQLToken("test"))
        expr = Select(id, title.startswith("!!_%"))
        result = [item async for item in await self.connection.execute(expr)]
        self.assertEqual(result, [(30,)])

    async def test_expr_endswith(self):
        await self.connection.execute("INSERT INTO test VALUES (30, 'blah_%!!')")
        await self.connection.execute("INSERT INTO test VALUES (40, 'blah!!')")
        id = Column("id", SQLToken("test"))
        title = Column("title", SQLToken("test"))
        expr = Select(id, title.endswith("_%!!"))
        result = [item async for item in await self.connection.execute(expr)]
        self.assertEqual(result, [(30,)])

    async def test_expr_contains_string(self):
        await self.connection.execute("INSERT INTO test VALUES (30, 'blah_%!!x')")
        await self.connection.execute("INSERT INTO test VALUES (40, 'blah!!x')")
        id = Column("id", SQLToken("test"))
        title = Column("title", SQLToken("test"))
        expr = Select(id, title.contains_string("_%!!"))
        result = [item async for item in await self.connection.execute(expr)]
        self.assertEqual(result, [(30,)])

    async def test_block_access(self):
        """Access to the connection is blocked by block_access()."""
        await self.connection.execute("SELECT 1")
        self.connection.block_access()
        with self.assertRaises(ConnectionBlockedError):
            await self.connection.execute("SELECT 1")
        with self.assertRaises(ConnectionBlockedError):
            await self.connection.commit()
        # Allow rolling back a blocked connection.
        await self.connection.rollback()
        # Unblock the connection, allowing access again.
        self.connection.unblock_access()
        await self.connection.execute("SELECT 1")

    async def test_wrap_exception_subclasses(self):
        """Subclasses of the generic DB-API exception types are wrapped."""
        db_api_operational_error = getattr(
            self.database._exception_module, 'OperationalError')
        operational_error_types = [
            type(name, (db_api_operational_error,), {})
            for name in ('A', 'B')]
        for error_type in operational_error_types:
            error = error_type('error message')
            wrapped = self.database._wrap_exception(OperationalError, error)
            self.assertTrue(isinstance(wrapped, error_type))
            self.assertTrue(isinstance(wrapped, OperationalError))
            self.assertEqual(error_type.__name__, wrapped.__class__.__name__)
            self.assertEqual(('error message',), wrapped.args)


class TwoPhaseCommitTest:

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.create_database()
        await self.create_connection()
        await self.drop_tables()
        await self.create_tables()

    async def asyncTearDown(self):
        await self.drop_tables()
        await self.drop_connection()
        await super().asyncTearDown()

    def create_database(self):
        raise NotImplementedError

    async def create_connection(self):
        self.connection = self.database.connect()

    async def create_tables(self):
        raise NotImplementedError

    async def drop_tables(self):
        try:
            await self.connection.execute("DROP TABLE test")
            await self.connection.commit()
        except:
            await self.connection.rollback()

    async def drop_connection(self):
        await self.connection.close()

    async def test_begin(self):
        """
        begin() starts a transaction that can be ended with a two-phase commit.
        """
        xid = Xid(0, "foo", "bar")
        await self.connection.begin(xid)
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.prepare()
        await self.connection.commit()
        await self.connection.rollback()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertTrue(await result.get_one())

    async def test_begin_inside_a_two_phase_transaction(self):
        """
        begin() can't be used if a two-phase transaction has already started.
        """
        xid1 = Xid(0, "foo", "bar")
        await self.connection.begin(xid1)
        xid2 = Xid(1, "egg", "baz")
        with self.assertRaises(ProgrammingError):
            await self.connection.begin(xid2)

    async def test_begin_after_commit(self):
        """
        After a two phase commit, it's possible to start a new transaction.
        """
        xid = Xid(0, "foo", "bar")
        await self.connection.begin(xid)
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.commit()
        await self.connection.begin(xid)
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertTrue(await result.get_one())

    async def test_begin_after_rollback(self):
        """
        After a tpc rollback, it's possible to start a new transaction.
        """
        xid = Xid(0, "foo", "bar")
        await self.connection.begin(xid)
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.rollback()
        await self.connection.begin(xid)
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertFalse(await result.get_one())

    async def test_prepare_outside_a_two_phase_transaction(self):
        """
        prepare() can't be used if a two-phase transaction has not begun yet.
        """
        with self.assertRaises(ProgrammingError):
            await self.connection.prepare()

    async def test_rollback_after_prepare(self):
        """
        Calling rollback() after prepare() actually rolls back the changes.
        """
        xid = Xid(0, "foo", "bar")
        await self.connection.begin(xid)
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.prepare()
        await self.connection.rollback()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertFalse(await result.get_one())

    async def test_mixing_standard_and_two_phase_commits(self):
        """
        It's possible to mix standard and two phase commits across different
        transactions.
        """
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.commit()
        xid = Xid(0, "foo", "bar")
        await self.connection.begin(xid)
        await self.connection.execute("INSERT INTO test VALUES (40, 'Title 40')")
        await self.connection.prepare()
        await self.connection.commit()
        result = await self.connection.execute("SELECT id FROM test "
                                         "WHERE id IN (30, 40)")
        self.assertEqual([(30,), (40,)], await result.get_all())

    async def test_recover_and_commit(self):
        """
        It's possible to recover and commit pending transactions that were
        prepared but not committed or rolled back.
        """
        # Prepare a transaction but leave it uncommitted
        await self.connection.begin(Xid(0, "foo", "bar"))
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.prepare()

        # Setup a new connection and recover the prepared transaction
        # committing it
        connection2 = await self.database.connect()
        self.addCleanup(connection2.close)
        result = await connection2.execute("SELECT id FROM test WHERE id=30")
        await connection2.rollback()
        self.assertFalse(await result.get_one())
        [xid] = await connection2.recover()
        self.assertEqual(0, xid.format_id)
        self.assertEqual("foo", xid.global_transaction_id)
        self.assertEqual("bar", xid.branch_qualifier)
        await connection2.commit(xid)
        self.assertEqual([], await connection2.recover())

        # Reconnect, changes are be visible
        await self.connection.close()
        self.connection = await self.database.connect()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertTrue(await result.get_one())

    async def test_recover_and_rollback(self):
        """
        It's possible to recover and rollback pending transactions that were
        prepared but not committed or rolled back.
        """
        # Prepare a transaction but leave it uncommitted
        await self.connection.begin(Xid(0, "foo", "bar"))
        await self.connection.execute("INSERT INTO test VALUES (30, 'Title 30')")
        await self.connection.prepare()

        # Setup a new connection and recover the prepared transaction
        # rolling it back
        connection2 = await self.database.connect()
        self.addCleanup(connection2.close)
        [xid] = await connection2.recover()
        self.assertEqual(0, xid.format_id)
        self.assertEqual("foo", xid.global_transaction_id)
        self.assertEqual("bar", xid.branch_qualifier)
        await connection2.rollback(xid)
        self.assertEqual([], await connection2.recover())

        # Reconnect, changes were rolled back
        await self.connection.close()
        self.connection = await self.database.connect()
        result = await self.connection.execute("SELECT id FROM test WHERE id=30")
        self.assertFalse(await result.get_one())


class UnsupportedDatabaseTest:

    helpers = [MakePath]

    dbapi_module_names = []
    db_module_name = None

    async def test_exception_when_unsupported(self):

        # Install a directory in front of the search path.
        module_dir = self.make_path()
        os.mkdir(module_dir)
        sys.path.insert(0, module_dir)

        # Copy the real module over to a new place, since the old one is
        # already using the real module, if it's available.
        db_module = __import__("storm.databases."+self.db_module_name,
                               None, None, [""])
        db_module_filename = db_module.__file__
        if db_module_filename.endswith(".pyc"):
            db_module_filename = db_module_filename[:-1]
        shutil.copyfile(db_module_filename,
                        os.path.join(module_dir, "_fake_.py"))

        dbapi_modules = {}
        for dbapi_module_name in self.dbapi_module_names:

            # If the real module is available, remove it from sys.modules.
            dbapi_module = sys.modules.pop(dbapi_module_name, None)
            if dbapi_module is not None:
                dbapi_modules[dbapi_module_name] = dbapi_module

            # Create a module which raises ImportError when imported, to fake
            # a missing module.
            dirname = self.make_path(path=os.path.join(module_dir,
                                                       dbapi_module_name))
            os.mkdir(dirname)
            self.make_path("raise ImportError",
                           os.path.join(module_dir, dbapi_module_name,
                                        "__init__.py"))

        # Finally, test it.
        import _fake_
        uri = URI("_fake_://db")

        try:
            self.assertRaises(DatabaseModuleError,
                              _fake_.create_from_uri, uri)
        finally:
            # Unhack the environment.
            del sys.path[0]
            del sys.modules["_fake_"]

            sys.modules.update(dbapi_modules)


class DatabaseDisconnectionMixin:

    environment_variable = ""
    host_environment_variable = ""
    default_port = None

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.create_database_and_proxy()
        await self.create_connection()

    async def asyncTearDown(self):
        await self.drop_connection()
        self.drop_database()
        self.proxy.close()
        await super().asyncTearDown()

    def is_supported(self):
        return bool(self.get_uri())

    def get_uri(self):
        """Return URI instance with a defined host (and port, for TCP)."""
        if not self.environment_variable:
            raise RuntimeError(
                "Define at least %s.environment_variable" %
                type(self).__name__)
        uri_str = os.environ.get(self.host_environment_variable)
        if uri_str:
            uri = URI(uri_str)
            if not uri.host:
                raise RuntimeError("The URI in %s must include a host." %
                                   self.host_environment_variable)
            if not uri.host.startswith("/") and not uri.port:
                if not self.default_port:
                    raise RuntimeError(
                        "Define at least %s.default_port" % type(self).__name)
                uri.port = self.default_port
            return uri
        else:
            uri_str = os.environ.get(self.environment_variable)
            if uri_str:
                uri = URI(uri_str)
                if uri.host:
                    if not uri.host.startswith("/") and not uri.port:
                        if not self.default_port:
                            raise RuntimeError(
                                "Define at least %s.default_port" %
                                type(self).__name)
                        uri.port = self.default_port
                    return uri
        return None

    def create_proxy(self, uri):
        """Create a TCP proxy forwarding requests to `uri`."""
        return ProxyTCPServer((uri.host, uri.port))

    def create_database_and_proxy(self):
        """Set up the TCP proxy and database object.

        The TCP proxy should forward requests on to the database.  The
        database object should point at the TCP proxy.
        """
        uri = self.get_uri()
        self.proxy = self.create_proxy(uri)
        uri.host, uri.port = self.proxy.server_address
        self.proxy_uri = uri
        self.database = create_database(uri)

    async def create_connection(self):
        self.connection = self.database.connect()

    async def drop_connection(self):
        await self.connection.close()

    def drop_database(self):
        pass


class DatabaseDisconnectionTest(DatabaseDisconnectionMixin):

    async def test_proxy_works(self):
        """Ensure that we can talk to the database through the proxy."""
        result = await self.connection.execute("SELECT 1")
        self.assertEqual(await result.get_one(), (1,))

    async def test_catch_disconnect_on_execute(self):
        """Test that database disconnections get caught on execute()."""
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.restart()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")

    async def test_catch_disconnect_on_commit(self):
        """Test that database disconnections get caught on commit()."""
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.restart()
        with self.assertRaises(DisconnectionError):
            await self.connection.commit()

    async def test_wb_catch_already_disconnected_on_rollback(self):
        """Connection.rollback() swallows disconnection errors.

        If the connection is being used outside of Storm's control,
        then it is possible that Storm won't see the disconnection.
        It should be able to recover from this situation though.
        """
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.restart()
        # Perform an action that should result in a disconnection.
        try:
            cursor = await self.connection._raw_connection.cursor()
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
        except Error as exc:
            self.assertTrue(self.connection.is_disconnection_error(exc))
        else:
            self.fail("Disconnection was not caught.")

        # Make sure our raw connection's rollback() raises a disconnection
        # error when called.
        try:
            await self.connection._raw_connection.rollback()
        except Error as exc:
            self.assertTrue(self.connection.is_disconnection_error(exc))
        else:
            self.fail("Disconnection was not raised.")

        # Our rollback() will catch and swallow that disconnection error,
        # though.
        await self.connection.rollback()

    async def test_wb_catch_already_disconnected(self):
        """Storm detects connections that have already been disconnected.

        If the connection is being used outside of Storm's control,
        then it is possible that Storm won't see the disconnection.
        It should be able to recover from this situation though.
        """
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.restart()
        # Perform an action that should result in a disconnection.
        try:
            cursor = await self.connection._raw_connection.cursor()
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
        except DatabaseError as exc:
            self.assertTrue(self.connection.is_disconnection_error(exc))
        else:
            self.fail("Disconnection was not caught.")
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")

    async def test_connection_stays_disconnected_in_transaction(self):
        """Test that connection does not immediately reconnect."""
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.restart()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")

    async def test_reconnect_after_rollback(self):
        """Test that we reconnect after rolling back the connection."""
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.restart()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")
        await self.connection.rollback()
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())

    async def test_catch_disconnect_on_reconnect(self):
        """Test that reconnection failures result in DisconnectionError."""
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.stop()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")
        # Rollback the connection, but because the proxy is still
        # down, we get a DisconnectionError again.
        await self.connection.rollback()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")

    async def test_close_connection_after_disconnect(self):
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
        self.proxy.stop()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")
        await self.connection.close()


class TwoPhaseCommitDisconnectionTest:

    async def test_begin_after_rollback_with_disconnection_error(self):
        """
        If a rollback fails because of a disconnection error, the two-phase
        transaction should be properly reset.
        """
        xid1 = Xid(0, "foo", "bar")
        await self.connection.begin(xid1)
        await self.connection.execute("SELECT 1")
        self.proxy.stop()
        await self.connection.rollback()
        self.proxy.start()
        xid2 = Xid(0, "egg", "baz")
        await self.connection.begin(xid2)
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())

    async def test_begin_after_with_statement_disconnection_error_and_rollback(self):
        """
        The two-phase transaction state is properly reset if a disconnection
        happens before the rollback.
        """
        xid1 = Xid(0, "foo", "bar")
        await self.connection.begin(xid1)
        self.proxy.close()
        with self.assertRaises(DisconnectionError):
            await self.connection.execute("SELECT 1")
        await self.connection.rollback()
        self.proxy.start()
        xid2 = Xid(0, "egg", "baz")
        await self.connection.begin(xid2)
        result = await self.connection.execute("SELECT 1")
        self.assertTrue(await result.get_one())
