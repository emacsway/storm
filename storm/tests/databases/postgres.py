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
from datetime import date, time, timedelta
import os
import json
from urllib.parse import urlunsplit

from storm.databases.postgres import (
    Postgres, compile, currval, Returning, Case, PostgresTimeoutTracer,
    make_dsn, JSONElement, JSONTextElement, JSON)
from storm.database import create_database
from storm.store import Store
from storm.exceptions import InterfaceError, ProgrammingError
from storm.variables import DateTimeVariable, BytesVariable
from storm.variables import ListVariable, IntVariable, Variable
from storm.properties import Int
from storm.exceptions import DisconnectionError, OperationalError
from storm.expr import (Union, Select, Insert, Update, Alias, SQLRaw, State,
                        Sequence, Like, Column, COLUMN, Cast, Func)
from storm.tracer import install_tracer, TimeoutError
from storm.uri import URI

# We need the info to register the 'type' compiler.  In normal
# circumstances this is naturally imported.
import storm.info
storm  # Silence lint.

from storm.tests import has_fixtures, has_subunit
from storm.tests.databases.base import (
    DatabaseTest, DatabaseDisconnectionTest, UnsupportedDatabaseTest,
    TwoPhaseCommitTest, TwoPhaseCommitDisconnectionTest)
from storm.tests.databases.proxy import ProxyTCPServer
from storm.tests.expr import column1, column2, column3, elem1, table1, TrackContext
from storm.tests.tracer import TimeoutTracerTestBase
from storm.tests.helper import AsyncTestHelper

try:
    import pgbouncer
except ImportError:
    has_pgbouncer = False
else:
    has_pgbouncer = True


def create_proxy_and_uri(uri):
    """Create a TCP proxy to a Unix-domain database identified by `uri`."""
    proxy = ProxyTCPServer(os.path.join(uri.host, ".s.PGSQL.5432"))
    proxy_host, proxy_port = proxy.server_address
    proxy_uri = URI(urlunsplit(
        ("postgres", "%s:%s" % (proxy_host, proxy_port), "/storm_test",
         "", "")))
    return proxy, proxy_uri


async def terminate_other_backends(connection):
    """Terminate all connections to the database except the one given."""
    pid_column = "procpid" if connection._database._version < 90200 else "pid"
    await connection.execute(
        "SELECT pg_terminate_backend(%(pid_column)s)"
        "  FROM pg_stat_activity"
        " WHERE datname = current_database()"
        "   AND %(pid_column)s != pg_backend_pid()" %
        {"pid_column": pid_column})


async def terminate_all_backends(database):
    """Terminate all connections to the given database."""
    connection = database.connect()
    await terminate_other_backends(connection)
    await connection.close()


class PostgresTest(DatabaseTest, AsyncTestHelper):

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    async def create_tables(self):
        await self.connection.execute("CREATE TABLE number "
                                "(one INTEGER, two INTEGER, three INTEGER)")
        await self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")
        await self.connection.execute("CREATE TABLE datetime_test "
                                "(id SERIAL PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME, td INTERVAL)")
        await self.connection.execute("CREATE TABLE bin_test "
                                "(id SERIAL PRIMARY KEY, b BYTEA)")
        await self.connection.execute("CREATE TABLE like_case_insensitive_test "
                                "(id SERIAL PRIMARY KEY, description TEXT)")
        await self.connection.execute("CREATE TABLE returning_test "
                                "(id1 INTEGER DEFAULT 123, "
                                " id2 INTEGER DEFAULT 456)")
        await self.connection.execute("CREATE TABLE json_test "
                                "(id SERIAL PRIMARY KEY, "
                                " json JSON)")

    async def drop_tables(self):
        await super().drop_tables()
        tables = ("like_case_insensitive_test", "returning_test", "json_test")
        for table in tables:
            try:
                await self.connection.execute("DROP TABLE %s" % table)
                await self.connection.commit()
            except:
                await self.connection.rollback()

    async def create_sample_data(self):
        await super().create_sample_data()
        await self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('hullah')")
        await self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('HULLAH')")
        await self.connection.commit()

    def test_wb_create_database(self):
        database = create_database("postgres://un:pw@ht:12/db")
        self.assertTrue(isinstance(database, Postgres))
        self.assertEqual(database._dsn,
                         "dbname=db host=ht port=12 user=un password=pw")

        database = create_database("postgres:postgres")
        self.assertEqual(database._dsn, "dbname=postgres")

    async def test_wb_version(self):
        version = self.database._version
        self.assertEqual(type(version), int)
        try:
            result = await self.connection.execute("SHOW server_version_num")
        except ProgrammingError:
            self.assertEqual(version, 0)
        else:
            server_version = int((await result.get_one())[0])
            self.assertEqual(version, server_version)

    async def test_utf8_client_encoding(self):
        connection = self.database.connect()
        result = await connection.execute("SHOW client_encoding")
        encoding = (await result.get_one())[0]
        self.assertEqual(encoding.upper(), "UTF8")
        await connection.close()

    async def test_unicode(self):
        raw_str = b"\xc3\xa1\xc3\xa9\xc3\xad\xc3\xb3\xc3\xba"
        uni_str = raw_str.decode("UTF-8")

        connection = self.database.connect()
        await connection.execute(
            (b"INSERT INTO test VALUES (1, '%s')" % raw_str).decode("UTF-8"))

        result = await connection.execute("SELECT title FROM test WHERE id=1")
        title = (await result.get_one())[0]

        self.assertTrue(isinstance(title, str))
        self.assertEqual(title, uni_str)
        await connection.close()

    async def test_unicode_array(self):
        raw_str = b"\xc3\xa1\xc3\xa9\xc3\xad\xc3\xb3\xc3\xba"
        uni_str = raw_str.decode("UTF-8")

        connection = self.database.connect()
        result = await connection.execute(
            (b"""SELECT '{"%s"}'::TEXT[]""" % raw_str).decode("UTF-8"))
        self.assertEqual((await result.get_one())[0], [uni_str])
        result = await connection.execute("""SELECT ?::TEXT[]""", ([uni_str],))
        self.assertEqual((await result.get_one())[0], [uni_str])
        await connection.close()

    async def test_time(self):
        connection = self.database.connect()
        value = time(12, 34)
        result = await connection.execute("SELECT ?::TIME", (value,))
        self.assertEqual((await result.get_one())[0], value)
        await connection.close()

    async def test_date(self):
        connection = self.database.connect()
        value = date(2007, 6, 22)
        result = await connection.execute("SELECT ?::DATE", (value,))
        self.assertEqual((await result.get_one())[0], value)
        await connection.close()

    async def test_interval(self):
        connection = self.database.connect()
        value = timedelta(365)
        result = await connection.execute("SELECT ?::INTERVAL", (value,))
        self.assertEqual((await result.get_one())[0], value)
        await connection.close()

    async def test_datetime_with_none(self):
        await self.connection.execute("INSERT INTO datetime_test (dt) VALUES (NULL)")
        result = await self.connection.execute("SELECT dt FROM datetime_test")
        variable = DateTimeVariable()
        result.set_variable(variable, (await result.get_one())[0])
        self.assertEqual(variable.get(), None)

    async def test_array_support(self):
        try:
            await self.connection.execute("DROP TABLE array_test")
            await self.connection.commit()
        except:
            await self.connection.rollback()

        await self.connection.execute("CREATE TABLE array_test "
                                "(id SERIAL PRIMARY KEY, a INT[])")

        variable = ListVariable(IntVariable)
        variable.set([1,2,3,4])

        state = State()
        statement = compile(variable, state)

        await self.connection.execute("INSERT INTO array_test VALUES (1, %s)"
                                % statement, state.parameters)

        result = await self.connection.execute("SELECT a FROM array_test WHERE id=1")

        array = (await result.get_one())[0]

        self.assertTrue(isinstance(array, list))

        variable = ListVariable(IntVariable)
        result.set_variable(variable, array)
        self.assertEqual(variable.get(), [1,2,3,4])

    async def test_array_support_with_empty(self):
        try:
            await self.connection.execute("DROP TABLE array_test")
            await self.connection.commit()
        except:
            await self.connection.rollback()

        await self.connection.execute("CREATE TABLE array_test "
                                "(id SERIAL PRIMARY KEY, a INT[])")

        variable = ListVariable(IntVariable)
        variable.set([])

        state = State()
        statement = compile(variable, state)

        await self.connection.execute("INSERT INTO array_test VALUES (1, %s)"
                                % statement, state.parameters)

        result = await self.connection.execute("SELECT a FROM array_test WHERE id=1")

        array = (await result.get_one())[0]

        self.assertTrue(isinstance(array, list))

        variable = ListVariable(IntVariable)
        result.set_variable(variable, array)
        self.assertEqual(variable.get(), [])

    async def test_expressions_in_union_order_by(self):
        # The following statement breaks in postgres:
        #     SELECT 1 AS id UNION SELECT 1 ORDER BY id+1;
        # With the error:
        #     ORDER BY on a UNION/INTERSECT/EXCEPT result must
        #     be on one of the result columns
        column = SQLRaw("1")
        Alias.auto_counter = 0
        alias = Alias(column, "id")
        expr = Union(Select(alias), Select(column), order_by=alias+1,
                     limit=1, offset=1, all=True)

        state = State()
        statement = compile(expr, state)
        self.assertEqual(statement,
                         'SELECT * FROM '
                         '((SELECT 1 AS id) UNION ALL (SELECT 1)) AS "_1" '
                         'ORDER BY id + ? LIMIT 1 OFFSET 1')
        self.assertVariablesEqual(state.parameters, [Variable(1)])

        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_one(), (1,))

    async def test_expressions_in_union_in_union_order_by(self):
        column = SQLRaw("1")
        alias = Alias(column, "id")
        expr = Union(Select(alias), Select(column), order_by=alias+1,
                     limit=1, offset=1, all=True)
        expr = Union(expr, expr, order_by=alias+1, all=True)
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(1,), (1,)])

    async def test_sequence(self):
        expr1 = Select(Sequence("test_id_seq"))
        expr2 = "SELECT currval('test_id_seq')"
        value1 = (await (await self.connection.execute(expr1)).get_one())[0]
        value2 = (await (await self.connection.execute(expr2)).get_one())[0]
        value3 = (await (await self.connection.execute(expr1)).get_one())[0]
        self.assertEqual(value1, value2)
        self.assertEqual(value3-value1, 1)

    def test_like_case(self):
        expr = Like("name", "value")
        statement = compile(expr)
        self.assertEqual(statement, "? LIKE ?")
        expr = Like("name", "value", case_sensitive=True)
        statement = compile(expr)
        self.assertEqual(statement, "? LIKE ?")
        expr = Like("name", "value", case_sensitive=False)
        statement = compile(expr)
        self.assertEqual(statement, "? ILIKE ?")

    async def test_case_default_like(self):

        like = Like(SQLRaw("description"), "%hullah%")
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(1,)])

        like = Like(SQLRaw("description"), "%HULLAH%")
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(2,)])

    async def test_case_sensitive_like(self):

        like = Like(SQLRaw("description"), "%hullah%", case_sensitive=True)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(1,)])

        like = Like(SQLRaw("description"), "%HULLAH%", case_sensitive=True)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(2,)])

    async def test_case_insensitive_like(self):

        like = Like(SQLRaw("description"), "%hullah%", case_sensitive=False)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(1,), (2,)])
        like = Like(SQLRaw("description"), "%HULLAH%", case_sensitive=False)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = await self.connection.execute(expr)
        self.assertEqual(await result.get_all(), [(1,), (2,)])

    async def test_none_on_string_variable(self):
        """
        Verify that the logic to enforce fix E''-styled strings isn't
        breaking on NULL values.
        """
        variable = BytesVariable(value=None)
        result = await self.connection.execute(Select(variable))
        self.assertEqual(await result.get_one(), (None,))

    def test_compile_table_with_schema(self):
        class Foo:
            __storm_table__ = "my schema.my table"
            id = Int("my.column", primary=True)
        self.assertEqual(compile(Select(Foo.id)),
                         'SELECT "my schema"."my table"."my.column" '
                         'FROM "my schema"."my table"')

    def test_compile_case(self):
        """The Case expr is compiled in a Postgres' CASE expression."""
        cases = [
            (Column("foo") > 3, "big"), (Column("bar") == None, 4)]
        state = State()
        statement = compile(Case(cases), state)
        self.assertEqual(
            "CASE WHEN (foo > ?) THEN ? WHEN (bar IS NULL) THEN ? END",
            statement)
        self.assertEqual(
            [3, "big", 4], [param.get() for param in state.parameters])

    def test_compile_case_with_default(self):
        """
        If a default is provided, the resulting CASE expression includes
        an ELSE clause.
        """
        cases = [(Column("foo") > 3, "big")]
        state = State()
        statement = compile(Case(cases, default=9), state)
        self.assertEqual(
            "CASE WHEN (foo > ?) THEN ? ELSE ? END", statement)
        self.assertEqual(
            [3, "big", 9], [param.get() for param in state.parameters])

    def test_compile_case_with_expression(self):
        """
        If an expression is provided, the resulting CASE expression uses the
        simple syntax.
        """
        cases = [(1, "one"), (2, "two")]
        state = State()
        statement = compile(Case(cases, expression=Column("foo")), state)
        self.assertEqual(
            "CASE foo WHEN ? THEN ? WHEN ? THEN ? END", statement)
        self.assertEqual(
            [1, "one", 2, "two"], [param.get() for param in state.parameters])

    def test_currval_no_escaping(self):
        expr = currval(Column("thecolumn", "theschema.thetable"))
        statement = compile(expr)
        expected = """currval('theschema.thetable_thecolumn_seq')"""
        self.assertEqual(statement, expected)

    def test_currval_escaped_schema(self):
        expr = currval(Column("thecolumn", "the schema.thetable"))
        statement = compile(expr)
        expected = """currval('"the schema".thetable_thecolumn_seq')"""
        self.assertEqual(statement, expected)

    def test_currval_escaped_table(self):
        expr = currval(Column("thecolumn", "theschema.the table"))
        statement = compile(expr)
        expected = """currval('theschema."the table_thecolumn_seq"')"""
        self.assertEqual(statement, expected)

    def test_currval_escaped_column(self):
        expr = currval(Column("the column", "theschema.thetable"))
        statement = compile(expr)
        expected = """currval('theschema."thetable_the column_seq"')"""
        self.assertEqual(statement, expected)

    def test_currval_escaped_column_no_schema(self):
        expr = currval(Column("the column", "thetable"))
        statement = compile(expr)
        expected = """currval('"thetable_the column_seq"')"""
        self.assertEqual(statement, expected)

    def test_currval_escaped_schema_table_and_column(self):
        expr = currval(Column("the column", "the schema.the table"))
        statement = compile(expr)
        expected = """currval('"the schema"."the table_the column_seq"')"""
        self.assertEqual(statement, expected)

    async def test_get_insert_identity(self):
        column = Column("thecolumn", "thetable")
        variable = IntVariable()
        result = await self.connection.execute("SELECT 1")
        where = result.get_insert_identity((column,), (variable,))
        self.assertEqual(compile(where),
                         "thetable.thecolumn = "
                         "(SELECT currval('thetable_thecolumn_seq'))")

    def test_returning_column_context(self):
        column2 = TrackContext()
        insert = Insert({column1: elem1}, table1, primary_columns=column2)
        compile(Returning(insert))
        self.assertEqual(column2.context, COLUMN)

    def test_returning_update(self):
        update = Update({column1: elem1}, table=table1,
                        primary_columns=(column2, column3))
        self.assertEqual(compile(Returning(update)),
                         'UPDATE "table 1" SET column1=elem1 '
                         'RETURNING column2, column3')

    def test_returning_update_with_columns(self):
        update = Update({column1: elem1}, table=table1,
                        primary_columns=(column2, column3))
        self.assertEqual(compile(Returning(update, columns=[column3])),
                         'UPDATE "table 1" SET column1=elem1 '
                         'RETURNING column3')

    async def test_execute_insert_returning(self):
        if self.database._version < 80200:
            return # Can't run this test with old PostgreSQL versions.

        column1 = Column("id1", "returning_test")
        column2 = Column("id2", "returning_test")
        variable1 = IntVariable()
        variable2 = IntVariable()
        insert = Insert({}, primary_columns=(column1, column2),
                            primary_variables=(variable1, variable2))
        await self.connection.execute(insert)

        self.assertTrue(variable1.is_defined())
        self.assertTrue(variable2.is_defined())

        self.assertEqual(variable1.get(), 123)
        self.assertEqual(variable2.get(), 456)

        result = await self.connection.execute("SELECT * FROM returning_test")
        self.assertEqual(await result.get_one(), (123, 456))

    async def test_wb_execute_insert_returning_not_used_with_old_postgres(self):
        """Shouldn't try to use RETURNING with PostgreSQL < 8.2."""
        column1 = Column("id1", "returning_test")
        column2 = Column("id2", "returning_test")
        variable1 = IntVariable()
        variable2 = IntVariable()
        insert = Insert({}, primary_columns=(column1, column2),
                            primary_variables=(variable1, variable2))
        self.database._version = 80109

        await self.connection.execute(insert)

        self.assertFalse(variable1.is_defined())
        self.assertFalse(variable2.is_defined())

        result = await self.connection.execute("SELECT * FROM returning_test")
        self.assertEqual(await result.get_one(), (123, 456))

    async def test_execute_insert_returning_without_columns(self):
        """Without primary_columns, the RETURNING system won't be used."""
        column1 = Column("id1", "returning_test")
        variable1 = IntVariable()
        insert = Insert({column1: 123}, primary_variables=(variable1,))
        await self.connection.execute(insert)

        self.assertFalse(variable1.is_defined())

        result = await self.connection.execute("SELECT * FROM returning_test")
        self.assertEqual(await result.get_one(), (123, 456))

    async def test_execute_insert_returning_without_variables(self):
        """Without primary_variables, the RETURNING system won't be used."""
        column1 = Column("id1", "returning_test")
        insert = Insert({}, primary_columns=(column1,))
        await self.connection.execute(insert)

        result = await self.connection.execute("SELECT * FROM returning_test")

        self.assertEqual(await result.get_one(), (123, 456))

    async def test_execute_update_returning(self):
        if self.database._version < 80200:
            return # Can't run this test with old PostgreSQL versions.

        column1 = Column("id1", "returning_test")
        column2 = Column("id2", "returning_test")
        await self.connection.execute(
            "INSERT INTO returning_test VALUES (1, 2)")
        update = Update({"id2": 3}, column1 == 1,
                        primary_columns=(column1, column2))
        result = await self.connection.execute(Returning(update))
        self.assertEqual(await result.get_one(), (1, 3))

    async def test_uri_parameters(self):
        database = create_database(
            os.environ["STORM_POSTGRES_URI"] + "?isolation=autocommit&application_name=test_name&sslmode=verify-full")

        connection = database.connect()
        self.addAsyncCleanup(connection.close)

        result = await connection.execute("SHOW TRANSACTION ISOLATION LEVEL")
        # It matches read committed in Postgres internel
        self.assertEqual((await result.get_one())[0], "read committed")

        result = await connection.execute("SHOW APPLICATION_NAME")
        self.assertEqual((await result.get_one())[0], "test_name")

        dsn = database._dsn
        self.assertIn("sslmode=verify-full", dsn)
        self.assertIn("application_name=test_name", dsn)
        self.assertNotIn("isolation=autocommit", dsn)

    async def test_isolation_autocommit(self):
        database = create_database(
            os.environ["STORM_POSTGRES_URI"] + "?isolation=autocommit")

        connection = database.connect()
        try:
            result = await connection.execute("SHOW TRANSACTION ISOLATION LEVEL")
            # It matches read committed in Postgres internel
            self.assertEqual((await result.get_one())[0], "read committed")

            await connection.execute("INSERT INTO bin_test VALUES (1, 'foo')")

            result = await self.connection.execute("SELECT id FROM bin_test")
            # I didn't commit, but data should already be there
            self.assertEqual(await result.get_all(), [(1,)])
            await connection.rollback()
        finally:
            await connection.close()

    async def test_isolation_read_committed(self):
        database = create_database(
            os.environ["STORM_POSTGRES_URI"] + "?isolation=read-committed")

        connection = database.connect()
        try:
            result = await connection.execute("SHOW TRANSACTION ISOLATION LEVEL")
            self.assertEqual((await result.get_one())[0], "read committed")

            await connection.execute("INSERT INTO bin_test VALUES (1, 'foo')")

            result = await self.connection.execute("SELECT id FROM bin_test")
            # Data should not be there already
            self.assertEqual(await result.get_all(), [])
            await connection.rollback()

            # Start a transaction
            result = await connection.execute("SELECT 1")
            self.assertEqual(await result.get_one(), (1,))

            await self.connection.execute("INSERT INTO bin_test VALUES (1, 'foo')")
            await self.connection.commit()

            result = await connection.execute("SELECT id FROM bin_test")
            # Data is already here!
            self.assertEqual(await result.get_one(), (1,))
            await connection.rollback()
        finally:
            await connection.close()

    async def test_isolation_serializable(self):
        database = create_database(
            os.environ["STORM_POSTGRES_URI"] + "?isolation=serializable")

        connection = database.connect()
        try:
            result = await connection.execute("SHOW TRANSACTION ISOLATION LEVEL")
            self.assertEqual((await result.get_one())[0], "serializable")

            # Start a transaction
            result = await connection.execute("SELECT 1")
            self.assertEqual(await result.get_one(), (1,))

            await self.connection.execute("INSERT INTO bin_test VALUES (1, 'foo')")
            await self.connection.commit()

            result = await connection.execute("SELECT id FROM bin_test")
            # We can't see data yet, because transaction started before
            self.assertEqual(await result.get_one(), None)
            await connection.rollback()
        finally:
            await connection.close()

    async def test_default_isolation(self):
        """
        The default isolation level is REPEATABLE READ, which is supported
        by psycopg v3.
        """
        result = await self.connection.execute("SHOW TRANSACTION ISOLATION LEVEL")
        import psycopg
        # psycopg v3 supports REPEATABLE READ
        self.assertEqual((await result.get_one())[0], "repeatable read")

    def test_unknown_serialization(self):
        self.assertRaises(ValueError, create_database,
            os.environ["STORM_POSTGRES_URI"] + "?isolation=stuff")

    def test_is_disconnection_error_with_ssl_syscall_error(self):
        """
        If the underlying driver raises a ProgrammingError with 'SSL SYSCALL
        error', we consider the connection dead and mark it as needing
        reconnection.
        """
        exc = ProgrammingError("SSL SYSCALL error: Connection timed out")
        self.assertTrue(self.connection.is_disconnection_error(exc))

    def test_is_disconnection_error_with_could_not_send_data(self):
        """
        If the underlying driver raises an OperationalError with 'could not
        send data to server', we consider the connection
        dead and mark it as needing reconnection.
        """
        exc = OperationalError("could not send data to server")
        self.assertTrue(self.connection.is_disconnection_error(exc))

    def test_is_disconnection_error_with_could_not_receive_data(self):
        """
        If the underlying driver raises an OperationalError with 'could not
        receive data from server', we consider the connection
        dead and mark it as needing reconnection.
        """
        exc = OperationalError("could not receive data from server")
        self.assertTrue(self.connection.is_disconnection_error(exc))

    async def test_json_element(self):
        "JSONElement returns an element from a json field."
        connection = self.database.connect()
        json_value = Cast('{"a": 1}', "json")
        expr = JSONElement(json_value, "a")
        # Need to cast as text since newer psycopg versions decode JSON
        # automatically.
        result = await connection.execute(Select(Cast(expr, "text")))
        self.assertEqual("1", (await result.get_one())[0])
        result = await connection.execute(Select(Func("pg_typeof", expr)))
        self.assertEqual("json", (await result.get_one())[0])
        await connection.close()

    async def test_json_text_element(self):
        "JSONTextElement returns an element from a json field as text."
        connection = self.database.connect()
        json_value = Cast('{"a": 1}', "json")
        expr = JSONTextElement(json_value, "a")
        result = await connection.execute(Select(expr))
        self.assertEqual("1", (await result.get_one())[0])
        result = await connection.execute(Select(Func("pg_typeof", expr)))
        self.assertEqual("text", (await result.get_one())[0])
        await connection.close()

    async def test_json_property(self):
        """The JSON property is encoded as JSON"""

        class TestModel:
            __storm_table__ = "json_test"

            id = Int(primary=True)
            json = JSON()

        connection = self.database.connect()
        value = {"a": 3, "b": "foo", "c": None}
        await connection.execute(
            "INSERT INTO json_test (json) VALUES (?)", (json.dumps(value),))
        await connection.commit()
        await connection.close()

        store = Store(self.database)
        obj = await (await store.find(TestModel)).one()
        await store.close()
        # The JSON object is decoded to python
        self.assertEqual(value, obj.json)


_max_prepared_transactions = None


class PostgresTwoPhaseCommitTest(TwoPhaseCommitTest, AsyncTestHelper):

    async def is_supported(self):
        uri = os.environ.get("STORM_POSTGRES_URI")
        if not uri:
            return False
        global _max_prepared_transactions
        if _max_prepared_transactions is None:
            database = create_database(uri)
            connection = database.connect()
            result = await connection.execute("SHOW MAX_PREPARED_TRANSACTIONS")
            _max_prepared_transactions = int((await result.get_one())[0])
            await connection.close()
        return _max_prepared_transactions > 0

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    async def create_tables(self):
        await self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")
        await self.connection.commit()


class PostgresUnsupportedTest(UnsupportedDatabaseTest, AsyncTestHelper):

    dbapi_module_names = ["psycopg"]
    db_module_name = "postgres"


class PostgresDisconnectionTest(DatabaseDisconnectionTest, TwoPhaseCommitDisconnectionTest,
                                AsyncTestHelper):

    environment_variable = "STORM_POSTGRES_URI"
    host_environment_variable = "STORM_POSTGRES_HOST_URI"
    default_port = 5432

    def create_proxy(self, uri):
        """See `DatabaseDisconnectionMixin.create_proxy`."""
        if uri.host.startswith("/"):
            return create_proxy_and_uri(uri)[0]
        else:
            return super().create_proxy(uri)

    async def test_rollback_swallows_InterfaceError(self):
        """Test that InterfaceErrors get caught on rollback().

        InterfaceErrors are a form of a disconnection error, so rollback()
        must swallow them and reconnect.
        """
        class FakeConnection:
            async def rollback(self):
                raise InterfaceError('connection already closed')
        self.connection._raw_connection = FakeConnection()
        try:
            await self.connection.rollback()
        except Exception as exc:
            self.fail('Exception should have been swallowed: %s' % repr(exc))


class PostgresDisconnectionTestWithoutProxyBase:
    # DatabaseDisconnectionTest uses a socket proxy to simulate broken
    # connections. This class tests some other causes of disconnection.

    database_uri = None

    def is_supported(self):
        return bool(self.database_uri) and super().is_supported()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = create_database(self.database_uri)

    async def test_terminated_backend(self):
        # The error raised when trying to use a connection that has been
        # terminated at the server is considered a disconnection error.
        connection = self.database.connect()
        await terminate_all_backends(self.database)
        with self.assertRaises(DisconnectionError):
            await connection.execute("SELECT current_database()")


if has_subunit:
    # Some of the following tests are prone to segfaults, presumably in
    # _psycopg.so. Run them in a subprocess if possible.
    from subunit import IsolatedTestCase
    class MisbehavingTestCase(AsyncTestHelper, IsolatedTestCase):
        pass
else:
    # If we can't run them in a subprocess we still want to create tests, but
    # prevent them from running, so that the skip is reported
    class MisbehavingTestCase(AsyncTestHelper):
        def is_supported(self):
            return False


class PostgresDisconnectionTestWithoutProxyUnixSockets(
    PostgresDisconnectionTestWithoutProxyBase, MisbehavingTestCase):
    """Disconnection tests using Unix sockets."""

    database_uri = os.environ.get("STORM_POSTGRES_URI")

class PostgresDisconnectionTestWithoutProxyTCPSockets(
    PostgresDisconnectionTestWithoutProxyBase, MisbehavingTestCase):
    """Disconnection tests using TCP sockets."""

    database_uri = os.environ.get("STORM_POSTGRES_HOST_URI")

    async def asyncSetUp(self):
        await super().asyncSetUp()
        if self.database.get_uri().host.startswith("/"):
            proxy, proxy_uri = create_proxy_and_uri(self.database.get_uri())
            self.addCleanup(proxy.close)
            self.database = create_database(proxy_uri)


class PostgresDisconnectionTestWithPGBouncerBase:
    # Connecting via pgbouncer <http://pgfoundry.org/projects/pgbouncer>
    # introduces new possible causes of disconnections.

    def is_supported(self):
        return (
            has_fixtures and has_pgbouncer and
            bool(os.environ.get("STORM_POSTGRES_HOST_URI")))

    async def asyncSetUp(self):
        await super().asyncSetUp()
        database_uri = URI(os.environ["STORM_POSTGRES_HOST_URI"])
        if database_uri.host.startswith("/"):
            proxy, database_uri = create_proxy_and_uri(database_uri)
            self.addCleanup(proxy.close)
        database_user = database_uri.username or os.environ['USER']
        database_dsn = make_dsn(database_uri)
        # Create a pgbouncer fixture.
        self.pgbouncer = pgbouncer.fixture.PGBouncerFixture()
        self.pgbouncer.databases[database_uri.database] = database_dsn
        self.pgbouncer.users[database_user] = "trusted"
        self.pgbouncer.admin_users = [database_user]
        self.useFixture(self.pgbouncer)
        # Create a Database that uses pgbouncer.
        pgbouncer_uri = database_uri.copy()
        pgbouncer_uri.host = self.pgbouncer.host
        pgbouncer_uri.port = self.pgbouncer.port
        self.database = create_database(pgbouncer_uri)

    async def test_terminated_backend(self):
        # The error raised when trying to use a connection through pgbouncer
        # that has been terminated at the server is considered a disconnection
        # error.
        connection = self.database.connect()
        await terminate_all_backends(self.database)
        with self.assertRaises(DisconnectionError):
            await connection.execute("SELECT current_database()")

    async def test_pgbouncer_stopped(self):
        # The error raised from a connection that is no longer connected
        # because pgbouncer has been immediately shutdown (via SIGTERM; see
        # man 1 pgbouncer) is considered a disconnection error.
        connection = self.database.connect()
        self.pgbouncer.stop()
        with self.assertRaises(DisconnectionError):
            await connection.execute("SELECT current_database()")


if has_fixtures:
    # Upgrade to full test case class with fixtures.
    from fixtures import TestWithFixtures
    class PostgresDisconnectionTestWithPGBouncer(
        PostgresDisconnectionTestWithPGBouncerBase,
        TestWithFixtures, AsyncTestHelper): pass


class PostgresTimeoutTracerTest(TimeoutTracerTestBase):

    tracer_class = PostgresTimeoutTracer

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])
        self.connection = self.database.connect()
        install_tracer(self.tracer)
        self.tracer.get_remaining_time = lambda: self.remaining_time
        self.remaining_time = 10.5

    async def asyncTearDown(self):
        await self.connection.close()
        await super().asyncTearDown()

    async def test_set_statement_timeout(self):
        result = await self.connection.execute("SHOW statement_timeout")
        self.assertEqual(await result.get_one(), ("10500ms",))

    async def test_connection_raw_execute_error(self):
        statement = "SELECT pg_sleep(0.5)"
        self.remaining_time = 0.001
        try:
            await self.connection.execute(statement)
        except TimeoutError as e:
            self.assertEqual("SQL server cancelled statement", e.message)
            self.assertEqual(statement, e.statement)
            self.assertEqual((), e.params)
        else:
            self.fail("TimeoutError not raised")
