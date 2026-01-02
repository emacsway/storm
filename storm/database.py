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

"""Basic database interfacing mechanisms for Storm.

This is the common code for database support; specific databases are
supported in modules in L{storm.databases}.
"""

import asyncio
from collections.abc import Callable
from functools import wraps

from storm.expr import Expr, State, compile
# Circular import: imported at the end of the module.
# from storm.tracer import trace
from storm.variables import Variable
from storm.xid import Xid
from storm.exceptions import (
    ClosedError, ConnectionBlockedError, DatabaseError, DisconnectionError,
    Error, ProgrammingError, wrap_exceptions)
from storm.uri import URI
import storm


__all__ = ["Database", "Connection", "Result",
           "convert_param_marks", "create_database", "register_scheme"]


STATE_CONNECTED = 1
STATE_DISCONNECTED = 2
STATE_RECONNECT = 3


class Result:
    """A representation of the results from a single SQL statement."""

    _closed = False

    def __init__(self, connection, raw_cursor):
        self._connection = connection # Ensures deallocation order.
        self._raw_cursor = raw_cursor
        if raw_cursor.arraysize == 1:
            # Default of 1 is silly.
            self._raw_cursor.arraysize = 10

    def __del__(self):
        """Close the cursor."""
        try:
            # For async cursors, we can't await in __del__, so just mark as closed
            # and ignore the coroutine/awaitable if returned
            if not self._closed:
                self._closed = True
                result = self._raw_cursor.close()
                # Ignore awaitable - cleanup will happen when event loop processes it
                if hasattr(result, 'close') and callable(result.close):
                    result.close()  # Prevent "coroutine was never awaited" warning
                self._raw_cursor = None
        except:
            pass

    async def close(self):
        """Close the underlying raw cursor, if it hasn't already been closed.
        """
        if not self._closed:
            self._closed = True
            result = self._raw_cursor.close()
            if hasattr(result, '__await__'):
                await result
            self._raw_cursor = None

    async def get_one(self):
        """Fetch one result from the cursor.

        The result will be converted to an appropriate format via
        L{from_database}.

        @raise DisconnectionError: Raised when the connection is lost.
            Reconnection happens automatically on rollback.

        @return: A converted row or None, if no data is left.
        """
        row = await self._connection._check_disconnect(self._raw_cursor.fetchone)
        if row is not None:
            return tuple(self.from_database(row))
        return None

    async def get_all(self):
        """Fetch all results from the cursor.

        The results will be converted to an appropriate format via
        L{from_database}.

        @raise DisconnectionError: Raised when the connection is lost.
            Reconnection happens automatically on rollback.
        """
        result = await self._connection._check_disconnect(self._raw_cursor.fetchall)
        if result:
            return [tuple(self.from_database(row)) for row in result]
        return result

    async def __aiter__(self):
        """Yield all results, one at a time.

        The results will be converted to an appropriate format via
        L{from_database}.

        @raise DisconnectionError: Raised when the connection is lost.
            Reconnection happens automatically on rollback.
        """
        while True:
            results = await self._connection._check_disconnect(
                self._raw_cursor.fetchmany)
            if not results:
                break
            for result in results:
                yield tuple(self.from_database(result))

    @property
    def rowcount(self):
        """
        See PEP 249 for further details on rowcount.

        @return: the number of affected rows, or None if the database
            backend does not provide this information. Return value
            is undefined if all results have not yet been retrieved.
        """
        if self._raw_cursor.rowcount == -1:
            return None
        return self._raw_cursor.rowcount

    def get_insert_identity(self, primary_columns, primary_variables):
        """Get a query which will return the row that was just inserted.

        This must be overridden in database-specific subclasses.

        @rtype: L{storm.expr.Expr}
        """
        raise NotImplementedError

    @staticmethod
    def set_variable(variable, value):
        """Set the given variable's value from the database."""
        variable.set(value, from_db=True)

    @staticmethod
    def from_database(row):
        """Convert a row fetched from the database to an agnostic format.

        This method is intended to be overridden in subclasses, but
        not called externally.

        If there are any peculiarities in the datatypes returned from
        a database backend, this method should be overridden in the
        backend subclass to convert them.
        """
        return row


class CursorWrapper:
    """A DB-API cursor, wrapping exceptions as StormError instances."""

    def __init__(self, cursor, database):
        super().__setattr__('_cursor', cursor)
        super().__setattr__('_database', database)

    def __getattr__(self, name):
        attr = getattr(self._cursor, name)
        if isinstance(attr, Callable):
            if asyncio.iscoroutinefunction(attr):
                @wraps(attr)
                async def async_wrapper(*args, **kwargs):
                    with wrap_exceptions(self._database):
                        return await attr(*args, **kwargs)
                return async_wrapper
            else:
                @wraps(attr)
                def wrapper(*args, **kwargs):
                    with wrap_exceptions(self._database):
                        return attr(*args, **kwargs)
                return wrapper
        else:
            return attr

    def __setattr__(self, name, value):
        return setattr(self._cursor, name, value)

    async def __aiter__(self):
        with wrap_exceptions(self._database):
            async for row in self._cursor:
                yield row

    async def __aenter__(self):
        return self

    async def __aexit__(self, type_, value, tb):
        with wrap_exceptions(self._database):
            if asyncio.iscoroutinefunction(self._cursor.close):
                await self.close()
            else:
                self.close()


class ConnectionWrapper:
    """A DB-API connection, wrapping exceptions as StormError instances."""

    def __init__(self, connection, database):
        self.__dict__['_connection'] = connection
        self.__dict__['_database'] = database

    def __getattr__(self, name):
        attr = getattr(self._connection, name)
        if isinstance(attr, Callable):
            if asyncio.iscoroutinefunction(attr):
                @wraps(attr)
                async def async_wrapper(*args, **kwargs):
                    with wrap_exceptions(self._database):
                        return await attr(*args, **kwargs)
                return async_wrapper
            else:
                @wraps(attr)
                def wrapper(*args, **kwargs):
                    with wrap_exceptions(self._database):
                        result = attr(*args, **kwargs)
                        # Check if the result is awaitable (handles decorators)
                        if hasattr(result, '__await__'):
                            async def awaiter():
                                return await result
                            return awaiter()
                        return result
                return wrapper
        else:
            return attr

    def __setattr__(self, name, value):
        return setattr(self._connection, name, value)

    async def __aenter__(self):
        return self

    async def __aexit__(self, type_, value, tb):
        with wrap_exceptions(self._database):
            if type_ is None and value is None and tb is None:
                result = self.commit()
                if hasattr(result, '__await__'):
                    await result
            else:
                result = self.rollback()
                if hasattr(result, '__await__'):
                    await result

    async def cursor(self):
        with wrap_exceptions(self._database):
            result = self._connection.cursor()
            if hasattr(result, '__await__'):
                raw_cursor = await result
            else:
                raw_cursor = result
            return CursorWrapper(raw_cursor, self._database)


class Connection:
    """A connection to a database.

    @cvar result_factory: A callable which takes this L{Connection}
        and the backend cursor and returns an instance of L{Result}.
    @type param_mark: C{str}
    @cvar param_mark: The dbapi paramstyle that the database backend expects.
    @type compile: L{storm.expr.Compile}
    @cvar compile: The compiler to use for connections of this type.
    """

    result_factory = Result
    param_mark = "?"
    compile = compile

    _blocked = False
    _closed = False
    _two_phase_transaction = False  # If True, a two-phase transaction has
                                    # been started with begin()
    _state = STATE_CONNECTED

    def __init__(self, database, event=None):
        self._database = database # Ensures deallocation order.
        self._event = event
        self._raw_connection = None  # Will be created lazily on first use

    def __del__(self):
        """Cleanup the connection.

        Note: In async mode, close() must be called explicitly via
        'await connection.close()' or by using 'async with connection:'.
        __del__ cannot call async methods.
        """
        # In async code, we cannot await close() from __del__.
        # Users must explicitly close the connection or use async context manager.
        pass

    def block_access(self):
        """Block access to the connection.

        Attempts to execute statements or commit a transaction will
        result in a C{ConnectionBlockedError} exception.  Rollbacks
        are permitted as that operation is often used in case of
        failures.
        """
        self._blocked = True

    def unblock_access(self):
        """Unblock access to the connection."""
        self._blocked = False

    async def execute(self, statement, params=None, noresult=False):
        """Execute a statement with the given parameters.

        @type statement: L{Expr} or C{str}
        @param statement: The statement to execute. It will be
            compiled if necessary.
        @param noresult: If True, no result will be returned.

        @raise ConnectionBlockedError: Raised if access to the connection
            has been blocked with L{block_access}.
        @raise DisconnectionError: Raised when the connection is lost.
            Reconnection happens automatically on rollback.

        @return: The result of C{self.result_factory}, or None if
            C{noresult} is True.
        """
        if self._closed:
            raise ClosedError("Connection is closed")
        if self._blocked:
            raise ConnectionBlockedError("Access to connection is blocked")
        if self._event:
            self._event.emit("register-transaction")
        await self._ensure_connected()
        if isinstance(statement, Expr):
            if params is not None:
                raise ValueError("Can't pass parameters with expressions")
            state = State()
            statement = self.compile(statement, state)
            params = state.parameters
        statement = convert_param_marks(statement, "?", self.param_mark)
        raw_cursor = await self.raw_execute(statement, params)
        if noresult:
            await self._check_disconnect(raw_cursor.close)
            return None
        return self.result_factory(self, raw_cursor)

    async def close(self):
        """Close the connection if it is not already closed."""
        if not self._closed:
            self._closed = True
            if self._raw_connection is not None:
                result = self._raw_connection.close()
                if hasattr(result, '__await__'):
                    await result
                self._raw_connection = None

    async def begin(self, xid):
        """Begin a two-phase transaction."""
        if self._two_phase_transaction:
            raise ProgrammingError("begin cannot be used inside a transaction")
        await self._ensure_connected()
        raw_xid = self._raw_xid(xid)
        await self._check_disconnect(self._raw_connection.tpc_begin, raw_xid)
        self._two_phase_transaction = True

    async def prepare(self):
        """Run the prepare phase of a two-phase transaction."""
        if not self._two_phase_transaction:
            raise ProgrammingError("prepare must be called inside a two-phase "
                                   "transaction")
        await self._check_disconnect(self._raw_connection.tpc_prepare)

    async def commit(self, xid=None):
        """Commit the connection.

        @param xid: Optionally the L{Xid} of a previously prepared
             transaction to commit. This form should be called outside
             of a transaction, and is intended for use in recovery.

        @raise ConnectionBlockedError: Raised if access to the connection
            has been blocked with L{block_access}.
        @raise DisconnectionError: Raised when the connection is lost.
            Reconnection happens automatically on rollback.

        """
        try:
            await self._ensure_connected()
            if xid:
                raw_xid = self._raw_xid(xid)
                await self._check_disconnect(self._raw_connection.tpc_commit, raw_xid)
            elif self._two_phase_transaction:
                await self._check_disconnect(self._raw_connection.tpc_commit)
                self._two_phase_transaction = False
            else:
                await self._check_disconnect(self._raw_connection.commit)
        finally:
            await self._check_disconnect(trace, "connection_commit", self, xid)

    async def recover(self):
        """Return a list of L{Xid}\\ s representing pending transactions."""
        await self._ensure_connected()
        raw_xids = await self._check_disconnect(self._raw_connection.tpc_recover)
        return [Xid(raw_xid[0], raw_xid[1], raw_xid[2])
                for raw_xid in raw_xids]

    async def rollback(self, xid=None):
        """Rollback the connection.

        @param xid: Optionally the L{Xid} of a previously prepared
             transaction to rollback. This form should be called outside
             of a transaction, and is intended for use in recovery.
        """
        try:
            if self._state == STATE_CONNECTED and self._raw_connection is not None:
                try:
                    if xid:
                        raw_xid = self._raw_xid(xid)
                        if asyncio.iscoroutinefunction(self._raw_connection.tpc_rollback):
                            await self._raw_connection.tpc_rollback(raw_xid)
                        else:
                            self._raw_connection.tpc_rollback(raw_xid)
                    elif self._two_phase_transaction:
                        if asyncio.iscoroutinefunction(self._raw_connection.tpc_rollback):
                            await self._raw_connection.tpc_rollback()
                        else:
                            self._raw_connection.tpc_rollback()
                    else:
                        if asyncio.iscoroutinefunction(self._raw_connection.rollback):
                            await self._raw_connection.rollback()
                        else:
                            self._raw_connection.rollback()
                except Error as exc:
                    if self.is_disconnection_error(exc):
                        self._raw_connection = None
                        self._state = STATE_RECONNECT
                        self._two_phase_transaction = False
                    else:
                        raise
                else:
                    self._two_phase_transaction = False
            else:
                self._two_phase_transaction = False
                self._state = STATE_RECONNECT
        finally:
            await self._check_disconnect(trace, "connection_rollback", self, xid)

    @staticmethod
    def to_database(params):
        """Convert some parameters into values acceptable to a database backend.

        It is acceptable to override this method in subclasses, but it
        is not intended to be used externally.

        This delegates conversion to any
        L{Variable <storm.variable.Variable>}\\ s in the parameter list, and
        passes through all other values untouched.
        """
        for param in params:
            if isinstance(param, Variable):
                yield param.get(to_db=True)
            else:
                yield param

    async def build_raw_cursor(self):
        """Get a new dbapi cursor object.

        It is acceptable to override this method in subclasses, but it
        is not intended to be called externally.
        """
        if asyncio.iscoroutinefunction(self._raw_connection.cursor):
            return await self._raw_connection.cursor()
        else:
            return self._raw_connection.cursor()

    async def raw_execute(self, statement, params=None):
        """Execute a raw statement with the given parameters.

        It's acceptable to override this method in subclasses, but it
        is not intended to be called externally.

        If the global C{DEBUG} is True, the statement will be printed
        to standard out.

        @return: The dbapi cursor object, as fetched from L{build_raw_cursor}.
        """
        raw_cursor = await self._check_disconnect(self.build_raw_cursor)
        await self._prepare_execution(raw_cursor, params, statement)
        args = self._execution_args(params, statement)
        await self._run_execution(raw_cursor, args, params, statement)
        return raw_cursor

    def _execution_args(self, params, statement):
        """Get the appropriate statement execution arguments."""
        if params:
            args = (statement, tuple(self.to_database(params)))
        else:
            args = (statement,)
        return args

    async def _run_execution(self, raw_cursor, args, params, statement):
        """Complete the statement execution, along with result reports."""
        try:
            await self._check_disconnect(raw_cursor.execute, *args)
        except Exception as error:
            await self._check_disconnect(
                trace, "connection_raw_execute_error", self, raw_cursor,
                statement, params or (), error)
            raise
        else:
            await self._check_disconnect(
                trace, "connection_raw_execute_success", self, raw_cursor,
                statement, params or ())

    async def _prepare_execution(self, raw_cursor, params, statement):
        """Prepare the statement execution to be run."""
        try:
            await self._check_disconnect(
                trace, "connection_raw_execute", self, raw_cursor,
                statement, params or ())
        except Exception as error:
            await self._check_disconnect(
                trace, "connection_raw_execute_error", self, raw_cursor,
                statement, params or (), error)
            raise

    async def _ensure_connected(self):
        """Ensure that we are connected to the database.

        If the connection is marked as dead, or if we can't reconnect,
        then raise DisconnectionError.
        """
        if self._blocked:
            raise ConnectionBlockedError("Access to connection is blocked")
        if self._raw_connection is None or self._state != STATE_CONNECTED:
            # Initial connection or reconnection needed
            if self._state == STATE_DISCONNECTED:
                raise DisconnectionError("Already disconnected")
            try:
                self._raw_connection = await self._database.raw_connect()
                self._state = STATE_CONNECTED
            except DatabaseError as exc:
                self._state = STATE_DISCONNECTED
                self._raw_connection = None
                raise DisconnectionError(str(exc))

    def is_disconnection_error(self, exc, extra_disconnection_errors=()):
        """Check whether an exception represents a database disconnection.

        This should be overridden by backends to detect whichever
        exception values are used to represent this condition.
        """
        return False

    def _raw_xid(self, xid):
        """Return a raw xid from the given high-level L{Xid} object."""
        return self._raw_connection.xid(xid.format_id,
                                        xid.global_transaction_id,
                                        xid.branch_qualifier)

    async def _check_disconnect(self, function, *args, **kwargs):
        """Run the given function, checking for database disconnections."""
        # Allow the caller to specify additional exception types that
        # should be treated as possible disconnection errors.
        extra_disconnection_errors = kwargs.pop(
            'extra_disconnection_errors', ())
        try:
            if asyncio.iscoroutinefunction(function):
                result = await function(*args, **kwargs)
            else:
                result = function(*args, **kwargs)
                # Check if the result is awaitable (coroutine, Future, Task, etc.)
                # This handles aiomysql/aiosqlite that use decorators
                if hasattr(result, '__await__'):
                    result = await result
            return result
        except Exception as exc:
            if self.is_disconnection_error(exc, extra_disconnection_errors):
                self._state = STATE_DISCONNECTED
                self._raw_connection = None
                raise DisconnectionError(str(exc))
            else:
                raise

    def preset_primary_key(self, primary_columns, primary_variables):
        """Process primary variables before an insert happens.

        This method may be overwritten by backends to implement custom
        changes in primary variables before an insert happens.
        """


class Database:
    """A database that can be connected to.

    This should be subclassed for individual database backends.

    @cvar connection_factory: A callable which will take this database
        and should return an instance of L{Connection}.
    """

    connection_factory = Connection

    def __init__(self, uri=None):
        self._uri = uri
        self._exception_types = {}

    def get_uri(self):
        """Return the URI object this database was created with."""
        return self._uri

    def connect(self, event=None):
        """Create a connection to the database.

        It calls C{self.connection_factory} to allow for ease of
        customization.

        @param event: The event system to broadcast messages with. If
            not specified, then no events will be broadcast.

        @return: An instance of L{Connection}.
        """
        return self.connection_factory(self, event)

    async def raw_connect(self):
        """Create a raw database connection.

        This is used by L{Connection} objects to connect to the
        database.  It should be overriden in subclasses to do any
        database-specific connection setup.

        @return: A DB-API connection object.
        """
        raise NotImplementedError

    @property
    def _exception_module(self):
        """The module where appropriate DB-API exception types are defined.

        Subclasses should set this if they support re-raising DB-API
        exceptions as StormError instances.
        """
        return None

    def _make_combined_exception_type(self, wrapper_type, dbapi_type):
        """Make a combined exception based on both DB-API and Storm.

        Storm historically defined its own exception types as ABCs and
        registered the DB-API exception types as virtual subclasses.
        However, this doesn't work properly in Python 3
        (https://bugs.python.org/issue12029).  Instead, we create and cache
        subclass-specific exception types that inherit from both StormError
        and the DB-API exception type, allowing code that catches either
        StormError (or subclasses) or the specific DB-API exceptions to keep
        working.

        @type wrapper_type: L{type}
        @param wrapper_type: The type of the wrapper exception to create; a
            subclass of L{StormError}.
        @type dbapi_type: L{type}
        @param dbapi_type: The type of the DB-API exception.

        @return: The combined exception type.
        """
        if dbapi_type.__name__ not in self._exception_types:
            self._exception_types[dbapi_type.__name__] = type(
                dbapi_type.__name__, (dbapi_type, wrapper_type), {})
        return self._exception_types[dbapi_type.__name__]

    def _wrap_exception(self, wrapper_type, exception):
        """Wrap a DB-API exception as a StormError instance.

        This constructs a wrapper exception with the same C{args} as the
        DB-API exception.  Subclasses may override this to set additional
        attributes on the wrapper exception.

        @type wrapper_type: L{type}
        @param wrapper_type: The type of the wrapper exception to create; a
            subclass of L{StormError}.
        @type exception: L{Exception}
        @param exception: The DB-API exception to wrap.

        @return: The wrapped exception; an instance of L{StormError}.
        """
        return self._make_combined_exception_type(
            wrapper_type, exception.__class__)(*exception.args)


def convert_param_marks(statement, from_param_mark, to_param_mark):
    # TODO: Add support for $foo$bar$foo$ literals.
    if from_param_mark == to_param_mark or from_param_mark not in statement:
        return statement
    tokens = statement.split("'")
    for i in range(0, len(tokens), 2):
        tokens[i] = tokens[i].replace(from_param_mark, to_param_mark)
    return "'".join(tokens)


_database_schemes = {}

def register_scheme(scheme, factory):
    """Register a handler for a new database URI scheme.

    @param scheme: the database URI scheme
    @param factory: a function taking a URI instance and returning a database.
    """
    _database_schemes[scheme] = factory


def create_database(uri):
    """Create a database instance.

    @param uri: An URI instance, or a string describing the URI. Some examples:

    "sqlite:"
        An in memory sqlite database.

    "sqlite:example.db"
        A SQLite database called example.db

    "postgres:test"
        The database 'test' from the local postgres server.

    "postgres://user:password@host/test"
        The database test on machine host with supplied user credentials,
        using postgres.

    "anything:..."
        Where 'anything' has previously been registered with
        L{register_scheme}.
    """
    if isinstance(uri, str):
        uri = URI(uri)
    if uri.scheme in _database_schemes:
        factory = _database_schemes[uri.scheme]
    else:
        module = __import__("%s.databases.%s" % (storm.__name__, uri.scheme),
                            None, None, [""])
        factory = module.create_from_uri
    return factory(uri)

# Deal with circular import.
from storm.tracer import trace
