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

from storm.databases.sqlite import SQLite
from storm.exceptions import ConnectionBlockedError
from storm.store import Store, block_access
from storm.tests.helper import TestHelper, MakePath
from storm.uri import URI


class BlockAccessTest(TestHelper):
    """Tests for L{block_access}."""

    helpers = [MakePath]

    def setUp(self):
        super(BlockAccessTest, self).setUp()
        database = SQLite(URI("sqlite:"))
        self.store = Store(database)

    def test_block_access(self):
        """
        The L{block_access} context manager blocks access to a L{Store}.  A
        L{ConnectionBlockedError} exception is raised if an attempt to access
        the underlying database is made while a store is blocked.
        """
        with block_access(self.store):
            self.assertRaises(ConnectionBlockedError, self.store.execute,
                              "SELECT 1")
        result = self.store.execute("SELECT 1")
        self.assertEqual([(1,)], list(result))

    def test_block_access_with_multiple_stores(self):
        """
        If multiple L{Store}s are passed to L{block_access} they will all be
        blocked until the managed context is left.
        """
        database = SQLite(URI("sqlite:%s" % self.make_path()))
        store = Store(database)
        with block_access(self.store, store):
            self.assertRaises(ConnectionBlockedError, self.store.execute,
                              "SELECT 1")
            self.assertRaises(ConnectionBlockedError, store.execute,
                              "SELECT 1")
