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
from __future__ import print_function

from storm.uri import URI, URIError
from storm.tests.helper import TestHelper


class URITest(TestHelper):

    def test_parse_defaults(self):
        uri = URI("scheme:")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.options, {})
        self.assertEqual(uri.username, None)
        self.assertEqual(uri.password, None)
        self.assertEqual(uri.host, None)
        self.assertEqual(uri.port, None)
        self.assertEqual(uri.database, None)

    def test_parse_no_colon(self):
        self.assertRaises(URIError, URI, "scheme")

    def test_parse_just_colon(self):
        uri = URI("scheme:")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.database, None)

    def test_parse_just_relative_database(self):
        uri = URI("scheme:d%61ta/base")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.database, "data/base")

    def test_parse_just_absolute_database(self):
        uri = URI("scheme:/d%61ta/base")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.database, "/data/base")

    def test_parse_host(self):
        uri = URI("scheme://ho%73t")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.host, "host")

    def test_parse_username(self):
        uri = URI("scheme://user%6eame@")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.host, None)

    def test_parse_username_password(self):
        uri = URI("scheme://user%6eame:pass%77ord@")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.password, "password")
        self.assertEqual(uri.host, None)

    def test_parse_username_host(self):
        uri = URI("scheme://user%6eame@ho%73t")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.host, "host")

    def test_parse_username_password_host(self):
        uri = URI("scheme://user%6eame:pass%77ord@ho%73t")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.password, "password")
        self.assertEqual(uri.host, "host")

    def test_parse_username_password_host_port(self):
        uri = URI("scheme://user%6eame:pass%77ord@ho%73t:1234")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.password, "password")
        self.assertEqual(uri.host, "host")
        self.assertEqual(uri.port, 1234)

    def test_parse_username_password_host_empty_port(self):
        uri = URI("scheme://user%6eame:pass%77ord@ho%73t:")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.password, "password")
        self.assertEqual(uri.host, "host")
        self.assertEqual(uri.port, None)

    def test_parse_username_password_host_port_database(self):
        uri = URI("scheme://user%6eame:pass%77ord@ho%73t:1234/d%61tabase")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.password, "password")
        self.assertEqual(uri.host, "host")
        self.assertEqual(uri.port, 1234)
        self.assertEqual(uri.database, "database")

    def test_parse_username_password_database(self):
        uri = URI("scheme://user%6eame:pass%77ord@/d%61tabase")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.username, "username")
        self.assertEqual(uri.password, "password")
        self.assertEqual(uri.host, None)
        self.assertEqual(uri.port, None)
        self.assertEqual(uri.database, "database")

    def test_parse_options(self):
        uri = URI("scheme:?a%62c=d%65f&ghi=jkl")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.host, None)
        self.assertEqual(uri.database, None)
        self.assertEqual(uri.options, {"abc": "def", "ghi": "jkl"})

    def test_parse_host_options(self):
        uri = URI("scheme://ho%73t?a%62c=d%65f&ghi=jkl")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.host, "host")
        self.assertEqual(uri.database, None)
        self.assertEqual(uri.options, {"abc": "def", "ghi": "jkl"})

    def test_parse_host_database_options(self):
        uri = URI("scheme://ho%73t/d%61tabase?a%62c=d%65f&ghi=jkl")
        self.assertEqual(uri.scheme, "scheme")
        self.assertEqual(uri.host, "host")
        self.assertEqual(uri.database, "database")
        self.assertEqual(uri.options, {"abc": "def", "ghi": "jkl"})

    def test_copy(self):
        uri = URI("scheme:///db?opt=value")
        uri_copy = uri.copy()
        self.assertTrue(uri_copy is not uri)
        self.assertTrue(uri_copy.__dict__ == uri.__dict__)
        self.assertTrue(uri_copy.options is not uri.options)

    def str(self, uri):
        self.assertEqual(str(URI(uri)), uri)

    def test_str_full_with_escaping(self):
        self.str("scheme://us%2Fer:pa%2Fss@ho%2Fst:0/d%3Fb?a%2Fb=c%2Fd&ghi=jkl")

    def test_str_no_path_escaping(self):
        self.str("scheme:/a/b/c")

    def test_str_scheme_only(self):
        self.str("scheme:")

    def test_str_username_only(self):
        self.str("scheme://username@/")

    def test_str_password_only(self):
        self.str("scheme://:password@/")

    def test_str_port_only(self):
        self.str("scheme://:0/")

    def test_str_host_only(self):
        self.str("scheme://host/")

    def test_str_database_only(self):
        self.str("scheme:db")

    def test_str_option_only(self):
        self.str("scheme:?a=b")

