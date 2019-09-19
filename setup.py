#!/usr/bin/env python

from __future__ import print_function

import os
import re

from setuptools import setup, Extension, find_packages


if os.path.isfile("MANIFEST"):
    os.unlink("MANIFEST")


BUILD_CEXTENSIONS = True


VERSION = re.search('version = "([^"]+)"',
                    open("storm/__init__.py").read()).group(1)


tests_require = [
    # Versions based on Lucid, where packaged.
    "fixtures >= 0.3.5",
    # pgbouncer (the Python module) is not yet packaged in Ubuntu.
    "pgbouncer >= 0.0.7",
    "psycopg2 >= 2.3.0",
    "testresources >= 0.2.4",
    "testtools >= 0.9.8",
    # timeline is not yet packaged in Ubuntu.
    "timeline >= 0.0.2",
    "transaction >= 1.0.0",
    "twisted >= 10.0.0",
    "zope.component >= 3.8.0",
    "zope.interface >= 4.0.0",
    "zope.security >= 3.7.2",
    ]


setup(
    name="storm",
    version=VERSION,
    description=(
        "Storm is an object-relational mapper (ORM) for Python "
        "developed at Canonical."),
    author="Gustavo Niemeyer",
    author_email="gustavo@niemeyer.net",
    maintainer="Storm Developers",
    maintainer_email="storm@lists.canonical.com",
    license="LGPL",
    url="https://storm.canonical.com",
    download_url="https://launchpad.net/storm/+download",
    packages=find_packages(),
    package_data={"": ["*.zcml"]},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        ("License :: OSI Approved :: GNU Library or "
         "Lesser General Public License (LGPL)"),
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    ext_modules=(BUILD_CEXTENSIONS and
                 [Extension("storm.cextensions", ["storm/cextensions.c"])]),
    # The following options are specific to setuptools but ignored (with a
    # warning) by distutils.
    include_package_data=True,
    zip_safe=False,
    install_requires=["six"],
    test_suite="storm.tests.find_tests",
    tests_require=tests_require,
    extras_require={"test": tests_require},
    )
