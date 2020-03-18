from __future__ import print_function

from twisted.python.failure import Failure
from twisted.internet.defer import execute

from storm.twisted.transact import Transactor


class FakeThreadPool(object):
    """
    A fake L{twisted.python.threadpool.ThreadPool}, running functions inside
    the main thread instead for easing tests.
    """

    def callInThreadWithCallback(self, onResult, func, *args, **kw):
        success = True
        try:
            result = func(*args, **kw)
        except:
            result = Failure()
            success = False

        onResult(success, result)


class FakeTransaction(object):

    def commit(self):
        pass

    def abort(self):
        pass


class FakeTransactor(Transactor):
    """
    A fake C{Transactor} wrapper that runs the given function in the main
    thread and performs basic checks on its return value.  If it has a
    C{__storm_table__} property a C{RuntimeError} is raised because Storm
    objects cannot be used outside the thread in which they were created.

    @seealso: L{Transactor}.
    """
    retries = 0
    on_retry = None

    sleep = lambda *args, **kwargs: None

    def __init__(self, transaction=None):
        if transaction is None:
            transaction = FakeTransaction()
        self._transaction = transaction

    def run(self, function, *args, **kwargs):
        deferred = execute(self._wrap, function, *args, **kwargs)
        return deferred.addCallback(self._check_result)

    def _check_result(self, result):
        if getattr(result, "__storm_table__", None) is not None:
            raise RuntimeError("Attempted to return a Storm object from a "
                               "transaction")
        return result
