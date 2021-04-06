Tutorial
========

Importing
---------

Let's start by importing some names into the namespace.

.. doctest::

    >>> from storm.locals import *

Basic definition
----------------

Now we define a type with some properties describing the information
we're about to map.

.. doctest::

    >>> class Person(object):
    ...     __storm_table__ = "person"
    ...     id = Int(primary=True)
    ...     name = Unicode()

Notice that this has no Storm-defined base class or constructor.

Creating a database and the store
---------------------------------

We still don't have anyone to talk to, so let's define an in-memory
SQLite database to play with, and a store using that database.

.. doctest::

    >>> database = create_database("sqlite:")
    >>> store = Store(database)

Three databases are supported at the moment: SQLite, MySQL, and PostgreSQL.
The parameter passed to :py:func:`~storm.database.create_database` is an
URI, as follows:

.. code-block:: python

    # database = create_database(
    #     "scheme://username:password@hostname:port/database_name")

The ``scheme`` may be ``sqlite``, ``mysql``, or ``postgres``.

Now we have to create the table that will actually hold the data
for our class.

.. doctest::

    >>> store.execute("CREATE TABLE person "
    ...               "(id INTEGER PRIMARY KEY, name VARCHAR)")
    <storm.databases.sqlite.SQLiteResult object at 0x...>

We got a result back, but we don't care about it for now. We could also
use ``noresult=True`` to avoid the result entirely.

Creating an object
------------------

Let's create an object of the defined class.

.. doctest::

    >>> joe = Person()
    >>> joe.name = u"Joe Johnes"

    >>> print(joe.id)
    None
    >>> print(joe.name)
    Joe Johnes

So far this object has no connection to a database. Let's add it to the
store we've created above.

.. doctest::

    >>> store.add(joe)
    <...Person object at 0x...>

    >>> print(joe.id)
    None
    >>> print(joe.name)
    Joe Johnes

Notice that the object wasn't changed, even after being added to the
store.  That's because it wasn't flushed yet.

The store of an object
----------------------

Once an object is added to a store, or retrieved from a store, its
relation to that store is known.  We can easily verify which store
an object is bound.

.. doctest::

    >>> Store.of(joe) is store
    True

    >>> Store.of(Person()) is None
    True

Finding an object
-----------------

Now, what would happen if we actually asked the store to give us
the person named "Joe Johnes"?

.. doctest::

    >>> person = store.find(Person, Person.name == u"Joe Johnes").one()

    >>> print(person.id)
    1
    >>> print(person.name)
    Joe Johnes

The person is there!  Yeah, ok, you were expecting it. :-)

We can also retrieve the object using its primary key.

.. doctest::

    >>> print(store.get(Person, 1).name)
    Joe Johnes

Caching behavior
----------------

One interesting thing is that this person is actually Joe, right? We've
just added this object, so there's only one Joe, why would there be
two different objects?  There isn't.

.. doctest::

    >>> person is joe
    True

What's going on behind the scenes is that each store has an object
cache. When an object is linked to a store, it will be cached by
the store for as long as there's a reference to the object somewhere,
or while the object is dirty (has unflushed changes).

Storm ensures that at least a certain number of recently used objects
will stay in memory inside the transaction, so that frequently used
objects are not retrieved from the database too often.

Flushing
--------

When we tried to find Joe in the database for the first time, we've
noticed that the ``id`` property was magically assigned. This happened
because the object was flushed implicitly so that the operation would
affect any pending changes as well.

Flushes may also happen explicitly.

.. doctest::

    >>> mary = Person()
    >>> mary.name = u"Mary Margaret"
    >>> store.add(mary)
    <...Person object at 0x...>

    >>> print(mary.id)
    None
    >>> print(mary.name)
    Mary Margaret

    >>> store.flush()
    >>> print(mary.id)
    2
    >>> print(mary.name)
    Mary Margaret

Changing objects with the Store
-------------------------------

Besides changing objects as usual, we can also benefit from the fact
that objects are tied to a database to change them using expressions.

.. doctest::

    >>> store.find(
    ...     Person, Person.name == u"Mary Margaret").set(name=u"Mary Maggie")
    >>> print(mary.name)
    Mary Maggie

This operation will touch every matching object in the database, and
also objects that are alive in memory.

Committing
----------

Everything we've done so far is inside a transaction. At this point,
we can either make these changes and any pending uncommitted changes
persistent by committing them, or we can undo everything by rolling
them back.

We'll commit them, with something as simple as

.. doctest::

    >>> store.commit()

That was straightforward. Everything is still the way it was, but now
changes are there "for real".

Rolling back
------------

Aborting changes is very straightforward as well.

.. doctest::

    >>> joe.name = u"Tom Thomas"

Let's see if these changes are really being considered by Storm
and by the database.

.. doctest::

    >>> person = store.find(Person, Person.name == u"Tom Thomas").one()
    >>> person is joe
    True

Yes, they are. Now, for the magic step (suspense music, please).

.. doctest::

    >>> store.rollback()

Erm.. nothing happened?

Actually, something happened.. with Joe.  He's back!

.. doctest::

    >>> print(joe.id)
    1
    >>> print(joe.name)
    Joe Johnes

Constructors
------------

So, we've been working for too long with people only. Let's introduce
a new kind of data in our model: companies.  For the company, we'll
use a constructor, just for the fun of it.  It will be the simplest
company class you've ever seen:

.. doctest:

    >>> class Company(object):
    ...     __storm_table__ = "company"
    ...     id = Int(primary=True)
    ...     name = Unicode()
    ...
    ...     def __init__(self, name):
    ...         self.name = name

Notice that the constructor parameter isn't optional.  It could be
optional, if we wanted, but our companies always have names.

Let's add the table for it.

.. doctest::

    >>> store.execute(
    ...     "CREATE TABLE company (id INTEGER PRIMARY KEY, name VARCHAR)",
    ...     noresult=True)

Then, create a new company.

.. doctest::

    >>> circus = Company(u"Circus Inc.")

    >>> print(circus.id)
    None
    >>> print(circus.name)
    Circus Inc.

The ``id`` is still undefined because we haven't flushed it.  In fact,
we haven't even **added** the company to the store.  We'll do
that soon.  Watch out.


References and subclassing
--------------------------

Now we want to assign some employees to our company.  Rather than
redoing the Person definition, we'll keep it as it is, since it's
general, and will create a new subclass of it for employees, which
include one extra field: the company id.

.. doctest::

    >>> class Employee(Person):
    ...     __storm_table__ = "employee"
    ...     company_id = Int()
    ...     company = Reference(company_id, Company.id)
    ...
    ...     def __init__(self, name):
    ...         self.name = name

Pay attention to that definition for a moment. Notice that it doesn't
define what's already in person, and introduces the ``company_id``,
and a ``company`` property, which is a reference to another class.  It
also has a constructor, but which leaves the company alone.

As usual, we need a table.  SQLite has no idea of what a foreign key is,
so we'll not bother to define it.

.. doctest::

    >>> store.execute(
    ...     "CREATE TABLE employee "
    ...     "(id INTEGER PRIMARY KEY, name VARCHAR, company_id INTEGER)",
    ...     noresult=True)

Let's give life to Ben now.

.. doctest::

    >>> ben = store.add(Employee(u"Ben Bill"))

    >>> print(ben.id)
    None
    >>> print(ben.name)
    Ben Bill
    >>> print(ben.company_id)
    None

We can see that they were not flushed yet. Even then, we can say
that Bill works on Circus.

.. doctest::

    >>> ben.company = circus

    >>> print(ben.company_id)
    None
    >>> print(ben.company.name)
    Circus Inc.

Of course, we still don't know the company id since it was not
flushed to the database yet, and we didn't assign an id explicitly.
Storm is keeping the relationship even then.

If whatever is pending is flushed to the database (implicitly or
explicitly), objects will get their ids, and any references are
updated as well (before being flushed!).

.. doctest::

    >>> store.flush()

    >>> print(ben.company_id)
    1
    >>> print(ben.company.name)
    Circus Inc.

They're both flushed to the database.  Now, notice that the Circus
company wasn't added to the store explicitly in any moment.  Storm
will do that automatically for referenced objects, for both objects
(the referenced and the referencing one).

Let's create another company to check something. This time we'll
flush the store just after adding it.

.. doctest::

    >>> sweets = store.add(Company(u"Sweets Inc."))
    >>> store.flush()
    >>> sweets.id
    2

Nice, we've already got the id of the new company. So, what would
happen if we changed **just the id** for Ben's company?

.. doctest::

    >>> ben.company_id = 2
    >>> print(ben.company.name)
    Sweets Inc.
    >>> ben.company is sweets
    True

Hah! **That** wasn't expected, was it? ;-)

Let's commit everything.

.. doctest::

    >>> store.commit()

Many-to-one reference sets
--------------------------

So, while our model says that employees work for a single company
(we only design normal people here), companies may of course have
multiple employees. We represent that in Storm using reference sets.

We won't define the company again. Instead, we'll add a new attribute
to the class.

.. doctest::

    >>> Company.employees = ReferenceSet(Company.id, Employee.company_id)

Without any further work, we can already see which employees are
working for a given company.

.. doctest::

    >>> sweets.employees.count()
    1

    >>> for employee in sweets.employees:
    ...     print(employee.id)
    ...     print(employee.name)
    ...     print(employee is ben)
    ...
    1
    Ben Bill
    True

Let's create another employee, and add him to the company, rather
than setting the company in the employee (it sounds better, at least).

.. doctest::

    >>> mike = store.add(Employee(u"Mike Mayer"))
    >>> sweets.employees.add(mike)

That, of course, means that Mike's working for a company, and so it
should be reflected elsewhere.

.. doctest::

    >>> mike.company_id
    2

    >>> mike.company is sweets
    True

Many-to-many reference sets and composed keys
---------------------------------------------

We want to represent accountants in our model as well.  Companies have
accountants, but accountants may also attend several companies, so we'll
represent that using a many-to-many relationship.

Let's create a simple class to use with accountants, and the relationship
class.

.. doctest::

    >>> class Accountant(Person):
    ...     __storm_table__ = "accountant"
    ...     def __init__(self, name):
    ...         self.name = name

    >>> class CompanyAccountant(object):
    ...     __storm_table__ = "company_accountant"
    ...     __storm_primary__ = "company_id", "accountant_id"
    ...     company_id = Int()
    ...     accountant_id = Int()

Hey, we've just declared a class with a composed key!

Now, let's use it to declare the many-to-many relationship in the
company.  Once more, we'll just stick the new attribute in the
existent object.  It may easily be defined at class definition
time.  Later we'll see another way to do that as well.

.. doctest::

    >>> Company.accountants = ReferenceSet(Company.id,
    ...                                    CompanyAccountant.company_id,
    ...                                    CompanyAccountant.accountant_id,
    ...                                    Accountant.id)

Done!  The order in which attributes were defined is important,
but the logic should be pretty obvious.

We're missing some tables, at this point.

.. doctest::

    >>> store.execute(
    ...     "CREATE TABLE accountant (id INTEGER PRIMARY KEY, name VARCHAR)",
    ...     noresult=True)

    >>> store.execute(
    ...     "CREATE TABLE company_accountant "
    ...     "(company_id INTEGER, accountant_id INTEGER,"
    ...     " PRIMARY KEY (company_id, accountant_id))",
    ...     noresult=True)

Let's give life to a couple of accountants, and register them
in both companies.

.. doctest::

    >>> karl = Accountant(u"Karl Kent")
    >>> frank = Accountant(u"Frank Fourt")

    >>> sweets.accountants.add(karl)
    >>> sweets.accountants.add(frank)

    >>> circus.accountants.add(frank)

That's it! Really!  Notice that we didn't even have to add them to
the store, since it happens implicitly by linking to the other object
which is already in the store, and that we didn't have to declare the
relationship object, since that's known to the reference set.

We can now check them.

.. doctest::

    >>> sweets.accountants.count()
    2

    >>> circus.accountants.count()
    1

Even though we didn't use the ``CompanyAccountant`` object explicitly,
we can check it if we're really curious.

.. doctest::

    >>> store.get(CompanyAccountant, (sweets.id, frank.id))
    <...CompanyAccountant object at 0x...>

Notice that we pass a tuple for the :py:meth:`~storm.store.Store.get`
method, due to the composed key.

If we wanted to know for which companies accountants are working,
we could easily define a reversed relationship:

.. doctest::

    >>> Accountant.companies = ReferenceSet(Accountant.id,
    ...                                     CompanyAccountant.accountant_id,
    ...                                     CompanyAccountant.company_id,
    ...                                     Company.id)

    >>> for name in sorted(company.name for company in frank.companies):
    ...     print(name)
    Circus Inc.
    Sweets Inc.

    >>> for company in karl.companies:
    ...     print(company.name)
    Sweets Inc.


Joins
-----

Since we've got some nice data to play with, let's try to make a
few interesting queries.

Let's start by checking which companies have at least one employee
named Ben.  We have at least two ways to do it.

First, with an implicit join.

.. doctest::

    >>> result = store.find(Company,
    ...                     Employee.company_id == Company.id,
    ...                     Employee.name.like(u"Ben %"))

    >>> for company in result:
    ...     print(company.name)
    Sweets Inc.

Then, we can also do an explicit join.  This is interesting for mapping
complex SQL joins to Storm queries.

.. doctest::

    >>> origin = [Company, Join(Employee, Employee.company_id == Company.id)]
    >>> result = store.using(*origin).find(
    ...     Company, Employee.name.like(u"Ben %"))

    >>> for company in result:
    ...     print(company.name)
    Sweets Inc.

If we already had the company, and wanted to know which of his employees
were named Ben, that'd have been easier.

.. doctest::

    >>> result = sweets.employees.find(Employee.name.like(u"Ben %"))

    >>> for employee in result:
    ...     print(employee.name)
    Ben Bill


Sub-selects
-----------

Suppose we want to find all accountants that aren't associated with a
company.  We can use a sub-select to get the data we want.

.. doctest::

    >>> laura = Accountant(u"Laura Montgomery")
    >>> store.add(laura)
    <...Accountant ...>

    >>> subselect = Select(CompanyAccountant.accountant_id, distinct=True)
    >>> result = store.find(Accountant, Not(Accountant.id.is_in(subselect)))
    >>> result.one() is laura
    True


Ordering and limiting results
-----------------------------

Ordering and limiting results obtained are certainly among the
simplest and yet most wanted features for such tools, so we want
to make them very easy to understand and use, of course.

A line of code is worth a thousand words, so here are a few examples
that demonstrate how it works:

.. doctest::

    >>> garry = store.add(Employee(u"Garry Glare"))

    >>> result = store.find(Employee)

    >>> for employee in result.order_by(Employee.name):
    ...     print(employee.name)
    Ben Bill
    Garry Glare
    Mike Mayer

    >>> for employee in result.order_by(Desc(Employee.name)):
    ...     print(employee.name)
    Mike Mayer
    Garry Glare
    Ben Bill

    >>> for employee in result.order_by(Employee.name)[:2]:
    ...     print(employee.name)
    Ben Bill
    Garry Glare


Multiple types with one query
-----------------------------

Sometimes, it may be interesting to retrieve more than one object involved
in a given query.  Imagine, for instance, that besides knowing which
companies have an employee named Ben, we also want to know who is the
employee.  This may be achieved with a query like follows:

.. doctest::

    >>> result = store.find((Company, Employee),
    ...                     Employee.company_id == Company.id,
    ...                     Employee.name.like(u"Ben %"))

    >>> for company, employee in result:
    ...     print(company.name)
    ...     print(employee.name)
    Sweets Inc.
    Ben Bill


The Storm base class
--------------------

So far we've been defining our references and reference sets using
classes and their properties.  This has some advantages, like being
easier to debug, but also has some disadvantages, such as requiring
classes to be present in the local scope, which potentially leads to
circular import issues.

To prevent that kind of situation, Storm supports defining these
references using the stringified version of the class and property
names.  The only inconvenience of doing so is that all involved
classes must inherit from the :py:class:`~storm.base.Storm` base class.

Let's define some new classes to show that.  To expose the point,
we'll refer to a class before it's actually defined.

.. doctest::

    >>> class Country(Storm):
    ...     __storm_table__ = "country"
    ...     id = Int(primary=True)
    ...     name = Unicode()
    ...     currency_id = Int()
    ...     currency = Reference(currency_id, "Currency.id")

    >>> class Currency(Storm):
    ...     __storm_table__ = "currency"
    ...     id = Int(primary=True)
    ...     symbol = Unicode()

    >>> store.execute(
    ...     "CREATE TABLE country "
    ...     "(id INTEGER PRIMARY KEY, name VARCHAR, currency_id INTEGER)",
    ...     noresult=True)

    >>> store.execute(
    ...     "CREATE TABLE currency (id INTEGER PRIMARY KEY, symbol VARCHAR)",
    ...     noresult=True)

Now, let's see if it works.

.. doctest::

    >>> real = store.add(Currency())
    >>> real.id = 1
    >>> real.symbol = u"BRL"

    >>> brazil = store.add(Country())
    >>> brazil.name = u"Brazil"
    >>> brazil.currency_id = 1

    >>> print(brazil.currency.symbol)
    BRL

Questions!? ;-)


Loading hook
------------

Storm allows classes to define a few different hooks are called
to act when certain things happen.  One of the interesting hooks
available is the ``__storm_loaded__`` one.

Let's play with it.  We'll define a temporary subclass of Person
for that.

.. doctest::

    >>> class PersonWithHook(Person):
    ...     def __init__(self, name):
    ...         print("Creating %s" % name)
    ...         self.name = name
    ...
    ...     def __storm_loaded__(self):
    ...         print("Loaded %s" % self.name)

    >>> earl = store.add(PersonWithHook(u"Earl Easton"))
    Creating Earl Easton

    >>> earl = store.find(PersonWithHook, name=u"Earl Easton").one()

    >>> store.invalidate(earl)
    >>> del earl
    >>> import gc
    >>> collected = gc.collect()

    >>> earl = store.find(PersonWithHook, name=u"Earl Easton").one()
    Loaded Earl Easton

Note that in the first find, nothing was called, since the object
was still in memory and cached.  Then, we invalidated the object
from Storm's internal cache and ensured that it was out-of-memory
by triggering a garbage collection.  After that, the object had
to be retrieved from the database again, and thus the hook was
called (and not the constructor!).


Executing expressions
---------------------

Storm also offers a way to execute expressions in a
database-agnostic way, when that's necessary.

For instance:

.. doctest::

    >>> result = store.execute(Select(Person.name, Person.id == 1))
    >>> (name,) = result.get_one()
    >>> print(name)
    Joe Johnes

This mechanism is used internally by Storm itself to implement
the higher level features.


Auto-reloading values
---------------------

Storm offers some special values that may be assigned to attributes
under its control.  One of these values is
:py:data:`~storm.store.AutoReload`.  When used, it will make the
object automatically reload the value from the database when touched.
Even primary keys may benefit from its use, as shown below.

.. doctest::

    >>> from storm.locals import AutoReload

    >>> ruy = store.add(Person())
    >>> ruy.name = u"Ruy"
    >>> print(ruy.id)
    None

    >>> ruy.id = AutoReload
    >>> print(ruy.id)
    4

This may be set as the default value for any attribute, making the
object be automatically flushed if necessary.


Expression values
-----------------

Besides auto-reloading, it's also possible to assign what we call
a "lazy expression" to an attribute.  Such expressions are flushed
to the database when the attribute is accessed, or when the object
is flushed to the database (INSERT/UPDATE time).

For instance:

.. doctest::

    >>> ruy.name = SQL(
    ...     "(SELECT name || ? FROM person WHERE id=4)", (" Ritcher",))
    >>> print(ruy.name)
    Ruy Ritcher

Notice that this is just an example of what **may** be done.  There's
no need to write SQL statements this way, if you don't want to.  You may
also use class-based SQL expressions provided in Storm, or even
not use lazy expressions at all.


Aliases
-------

So now let's say that we want to find every pair of people that work
for the same company.  I have no idea about why one would *want* to
do that, but that's a good case for us to exercise aliases.

First, we create an alias for the `Employee` class.

.. doctest::

    >>> from storm.info import ClassAlias
    >>> AnotherEmployee = ClassAlias(Employee)

Nice, isn't it?

Now we can easily make the query we want, in a straightforward way:

.. doctest::

    >>> result = store.find((Employee, AnotherEmployee),
    ...                     Employee.company_id == AnotherEmployee.company_id,
    ...                     Employee.id > AnotherEmployee.id)

    >>> for employee1, employee2 in result:
    ...     print(employee1.name)
    ...     print(employee2.name)
    Mike Mayer
    Ben Bill

Woah!  Mike and Ben work for the same company!

(Quiz for the attentive reader: why is *greater than* being used in
the query above?)


Debugging
---------

Sometimes you just need to see which statements Storm is executing.  A
debug tracer built on top of Storm's tracing system can be used to see
what's going on under the hood.  A tracer is an object that gets
notified when interesting events occur, such as when Storm executes a
statement.  A function to enable and disable statement tracing is
provided.  Statements are logged to sys.stderr by default, but a
custom stream may also be used.

.. doctest::

    >>> import sys
    >>> from storm.tracer import debug

    >>> debug(True, stream=sys.stdout)
    >>> result = store.find((Employee, AnotherEmployee),
    ...                     Employee.company_id == AnotherEmployee.company_id,
    ...                     Employee.id > AnotherEmployee.id)
    >>> list(result)
    [...] EXECUTE: ...'SELECT employee.company_id, employee.id, employee.name, "...".company_id, "...".id, "...".name FROM employee, employee AS "..." WHERE employee.company_id = "...".company_id AND employee.id > "...".id', ()
    [...] DONE
    [(<...Employee object at ...>, <...Employee object at ...>)]

    >>> debug(False)
    >>> list(result)
    [(<...Employee object at ...>, <...Employee object at ...>)]


Much more!
----------

There's a lot more about Storm to be shown.  This tutorial is just a
way to get initiated on some of the concepts.  If your questions are
not answered somewhere else, feel free to ask them in the mailing
list.

..
    >>> Currency._storm_property_registry.clear()
    >>> Country._storm_property_registry.clear()
