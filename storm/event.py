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

import weakref

from storm import has_cextensions


__all__ = ["EventSystem"]


class EventSystem(object):
    """A system for managing hooks that are called when events are emitted.

    Hooks are callables that take the event system C{owner} as their first
    argument, followed by the arguments passed when emitting the event,
    followed by any additional C{data} arguments given when registering the
    hook.

    Hooks registered for a given event C{name} are stored without ordering:
    no particular call order may be assumed when an event is emitted.
    """

    def __init__(self, owner):
        """
        @param owner: The object that owns this event system.  It is passed
            as the first argument to each hook function.
        """
        self._owner_ref = weakref.ref(owner)
        self._hooks = {}

    def hook(self, name, callback, *data):
        """Register a hook.

        @param name: The name of the event for which this hook should be
            called.
        @param callback: A callable which should be called when the event is
            emitted.
        @param data: Additional arguments to pass to the callable, after the
            C{owner} and any arguments passed when emitting the event.
        """
        callbacks = self._hooks.get(name)
        if callbacks is None:
            self._hooks.setdefault(name, set()).add((callback, data))
        else:
            callbacks.add((callback, data))

    def unhook(self, name, callback, *data):
        """Unregister a hook.

        This ignores attempts to unregister hooks that were not already
        registered.

        @param name: The name of the event for which this hook should no
            longer be called.
        @param callback: The callable to unregister.
        @param data: Additional arguments that were passed when registering
            the callable.
        """
        callbacks = self._hooks.get(name)
        if callbacks is not None:
            callbacks.discard((callback, data))

    def emit(self, name, *args):
        """Emit an event, calling any registered hooks.

        @param name: The name of the event.
        @param args: Additional arguments to pass to hooks.
        """
        owner = self._owner_ref()
        if owner is not None:
            callbacks = self._hooks.get(name)
            if callbacks:
                for callback, data in tuple(callbacks):
                    if callback(owner, *(args+data)) is False:
                        callbacks.discard((callback, data))


if has_cextensions:
    from storm.cextensions import EventSystem
