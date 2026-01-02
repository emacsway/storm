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
import asyncio
import weakref

from storm import has_cextensions


__all__ = ["EventSystem"]


class EventSystem:
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
                    if asyncio.iscoroutinefunction(callback):
                        # For async callbacks, create a task to run them
                        # This allows emit() to be called from synchronous code
                        async def run_callback():
                            result = await callback(owner, *(args+data))
                            if result is False:
                                callbacks.discard((callback, data))
                        try:
                            loop = asyncio.get_running_loop()
                            loop.create_task(run_callback())
                        except RuntimeError:
                            # No event loop running, can't schedule async callback
                            pass
                    else:
                        result = callback(owner, *(args+data))
                        # Check if result is awaitable (handles decorators)
                        if hasattr(result, '__await__'):
                            # Schedule as a coroutine
                            async def run_awaitable():
                                r = await result
                                if r is False:
                                    callbacks.discard((callback, data))
                            try:
                                loop = asyncio.get_running_loop()
                                loop.create_task(run_awaitable())
                            except RuntimeError:
                                pass
                        elif result is False:
                            callbacks.discard((callback, data))

    async def emit_async(self, name, *args):
        """Emit an event asynchronously, awaiting any async callbacks.

        This method should be used when calling from async code and you need
        to wait for async callbacks to complete (e.g., resolve-lazy-value).

        @param name: The name of the event.
        @param args: Additional arguments to pass to hooks.
        """
        owner = self._owner_ref()
        if owner is not None:
            callbacks = self._hooks.get(name)
            if callbacks:
                for callback, data in tuple(callbacks):
                    if asyncio.iscoroutinefunction(callback):
                        result = await callback(owner, *(args+data))
                    else:
                        result = callback(owner, *(args+data))
                        # Check if result is awaitable (handles decorators)
                        if hasattr(result, '__await__'):
                            result = await result
                    if result is False:
                        callbacks.discard((callback, data))


# C extension EventSystem is not async-compatible, so we always use the Python version
# if has_cextensions:
#     from storm.cextensions import EventSystem
