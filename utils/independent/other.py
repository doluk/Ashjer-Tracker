import asyncio
from enum import Enum

import nest_asyncio

import re


class ClassIterator:
    '''makes arbitrary classes iterable by iterating over their __dict__
    Parameters
    ----------
        class_instance
            an instance of any class
    '''

    def __init__(self, class_instance):
        self.iter = class_instance.__dict__.__iter__()

    def __next__(self):
        return self.iter.__next__()


class AsyncIteratorExecutor:
    '''converts a regular iterable into an asynchronous iterator
    Parameters
    ----------
        iterable: iterable
            the iterable to convert
        loop: asyncio event loop
            the event loop to run the async iterator on
        executor
            the executor executing the async run
    '''

    def __init__(self, iterable, loop=None, executor=None):
        self.__iterator = iterable.__iter__()
        self.__loop = loop or asyncio.get_event_loop()
        self.__executor = executor

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.__loop.run_in_executor(
            self.__executor, next, self.__iterator, self)
        if value is self:
            raise StopAsyncIteration
        return value


def findall(elem: str, string: str) -> list[int]:
    """find all occurrences of an element in a string
    Parameters
    ----------
        elem: string
            the element to find
        string: string
            the string to search

    Returns
    -------
        list of integer or -1
            a list of all indices where the element was found

    Raises
    ------
        ValueError
            if the element is not in the string
    """

    occurrences = []
    while string:
        # search right to left to avoid index issues
        try:
            idx = string.rindex(elem)
            string = string[:idx]
            occurrences.append(idx)
        except ValueError:
            if not occurrences:
                raise
            break
    return occurrences[::-1]


def split_at(positions: list[int], string: str) -> list[str]:
    """split a string at certain positions (dropping the respective characters)
    Parameters
    ----------
        positions: list of integer
            the positions to split at, in ascending order
        string: string
            the string to split

    Returns
    -------
        list of string
            the substrings
    """

    substrings = []
    for pos in positions[::-1]:
        substrings.append(string[pos + 1:])
        string = string[:pos]
    substrings.append(string)
    return substrings[::-1]

def asyncio_run(future, as_task=True):
    """
    A better implementation of `asyncio.run`.

    :param future: A future or task or call of an async method.
    :param as_task: Forces the future to be scheduled as task (needed for e.g. aiohttp).
    """

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:  # no event loop running:
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(_to_task(future, as_task, loop))
    else:
        nest_asyncio.apply(loop)
        return asyncio.run(_to_task(future, as_task, loop))


def _to_task(future, as_task, loop):
    if not as_task or isinstance(future, asyncio.Task):
        return future
    return loop.create_task(future)

def validate_url(input: str)->bool:
    """Validate the input could be a valid url by regex matching"""
    import re
    regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex,input) is not None


class ExtendedEnum(Enum):
    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))

    @classmethod
    def names(cls):
        return list(map(lambda c: c.name, cls))
