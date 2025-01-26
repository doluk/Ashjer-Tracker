# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 16:01:15 2021

@author: Lukas
"""

from __future__ import annotations

import os
from asyncio import run
from datetime import datetime

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler import AsyncScheduler
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.triggers.interval import IntervalTrigger


engine = create_async_engine(f'postgresql+asyncpg://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:'
                             f'{os.getenv("DB_PORT")}/{os.getenv("DB_DATABASE")}')
data_store = SQLAlchemyDataStore(engine, schema="scheduler")
scheduler = AsyncScheduler(data_store=data_store)

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
