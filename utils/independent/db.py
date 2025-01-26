# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 06:30:49 2019

@author: Lukas
"""

###############################################################################
# Imports
###############################################################################

import asyncio
import os

import asyncpg
import re

from typing import List

# custom modules
from . import errors



# allow nested event loops
#nest_asyncio.apply()


###############################################################################
# Functions
###############################################################################

def format_query(query: str, args: list) -> str:
    """auxiliary function to substitute placeholders for real values
    Parameters
    ----------
        query: string
            an SQL query with asyncpg-style placeholders, e.g. $1
        args: list
            a list of replacements. len(args) must equal the highest order of placeholders

    Returns
    -------
        string
            the modified query
    """

    positions = [int(p) - 1 for p in re.findall(r'\$(\d+)', query)]
    _args = [args[i] for i in positions]

    return re.sub(r'\$\d+', '{}', query).format(*_args)


async def create_connection(cfg: dict) -> asyncpg.pool:
    """create a connection pool to allow for concurrent database access
    Parameters
    ----------
        cfg: dict
            a dictionary containing hostname, port number, database name, database user&password

    Returns
    -------
        asyncpg.pool
            the connection pool

    Raises
    ------
        any asyncpg errors encountered
    """
    if "schema" in cfg:
        schema = cfg.pop('schema')

    db_pool = await asyncpg.create_pool(**cfg)
    return db_pool


async def fetchrow(query: str, *args) -> asyncpg.Record:
    """Execute an arbitrary SELECT statement and return the first row
    Parameters
    ----------
        query: string
            the query to execute
        *args
            replacements if the query contains a prepared statement

    Returns
    -------
        asyncpg.Record
            the first row of results

    Raises
    ------
        errors.NotFoundException
            if no data was found
        any asyncpg errors encountered
    """
    async with db.acquire() as conn:
        res = await conn.fetchrow(query, *args)
        if not res:
            # no data found, raise a custom error
            query = format_query(query, args)
            raise errors.NotFoundException(f'No values returned for query\n"{query}"')
    return res


async def fetch(query: str, *args) -> List[asyncpg.Record]:
    """Execute an arbitrary SELECT statement and return all rows
    Parameters
    ----------
        query: string
            the query to execute
        *args
            replacements if the query contains a prepared statement

    Returns
    -------
        list of asyncpg.Record
            the results

    Raises
    ------
        errors.NotFoundException
            if no data was found
        any asyncpg errors encountered
    """
    async with db.acquire() as conn:
        res = await conn.fetch(query, *args)
    if not res:
        # no data found, raise a custom error
        query = format_query(query, args)
        raise errors.NotFoundException(f'No values returned for query\n"{query}"')

    return res


async def execute(query: str, *args) -> str:
    """execute an arbitrary non-SELECT-statement and return the database response
    Parameters
    ----------
        query: string
            the query to execute
        *args
            replacements if the query contains a prepared statement

    Returns
    -------
        string
            the database response

    Raises
    ------
        any asyncpg errors encountered
    """

    async with db.acquire() as conn:
        if args and isinstance(args[0], (list, tuple)):
            res = await conn.executemany(query, *args)
        elif args:
            res = await conn.execute(query, *args)
        else:
            res = await conn.execute(query)
    return res

cfg = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}
db: asyncpg.Pool = asyncio.get_event_loop().run_until_complete(create_connection(cfg))
