
###############################################################################
# Imports
###############################################################################

import asyncio
import nest_asyncio
import re

from aiogoogle import Aiogoogle, HTTPError
from typing import Any, List, Optional, Union

from . import errors
# custom modules
from .gsheets_oauth2 import CredManager


nest_asyncio.apply()


###############################################################################
# Classes and Functions
###############################################################################

async def create_service():
    """create the connector service
    Returns
    -------
        the connector service
    """

    async with Aiogoogle() as aiogoogle:
        return await aiogoogle.discover('sheets', 'v4')


class APIClient:
    """a class handling the connection to the google sheets API
    """

    def __init__(self):
        self.credmgr = CredManager('credentials_google_ccn.json', 'token_google_ccn.json')
        self.service = asyncio.get_event_loop().run_until_complete(create_service())

    async def execute(self, request):
        """execute an API request
        """

        # maybe refresh credentials before requesting
        await self.credmgr.refresh()
        async with Aiogoogle(user_creds=self.credmgr.user_creds,
                             client_creds=self.credmgr.client_creds) as client:
            return await client.as_user(request)


async def handle_exception(ctx, exc: HTTPError, mode: Optional[str] = 'read', respond: Optional[bool] = False):
    """handle an exception occurred during an HTTP call to Google sheets
    Parameters
    ----------
        ctx: utils.commands.CommandContext
            the context the failed HTTP request originated from
        exc: HTTPError
            the HTTP error
        mode: string
            the operation mode that was attempted. One of 'read', 'write'
        respond: boolean
            whether to respond to the context. If False, send a regular message to the context channel
    """

    if exc.res.status_code == 403:
        err = 'I don\'t have the permission to read the Google Sheet and its responses'
    elif exc.res.status_code == 404:
        err = 'The Google Sheet id is invalid'
    elif exc.res.status_code in (503, 504):
        raise FileNotFoundError()
    else:
        if mode == 'write':
            err = 'Could not update the Google Sheet. The most common causes for this are:\n' \
                '- the sheet is in Excel (XLSX) format\n- one or more ranges in your setup are misspelled' \
                '\n- you are trying to write more data than a range can hold\n- you are trying to ' \
                'write to a protected cell'
        else:
            err = 'Could not parse the Google Sheet. The most common causes for this are:\n' \
                '- the sheet is in Excel (XLSX) format\n- one or more ranges in your setup are misspelled'
    if respond:
        return err
    else:
        return err


def maybe_extract_sheet_id(sheet_str: str) -> str:
    """attempts to extract the sheet id from a Google sheets URL
    Parameters
    ----------
        sheet_str: string
            the candidate string

    Returns
    -------
        string
            the sheet id, if found within the URL. Otherwise, returns the full string
    """

    pat = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    match = re.findall(pat, sheet_str)
    if match:
        return match[0]
    return sheet_str


async def read(sheet_id: str, ranges: Union[List[str], str], maj_dim: str = 'ROWS') -> list[Any]:
    """read one or multiple ranges from a spreadsheet
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the url
        ranges: string or list of string
            the range(s) to read from
        maj_dim: string
            the major dimension. One of 'ROWS' or 'COLUMNS'

    Returns
    -------
        list
            the values in the requested ranges

    Raises
    ------
        errors.NotFoundException
            if no values were found in the requested ranges
        any HTTP errors occurring during the connection to google sheets
    """

    # are we reading one or multiple ranges?
    batch_mode = isinstance(ranges, (list, tuple))

    # build request
    if batch_mode:
        request = gs.service.spreadsheets.values.batchGet(
            spreadsheetId=sheet_id, ranges=ranges, majorDimension=maj_dim)
    else:
        request = gs.service.spreadsheets.values.get(
            spreadsheetId=sheet_id, range=ranges, majorDimension=maj_dim)

    response = await gs.execute(request)

    # did we get values back?
    raise_404 = (batch_mode and not any('values' in r for r in response['valueRanges'])) or \
        (not batch_mode and 'values' not in response)
    if raise_404:
        raise errors.NotFoundException('Google Sheets found no values in the requested ranges')

    # extract and return values
    if batch_mode:
        return [r.get('values', []) for r in response['valueRanges']]
    else:
        return response['values']


async def write(sheet_id: str, ranges: Union[List[str], str], values: list,
                maj_dim: str = 'ROWS') -> None:
    """write to one or multiple ranges within a sheet
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the url
        ranges: string or list of string
            the range(s) to write to. The number of ranges must be equal to the number of values
        values: list
            the values to write to the sheet. The number of values must be equal to the number
            of ranges
        maj_dim: string
            the major dimension. One of 'ROWS' or 'COLUMNS'

    Raises
    ------
        any HTTP errors occurring during the connection to google sheets
    """

    # are we writing to one or multiple ranges?
    batch_mode = isinstance(ranges, (list, tuple))

    # build request
    if batch_mode:
        data = [{'range': rng, 'majorDimension': maj_dim, 'values': val}
                for rng, val in zip(ranges, values)]
        payload = {'data': data, 'valueInputOption': 'USER_ENTERED'}
        request = gs.service.spreadsheets.values.batchUpdate(
            spreadsheetId=sheet_id, json=payload)
    else:
        payload = {'range': ranges, 'majorDimension': maj_dim,
                   'values': [values]}
        request = gs.service.spreadsheets.values.update(
            spreadsheetId=sheet_id, range=ranges,
            valueInputOption='USER_ENTERED', json=payload)

    await gs.execute(request)


async def delete(sheet_id: str, ranges: Union[List[str], str]) -> None:
    """delete from one or multiple ranges within a sheet
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the url
        ranges: string or list of string
            the range(s) to clear

    Raises
    ------
        any HTTP errors occurring during the connection to google sheets
    """

    batch_mode = isinstance(ranges, (list, tuple))
    if batch_mode:
        payload = {'ranges': ranges}
        request = gs.service.spreadsheets.values.batchClear(
            spreadsheetId=sheet_id, json=payload)
    else:
        request = gs.service.spreadsheets.values.clear(
            spreadsheetId=sheet_id, range=ranges, json={})

    await gs.execute(request)


async def copy_tab(sheet_id: str, gid: int) -> None:
    """create a copy of a tab
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the URL
        gid: integer
            the grid id of the tab in the sheet

    Raises
    ------
        any HTTP errors occurring during the connection to google sheets
    """

    request = gs.service.spreadsheets.sheets.copyTo(
        spreadsheetId=sheet_id, sheetId=gid, json={'destinationSpreadsheetId': sheet_id})

    await gs.execute(request)


async def map_sheetname_gid(sheet_id: str) -> dict:
    """map the names of all sheets in a spreadsheet to their gid
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the URL

    Returns
    -------
        dict
            a mapping from names to gids

    Raises
    ------
        any HTTP errors occurring during the connection to google sheets
    """

    request = gs.service.spreadsheets.get(spreadsheetId=sheet_id)

    metadata = await gs.execute(request)

    mapping = {sheet.get('properties').get('title'): sheet.get('properties').get('sheetId')
               for sheet in metadata.get('sheets', '')}

    return mapping


async def rename_tabs(sheet_id: str, gids: list[int], names: list[str]) -> None:
    """rename a tab
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the URL
        gids: list of integer
            the grid ids of the tab in the sheet
        names: list of string
            the new names of the tab

    Raises
    ------
        any HTTP errors occurring during the connection to google sheets
    """

    payload = {
        'requests': [{'updateSheetProperties': {'properties': {'sheetId': gid, 'title': name}, 'fields': 'title'}}
                     for gid, name in zip(gids, names)]}
    request = gs.service.spreadsheets.batchUpdate(spreadsheetId=sheet_id, json=payload)

    await gs.execute(request)


async def rm_tab(sheet_id: str, gid: Union[List[int], int]) -> None:
    """delete a tab from a sheet
    Parameters
    ----------
        sheet_id: string
            the spreadsheet's id as within the URL
        gid: integer or list of integer
            the grid id of the tab(s) in the sheet

    Raises
    ------
        any HTTP errors occurring during the connection to google sheets
    """

    # ensure list
    if isinstance(gid, int):
        gid = [gid]

    payload = {'requests': [{'deleteSheet': {'sheetId': g}} for g in gid]}
    request = gs.service.spreadsheets.batchUpdate(spreadsheetId=sheet_id, json=payload)

    await gs.execute(request)


###############################################################################
# Instantiate gsheet service
###############################################################################

gs = APIClient()