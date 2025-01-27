from __future__ import annotations
import asyncio
import dataclasses
import datetime
import functools
import io
import json
import logging
import os.path
import pathlib
import re
import time
import traceback
from typing import Callable, Coroutine, Dict, List, Literal, Optional, Tuple, Type, Union
import shutil
import aiofiles
import aiohttp
import asyncpg
import coc.errors
import PIL.Image
from async_property import async_cached_property
import discord
from discord.ext import commands
from discord import Option
from dateutil import tz
from utils import db, errors
from utils.independent.automation import scheduler
from utils.independent.other import ExtendedEnum
from utils.independent.config import ConfigObj

logger = logging.getLogger(f'ct.{__name__}')
logger.setLevel(logging.WARNING)
LOG = log = logger


def largest_common_subset(sets: List[set]) -> set:
    """Get the largest common subset of a list of sets."""
    if not sets:
        return set()
    if len(sets) == 1:
        return sets[0]
    return set.intersection(*sets)



class Choice(discord.OptionChoice):
    """A class to represent a choice in a slash command option."""
    
    def __eq__(self, other):
        return self.name == other.name and self.value == other.value
    
    def __hash__(self):
        return hash((self.name, self.value))
    
    def __str__(self):
        return f"{self.name} ({self.value})"
    
    def __repr__(self):
        return f"Choice({self.name}, {self.value})"

class Bot:
    """Class to represent the Bot

    Attributes:
    -----------
        client: discord.commands.Bot
            the discord Bot client
        config: Config
            the config of the bot
        clash_client: coc.Client
            the coc.py-Client
        colors: ConfigObj
            the discord Colors of bot.config.embed_colors
        public_guilds: List[int]
            the List of guild ids for public commands (should be empty for making them global)
        ccn_related_guilds: List[int]
            the List of guild ids from GCC related Guilds
        dev_guilds: List[int]
            the list of guild ids for dev commands
        custom_emojis: ConfigObj
            the ConfigObj with the custom emoji's
    """
    
    def __init__(self):
        intents = discord.Intents().all()
        
        
        self.client = commands.Bot(intents=intents, case_insensitive=True, auto_sync_commands=True,
                                   default_command_contexts=[discord.InteractionContextType.guild],
                                   default_command_integration_types=[discord.IntegrationType.guild_install],
                                   owner_ids=[int(x) for x in os.getenv("PERMISSION_LEVEL_DEV", '').split(',') if x])
        
        self.clash_client = coc.EventsClient(key_names=os.getenv('COC_API_KEY_NAME'), raw_attribute=True, key_count=10)
        self.cogs_choices = Option(str, 'Cogs', choices=[x[:-3] for x in os.listdir('Cogs') if x.endswith(".py")])
        
        self.related_guilds = [int(x.strip()) for x in os.getenv('PERMISSION_TRUSTED_SERVERS', '').split(',') if x.strip().isnumeric()]
        self.dev_guilds = self.related_guilds
        self.logger = logger

    async def login_clash_clients(self):
        await self.clash_client.login(email=os.getenv('COC_API_EMAIL'), password=os.getenv('COC_API_PASSWORD'))


print('Creating bot')
bot = Bot()

datetime_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H', '%Y-%m-%d',
                    '%Y-%m-%d %H:%M:%S %Z', '%Y-%m-%d %H:%M %Z', '%Y-%m-%d %H %Z', '%Y-%m-%d %Z',
                    '%Y%m%dT%H%M%S', '%Y.%m.%dT%H%M',
                    '%Y.%m.%dT%H', '%Y.%m.%d', '%y.%m.%d', '%d.%m.%Y %H:%M:%S', '%d.%m.%Y %H:%M', '%d.%m.%Y %H',
                    '%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y %H', '%d/%m/%Y',
                    '%d/%m/%y', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m/%d/%Y %H', '%m/%d/%Y', '%m/%d', '%m/%d/%y',
                    '%H:%M:%S', '%H:%M', '%H']

standard_duration_re = re.compile(
        r"^"
        r"(?:(?P<days>-?\d+) (days?, )?)?"
        r"(?P<sign>-?)"
        r"((?:(?P<hours>\d+):)(?=\d+:\d+))?"
        r"(?:(?P<minutes>\d+):)?"
        r"(?P<seconds>\d+)"
        r"(?:[.,](?P<microseconds>\d{1,6})\d{0,6})?"
        r"$"
)

# Support the sections of ISO 8601 date representation that are accepted by
# timedelta
iso8601_duration_re = re.compile(
        r"^(?P<sign>[-+]?)"
        r"P"
        r"(?:(?P<days>\d+([.,]\d+)?)D)?"
        r"(?:T"
        r"(?:(?P<hours>\d+([.,]\d+)?)H)?"
        r"(?:(?P<minutes>\d+([.,]\d+)?)M)?"
        r"(?:(?P<seconds>\d+([.,]\d+)?)S)?"
        r")?"
        r"$"
)

duration_re = re.compile(r"(?:(?P<days>\d+([.,]\d+)?)(?:days|day|d))?[: ]?T?"
                         r"(?:(?P<hours>\d+([.,]\d+)?)(?:hours|hour|h))?[: ]?"
                         r"(?:(?P<minutes>\d([.,]\d+)?)(?:m|min|minutes|mins|minute))?[: ]?"
                         r"(?:(?P<seconds>\d+([.,]\d+)?)(?:s|sec|second|seconds))?", re.IGNORECASE)

# Support PostgreSQL's day-time interval format, e.g. "3 days 04:05:06". The
# year-month and mixed intervals cannot be converted to a timedelta and thus
# aren't accepted.
postgres_interval_re = re.compile(
        r"^"
        r"(?:(?P<days>-?\d+) (days? ?))?"
        r"(?:(?P<sign>[-+])?"
        r"(?P<hours>\d+):"
        r"(?P<minutes>\d\d):"
        r"(?P<seconds>\d\d)"
        r"(?:\.(?P<microseconds>\d{1,6}))?"
        r")?$"
)

class DiscordTimestamps(ExtendedEnum):
    default = ''
    short_time = 't'  # 3:01 PM
    long_time = 'T'  # 3:01:34 PM
    short_date = 'd'  # 20/04/2021
    long_date = 'D'  # November 28, 2018
    short_timestamp = 'f'  # 20/04/2021 3:01 PM
    long_timestamp = 'F'  # Wednesday, November 28, 2018 3:01 PM
    relative = 'R'  # 2 years ago
    timestamp = 'c'  # 20/04/2021 3:01:34 PM

class Datetime(datetime.datetime):
    """An utils class to convert datetime strings to datetime objects"""
    dc_styles = DiscordTimestamps
    
    def __new__(cls, *args, **kwargs):
        if not args or len(args) < 3:
            now = datetime.datetime.now(datetime.timezone.utc)
            args = [now.year, now.month, now.day]
        return super().__new__(cls, *args, **kwargs)
    
    @classmethod
    async def convert(cls, ctx: discord.ApplicationContext, argument: str):
        """Convert a string to a Datetime object"""
        argument = argument.strip()
        try:
            temp = cls.by_input(argument)
            if not temp:
                raise errors.CustomConversionError("Invalid datetime format")
            return temp
        except ValueError:
            raise errors.CustomConversionError("Invalid datetime format")
        except Exception:
            LOG.error(traceback.format_exc() + f'\n{argument=}')
            raise errors.CustomConversionError("Invalid datetime format")
    
    @classmethod
    def by_input(cls, input_value, format_str: Optional[str] = None, allow_relative: Optional[bool] = True) -> Optional['Datetime']:
        import _strptime
        return_value = None
        if format_str is not None:
            try:
                return_value = _strptime._strptime_datetime(cls, input_value, format_str)
            except ValueError:
                pass
        for f in datetime_formats:
            try:
                return_value = _strptime._strptime_datetime(cls, input_value, f)
            except ValueError:
                pass
            except Exception:
                traceback.print_exc()
        if not return_value and input_value:
            try:
                return_value = cls.fromtimestamp(int(input_value), tz=datetime.timezone.utc)
            except ValueError:
                pass
        if allow_relative and not return_value and input_value:
            # try as relative time
            return_value = cls.by_timedelta(input_value)
        
        if return_value:
            return return_value.replace(tzinfo=datetime.timezone.utc)
        
        raise ValueError(f"Invalid format string '{format_str}'")
    
    @classmethod
    def by_dt(cls, dt: datetime.datetime) -> Optional['Datetime']:
        """
        Creates a new Datetime object based on the given datetime object.

        :param dt: The datetime object to create the Datetime object from.
        :type dt: datetime.datetime
        :return: A new Datetime object based on the given datetime object, or None if dt is None.
        :rtype: Optional['Datetime']
        """
        if dt is None:
            return None
        return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)
    
    @property
    def string(self):
        return self.strftime('%Y-%m-%d %H:%M')
    
    @staticmethod
    def parse_duration(value):
        """Parse a duration string and return a datetime.timedelta.

        The preferred format for durations in Django is '%d %H:%M:%S.%f'.

        Also supports ISO 8601 representation and PostgreSQL's day-time interval
        format.
        """
        match = (
                standard_duration_re.match(value)
                or iso8601_duration_re.match(value)
                or postgres_interval_re.match(value)
                or duration_re.match(value)
        )
        if match:
            kw = match.groupdict()
            sign = -1 if kw.pop("sign", "+") == "-" else 1
            if kw.get("microseconds"):
                kw["microseconds"] = kw["microseconds"].ljust(6, "0")
            kw = {k: float(v.replace(",", ".")) for k, v in kw.items() if v is not None}
            days = datetime.timedelta(kw.pop("days", 0.0) or 0.0)
            if match.re == iso8601_duration_re:
                days *= sign
            return days + sign * datetime.timedelta(**kw)
    
    @classmethod
    def by_timedelta(cls, td: Union[datetime.timedelta, str]) -> Optional['Datetime']:
        """
        Creates a new Datetime object based on the given timedelta object.

        :param td: The timedelta object to create the Datetime object from.
        :type td: Union[datetime.timedelta, str]
        :return: A new Datetime object based on the given timedelta object, or None if td is None.
        :rtype: Optional['Datetime']
        """
        if td is None or not isinstance(td, (datetime.timedelta, str)):
            return None
        if isinstance(td, str):
            td = cls.parse_duration(td)
        if not isinstance(td, datetime.timedelta):
            return None
        return cls.now(tz=datetime.timezone.utc) + td
    
    def to_discord(self,
                   style: Union[
                       DiscordTimestamps,
                       Literal['t', 'T', 'f', 'F', 'd', 'D', 'R', 'c', ''],
                   ] = DiscordTimestamps.timestamp):
        if isinstance(style, str):
            style = self.dc_styles(style)
        if style == self.dc_styles.default:
            return f'<t:{self.t_stamp}>'
        elif style == self.dc_styles.timestamp:
            return (f'{self.to_discord(self.dc_styles.short_date)} '
                    f'{self.to_discord(self.dc_styles.long_time)}')
        else:
            return f'<t:{self.t_stamp}:{style.value}>'
    
    @property
    def t_stamp(self):
        """Return the integer timestamp of the datetime object in UTC"""
        return int(self.replace(tzinfo=datetime.timezone.utc).timestamp())
    
    @classmethod
    def option(cls,
               name: str = 'datetime',
               description: str = "datetime",
               required: bool = True,
               autocomplete: Union[Callable, Coroutine, None, Literal['str']] = 'cls',
               default: Optional[Union[str, int]] = None,
               **kwargs):
        """A decorator that can be used instead of typehinting :class:`.Option`."""
        if autocomplete == 'cls':
            autocomplete = cls.autocomplete
        
        if not required:
            kwargs['default'] = default
        
        def decorator(func):
            resolved_name = kwargs.pop("parameter_name", None) or name
            
            func.__annotations__[resolved_name] = discord.Option(cls, name=name, description=description,
                                                                 autocomplete=autocomplete,
                                                                 required=required, **kwargs)
            return func
        
        return decorator
    
    @staticmethod
    async def autocomplete(ctx: discord.AutocompleteContext,
                           allow_relative: Optional[bool] = True,
                           suggestions: Literal[
                               'relative',
                               'month start',
                               'month end',
                               'week start',
                               'week end',
                               'day start',
                               'day end',
                               'hour start',
                               'hour end',
                               None] = 'relative'):
        """Autocomplete a Datetime"""
        if ctx.value:
            try:
                current_value = Datetime.by_input(ctx.value)
                current_value = current_value.replace(tzinfo=datetime.timezone.utc)
            except TypeError:
                current_value = None
            except ValueError:
                current_value = None
            if current_value:
                # Create a list of choices same time but in the most common timezones
                choices = []
                for tzinfo in ['UTC',
                               'Asia/Kolkata',
                               'Asia/Shanghai',
                               'Europe/Berlin',
                               'Europe/Lisbon',
                               'US/Eastern',
                               'US/Central',
                               'US/Pacific',
                               ]:
                    timezone = tz.gettz(tzinfo)
                    choices.append(Choice(name=current_value.astimezone(timezone).strftime('%Y-%m-%d %H:%M %Z'),
                                          value=current_value.strftime('%Y-%m-%d %H:%M')))
                return choices
            try:
                return [Choice(name=ctx.value, value=ctx.value)]
            except Exception:
                pass
        else:
            if suggestions is not None:
                return Datetime.suggest_options(suggestions)
            return []
    
    @classmethod
    def autocomp(cls,
                 allow_relative: Optional[bool] = True,
                 suggestions: Literal[
                     'relative',
                     'month start',
                     'month end',
                     'week start',
                     'week end',
                     'day start',
                     'day end',
                     'hour start',
                     'hour end',
                     None] = 'relative'):
        """Autocomplete a Datetime"""
        kwargs = {
        }
        
        if allow_relative is not None:
            kwargs['allow_relative'] = allow_relative
        if suggestions is not None:
            kwargs['suggestions'] = suggestions
        
        func = functools.partial(cls.autocomplete, **kwargs)
        func._is_coroutine = object()
        return func
    
    @staticmethod
    def suggest_options(suggestions: Literal[
        'relative',
        'month start',
        'month end',
        'week start',
        'week end',
        'day start',
        'day end',
        'hour start',
        'hour end',
        None] = 'relative'):
        """Create a List of autocomplete choices for some generic cases"""
        choices = []
        now = datetime.datetime.now(datetime.timezone.utc)
        if suggestions == 'relative':
            # create in 24 hours, 36 hours, 48 hours, 60 hours, 72 hours
            hours_24 = now + datetime.timedelta(hours=24)
            hours_36 = now + datetime.timedelta(hours=36)
            hours_48 = now + datetime.timedelta(hours=48)
            hours_60 = now + datetime.timedelta(hours=60)
            hours_72 = now + datetime.timedelta(hours=72)
            # create in 4, 5, 6, 7, 8, 9, 10, 14 days
            days_4 = now + datetime.timedelta(days=4)
            days_5 = now + datetime.timedelta(days=5)
            days_6 = now + datetime.timedelta(days=6)
            days_7 = now + datetime.timedelta(days=7)
            days_8 = now + datetime.timedelta(days=8)
            days_9 = now + datetime.timedelta(days=9)
            days_10 = now + datetime.timedelta(days=10)
            days_14 = now + datetime.timedelta(days=14)
            choices.append(Choice(name="In 1 day", value=hours_24.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 1.5 days", value=hours_36.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 2 days", value=hours_48.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 2.5 days", value=hours_60.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 3 days", value=hours_72.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 4 days", value=days_4.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 5 days", value=days_5.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 6 days", value=days_6.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 7 days", value=days_7.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 8 days", value=days_8.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 9 days", value=days_9.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 10 days", value=days_10.strftime('%Y-%m-%d %H:%M')))
            choices.append(Choice(name="In 14 days", value=days_14.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'month start':
            # create 11 choices for the start of the month first the current month and then going back 10 months
            month_start = now.replace(day=2, hour=0, minute=0, second=0, microsecond=0)
            for i in range(11):
                month_start = month_start - datetime.timedelta(days=1)
                month_start = month_start.replace(day=1)
                choices.append(Choice(name='Begin of ' + month_start.strftime('%b %y'),
                                      value=month_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'month end':
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=32)
            for i in range(11):
                month_start = month_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                month_start = month_start - datetime.timedelta(seconds=1)
                choices.append(Choice(name='End of ' + month_start.strftime('%b %y'),
                                      value=month_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'week start':
            # create 20 choices for the start of the week first the current week and then going back 19 weeks
            week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            for i in range(20):
                week_start = week_start - datetime.timedelta(days=week_start.weekday())
                choices.append(Choice(name='Begin of week ' + week_start.strftime('%W %y'),
                                      value=week_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'week end':
            week_start = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=7)
            for i in range(20):
                week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=week_start.weekday())
                week_start = week_start - datetime.timedelta(seconds=1)
                choices.append(Choice(name='End of week ' + week_start.strftime('%W %y'),
                                      value=week_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'day start':
            # create 24 choices for the start of the day first the current day and then going back 23 days
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
            for i in range(24):
                day_start = day_start - datetime.timedelta(days=1)
                choices.append(Choice(name='Begin of ' + day_start.strftime('%d %b %y'),
                                      value=day_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'day end':
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
            for i in range(24):
                day_start = day_start.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(seconds=1)
                choices.append(Choice(name='End of ' + day_start.strftime('%d %b %y'),
                                      value=day_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'hour start':
            # create 24 choices for the start of the hour first the current hour and then going back 23 hours
            hour_start = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
            for i in range(24):
                hour_start = hour_start - datetime.timedelta(hours=1)
                choices.append(Choice(name=hour_start.strftime('%Y-%m-%d %H:%M'),
                                      value=hour_start.strftime('%Y-%m-%d %H:%M')))
        elif suggestions == 'hour end':
            hour_start = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
            for i in range(24):
                hour_start = hour_start.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(seconds=1)
                choices.append(Choice(name=hour_start.strftime('%Y-%m-%d %H:%M'),
                                      value=hour_start.strftime('%Y-%m-%d %H:%M')))
        return choices


class Account(object):
    """

    class Account(object):
        """
    table = 'accounts'
    key = 'account_tag'
    id: Optional[str]
    
    def __init__(self, record: Union[asyncpg.Record, dict, None] = None):
        """Initialize the account object"""
        if record is None:
            record = {}
        self.tag = record.get('account_tag')
        self.id = self.tag
        self.name = record.get('account_name')
        self.tracking = record.get('tracking_active')
        self.last_updated = Datetime.by_dt(record.get('last_updated'))
    
    async def refresh(self):
        """Refresh the worker object from the database"""
        record = await db.fetchrow("SELECT * FROM accounts WHERE account_tag = $1", self.tag)
        self.__init__(record)
        return self
    
    @classmethod
    async def by_id(cls, id_in: str) -> 'Account':
        """Get an Account object by its id"""
        id_in = coc.utils.correct_tag(str(id_in))
        try:
            record = await db.fetchrow("SELECT * FROM accounts a WHERE account_tag = $1", id_in)
            worker = cls(record)
            return worker
        except errors.NotFoundException:
            raise errors.CustomConversionError(f"Account with id \"{id_in}\" not found")
    
    @classmethod
    async def by_meta(cls, meta: str) -> 'Account':
        """Get an account object by its meta"""
        try:
            records = await db.fetch("SELECT * from accounts a "
                                     "WHERE account_name = $1", meta)
            if len(records) > 1:
                raise errors.CustomConversionError(f"Multiple accounts with meta \"{meta}\" found")
            item = cls(records[0])
            return item
        except errors.NotFoundException:
            pass
        raise errors.CustomConversionError(f"Account with meta \"{meta}\" not found")
    
    @classmethod
    async def convert(cls,
                      ctx: Union[discord.ApplicationContext, discord.AutocompleteContext],
                      argument: Union[str, int]):
        """Convert an account name to an account object"""
        obj = None
        try:
            obj = await cls.by_id(argument)
        except (errors.NotFoundException, errors.CustomConversionError):
            pass
        if not obj:
            try:
                obj = await cls.by_meta(argument)
            except (errors.NotFoundException, errors.CustomConversionError):
                pass
        if obj:
            return obj
        else:
            raise errors.CustomConversionError(f"No account for `{str(argument)}` found. Please check your input.")
    
    @classmethod
    def autocomp(cls,
                 filter_active: Optional[bool] = None,
                 filter_player: Optional[bool] = None,
                 force_filter: Optional[bool] = None):
        """Autocomplete a worker name"""
        kwargs = {
        }
        if filter_active is not None:
            kwargs['filter_active'] = filter_active
        if filter_player is not None:
            kwargs['filter_player'] = filter_player
        if force_filter is not None:
            kwargs['force_filter'] = force_filter
        func = functools.partial(cls.autocomplete, **kwargs)
        func._is_coroutine = object()
        return func
    
    @staticmethod
    async def autocomplete(ctx: Union[discord.ApplicationContext, discord.AutocompleteContext],
                           filter_active: Optional[bool] = True,
                           filter_player: Optional[bool] = True,
                           force_filter: Optional[bool] = False):
        """Autocomplete an account name"""
        sets = []
        final_set = {}
        value_as_tag = coc.utils.correct_tag(ctx.value or "") + "%"
        if filter_player and 'player' in ctx.options:
            option = ctx.options['player']
            try:
                item = await Player.convert(ctx, option)
            except errors.CustomConversionError:
                item = None
            if item:
                try:
                    workers = await db.fetch(
                            "SELECT a.account_tag, account_name from accounts a "
                            "left outer join player_accounts pa on pa.account_tag = a.account_tag "
                            "left outer join players p on p.player_id = pa.player_id "
                            "where (CASE "
                            "WHEN length($1) = 0  "
                            "THEN true "
                            "WHEN length($1) > 2  "
                            "THEN similarity($1, account_name) > 0.08 and "
                            "levenshtein(lower($1), lower(account_name), 1, 4, 3) < length("
                            "account_name) "
                            "ELSE similarity($1, account_name) > 0 or "
                            "levenshtein(lower($1), lower(account_name), 1, 4, 3) < length( "
                            "account_name) END or a.account_tag ilike $4) and ($2::bool is null or "
                            "a.tracking_active = $2) and p.player_id = $3 "
                            "ORDER BY similarity(account_name, $1) DESC,"
                            " ts_rank(a.ts, plainto_tsquery('simple', $1)) DESC,"
                            " levenshtein(lower($1), lower(account_name), 1, 4, 3), account_name LIMIT 25",
                            ctx.value or "", filter_active, item.id, value_as_tag)
                    temp = {Choice(name=f"{item.get('account_name')} [{item.get('account_tag')}]", 
                                   value=str(item.get('account_tag')))
                            for item in workers}
                    sets.append(temp)
                except errors.NotFoundException:
                    pass
        final_set = largest_common_subset(sets)
        if (final_set and len(final_set) > 0) or force_filter:
            return list(final_set)
        
        try:
            workers = await db.fetch(
                    "SELECT a.account_tag, account_name from accounts a "
                    "where (CASE "
                    "WHEN length($1) = 0 "
                    "THEN true "
                    "WHEN length($1) > 2 "
                    "THEN similarity($1, account_name) > 0.08 and "
                    "levenshtein(lower($1), lower(account_name), 1, 4, 3) < length(account_name)"
                    "ELSE similarity($1, account_name) > 0 or "
                    "levenshtein(lower($1), lower(account_name), 1, 4, 3) < length("
                    "account_name) END or account_tag ilike $3) and ($2::bool is null or a.tracking_active = "
                    "$2) "
                    "ORDER BY similarity(account_name, $1) DESC, "
                    "ts_rank(a.ts, plainto_tsquery('simple', $1)) DESC,"
                    " levenshtein(lower($1), lower(account_name), 1, 4, 3), account_name LIMIT 25",
                    ctx.value or "", filter_active, value_as_tag)
            matches = {}
            for item in workers:
                matches[item.get('account_tag')] = Choice(name=f"{item.get('account_name')} [{item.get('account_tag')}]", 
                                   value=str(item.get('account_tag')))
            return list(matches.values())
        except errors.NotFoundException:
            return []
    
    @staticmethod
    def slash_option(cls, required: bool = True) -> 'Account':
        """Returns a slash command option"""
        return discord.Option(cls,
                              description="Account of interest",
                              name="account",
                              required=required,
                              autocomplete=cls.autocomplete)
    
    @classmethod
    def option(cls,
               name: str = 'account',
               description: str = "Account of interest",
               required: bool = True,
               autocomplete: Union[Callable, Coroutine, None, Literal['str']] = 'cls',
               default: Optional[Union[str, int]] = None,
               **kwargs):
        """A decorator that can be used instead of typehinting :class:`.Option`."""
        if autocomplete == 'cls':
            autocomplete = cls.autocomplete
        if not required:
            kwargs['default'] = default
        
        def decorator(func):
            resolved_name = kwargs.pop("parameter_name", None) or name
            
            func.__annotations__[resolved_name] = discord.Option(cls, name=name, description=description,
                                                                 required=required, autocomplete=autocomplete,
                                                                 **kwargs)
            return func
        
        return decorator
    
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return f"Account({self.id}, {self.name})"
    
    def __eq__(self, other):
        return self.id == other.id
    
    def __hash__(self):
        return hash(self.id)
    
    def __bool__(self):
        return self.id is not None
    
    def __int__(self):
        return self.id
    
    
    async def get_player(self) -> Union['Player', None]:
        """
        Attempts to fetch a Discord channel or thread using the given channel ID.
        Returns the channel object if it's a TextChannel or Thread, otherwise returns None.

        :return: Discord channel or thread if found, None otherwise
        """
        try:
            [player_id] = await db.fetchrow('SELECT player_id FROM player_accounts WHERE account_tag = $1', self.tag)
        except errors.NotFoundException:
            return None
        except Exception:
            return None
        return await Player.by_id(player_id)




class Player(object):
    """

    class Player(object):
        """
    table = 'players'
    key = 'player_id'
    id: Optional[int]
    
    def __init__(self, record: Union[asyncpg.Record, dict, None] = None):
        """Initialize the player object"""
        if record is None:
            record = {}
        self.id = record.get('player_id')
        self.name = record.get('player_name')
        self.discord_id = record.get('player_discord_id')
    
    async def refresh(self):
        """Refresh the player object from the database"""
        record = await db.fetchrow("SELECT * FROM players WHERE player_id = $1", self.id)
        self.__init__(record)
        return self
    
    @classmethod
    async def by_id(cls, id_in: int) -> 'Player':
        """Get a Player object by its id"""
        try:
            record = await db.fetchrow("SELECT * FROM players WHERE player_id = $1", id_in)
            player = cls(record)
            return player
        except errors.NotFoundException:
            raise errors.CustomConversionError(f"Player with id \"{id_in}\" not found")
    
    @classmethod
    async def by_meta(cls, meta: str) -> 'Player':
        """Get a Player object by its meta"""
        try:
            records = await db.fetch("SELECT * from players w "
                                     "WHERE player_name = $1", meta)
            if len(records) > 1:
                raise errors.CustomConversionError(f"Multiple players with meta \"{meta}\" found")
            item = cls(records[0])
            return item
        except errors.NotFoundException:
            pass
        raise errors.CustomConversionError(f"Player with meta \"{meta}\" not found")
    
    @classmethod
    async def convert(cls,
                      ctx: Union[discord.ApplicationContext, discord.AutocompleteContext],
                      argument: Union[str, int]):
        """Convert a player input to a Player object"""
        a_in = None
        obj = None
        try:
            a_in = int(argument)
        except (TypeError, ValueError):
            pass
        if a_in:
            try:
                obj = await cls.by_id(a_in)
            except (errors.NotFoundException, errors.CustomConversionError):
                pass
        if not obj:
            try:
                obj = await cls.by_meta(argument)
            except (errors.NotFoundException, errors.CustomConversionError):
                pass
        if obj:
            return obj
        else:
            raise errors.CustomConversionError(f"No player for `{str(argument)}` found. Please check your input.")
    
    @classmethod
    def autocomp(cls,
                 filter_account: Optional[bool] = None,
                 force_filter: Optional[bool] = None):
        """Autocomplete a player name"""
        kwargs = {
        }
        if filter_account is not None:
            kwargs['filter_account'] = filter_account
        if force_filter is not None:
            kwargs['force_filter'] = force_filter
        func = functools.partial(cls.autocomplete, **kwargs)
        func._is_coroutine = object()
        return func
    
    @staticmethod
    async def autocomplete(ctx: Union[discord.ApplicationContext, discord.AutocompleteContext],
                           filter_account: Optional[bool] = None,
                           force_filter: Optional[bool] = False):
        """Autocomplete a player name"""
        sets = []
        final_set = {}
        if filter_account and 'account' in ctx.options:
            option = ctx.options['account']
            try:
                account = await Account.convert(ctx, option)
            except errors.CustomConversionError:
                account = None
            if account:
                try:
                    players = await db.fetch(
                            "SELECT p.player_id, player_name from players p "
                            "left outer join player_accounts pa on p.player_id = pa.player_id "
                            "left outer join accounts a on a.account_tag = pa.account_tag "
                            "where CASE "
                            "WHEN length($1) = 0  "
                            "THEN true "
                            "WHEN length($1) > 2  "
                            "THEN similarity($1, player_name) > 0.08 and "
                            "levenshtein(lower($1), lower(player_name), 1, 4, 3) < length("
                            "player_name) "
                            "ELSE similarity($1, player_name) > 0 or "
                            "levenshtein(lower($1), lower(player_name), 1, 4, 3) < length( "
                            "player_name) END and a.account_tag = $3 "
                            "ORDER BY similarity(player_name, $1) DESC,"
                            " ts_rank(p.ts, plainto_tsquery('simple', $1)) DESC,"
                            " levenshtein(lower($1), lower(player_name), 1, 4, 3), player_name LIMIT 25",
                            ctx.value or "", account.id)
                    temp = {Choice(name=item.get('player_name'), value=str(item.get('player_id')))
                            for item in players}
                    sets.append(temp)
                except errors.NotFoundException:
                    pass
        final_set = largest_common_subset(sets)
        if (final_set and len(final_set) > 0) or force_filter:
            return list(final_set)
        
        try:
            players = await db.fetch(
                    "SELECT p.player_id, player_name from players p "
                    "where CASE "
                    "WHEN length($1) = 0 "
                    "THEN true "
                    "WHEN length($1) > 2 "
                    "THEN similarity($1, player_name) > 0.08 and "
                    "levenshtein(lower($1), lower(player_name), 1, 4, 3) < length(player_name)"
                    "ELSE similarity($1, player_name) > 0 or "
                    "levenshtein(lower($1), lower(player_name), 1, 4, 3) < length("
                    "player_name) END "
                    "ORDER BY similarity(player_name, $1) DESC, "
                    "ts_rank(p.ts, plainto_tsquery('simple', $1)) DESC,"
                    " levenshtein(lower($1), lower(player_name), 1, 4, 3), player_name LIMIT 25",
                    ctx.value or "")
            matches = {}
            for item in players:
                matches[item.get('player_id')] = Choice(name=item.get('player_name'),
                                                        value=str(item.get('player_id')))
            return list(matches.values())
        except errors.NotFoundException:
            return []
    
    @staticmethod
    def slash_option(cls, required: bool = True) -> 'Player':
        """Returns a slash command option"""
        return discord.Option(cls,
                              description="Player of interest",
                              name="player",
                              required=required,
                              autocomplete=cls.autocomplete)
    
    @classmethod
    def option(cls,
               name: str = 'player',
               description: str = "Player of interest",
               required: bool = True,
               autocomplete: Union[Callable, Coroutine, None, Literal['str']] = 'cls',
               default: Optional[Union[str, int]] = None,
               **kwargs):
        """A decorator that can be used instead of typehinting :class:`.Option`."""
        if autocomplete == 'cls':
            autocomplete = cls.autocomplete
        if not required:
            kwargs['default'] = default
        
        def decorator(func):
            resolved_name = kwargs.pop("parameter_name", None) or name
            
            func.__annotations__[resolved_name] = discord.Option(cls, name=name, description=description,
                                                                 required=required, autocomplete=autocomplete,
                                                                 **kwargs)
            return func
        
        return decorator
    
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return f"player({self.id}, {self.name})"
    
    def __eq__(self, other):
        return self.id == other.id
    
    def __hash__(self):
        return hash(self.id)
    
    def __bool__(self):
        return self.id is not None
    
    def __int__(self):
        return self.id
    
    async def get_accounts(self, active: Optional[bool] = None) -> List[asyncpg.Record]:
        """Get all players that are currently supplying this item"""
        try:
            records = await db.fetch("SELECT a.* FROM player_accounts pa "
                                     "JOIN accounts a on a.account_tag = pa.account_tag "
                                     "WHERE pa.player_id = $1 and ($2::bool is null or a.tracking_active = $2) "
                                     "ORDER BY account_name, a.account_tag", self.id, active)
            return records
        except errors.NotFoundException:
            return []

    async def get_user(self) -> Optional[discord.User]:
        """
        Fetches and returns a Discord user based on the discord_id property of the instance.

        :return: The Discord user object if found, otherwise None.
        :rtype: Optional[discord.User]
        """
        try:
            user = await bot.client.fetch_user(self.discord_id)
            return user
        except discord.NotFound:
            return None
        except discord.Forbidden:
            return None

print(f'{__file__} imported at {datetime.datetime.now(datetime.timezone.utc)}')
