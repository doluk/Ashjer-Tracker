"""
Created on Wed Apr  8 17:57:10 2020
@author: Lukas
"""

###############################################################################
# Setup
###############################################################################

import traceback
import datetime


###############################################################################
# Constants and Classes
###############################################################################

# error and event codes
from discord.ext import commands

READ_DB_ERR = 1
WRITE_DB_ERR = 2
READ_GS_ERR = 3
WRITE_GS_ERR = 4
SC_API_ERR = 5
COS_API_ERR = 6
INTERNAL_ERR = 7
ORCHESTRATOR_ERR = 8

LOGON_EVT = 1
LOGOFF_EVT = 2
GUILD_JOIN_EVT = 3
GUILD_LEAVE_EVT = 4

# log templates
ERROR_TEMPLATE = '''\n
###############################################################################
# ERROR ({})
# In {}: {}
# {}: {}
#
{}'''
INFO_TEMPLATE = '''\n
###############################################################################
# INFO ({})
#
{}'''


class BotError(Exception):
    '''base class for all bot-related errors
    Parameters
    ----------
        code: integer
            the error code
        tb: string
            the error traceback
        timestamp: string
            the time of the error
        msg: discord.Message
            the discord message that caused the error
    Methods
    -------
        get_info
            return a logger.ErrorObject containing info on the error
    '''

    def __init__(self, code, msg):
        self.code = code
        self.tb = traceback.format_exc()
        self.timestamp = str(datetime.datetime.now())[:-7]
        self.msg = msg

    def get_info(self):
        '''return info on the error
        Returns
        -------
            logger.ErrorObject
        '''

        # parse msg (with scheduled operations, msg will list, not discord.Message)
        if isinstance(self.msg, list):
            p1, p2, p3, p4 = self.msg
        else:
            p1 = f'{self.msg.author.name}#{self.msg.author.discriminator}'
            p2 = f'{self.msg.guild.name} ({self.msg.guild.id})'
            p3 = f'{self.msg.channel.name} ({self.msg.channel.id})'
            p4 = self.msg.content

        # switch error code, efficient in terms of memory/performance
        if self.code == READ_DB_ERR:
            dc_msg = '❌ Something went wrong while reading from the database.'
        elif self.code == WRITE_DB_ERR:
            dc_msg = '❌ Something went wrong while writing to the database.'
        elif self.code == READ_GS_ERR:
            dc_msg = '❌ Something went wrong while reading from google sheets.'
        elif self.code == WRITE_GS_ERR:
            dc_msg = '❌ Something went wrong while writing to google sheets.'
        elif self.code == INTERNAL_ERR:
            dc_msg = '❌ Something went wrong while executing the request.'
        elif self.code == SC_API_ERR:
            dc_msg = '❌ Something went wrong while fetching data from Supercell.'
        elif self.code == COS_API_ERR:
            dc_msg = '❌ Something went wrong while fetching the player history.'
        elif self.code == ORCHESTRATOR_ERR:
            dc_msg = '❌ Something went wront while executing an automated task.'
        else:
            raise ValueError('Invalid error code')

        # build and return the error object
        return ErrorObject(self.timestamp, p1, p2, p3, p4, self.tb, dc_msg)


class ErrorObject:
    '''info object for BotError class instances
    Parameters
    ----------
        timestamp: string
            time of the event
        guild: string
            guild info
        channel: string
            channel info
        author: string
            command author info
        source_comd: string
            source command
        tb: string
            error traceback
        dc_msg: string
            discord error message
    Methods
    -------
        to_log
            convert the error object into a log string
        to_discord
            convert the error object into a discord response
    '''

    def __init__(self, timestamp, guild, channel, author, source_comd, tb, dc_msg):
        self.timestamp = timestamp
        self.guild = guild
        self.channel = channel
        self.author = author
        self.source_cmd = source_comd
        self.tb = tb
        self.dc_msg = dc_msg

    def to_log(self):
        '''convert the error object into a log string
        Returns
        -------
            log_string: string
                a string representation of the error
        '''

        return ERROR_TEMPLATE.format(self.timestamp, self.guild, self.channel,
                                     self.author, self.source_cmd, self.tb)

    def to_discord(self):
        '''convert the error object into a discord response
        Returns
        -------
            dc_msg: string
                a discord representation of the error
        '''

        return self.dc_msg + f' A log entry with id `{self.timestamp}` has been logged. ' + \
               'Please refer to this id when you report this issue to the devs.'


class BotEvent:
    '''base class for all bot-related events
    Parameters
    ----------
        code: integer
            the event code
        timestamp: string
            the time of the event
    Methods
    -------
        get_info
            return a logger.InfoObject containing info on the event
    '''

    def __init__(self, code, msg=None, guild=None):
        self.code = code
        self.timestamp = str(datetime.datetime.now())[:-7]
        self.msg = msg
        self.guild = guild

    def get_info(self):
        # switch error code, efficient in terms of memory/performance
        if self.code == LOGON_EVT:
            info = self.msg
        elif self.code == LOGOFF_EVT:
            info = f'Logging off as requested by {self.msg.author.name}#' + \
                   f'{self.msg.author.discriminator}'
        elif self.code == GUILD_JOIN_EVT:
            info = f'Joined {self.guild.name} ({self.guild.id})'
        elif self.code == GUILD_LEAVE_EVT:
            info = f'Left {self.guild.name} ({self.guild.id})'
        else:
            raise ValueError('Invalid event code')

        return InfoObject(self.timestamp, info)


class InfoObject:
    '''info object for BotEvent class instances
    Parameters
    ----------
        timestamp: string
            time of the event
        info: string
            info on the event
    Methods
    -------
        to_log
            convert the info object into a log string
    '''

    def __init__(self, timestamp, info):
        self.timestamp = timestamp
        self.info = info

    def to_log(self):
        '''convert the info object into a log string
        Returns
        -------
            log_string: string
                a string representation of the info
        '''

        return INFO_TEMPLATE.format(self.timestamp, self.info)


class NotFoundException(Exception):
    '''class to signal empty responses from the database or google sheets
    '''

    def __init__(self, reason=None):
        self.reason=reason


class BotExit(Exception):
    '''class to signal a handled exit from a bot subroutine'''

    def __init__(self, reason=None):
        self.reason = reason


class CustomConversionError(commands.CommandError):
    """
    Raised when a custom converter fails.
    """
    pass