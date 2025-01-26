from apscheduler import CoalescePolicy, ConflictPolicy
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()

import logging
from logging import handlers
import os
import time
import traceback

import asyncio
import asyncpg
import coc
import discord
import datetime
from discord import Option

import nest_asyncio
nest_asyncio.apply()

import sys

from utils.independent.automation import scheduler
from utils.independent.webhook_error_handler import WebhookHandler
from utils import db, errors
from utils.independent import permissions as perm

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

logger = logging.getLogger('ct')

file_handler = logging.handlers.TimedRotatingFileHandler(filename=f"Logs/at_bot", when='MIDNIGHT', encoding='utf-8', backupCount=0,
                                                         utc=False)
file_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s-%(name)s:\n'
                                            '%(module)s - %(funcName)s - %(lineno)d:\n'
                                            '%(message)s\n\n'))
file_handler.namer = lambda name: name.replace(".log.", "_") + ".log"
logger.addHandler(file_handler)
if os.getenv('DISCORD_WEBHOOK_ERROR_URL'):
    webhook_handler = WebhookHandler(webhook_url=os.getenv('DISCORD_WEBHOOK_ERROR_URL'), level=logging.WARNING)
    webhook_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s-%(name)s:\n'
                                                   '%(module)s - %(funcName)s - %(lineno)d:\n'
                                                   '%(message)s\n\n'))
    logger.addHandler(webhook_handler)

if os.getenv('PRODUCTION', '0') == '0':
    # setup stream logger
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s-%(name)s:\n'
                                                   '%(module)s - %(funcName)s - %(lineno)d:\n'
                                                   '%(message)s\n\n'))
    logger.addHandler(stream_handler)

logger.setLevel(logging.ERROR)


from custom_dataclasses import bot

print('Finished imports')

# nest_asyncio.apply()
log_core = logging.getLogger(f"{logger.name}.core")
nest_asyncio.apply()


@bot.client.before_invoke
async def logging(ctx: discord.ApplicationContext):
    if 'py' not in ctx.command.qualified_name and 'sql' not in ctx.command.qualified_name and 'claim' not in ctx.command.qualified_name:
        await ctx.defer()
    else:
        return


@bot.client.event
async def on_ready():
    log_core.warning(f"Logged on as {bot.client.user.name}")
    print(f"Logged on as {bot.client.user.name}")
    from utils.tracking import get_leaderboard, get_tracked_players
    
    # TODO Update permanent routines
    
    try:
        #await scheduler.add_schedule(get_leaderboard, IntervalTrigger(minutes=1),
        #                             id='get_leaderboard', misfire_grace_time=60, conflict_policy=ConflictPolicy.replace,
        #                             max_running_jobs=1, coalesce=CoalescePolicy.earliest)
        await scheduler.add_schedule(get_tracked_players, IntervalTrigger(minutes=1),
                                     id='get_tracked_players', misfire_grace_time=60, conflict_policy=ConflictPolicy.replace,
                                     max_running_jobs=1, coalesce=CoalescePolicy.earliest)
    except Exception as e:
        bot.logger.error(f'Error scheduling player tracking\n{traceback.format_exc()}')
    await bot.client.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name=f"SlashCommands (/)"))


async def main():
    print('Logging in clash clients')
    await bot.login_clash_clients()
    # start discord bot
    async with scheduler:
        await scheduler.start_in_background()
        await bot.client.start(os.getenv('DISCORD_BOT_TOKEN'))


if __name__ == '__main__':
    print('Load cogs')
    if True:
        for filename in os.listdir('Cogs'):
            if filename.endswith(".py"):
                print(f"try to load {filename}")
                try:
                    bot.client.load_extension(f'Cogs.{filename[:-3]}')
                    print(f'Cogs.{filename[:-3]}')
                except Exception as e:
                    log_core.warning(f"{type(e)}: {e}")
                    print(f"Could not load {filename[:-3]}: {type(e)}\n\t\t{traceback.format_exc()}")
    print('loaded cogs')
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        time.sleep(0.1)
        raise e
    time.sleep(0.1)
