import io
import logging
import os
import traceback

import aiohttp
import coc.errors
import discord
from discord.ext import commands
from datetime import datetime

import PIL

from custom_dataclasses import bot, logger
from utils import errors
from utils.independent import permissions as perm


log_error = logging.getLogger(f"{logger.name}.error")
log_error.setLevel(logging.ERROR)

class error_handler(commands.Cog):
    def __init__(self, client):
        self.client = client

    @commands.Cog.listener()
    async def on_application_command_error(self, ctx: discord.ApplicationContext, error):
        """The event triggered when an error is raised while invoking a command.
        Parameters
        ------------
        ctx: commands.Context
            The context used for command invocation.
        error: commands.CommandError
            The Exception raised.
        """
        # This prevents any commands with local handlers being handled here in on_command_error.


        if hasattr(ctx.command, 'on_error'):
            return

        # This prevents any cogs with an overwritten cog_command_error being handled here.
        cog = ctx.cog
        if cog and cog._get_overridden_method(cog.cog_command_error) is not None:
            return

        ignored = (perm.CommandRestrictionError,perm.ChannelLockedError)
        no_webhook = (errors.CustomConversionError, commands.errors.CheckFailure,
                      commands.errors.MissingRequiredArgument,
                      commands.errors.BadUnionArgument, commands.BadArgument, PIL.UnidentifiedImageError,
                      aiohttp.client_exceptions.InvalidURL,discord.errors.NotFound)
        handled = (commands.DisabledCommand, commands.errors.CommandNotFound)
        quote_errors = (
            commands.InvalidEndOfQuotedStringError, commands.UnexpectedQuoteError, commands.ExpectedClosingQuoteError)

        # Allows us to check for original exceptions raised and sent to CommandInvokeError.
        # If nothing is found. We keep the exception passed to on_command_error.
        error = getattr(error, 'original', error)

        # Anything in ignored will return and prevent anything happening.
        if isinstance(error, ignored):
            return

        if isinstance(error, handled):
            embed = discord.Embed(title="An error occurred.", description=str(error),
                                  color=discord.Color.from_rgb(255, 0, 0))
            return await ctx.respond(embed=embed)
        if isinstance(error, coc.errors.Maintenance):
            embed = discord.Embed(title="The Clash of Clans API is currently unresponsive.", description="Please try "
                                                                                                         "again later",
                                  color=discord.Color.from_rgb(255, 0, 0))
            await ctx.respond(embed=embed)

        elif isinstance(error, discord.ext.commands.TooManyArguments):
            embed = discord.Embed(title="An error occurred.",
                                  description="Too many input arguments, please make sure you quote your team name.",
                                  color=discord.Color.from_rgb(255, 0, 0))
            await ctx.respond(embed=embed)

        elif isinstance(error, commands.CheckFailure):
            embed = discord.Embed(title="Missing permissions.", description=str(error),
                                  color=discord.Color.from_rgb(255, 0, 0))
            await ctx.respond(embed=embed)

        elif isinstance(error, commands.NoPrivateMessage):
            try:
                await ctx.author.send(f'{ctx.command} can not be used in Private Messages.')
            except discord.HTTPException:
                pass

        elif isinstance(error, commands.MissingRequiredArgument):
            embed = discord.Embed(title="An error occurred.",
                                  description=f"A needed Parameter is missing.\n",
                                  color=discord.Color.from_rgb(255, 0, 0))
            await ctx.respond(embed=embed)

        elif isinstance(error, quote_errors):
            embed = discord.Embed(
                title="An error occurred.",
                description='You used `"` wrong.\nPlease check that are spaces after all ending " and no odd amount.',
                color=discord.Color.from_rgb(255, 0, 0),
            )

            await ctx.respond(embed=embed)

        elif isinstance(error, (errors.CustomConversionError, discord.ext.commands.errors.BadUnionArgument,
                                commands.BadArgument)):
            embed = discord.Embed(title="Nothing found.",
                                  description=error.args[0] if len(error.args) > 0 else error,
                                  color=discord.Color.from_rgb(255, 0, 0))
            await ctx.respond(embed=embed)
        else:
            embed = discord.Embed(title="An error occurred.",
                                  description="Please report this to a bot developer. Thank you!",
                                  color=discord.Color.from_rgb(255, 0, 0))

            try:
                await ctx.respond(embed=embed)
            except Exception:
                pass
            # all other errors will be logged


        if isinstance(error, no_webhook):
            return
        exc = ''.join(
                traceback.format_exception(type(error), error, error.__traceback__))
        try:
            doc_string = f"{ctx.command.qualified_name} " \
                         f"`{'` `'.join([str(c['name'])+':'+str(c['value']) for c in ctx.selected_options])}`\n" \
                         f"{type(error)}:{error.msg if hasattr(error, 'msg') else error.args}"
            logger.error(doc_string+'\n'+exc)
        except Exception as e:
            logger.critical(str(type(e)) + ": " + str(e))
        e = discord.Embed(title='Command Error', colour=discord.Color.green())
        e.add_field(name='Author', value=f'{ctx.author} (ID: {ctx.author.id})')

        fmt = f'Channel: {ctx.channel} (ID: {ctx.channel.id}) <#{ctx.channel.id}>'
        if ctx.guild:
            fmt = f'{fmt}\nGuild: {ctx.guild} (ID: {ctx.guild.id})'

        e.add_field(name='Location', value=fmt, inline=False)
        e.add_field(name='Information', value=f"Name: {ctx.author.name}\n Time: <t"
                                              f":{int(datetime.timestamp(datetime.utcnow()))}:f>", inline=False)
        if ctx.selected_options:
            args = [f'**{c["name"]}**:`{c["value"]}`' for c in ctx.selected_options]
            e.add_field(name='Convent', value=f"Command Name: {ctx.command.qualified_name}\nArgs: {' '.join(args)}\n",
                        inline=False)
        else:
            e.add_field(name='Convent', value=f"Command Name: {ctx.command.qualified_name}\n",
                        inline=False)



        if len(exc) > 2000:
            fp = io.BytesIO(exc.encode('utf-8'))
            e.description = "Traceback was too long."
            async with aiohttp.ClientSession() as session:
                webhook = discord.Webhook.from_url(os.getenv('DISCORD_WEBHOOK_ERROR_URL'), session=session)
                await webhook.send(embed=e, file=discord.File(fp, 'traceback.txt'))
            return

        e.description = f'```py\n{exc}\n```'
        async with aiohttp.ClientSession() as session:
            webhook = discord.Webhook.from_url(os.getenv('DISCORD_WEBHOOK_ERROR_URL'), session=session)
            await webhook.send(embed=e)



def setup(client):
    client.add_cog(error_handler(client))
