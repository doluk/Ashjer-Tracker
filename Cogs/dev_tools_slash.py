import asyncio
import datetime
import io
import textwrap
from contextlib import redirect_stdout
import importlib
import logging
import os
import re
import sys
import traceback


import coc
import discord
from discord.ext import commands
from discord.commands import Option, SlashCommandGroup
from discord.ui import InputText, Modal


from utils import db, errors, message_utils
from utils.independent import permissions as perm
from utils.independent.automation import scheduler
from utils.independent.config import ConfigObj
from utils.message_utils import Paginator
from custom_dataclasses import bot, logger


log_moderation = logging.getLogger(f"{logger.name}.moderation")
log_moderation.setLevel(logging.ERROR)

guilds = [763103282507808778, 632582153158393929, 927521828480630804]


async def cogs_search(ctx: discord.AutocompleteContext):
    "Returns a list of matching cogs"
    return [x.replace('.py','') for x in os.listdir('Cogs') if x.endswith(".py") and ctx.value.lower() in x.lower()]


async def loaded_cogs_search(ctx: discord.AutocompleteContext):
    "Returns a list of matching cogs"
    return [x.lower() for x in bot.client.cogs if ctx.value.lower() in x.lower()]


async def modules_search(ctx: discord.AutocompleteContext):
    "Returns a list of matching modules"
    # generate list of modules
    rel_modules = []
    for dirpath, dirnames, filenames in os.walk('utils'):
        for filename in filenames:
            if filename in ['__init__.py', 'external_source.py', 'regenerate_credentials_from_credentials_gcc.py',
                            'template_recognition.py', 'tracking.py']:
                continue
            if filename.endswith('.py') and not filename.startswith('__'):
                rel_modules.append(os.path.relpath(os.path.join(dirpath, filename), ).replace('.py', ''))
    # add the files in the root directory
    rel_modules.append('core')
    rel_modules.append('custom_dataclasses')
    # add the files from Cogs
    for filename in os.listdir('Cogs'):
        if filename.endswith('.py'):
            rel_modules.append(os.path.relpath(filename).replace('.py', ''))
    # only include files which are already loaded and in the python path
    matches = [x for x in rel_modules if ctx.value.lower() in x.lower()]
    if len(matches) > 24:
        return matches[:24]
    return matches




def cleanup_code(content: str) -> str:
    '''Automatically removes code blocks from the code and reformats linebreaks
    '''

    # remove ```py\n```
    if content.startswith('```') and content.endswith('```'):
        return '\n'.join(content.split('\n')[1:-1])

    return '\n'.join(content.split(';'))


def _e(msg: str) -> str:
    '''escape discord markdown characters
    Parameters
    ----------
        msg: string
            the text to escape characters in
    Returns
    -------
        the message including escape characters
    '''

    return re.sub(r'(\*|~|_|\||`)', r'\\\1', msg)


def e_(msg: str) -> str:
    '''unescape discord markdown characters
    Parameters
    ----------
        msg: string
            the text to remove escape characters from
    Returns
    -------
        the message excluding escape characters
    '''

    return re.sub(r'\\(\*|~|_|\||`)', r'\1', msg)


class dev_tools_slash(commands.Cog):
    def __init__(self, client: commands.Bot):
        self.client = client

    dev = SlashCommandGroup('dev', "Various dev commands", guild_ids=bot.related_guilds)
    
    @dev.command(guild_ids=bot.related_guilds)
    @perm.dev()
    async def db_structure(self,
                           ctx: discord.ApplicationContext,
                           schemas: Option(description='Schema', required=False) = 'None',
                           table_filter: Option(description='Table Filter', required=False) = '',
                           pagination: Option(description='Pagination', choices=['No', 'Yes'], required=False) = 'Yes'):
        """Show the structure of the database"""
        if schemas == 'None':
            [search_path] = await db.fetchrow("SELECT setting FROM pg_settings WHERE name = 'search_path';")
        else:
            search_path = schemas
        schemas = str(search_path).split(', ')
        
        # Query the postgresql database for the tables
        query = """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ANY($1::text[]) and table_schema not in ('pg_catalog', 'information_schema') and ($2::text = '' or
        table_name::text ilike $2)
        ORDER BY table_name;
        """
        try:
            tables = await db.fetch(query, schemas, table_filter)
        except Exception as e:
            await ctx.respond(f'```py\n{e.__class__.__name__}: {str(e)}\n```')
            return
        embeds = []
        isnt_nullable = "not null"
        is_nullable = "        "
        for table in tables:
            query = """
            SELECT ordinal_position::text as ordinal_position,
            column_name::text as column_name,
            data_type::text as data_type,
            CASE WHEN is_nullable = 'NO' THEN 'not null' ELSE '' END as is_nullable,
            CASE
            WHEN is_generated = 'ALWAYS' and column_name::text = 'ts' and data_type::text = 'tsvector' THEN 'TSVECTOR'
            WHEN is_generated = 'ALWAYS' THEN generation_expression::text
            WHEN is_identity = 'YES' AND identity_generation is not null THEN 'IDENTITY(' || identity_generation || ')'
            ELSE column_default::text END as default_value
             FROM information_schema.columns c where
            c.table_schema = $1 and table_name = $2
            """
            query_length = """
            WITH data as (
            SELECT ordinal_position::text as ordinal_position,
            column_name::text as column_name,
            data_type::text as data_type,
            CASE WHEN is_nullable = 'NO' THEN 'not null' ELSE '' END as is_nullable,
            CASE
            WHEN is_generated = 'ALWAYS' and column_name::text = 'ts' and data_type::text = 'tsvector' THEN 'TSVECTOR'
            WHEN is_generated = 'ALWAYS' THEN generation_expression::text WHEN is_identity = 'YES' AND identity_generation is not
            null THEN 'IDENTITY(' || identity_generation || ')'
             WHEN column_name = 'ts' and generation_expression ilike '%to_tsvector%' THEN 'TSVECTOR'
             ELSE column_default::text END as default_value
             FROM information_schema.columns c where
            c.table_schema = $1 and table_name = $2)
            SELECT max(textlen(ordinal_position)) as ordinal_position,
            max(textlen(column_name)) as column_name,
            max(textlen(data_type)) as data_type,
            max(textlen(is_nullable)) as is_nullable,
            max(textlen(default_value)) as default_value
            FROM data
            """
            try:
                column_length = await db.fetchrow(query_length, table[0], table[1])
            except Exception as e:
                column_length = {'ordinal_position': 2, 'column_name': 25, 'data_type': 25, 'is_nullable': 8, 'default_value': 25}
            try:
                columns = await db.fetch(query, table[0], table[1])
            except Exception as e:
                await ctx.respond(f'```py\n{e.__class__.__name__}: {str(e)}\n```')
                return
            description = ''
            for col in columns:
                temp = "`"
                temp += col.get('ordinal_position', '').rjust(column_length['ordinal_position']) + ' | '
                temp += col.get('column_name', '').ljust(column_length['column_name']) + ' '
                temp += col.get('data_type', '').ljust(column_length['data_type'])
                if col.get('is_nullable', '') != '':
                    temp += ' ' + col.get('is_nullable', '').rjust(column_length['is_nullable'])
                default_value = col.get('default_value', '') or ''
                if default_value != '':
                    temp += ' ' + default_value.rjust(column_length['default_value'])
                temp += '`'
                description += temp + '\n'
            embed = discord.Embed(title=f'{table.get("table_type")} {table.get("table_schema")}.{table.get("table_name")}',
                                  description=description)
            embeds.append(embed)
        if pagination == 'Yes':
            await Paginator(embeds, ctx).run()
        else:
            for embed in embeds:
                await ctx.respond(embed=embed)

    @dev.command(guild_ids=bot.related_guilds)
    @perm.dev()
    async def sync(self, ctx: discord.ApplicationContext, force: Option(str,'Force sync',choices=['0','1'],
                                                                        required=False)='0'):
        """Sync all the bot commands (Dev Only)"""
        switch = int(force)
        await ctx.respond('Start syncing')
        await bot.client.sync_commands(force=True if switch == 1 else False)
        await ctx.respond('Finished syncing')

    @dev.command(guild_ids=bot.related_guilds)
    @perm.dev()
    async def shutdown(self, ctx: discord.ApplicationContext):
        """Shutdown the bot (Dev Only)"""
        await ctx.respond("Shutting down!")
        await self.client.close()

    @perm.dev()
    @dev.command(guild_ids=bot.related_guilds)
    async def ureload(self, ctx: discord.ApplicationContext,
                      extension: Option(str, 'extension', autocomplete=modules_search)):
        """Reloads a Module (Dev Only)"""

        try:
            # reload module
            module_name = e_(extension.replace('/', '.').replace('.py', ''))
            mod = sys.modules[module_name]
            importlib.reload(mod)
        except KeyError:
            await ctx.respond('No such module')
            return
        except Exception as e:
            raise e
        await ctx.respond(' Reloaded `{}`'.format(', '.join([extension])))

    @perm.dev()
    @dev.command(guild_ids=bot.related_guilds)
    async def restart(self, ctx: discord.ApplicationContext):
        """Reloads a Cog (Dev Only)"""
        import os
        if scheduler and scheduler.state != 0:
            await ctx.respond('Shutting down scheduler')
            scheduler.shutdown()
        await ctx.respond("Restarting now!")
        os.system('systemctl restart ccn_bot')



    @perm.dev()
    @dev.command(guild_ids=bot.related_guilds)
    async def dumpdb(self, ctx: discord.ApplicationContext):
        await ctx.respond("starting db dump!")
        import os
        from utils.independent.db import cfg
        import datetime
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
        os.system(f'pg_dump postgresql://{cfg["user"]}:{cfg["password"]}@{cfg["host"]}:{cfg["port"]}/'
                  f'{cfg["database"]} > db_dumps/dump-{now}.sql')
        try:
            file = discord.File(f"db_dumps/dump-{now}.sql")
            await ctx.respond(file=file)
        except discord.errors.HTTPException:  # file too big
            pass

    @dev.command(guild_ids=bot.related_guilds)
    @perm.dev()
    async def sql(self, ctx: discord.ApplicationContext):
        """Execute SQL Code (Dev Only)"""

        class MyModal(Modal):
            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.add_item(InputText(label="SQL Code", value="", style=discord.InputTextStyle.long, ))

            async def callback(self, interaction: discord.Interaction):
                query = self.children[0].value
                if query.startswith('```sql') and query.endswith('```'):
                    query = query[6:-3]
                elif query.startswith('```py') and query.endswith('```'):
                    query = query[5:-3]
                elif query.startswith('```') and query.endswith('```'):
                    query = query[3:-3]
                elif query.startswith('`') and query.endswith('`'):
                    query = query[1:-1]
                while query.startswith('\n'):
                    query = query[1:]
                while query.endswith('\n'):
                    query = query[:-1]
                    
                await interaction.response.send_message(embed=discord.Embed(title='Query', description=f'```sql\n'
                                                                                                       f'{query}```'))
                try:
                    if query.split()[0].upper() == 'SELECT':
                        result = await db.fetch(e_(query))
                    else:
                        result = await db.execute(e_(query))
                except Exception as e:
                    await interaction.followup.send(
                        embed=discord.Embed(title='The database returned the following error:',
                                            description=f'{e.__class__.__name__}: {str(e)}',
                                            color=discord.Color.red()))
                    return
                if not isinstance(result, str):
                    output = []
                    rslt = ""
                    for r in result:
                        if len(rslt) + len(r) < 2000:
                            rslt += str(r) + "\n"
                        else:
                            output.append(rslt)
                            rslt = ""
                            rslt += str(r) + "\n"
                    output.append(rslt)
                else:
                    output = [result]
                if len(output) < 2:
                    await interaction.followup.send(
                            embed=discord.Embed(title='The database returned the following result:',
                                                description=output[0],
                                                color=discord.Color.green()))
                else:
                    await interaction.followup.send(
                            embed=discord.Embed(title='The database returned the following result:',
                                                description=output[0],
                                                color=discord.Color.green()))
                    for i in output[1:]:
                        await interaction.followup.send(embed=discord.Embed(description=i, color=discord.Color.green()))

        modal = MyModal(title='ENTER SQL CODE')
        await ctx.interaction.response.send_modal(modal)

    @dev.command(guild_ids=bot.related_guilds)
    @perm.dev()
    async def py(self, ctx: discord.ApplicationContext):
        """Execute Py Code (Dev Only)"""

        class MyModal(Modal):
            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.add_item(InputText(label="Py Code", value="", style=discord.InputTextStyle.long, ))

            async def callback(self, interaction: discord.Interaction):
                query = self.children[0].value
                if query.startswith('```sql') and query.endswith('```'):
                    query = query[6:-3]
                elif query.startswith('```py') and query.endswith('```'):
                    query = query[5:-3]
                elif query.startswith('```') and query.endswith('```'):
                    query = query[3:-3]
                elif query.startswith('`') and query.endswith('`'):
                    query = query[1:-1]
                while query.startswith('\n'):
                    query = query[1:]
                while query.endswith('\n'):
                    query = query[:-1]
                stmts = query
                await interaction.response.send_message(embed=discord.Embed(title='Query', description=f'```py\n'
                                                                                                       f'{stmts}```'))

                env = {'client': bot.client, 'channel': ctx.channel, 'author': ctx.author, 'guild': ctx.guild,
                       'message': ctx.message, 'discord': discord, 'asyncio': asyncio, 'coc': coc,
                       'scheduler': scheduler, 'db': db, 'ctx': ctx,
                       'Paginator': message_utils.Paginator, 'Confirmator': message_utils.Confirmator, 'bot': bot}

                env.update(globals())

                stmts = cleanup_code(e_(stmts))
                stdout = io.StringIO()

                to_compile = f'async def func():\n{textwrap.indent(stmts, "  ")}'

                try:
                    exec(to_compile, env)
                except Exception as e:
                    return await interaction.followup.send(f'```py\n{e.__class__.__name__}: {e}\n```')
                func = env['func']
                try:
                    with redirect_stdout(stdout):
                        ret = await func()
                except Exception:
                    value = stdout.getvalue()
                    values = value.split('\n')
                    buf = f"{len(values)} lines output\n"
                    buffer = []
                    for v in values:
                        if len(buf) + len(v) < 1800:
                            buf += v + "\n"
                        else:
                            buffer.append(buf)
                            buf = v + "\n"
                    buffer.append(buf)
                    for i, b in enumerate(buffer):
                        await interaction.followup.send(embed=discord.Embed(description=f'```py\n{b}```'))
                    await interaction.followup.send(
                        embed=discord.Embed(description=f'```py\n{traceback.format_exc()}\n```'))
                    return False
                else:
                    value = stdout.getvalue()
                    values = value.split('\n')
                    buf = f"{len(values)} lines output\n"
                    buffer = []
                    for v in values:
                        if len(v) < 4000:
                            if len(buf) + len(v) < 500:
                                buf += v + "\n"
                            else:
                                buffer.append(buf)
                                buf = v + "\n"
                        else:
                            for x in range(0,len(v),4000):
                                if x + 4000 < len(v):
                                    buffer.append(v[x:x+4000])
                                else:
                                    buffer.append(v[x:])
                    buffer.append(buf)
                    for i, b in enumerate(buffer):
                        await interaction.followup.send(embed=discord.Embed(description=f'```py\n{b}```'))
                    if ret is not None:
                        ret = ret.split('\n')
                        buf = f"{len(ret)} lines output\n"
                        buffer = []
                        for v in ret:
                            if len(buf) + len(v) < 500:
                                buf += v + "\n"
                            else:
                                buffer.append(buf)
                                buf = v + "\n"
                        buffer.append(buf)
                        for i, b in enumerate(buffer):
                            await interaction.followup.send(embed=discord.Embed(description=f'```py\n{b}```'))
        modal = MyModal(title='ENTER PY CODE')
        await ctx.interaction.response.send_modal(modal)

    @dev.command(guild_ids=bot.related_guilds)
    @perm.staff()
    async def dblook(self, ctx: discord.ApplicationContext, query: Option(str, 'Query')):
        """Return results of simple select queries as csv"""
        if ('insert' in query.lower() or 'delete' in query.lower() or 'update' in query.lower() or 'drop' in \
                query.lower() or 'alter' in query.lower()) and ctx.user.id not in \
                bot.config.permissions.permission_levels.dev:
            await ctx.respond('There are words in your query which concerns me. Please ask a bot dev to run this '
                              'query for you.')
            return
        try:
            result = await db.fetch(e_(query))
        except Exception as e:
            await ctx.respond(embed=discord.Embed(title='The database returned the following error:',
                                               description=f'{e.__class__.__name__}: {str(e)}',
                                               color=discord.Color.red()))
            return
        try:
            content = ', '.join(result[0].keys()) + '\n' + '\n'.join(', '.join(str(el) for el in row) for row in result)
            stream = io.BytesIO(content.encode('utf-8'))
            await ctx.respond('here is your result',file=discord.File(stream, filename='result.csv'))
        except Exception as e:
            await ctx.respond(embed=discord.Embed(description=f'{e.__class__.__name__}: {str(e)}',
                                                  color=discord.Color.red()))
            return

    @dev.command(guild_ids=bot.related_guilds)
    @perm.staff()
    async def showcommands(self,ctx: discord.ApplicationContext):
        embeds = []
        commands_outside_groups = []
        for cmd in await bot.client.http.get_guild_commands(bot.client.application_id, ctx.guild.id):
            try:
                options = cmd['options']
            except KeyError:
                commands_outside_groups.append(f"**{cmd['name']}** {cmd['description']}")
                continue
            if all([option['type']>2 for option in options]):
                commands_outside_groups.append(f"**{cmd['name']}** {cmd['description']}")
                continue
            else:
                sub_commands = sorted([f"**{option['name']}** {option['description']}" for option in options if option[
                                          'type']<=2],key= lambda x: x)
                embed = discord.Embed(title=f'Guild Commands of the {cmd["name"]} group',
                                      description="\n".join(sub_commands))
                embeds.append(embed)
        embeds = sorted(embeds, key=lambda x: x.description)
        embed = discord.Embed(title='Ungrouped guild commands')
        desc = ''
        commands_outside_groups = sorted(commands_outside_groups,key=lambda x:x)
        for command in commands_outside_groups:
            if len(desc) + len(command) + len('\n') < 4000:
                desc += command + '\n'
            else:
                embed.description=desc
                embeds.append(embeds)
                desc=command + '\n'
        if desc:
            embed.description = desc
            embeds.append(embed)
        global_commands_outside_groups = []
        global_embeds = []
        for cmd in await bot.client.http.get_global_commands(bot.client.application_id):
            try:
                options = cmd['options']
            except KeyError:
                global_commands_outside_groups.append(f"**{cmd['name']}** {cmd['description']}")
                continue
            if all([option['type'] > 2 for option in options]):
                global_commands_outside_groups.append(f"**{cmd['name']}** {cmd['description']}")
                continue
            else:
                sub_commands = sorted([f"**{option['name']}** {option['description']}" for option in options if option[
                    'type'] <= 2], key=lambda x: x)
                embed = discord.Embed(title=f'Global Commands of the {cmd["name"]} group',
                                      description="\n".join(sub_commands))
                global_embeds.append(embed)
        global_embeds = sorted(global_embeds, key=lambda x: x.description)
        embed = discord.Embed(title='Ungrouped global commands')
        desc = ''
        global_commands_outside_groups = sorted(global_commands_outside_groups, key=lambda x: x)
        embeds.extend(global_embeds)
        for command in global_commands_outside_groups:
            if len(desc) + len(command) + len('\n') < 4000:
                desc += command + '\n'
            else:
                embed.description = desc
                embeds.append(embeds)
                desc = command + '\n'
        if desc:
            embed.description = desc
            embeds.append(embed)
        await Paginator(embeds,ctx).run()




def setup(client):
    client.add_cog(dev_tools_slash(client))
