import os
from typing import Union

import discord
# import discord_slash
from discord.ext import commands
from . import db, errors


class CommandRestrictionError(commands.CheckFailure):
    pass


class ChannelLockedError(commands.CheckFailure):
    pass


def match_in_lists(l1: list, l2: list) -> bool:
    """
    Checks if at least one item of l1 matches an item from l2
    :param l1: List
    :param l2: List
    :return: boolean whether a match between the lists has been found
    """
    for item in l1:
        if item in l2:
            return True
    return False


async def individual_perms(ctx: Union[commands.Context, discord.ApplicationContext]):
    command_name = str(ctx.command)
    if isinstance(ctx.author, discord.Member):
        user: discord.Member = ctx.author
        role_ids = tuple([i.id for i in user.roles if i])
        try:
            [data] = await db.fetchrow(f"SELECT COUNT(*) FROM permissions where name = $1 and "
                                       f"(user_id = {ctx.author.id} or role_id in {role_ids})", command_name)
        except errors.NotFoundException:
            return False
        except Exception as e:
            return False
    elif isinstance(ctx.author, discord.User):
        try:
            [data] = await db.fetchrow(f"SELECT COUNT(*) FROM permissions where name = $1 and "
                                       f"user_id = {ctx.author.id}", command_name)
        except errors.NotFoundException:
            return False
        except Exception as e:
            return False
    else:
        return False
    if data == 0:
        return False
    else:
        return True


def dev():
    """
    Decorator for checking whether a user has a 'dev' role
    This is added as a check to the command
    """

    async def predicate(ctx: Union[commands.Context, discord.ApplicationContext]):
        dev_ids = [int(x) for x in os.getenv('PERMISSION_LEVEL_DEV', '').split(',') if x]
        if ctx.author.id in dev_ids:
            return True
        else:
            if isinstance(ctx, str):
                raise discord.ext.commands.CheckFailure('You must be a `dev` to execute this command!')

            if isinstance(ctx, commands.Context):
                raise discord.ext.commands.CheckFailure('You must be a `dev` to execute this command!')
            raise discord.ext.commands.CheckFailure('You must be a `dev` to execute this command!')


    return commands.check(predicate)


def staff():
    """
    Decorator for checking whether a user has a 'dev' role
    This is added as a check to the command
    """

    async def predicate(ctx: Union[commands.Context, discord.ApplicationContext]):
        related_guilds = [int(x.strip()) for x in os.getenv('PERMISSION_TRUSTED_SERVERS', '').split(',') if x.strip().isnumeric()]
        if ctx.author.id in [int(x) for x in os.getenv('PERMISSION_LEVEL_DEV', '').split(',') if x]:
            return True
        check = await individual_perms(ctx)
        if check:
            result = True
        else:
            if not ctx.guild:
                result = False
            elif ctx.guild.id in related_guilds:
                if hasattr(ctx.author, 'roles'):
                    roles = [str(i) for i in ctx.author.roles]
                    perm_roles = os.getenv('PERMISSION_LEVEL_MANAGER', '').split(',')
                    if match_in_lists(roles, perm_roles):
                        result = True
                    else:
                        result = False
                else:
                    result = False
            else:
                result = False
        if isinstance(ctx, commands.Context):
            if isinstance(ctx.command, str):
                if result:
                    return result
                else:
                    raise discord.ext.commands.CheckFailure('You must be a `staff` to execute this command!')


            if result:
                return result
            else:
                raise discord.ext.commands.CheckFailure('You must be a `staff` to execute this command!')
        if result:
            return result
        else:
            raise discord.ext.commands.CheckFailure('You must be a `staff` to execute this command!')

    return commands.check(predicate)

def admin():
    """
    Decorator for checking whether a user has a 'admin' role
    This is added as a check to the command
    """

    async def predicate(ctx: Union[commands.Context, discord.ApplicationContext]):
        check = await individual_perms(ctx)
        if check:
            return check
        if not ctx.guild:
            return False
        roles = [str(i) for i in ctx.author.roles]
        perm_roles = os.getenv('PERMISSION_LEVEL_MANAGER', '').split(',')
        if match_in_lists(roles, perm_roles):
            return True
        else:
            if isinstance(ctx, commands.Context):
                raise discord.ext.commands.CheckFailure('You must be an `admin` to execute this command!')
            raise discord.ext.commands.CheckFailure('You must be an `admin` to execute this command!')
    return commands.check(predicate)


def dev_server():
    """
    Decorator for checking whether a server is in the stored dev_servers
    This is added as a check to the command
    """

    async def predicate(ctx: Union[commands.Context, discord.ApplicationContext]):
        if not ctx.guild:
            return False
        trusted_servers = [int(x.strip()) for x in os.getenv('PERMISSION_TRUSTED_SERVERS', '').split(',') if x.strip().isnumeric()]
        if ctx.guild.id in trusted_servers:
            return True
        else:
            if isinstance(ctx, commands.Context):
                raise discord.ext.commands.CheckFailure('Please execute this command in the dev server!')
            raise discord.ext.commands.CheckFailure('Please execute this command in the dev server!')
    return commands.check(predicate)


def staff_server():
    """
    Decorator for checking whether a server is in the stored 'staff' servers
    This is added as a check to the command
    """

    async def predicate(ctx: Union[commands.Context, discord.ApplicationContext]):
        if not ctx.guild:
            return False
        trusted_servers = [int(x.strip()) for x in os.getenv('PERMISSION_TRUSTED_SERVERS', '').split(',') if x.strip().isnumeric()]
        if ctx.guild.id in trusted_servers:
            return True
        else:
            if isinstance(ctx, commands.Context):
                raise discord.ext.commands.CheckFailure("Please execute this command in the staff server!")
            raise discord.ext.commands.CheckFailure("Please execute this command in the staff server!")
    return commands.check(predicate)


async def is_staff(ctx: Union[commands.Context, discord.ApplicationContext]):
    check = await individual_perms(ctx)
    if check:
        return check
    if not ctx.guild:
        return False
    if ctx.guild.id in [int(x.strip()) for x in os.getenv('PERMISSION_TRUSTED_SERVERS', '').split(',') if x.strip().isnumeric()]:
        if hasattr(ctx.author, 'roles'):
            roles = [str(i) for i in ctx.author.roles]
            perm_roles = os.getenv('PERMISSION_LEVEL_MANAGER', '').split(',')
            if match_in_lists(roles, perm_roles):
                return True
            else:
                return False
        else:
            return False
    else:
        return False


