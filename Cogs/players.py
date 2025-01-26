import traceback
from typing import Literal, Optional

import discord
from discord import option

from discord.ext import commands
from discord.commands import Option, SlashCommandGroup
from discord.ui import InputText, Modal
from numpy.f2py.crackfortran import requiredpattern

from custom_dataclasses import Account, bot, Datetime, Player
from utils import errors
from utils.independent import db, permissions as perm
from utils.message_utils import Confirmator, Paginator

LOG = bot.logger.getChild(__name__)


class Players(commands.Cog):
    def __init__(self, bot_client: commands.Bot):
        self.client = bot_client
    
    player_group = SlashCommandGroup("players", "player commands",)
    player_manage_group = player_group.create_subgroup("manage", "player management commands")
    player_account_group = player_group.create_subgroup("account", "player account commands")
    
    @player_group.command(name="create", description="Add a new player to the bot")
    @option(description="Name of the player",
            name="name",
            type=str)
    @option(description="Discord account if known",
            required=False, name="user", type=discord.User, default=None)
    @perm.staff()
    async def add_player(self,
                         ctx: discord.ApplicationContext,
                         name: str,
                         user: Optional[discord.User] = None,
                         ):
        """Adds a new player to the bot"""
        # Create entry
        if user:
            try:
                player_by_discord = await db.fetch('SELECT * from players p where player_discord_id = $1', user.id)
            except errors.NotFoundException:
                player_by_discord = []
            if player_by_discord:
                description = "\n".join([f"#{p['id']} {discord.utils.escape_markdown(p['player_name'])} "
                                         f"({p['player_discord_id']})" for p in player_by_discord])
                return await ctx.respond(embed=discord.Embed(title=f"Player with discord id {user.id} already exists",
                                                             description=description,
                                                             color=discord.Color.red()))
        try:
            player_options = await db.fetch('SELECT * from players p where player_name ilike $1',
                                            f"%{name}%")
        except errors.NotFoundException:
            player_options = []
        if player_options:
            desc = ""
            for p in player_options:
                desc += f"#{p['id']} {p['player_name']} ({p['player_discord_id']})\n"
            await ctx.respond(embed=discord.Embed(title=f"There are already players with a similar name"))
            check = await Confirmator(ctx=ctx,
                                      timeout=90,
                                      confirmer=ctx.user.id,
                                      text="Are you sure you want to create another player?").run()
            if not check:
                return await ctx.respond(embed=discord.Embed(title="Player creation cancelled", color=discord.Color.red()))
        try:
            [player_id] = await db.fetchrow('INSERT INTO players (player_name, player_discord_id) VALUES ($1, $2) '
                                            'RETURNING player_id', name, user.id if user else None)
        except Exception as e:
            raise e
        player = await Player.by_id(player_id)
        p_name = discord.utils.escape_markdown(player.name)
        embed = discord.Embed(title=f"Player {p_name} added to the bot.",
                              description=f"This player is now in the bot as {p_name} [{player.id}]." + (f" {user.mention} is "
                                                                                                              f"connected." if user else
                                                                                                              ""),
                              color=discord.Color.green())
        await ctx.respond(embed=embed)
        
    @player_manage_group.command(name="user", description="Manage a player's discord account")
    @Player.option()
    @option(description="Discord account if known", type=discord.User, required=False, default=None, name="user")
    @perm.staff()
    async def manage_player_user(self,
                                 ctx: discord.ApplicationContext,
                                 player: Player,
                                 user: Optional[discord.User] = None
                                 ):
        """Manage a player's discord account"""
        if not user and player.discord_id:
            old_user = await player.get_user()
            try:
                await db.execute("UPDATE players SET player_discord_id = $1 WHERE player_id = $2", None, player.id)
            except Exception as e:
                raise e
            
            embed = discord.Embed(title=f"Disconnected discord account from player {discord.utils.escape_markdown(player.name)}"
                                        f" ({player.id}).",
                                  description=f"This player was previously connected to {old_user.mention} [{old_user.id}].",
                                  color=discord.Color.green())
            await ctx.respond(embed=embed)
            return
        if not user and not player.discord_id:
            return await ctx.respond(embed=discord.Embed(title=f"Player {discord.utils.escape_markdown(player.name)} ({player.id}) "
                                                               f"has no discord account.",
                                                         description="There is nothing to do.",
                                                         color=discord.Color.yellow()))
        if user.id == player.discord_id:
            return await ctx.respond(embed=discord.Embed(title=f"Player {discord.utils.escape_markdown(player.name)} ({player.id}) "
                                                               f"has already connected this discord "
                                                               f"account.",
                                                         description=f"This player is already connected to {user.mention} [{user.id}]. "
                                                                     f"There is nothing to do.",
                                                         color=discord.Color.green()))
        try:
            other_players = await db.fetch("SELECT * from players p where p.player_discord_id = $1", user.id)
        except errors.NotFoundException:
            other_players = []
        if other_players:
            desc = ""
            for p in other_players:
                desc += f"#{p['id']} {discord.utils.escape_markdown(p['player_name'])} ({p['player_discord_id']})\n"
            return await ctx.respond(embed=discord.Embed(title=f"Discord id {user.id} is already connected to another player",
                                                         description=desc,
                                                         color=discord.Color.red()))
        try:
            await db.execute("UPDATE players SET player_discord_id = $1 WHERE player_id = $2", user.id, player.id)
        except Exception as e:
            raise e
        embed = discord.Embed(title=f"Connected discord account to player {discord.utils.escape_markdown(player.name)} ({player.id}).",
                              description=f"This player is now connected to {user.mention} [{user.id}].")
        await ctx.respond(embed=embed)

    
    @player_group.command(name="list", description="List all players")
    @perm.staff()
    @option(name="filter_by", description="Filter the players", required=False, choices=["all", "has discord", "no discord"],
            default="all", type=str)
    @option(name="search", description="Search for players", required=False, default="", type=str)
    async def list_players(self,
                           ctx: discord.ApplicationContext,
                           filter_by: str = "all",
                           search: str = ""):
        """List all players"""
        if filter_by == "has discord":
            query = "SELECT * from players p where player_discord_id is not null and player_name ilike $1 ORDER BY player_name"
            filter_by = " with discord"
        elif filter_by == "no discord":
            query = "SELECT * from players p where player_discord_id is null and player_name ilike $1 ORDER BY player_name"
            filter_by = " without discord"
        else:
            query = "SELECT * from players p where player_name ilike $1 ORDER BY player_name"
        
        try:
            workers = await db.fetch(query, f"%{search}%")
        except errors.NotFoundException:
            embed = discord.Embed(title="No players found", description="Set up players with `/players insert`",
                                  color=discord.Color.red())
            return await ctx.respond(embed=embed)
        desc = ""
        embeds = []
        embed_title = f"Players{filter_by}" + (f" matching `{search}`" if search else "")
        for worker_row in workers:
            worker = await Player.by_id(worker_row[0])
            temp = f"# {discord.utils.escape_markdown(worker.name)} `ID {worker.id}`\n"
            if worker.discord_id:
                temp += f"`{worker.discord_id}` <@{worker.discord_id}>\n"
            temp += "\n"
            if len(desc + temp) > 2000:
                embed = discord.Embed(title=embed_title, description=desc, color=discord.Color.green())
                embeds.append(embed)
                desc = temp
            else:
                desc += temp
        if desc:
            embed = discord.Embed(title=embed_title, description=desc, color=discord.Color.green())
            embeds.append(embed)
        await Paginator(embeds, ctx).run()
    
    @player_group.command(name="info", description="Information about a player")
    @perm.staff()
    @Player.option()
    async def player_info(self, ctx: discord.ApplicationContext, player: Player):
        """Information about a player"""
        accounts = await player.get_accounts()
        desc = ""
        for acc in accounts:
            try:
                account = await Account.by_id(acc.get('account_tag'))
            except errors.NotFoundException:
                continue
            desc += f"- {discord.utils.escape_markdown(account.name)} ({account.tag}) {'Active' if account.tracking else 'Inactive'}\n"
        embed = discord.Embed(title=f"Player {discord.utils.escape_markdown(player.name)} ({player.id})", color=discord.Color.green(),
                              description=desc)
        
        
        await ctx.respond(embed=embed)
        
    @player_account_group.command(name="add", description="Add an account to a player")
    @Player.option()
    @Account.option()
    @perm.staff()
    async def add_account(self,
                          ctx: discord.ApplicationContext,
                           player: Player,
                           account: Account
                           ):
        """Add an account to a player"""
        # lookup if the account already belongs to a player
        player_old = await account.get_player()
        if player_old:
            # account already belongs to a player
            embed = discord.Embed(title=f"Account {account.tag} is already in player {player_old.name} ({player_old.id}).",
                                  color=discord.Color.yellow())
            return await ctx.respond(embed=embed)
        try:
            await db.execute('INSERT INTO player_accounts (player_id, account_tag) VALUES ($1, $2)', player.id, account.tag)
        except Exception as e:
            raise e
        embed = discord.Embed(title=f"Account {account.tag} added to player {player.name} ({player.id}).",
                              color=discord.Color.green())
        await ctx.respond(embed=embed)
        
    @player_account_group.command(name="remove", description="Remove an account from a player")
    @Player.option()
    @Account.option(autocomplete=Account.autocomp(filter_player=True, force_filter=True))
    @perm.staff()
    async def remove_account(self,
                             ctx: discord.ApplicationContext,
                             player: Player,
                             account: Account
                             ):
        """Remove an account from a player"""
        player_old = await account.get_player()
        if player_old.id != player.id:
            embed = discord.Embed(title=f"Account {account.tag} is not in player {player.name} ({player.id}).",
                                  color=discord.Color.yellow())
            return await ctx.respond(embed=embed)
        try:
            await db.execute('DELETE FROM player_accounts WHERE player_id = $1 AND account_tag = $2', player.id, account.tag)
        except Exception as e:
            raise e
        embed = discord.Embed(title=f"Account {account.tag} removed from player {player.name} ({player.id}).",
                              color=discord.Color.green())
        await ctx.respond(embed=embed)



def setup(bot_client):
    bot_client.add_cog(Players(bot_client))