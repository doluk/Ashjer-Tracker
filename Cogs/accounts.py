import datetime
import traceback
from typing import Literal

import discord
from discord import option

from discord.ext import commands
from discord.commands import Option, SlashCommandGroup
from discord.mentions import default
from discord.ui import InputText, Modal

from custom_dataclasses import Account, bot, Datetime, Player
from utils import errors
from utils.independent import db, permissions as perm
from utils.message_utils import Paginator

LOG = bot.logger.getChild(__name__)


class Accounts(commands.Cog):
    def __init__(self, bot_client: commands.Bot):
        self.client = bot_client
    
    
    account_group = SlashCommandGroup("accounts", "account commands",
                                      )
    tracking_group = account_group.create_subgroup("tracking", "tracking commands")
    
    @account_group.command(name="insert", description="Adds an account to the bot")
    @option(description="Should be tracked",
            default="Active",
            required=False,
            name="tracking",
            choices=["Active", "Inactive"],
            type=str)
    @perm.staff()
    async def add_account(self,
                          ctx: discord.ApplicationContext,
                          account_tag: Option(str, "The account tag to add to the bot"),
                          tracking: Literal["Active", "Inactive"] = "Active"
                          ):
        """Adds an account to the bot"""
        try:
            player = await bot.clash_client.get_player(account_tag)
        except Exception:
            return await ctx.respond("Invalid account tag")
        try:
            db_player = await Account.by_id(player.tag)
        except Exception:
            db_player = None
        if db_player:
            tracking_status = "active" if db_player.tracking else "inactive"
            last_update = db_player.last_updated.to_discord() if db_player.last_updated else "Never"
            embed = discord.Embed(title=f"Account {account_tag} is already in the bot.",
                                  description=f"This account is already in the bot as {db_player.name} [{db_player.tag}]. The tracking "
                                              f"is currently {tracking_status} and the last update was {last_update}.\nIf you want to "
                                              f"adjust the tracking status, use `/accounts tracking manage`.",
                                  color=discord.Color.yellow())
            return await ctx.respond(embed=embed)
        # Create entry
        try:
            await db.execute('INSERT INTO accounts (account_tag, account_name, tracking_active) VALUES ($1, $2, $3)',
                             account_tag, player.name, tracking == "Active")
        except Exception as e:
            raise e
        embed = discord.Embed(title=f"Account {account_tag} added to the bot.",
                              description=f"This account is now in the bot as {player.name} [{player.tag}]. Tracking was set to {tracking}",
                              color=discord.Color.green())
        await ctx.respond(embed=embed)
    
    @tracking_group.command(name="manage", description="Adjusts the tracking status of an account")
    @perm.staff()
    @Account.option()
    @option(description="Should be tracked",
            default="Active",
            required=False,
            name="tracking",
            choices=["Active", "Inactive"],
            type=str)
    async def manage_tracking(self,
                              ctx: discord.ApplicationContext,
                              account: Account,
                              tracking: Literal["Active", "Inactive"]):
        """Adjusts the tracking status of an account"""
        
        former_status = "active" if account.tracking else "inactive"
        if former_status == tracking.lower():
            return await ctx.respond(embed=discord.Embed(title=f"Tracking status of account {account.tag} is already {tracking}.",
                                                         color=discord.Color.yellow()))
        try:
            await db.execute('UPDATE accounts SET tracking_active = $1 WHERE account_tag = $2', tracking == "Active",
                             account.tag)
        except Exception as e:
            raise e
        embed = discord.Embed(title=f"Tracking status of account {account.tag} adjusted.",
                              description=f"Tracking status of account {account.tag} is now {tracking.lower()} instead of {former_status}.",
                              color=discord.Color.green())
        await ctx.respond(embed=embed)
    
    @account_group.command(name="list", description="List all accounts")
    @perm.staff()
    @option(name="filter", description="Filter the accounts", required=False, choices=["all", "active", "inactive"], default="all",
            type=str)
    @option(name="search", description="Search for accounts", required=False, default="", type=str)
    async def list_accounts(self,
                            ctx: discord.ApplicationContext,
                            filter_by: str = "all",
                            search: str = ""):
        """List all accounts"""
        if filter_by == "active":
            worker_active = True
        elif filter_by == "inactive":
            worker_active = False
        else:
            worker_active = None
        try:
            workers = await db.fetch("SELECT account_tag from accounts a where ($1::bool is null or tracking_active = $1) and "
                                     "($2::text = '' or account_name ilike $2) ORDER by account_name", worker_active, search)
        except errors.NotFoundException:
            embed = discord.Embed(title="No accounts found", description="Set up accounts with `/accounts insert`",
                                  color=discord.Color.red())
            return await ctx.respond(embed=embed)
        desc = ""
        embeds = []
        embed_title = f"{filter_by.capitalize()} accounts" + (f" matching `{search}`" if search else "")
        for worker_row in workers:
            worker = await Account.by_id(worker_row[0])
            temp = f"# {worker.name} `ID {worker.id}`\n"
            if worker.tracking:
                temp += "**Active**\n"
            else:
                temp += "**Inactive**\n"
            
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

    @account_group.command(name="info", description="Information about an account")
    @perm.staff()
    @Account.option()
    async def account_info(self, ctx: discord.ApplicationContext, account: Account):
        """Information about an account"""
        try:
            stats = await db.fetchrow("SELECT sum(times_observed) as entries, min(first_observed) as first_tracked "
                                      "from account_tracking a"
                                      " where a.account_tag = $1", account.tag)
        except errors.NotFoundException:
            stats = {'entries': 0, 'first_tracked': None}
        embed = discord.Embed(title=f"Account {account.tag} ({account.name})", color=discord.Color.green())
        embed.add_field(name="Tracking status", value="Active" if account.tracking else "Inactive", inline=False)
        embed.add_field(name="Tracking since", value=account.last_updated.to_discord() if account.last_updated else "Never", inline=False)
        embed.add_field(name="Number of entries", value=stats['entries'], inline=False)
        player = await account.get_player()
        if player:
            embed.add_field(name="Player", value=f"{player.name} [{player.id}]", inline=False)
        await ctx.respond(embed=embed)
        
    @account_group.command(name="battle_log", description="View the battle log of an account")
    @perm.staff()
    @Account.option()
    @Datetime.option(name="after", description="Show battles after this date", required=False)
    @Datetime.option(name="before", description="Show battles before this date", required=False)
    @option(name="filter_opponent", description="Filter the battles for known opponents", required=False,
            choices=["all", "known opponents", "unknown opponents"], type=str, default="all")
    @option(name="filter_battle", description="Filter the battles for attack/defense", required=False,
            choices=["all", "attacks", "defenses"], type=str, default="all")
    @option(name="limit", description="Limit for the shown battles", type=int, default=None, required=False)
    async def account_battle_log(self,
                                 ctx: discord.ApplicationContext,
                                 account: Account,
                                 after: Datetime = None,
                                 before: Datetime = None,
                                 filter_opponent: Literal["all", "known opponents", "unknown opponents"] = "all",
                                 filter_battle: Literal["all", "attacks", "defenses"] = "all",
                                 limit: int = None):
        """View the battle log of an account"""
        if after is None:
            after = Datetime.now() - datetime.timedelta(days=2)
        if before is None:
            before = Datetime.now()
        if limit is None:
            limit = 5000
        query = """WITH data as (SELECT account_tag, account_name, date_trunc('min', requested_at) as requested_at,
                    builder_base_trophies_new as trophies_new, trophies_difference as trophies_diff from account_tracking_v3 where
                    requested_at >= $2 and requested_at < $3)
                    SELECT w1.account_name as p_name, w1.account_tag as p_tag, w1.requested_at,
                     w1.trophies_new as p_trophies, w1.trophies_diff as p_trophies_diff,
                     w2.account_name as s_name, w2.account_tag as s_tag, w2.trophies_new as s_trophies
                     FROM data w1 left outer join data w2 on
                    w1.requested_at = w2.requested_at and
                    w1.account_tag !=
                    w2.account_tag and w1.trophies_diff + w2.trophies_diff = 0 where w1.account_tag = $1 and w1.requested_at >= $2
                    and w1.requested_at < $3 order by w1.requested_at desc LIMIT $4"""
        if filter_opponent == "known opponents":
            opponent_filter = "opponent_known"
            
        elif filter_opponent == "unknown opponents":
            opponent_filter = "opponent_unknown"
        else:
            opponent_filter = None
        try:
            battles = await db.fetch(query, account.tag, after, before, limit)
        except errors.NotFoundException:
            return await ctx.respond(embed=discord.Embed(title=f"No battles found for account {account.tag} "
                                                               f"between {after.to_discord()} and {before.to_discord()}.",
                                                         color=discord.Color.red()))
        embeds = []
        desc = f"Showing {len(battles)} battles between {after.to_discord()} and {before.to_discord()} with {filter_opponent}"
        if filter_battle != "all":
            desc += f" showing only {filter_battle}"
        embed = discord.Embed(title=f"Battle log for account {account.tag} ({account.name})", color=discord.Color.green(),
                              description=desc)
        for battle in battles:
            if filter_opponent == "known opponents" and battle.get('s_tag') is None:
                continue
            elif filter_opponent == "unknown opponents" and battle.get('s_tag') is not None:
                continue
            p_name = discord.utils.escape_markdown(battle['p_name'])
            s_name = discord.utils.escape_markdown(battle['s_name'] or "unknown")
            p_tag = battle['p_tag']
            s_tag = battle['s_tag'] or "unknown"
            p_trophies = battle['p_trophies']
            s_trophies = battle['s_trophies']
            if battle.get('p_trophies_diff', 0) and battle.get('p_trophies_diff', 0) > 0:
                field_name = f"Attack against {s_name}"
                emoji_value = "➕"
                if filter_battle == "defenses":
                    continue
            elif battle.get('p_trophies_diff', 0) and battle.get('p_trophies_diff', 0) < 0:
                field_name = f"Defense against {s_name}"
                emoji_value = "➖"
                if filter_battle == "attacks":
                    continue
            else:
                field_name = f"Battle against {s_name}"
                emoji_value = "❓"
            if battle.get('s_tag'):
                field_name += f" ({s_tag}, 🏆{s_trophies})"
            requested_at = Datetime.by_dt(battle.get('requested_at'))
            field_value = (f"{requested_at.to_discord()}\n"
                           f"🏆{p_trophies - battle.get('p_trophies_diff', 0)} {emoji_value} {battle.get('p_trophies_diff')} -> "
                           f"{p_trophies}")
            if len(embed.fields) > 23:
                embeds.append(embed)
                embed = discord.Embed(title=f"Battle log for account {account.tag} ({account.name})", color=discord.Color.green(),
                                      description=desc)
            embed.add_field(name=field_name, value=field_value, inline=False)
        if len(embed.fields) > 0:
            embeds.append(embed)
        if len(embeds) == 0:
            return await ctx.respond(embed=discord.Embed(title=f"No battles found for account {account.tag} "
                                                               f"between {after.to_discord()} and {before.to_discord()} with "
                                                               f"{filter_opponent}" +
                                                               (f" showing only {filter_battle}" if filter_battle != "all" else ""),
                                                         color=discord.Color.red()))
        await Paginator(embeds, ctx).run()




def setup(bot_client):
    bot_client.add_cog(Accounts(bot_client))