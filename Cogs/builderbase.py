import datetime
import os
from typing import Literal

import aiohttp
import discord
from discord import option

from discord.ext import commands
from discord.commands import Option, SlashCommandGroup
from discord.ui import InputText, Modal

from custom_dataclasses import Account, Datetime, Player
from utils import errors, permissions as perm
from utils.independent import db
from utils.message_utils import Paginator


class BuilderBase(commands.Cog):
    def __init__(self, bot_client: commands.Bot):
        self.client = bot_client
        
    tracking_group = SlashCommandGroup("tracking", "Tracking commands")
    
    @tracking_group.command(name="versus_log", description="Logs between two accounts")
    @perm.staff()
    @Account.option(name="account1")
    @Account.option(name="account2")
    @Datetime.option(name="after", description="Show battles after this date", required=False)
    @Datetime.option(name="before", description="Show battles before this date", required=False)
    @option(name="filter_opponent", description="Filter the battles for known opponents", required=False,
            choices=["all", "known opponents", "unknown opponents"], type=str, default="all")
    @option(name="filter_battle", description="Filter the battles for attack/defense", required=False,
            choices=["all", "attacks", "defenses"], type=str, default="all")
    @option(name="limit", description="Limit for the shown battles", type=int, default=None, required=False)
    async def account_battle_log(self,
                                 ctx: discord.ApplicationContext,
                                 account1: Account,
                                 account2: Account,
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
                    and w1.requested_at < $3 and w2.account_tag = $5 order by w1.requested_at desc LIMIT $4"""
        if filter_opponent == "known opponents":
            opponent_filter = "opponent_known"
        
        elif filter_opponent == "unknown opponents":
            opponent_filter = "opponent_unknown"
        else:
            opponent_filter = None
        try:
            battles = await db.fetch(query, account1.tag, after, before, limit, account2.tag)
        except errors.NotFoundException:
            return await ctx.respond(embed=discord.Embed(title=f"No battles found for account {account1.tag} vs {account2.tag} "
                                                               f"between {after.to_discord()} and {before.to_discord()}.",
                                                         color=discord.Color.red()))
        embeds = []
        desc = f"Showing {len(battles)} battles between {after.to_discord()} and {before.to_discord()} with {filter_opponent}"
        if filter_battle != "all":
            desc += f" showing only {filter_battle}"
        embed = discord.Embed(title=f"Battle log for account {account1.tag} ({account1.name}) vs {account2.tag} ({account2.name})",
                              color=discord.Color.green(),
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
                embed = discord.Embed(title=f"Battle log for account {account1.tag} ({account1.name}) vs {account2.tag} ({account2.name})",
                                      color=discord.Color.green(),
                                      description=desc)
            embed.add_field(name=field_name, value=field_value, inline=False)
        if len(embed.fields) > 0:
            embeds.append(embed)
        if len(embeds) == 0:
            return await ctx.respond(embed=discord.Embed(title=f"No battles found for account {account1.tag} vs {account2.tag} "
                                                               f"between {after.to_discord()} and {before.to_discord()} with "
                                                               f"{filter_opponent}" +
                                                               (f" showing only {filter_battle}" if filter_battle != "all" else ""),
                                                         color=discord.Color.red()))
        await Paginator(embeds, ctx).run()
    


def setup(bot_client):
    bot_client.add_cog(BuilderBase(bot_client))