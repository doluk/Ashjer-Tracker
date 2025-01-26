import os

import aiohttp
import discord

from discord.ext import commands
from discord.commands import Option, SlashCommandGroup
from discord.ui import InputText, Modal

from custom_dataclasses import Account, Datetime, Player
from utils import errors
from utils.independent import db


class BuilderBase(commands.Cog):
    def __init__(self, bot_client: commands.Bot):
        self.client = bot_client
        
    tracking_group = SlashCommandGroup("tracking", "Tracking commands")
    
    @tracking_group.command(name="versus_log", description="Logs between two accounts")
    @Account.option(name="account1")
    @Account.option(name="account2")
    @Datetime.option("after")
    @Datetime.option("before")
    async def versus_log(self,
                         ctx: discord.ApplicationContext,
                         account1: Account,
                         account2: Account,
                         after: Datetime,
                         before: Datetime,
                         ):
        # SELECT all the winners with their trophy change compared to the previous requested leaderboard
        query = """WITH
    winner_raw as (SELECT l.* from account_tracking_v2 l where date_trunc('min',requested_at) > $1 and ),
	winners as (SELECT w1.account_tag as winner_tag, w1.account_name as winner_name, date_trunc('min',w1.requested_at) as
	winner_last_request,
	date_trunc('min',w2.requested_at) as
	winner_second_last_request,
	w1.builder_base_trophies - w2.builder_base_trophies as winner_diff from winner_raw w1 join winner_raw w2 on w1.account_tag =
	w2.account_tag
	and date_trunc('min',w1.requested_at) != date_trunc('min',w2.requested_at) where w1.builder_base_trophies > w2.builder_base_trophies
	                                                                             and w1.requested_at > w2.requested_at),
	losers as (SELECT w1.account_tag as loser_tag, w1.account_name as loser_name, date_trunc('min',w1.requested_at) as
	loser_last_request, date_trunc('min',w2.requested_at) as
	loser_second_last_request,
	w2.builder_base_trophies - w1.builder_base_trophies as loser_diff,
	w1.builder_base_trophies, w2.builder_base_trophies from winner_raw w1 join winner_raw w2 on w1.account_tag =
	w2.account_tag
	and date_trunc('min',w1.requested_at) != date_trunc('min',w2.requested_at) where w1.builder_base_trophies < w2.builder_base_trophies
	                                                                           and w1.requested_at > w2.requested_at)
    SELECT
	w.*, l.loser_tag, l.loser_name from winners w join losers l on l.loser_diff = w.winner_diff and w.winner_last_request =
	l.loser_last_request and
	w.winner_second_last_request = l.loser_second_last_request
	order by w.winner_tag, l.loser_tag, w.winner_diff
    	"""
        try:
            matches = await db.fetch(query)
        except errors.NotFoundException:
            return
        output = {}
        for m in matches:
            w_name = discord.utils.escape_markdown(m.get('winner_name'))
            l_name = discord.utils.escape_markdown(m.get('loser_name'))
            first_request = Datetime.by_dt(m.get('winner_last_request'))
            second_request = Datetime.by_dt(m.get('winner_second_last_request'))
            try:
                [winner_id] = await db.fetchrow('SELECT player_id from player_accounts where account_tag = $1', m.get('winner_tag'))
            except errors.NotFoundException:
                winner_id = None
            try:
                [loser_id] = await db.fetchrow('SELECT player_id from player_accounts where account_tag = $1', m.get('loser_tag'))
            except errors.NotFoundException:
                loser_id = None
            key_str = f"{w_name} ({m.get('winner_tag')}"
            if key_str not in output:
                temp = []
            else:
                temp = output[key_str]
            temp.append({
                'winner': key_str,
                'winner_id': winner_id,
                'loser_id': loser_id,
                'loser': f"{l_name} ({m.get('loser_tag')})",
                'first_request': first_request,
                'second_request': second_request,
                'diff': m.get('winner_diff')
            })
        embeds = []
        for k, v in output.items():
            title = f"{k}"
            winner_id = v[0].get('winner_id')
            if winner_id is not None:
                title += f" ID {winner_id}"
            
            embed = discord.Embed(title=title,
                                  color=discord.Color.yellow())
            for t in v:
                if t.get('winner_id') is not None and t.get('loser_id') is not None and t.get('winner_id') == t.get('loser_id'):
                    player = await Player.by_id(t.get('winner_id'))
                    embed = discord.Embed(title=title,
                                          description=f"vs {t.get('loser')}\nBoth accounts belong to the same player "
                                                      f"{discord.utils.escape_markdown(player.name)} ({player.id}).\n"
                                                      f"Attack happened between {t.get('second_request').to_discord()} and"
                                                      f" {t.get('first_request').to_discord()}.",
                                          color=discord.Color.red())
                    embeds.append(embed)
                    continue
                if len(embed.fields) == 24:
                    embeds.append(embeds)
                    embed = discord.Embed(title=title,
                                          color=discord.Color.yellow())
                embed.add_field(name=f"vs {t.get('loser')}" + (f" ID {t.get('loser_id')}" if t.get('loser_id') else ""),
                                value=f"{t.get('diff')} trophies\nAttack happened between {t.get('second_request').to_discord()} and"
                                      f" {t.get('first_request').to_discord()}.",
                                inline=False)
            embeds.append(embed)
        # sendout embeds to webhook
        async with aiohttp.ClientSession() as session:
            webhook = discord.Webhook.from_url(os.getenv('DISCORD_WEBHOOK_LB_LOG_URL'), session=session)
            # send the embeds in batches of 5
            for i in range(0, len(embeds), 5):
                await webhook.send(embeds=embeds[i:i + 5])
    


def setup(bot_client):
    bot_client.add_cog(BuilderBase(bot_client))