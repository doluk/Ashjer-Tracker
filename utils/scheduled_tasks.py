
import logging

import pathlib
import random
import time
from decimal import Decimal
from math import ceil

from coc.http import Route
from discord import PermissionOverwrite
import datetime
import asyncio
import io
import json

import os.path
import traceback
from dataclasses import dataclass
from typing import List, Literal, Optional, Union

import aiohttp
import asyncpg
import coc
import discord
import PIL

from PIL import Image
print(f'{__file__} imported at {datetime.datetime.now()}')
from . import errors, message_utils, db, gsheets
from .independent.automation import scheduler, AsyncIteratorExecutor


log_scheduled_tasks = logging.getLogger(f'ccn.utils.{__name__}')
log_scheduled_tasks.setLevel(logging.ERROR)
log_scheduled_tasks.addHandler(logging.FileHandler('Logs/scheduled_tasks.log', encoding='utf-8'))
log_scheduled_tasks.error('TEST')
HOME_HERO_ORDER = HERO_ORDER = ["Barbarian King", "Archer Queen", "Grand Warden", "Royal Champion"]

staff_override = PermissionOverwrite(read_messages=True, attach_files=True,
                                     manage_permissions=True, manage_messages=True, embed_links=True,
                                     read_message_history=True, send_messages=True)
every1_override = PermissionOverwrite(read_messages=False)
rep_override = PermissionOverwrite(read_messages=True, attach_files=True, embed_links=True)

def get_datetime(input):
    if input:
        if len(input) > 19:
            input = input[:19]
        input = datetime.datetime.strptime(input, "%Y-%m-%dT%H:%M:%S")
    return input



def pad_arr(arr: list, l: int, fill: str = None):
    """pad an array to a specified length
	Parameters
	----------
		arr: list
			the array to pad
		l: integer
			the length to pad to
		fill: string
			what to pad with

	Returns
	-------
		the padded list
	"""
    
    if len(arr) > l:
        raise TypeError(f'Too many arguments: expected at most {str(l)}, but got {str(len(arr))}')
    return arr + [fill for _ in range(l - len(arr))]


async def update_player_db():
    chnl = await bot.client.fetch_channel(1000761963711823912)
    if not chnl:
        return
    try:
        
        tags = await db.fetch("SELECT player_tag,th_level FROM player")
        playertags = [(a[0], a[1]) if isinstance(a, asyncpg.Record) else a for a in tags]
        await chnl.send(f"{len(playertags)} players in the database, starting now to update them")
        content = []
        async for playerentry in AsyncIteratorExecutor(playertags):
            try:
                player = await bot.clash_client.get_player(playerentry[0])
            except coc.errors.NotFound:
                continue
            hero_levels = [hero.level for hero in player.heroes if hero.name in HOME_HERO_ORDER]
            content.append([player.tag, [player.name, player.town_hall], pad_arr(hero_levels, 4)])
            if playerentry[1] != player.town_hall:
                await chnl.send(f'Detected possible upgrade conflict for player {player.tag}')
        
        details = content
        details = {d[0]: (d[1], d[2]) for d in details}
        results = [details.get(tag[0], ([None] * 2, [None] * 4)) for tag in playertags]
        errs = list(set([x[0] for x in playertags]) - set(details.keys()))
        updates = results
        erraneous_tags = [e for e in errs]
        # write updates back to the roster
        meta, heroes = [update for update in zip(*updates)]
        
        query = 'UPDATE player SET player_name=$1, th_level=$2 ' \
                'WHERE player_tag=$3'
        await db.execute(query, [(*meta_info, tag_info[0]) for meta_info, heroes_info, tag_info in
                                 zip(meta, heroes, playertags) if not (tag_info[0] in erraneous_tags or not
            meta_info[0])])
    except Exception:
        traceback.print_exc()
        try:
            await chnl.send(f'```py\n{traceback.format_exc()}\n```')
        except Exception:
            pass
        return
    await chnl.send(f'success {len(tags)}')
    await chnl.send(str(erraneous_tags))


async def save_result(match_id: int, war: coc.ClanWar):
    # compile and save results
    try:
        war_result = o_war_result = 'ongoing'
        if war.state == 'warEnded':
            if war.status == 'won':
                war_result = 'win'
                o_war_result = 'loss'
            elif war.status == 'lost':
                war_result = 'loss'
                o_war_result = 'win'
            elif war.status == 'tie' and war.clan.average_attack_duration < war.opponent.average_attack_duration:
                war_result = 'win'
                o_war_result = 'loss'
            elif war.status == 'tie' and war.clan.average_attack_duration > war.opponent.average_attack_duration:
                war_result = 'loss'
                o_war_result = 'win'
            elif war.status == 'tie':
                war_result = 'tie'
                o_war_result = 'tie'
            else:
                war_result = 'unknown'
                o_war_result = 'unknown'
        await db.execute('UPDATE match_results '
                         'SET stars = $2, perc=$3,duration=$4 , result = $6, attacks_used = $7 '
                         'WHERE match_id = $1 and clan_tag = $5',
                         match_id, war.clan.stars, war.clan.destruction, war.clan.average_attack_duration,
                         war.clan.tag, war_result, war.clan.attacks_used)
        await db.execute('UPDATE match_results '
                         'SET stars = $2, perc=$3,duration=$4, result = $6, attacks_used = $7  '
                         'WHERE match_id = $1 and clan_tag = $5',
                         match_id, war.opponent.stars,
                         war.opponent.destruction, war.opponent.average_attack_duration, war.opponent.tag, o_war_result,
                         war.opponent.attacks_used)
    except Exception:
        log_scheduled_tasks.error(traceback.format_exc())



async def check_cwl_wars(match_id: int, clan_tag: str, opponent_tag: str, battle_start: datetime.datetime,
                         raw_data=None):
    # try cwl spreadsheet
    match = None
    data = {}
    data_in = {}
    try:
        if not raw_data:
            raw_data = await gsheets.read('1n7NiX_70NVZ2K2d2Oz1ExBKTsLH3QL2lr2HZJPBwcDs', 'Matches!A4:K')
        match: Optional[Match] = await Match.by_id(match_id)
        # war tags index 3, 7, war start 10
        for c, d in enumerate(raw_data):
            if len(d) < 11 or not d[10] or not d[3] or not d[7]:
                continue
            try:
                match_date = datetime.datetime.strptime(d[10], "%m/%d/%Y %H:%M:%S")
                match_date_min = match_date - datetime.timedelta(minutes=15)
                match_date_max = match_date + datetime.timedelta(minutes=15)
                if clan_tag in (d[3], d[7]) and opponent_tag in (
                        d[3], d[7]) and match_date_min < battle_start < match_date_max:
                    await db.execute('UPDATE match set active = True where match_id = $1', match_id)
                    try:
                        await db.execute(
                                'INSERT INTO match_construct(match_id, event_id, tournament_id) '
                                'VALUES($1,$2, $3)',
                                match_id, 39, 53)
                    except Exception:
                        await db.execute(
                                'UPDATE match_construct set event_id = $2, tournament_id = $3 '
                                'WHERE match_id = $1', match_id, 39, 53)
                    try:
                        channel = await bot.client.fetch_channel(1122381801067909221)
                        t1: Union[Team, UnkownTeam] = await match.team_a
                        t2: Union[Team, UnkownTeam] = await match.team_b
                        logo_t1 = discord.utils.escape_markdown(t1.name)
                        logo_t2 = discord.utils.escape_markdown(t2.name)
                        t = await Event().from_id(53)
                        embed = discord.Embed(title=f"New Match {match_id} for {t.get('tournament_name')}",
                                              description=f" {logo_t1} vs {logo_t2}")
                        if t.logo_url:
                            embed.set_thumbnail(url=t.logo_url)
                        await channel.send(embed=embed)
                    except Exception:
                        traceback.print_exc()
                    return 'CWL'
            except Exception:
                traceback.print_exc()
                continue
    except Exception:
        log_scheduled_tasks.error("CWL WAR LOOKUP" + traceback.format_exc() + f'\n\n{data_in=}\n{data=}\n{match=}\
		n{match_id=}')
        try:
            channel = await bot.client.fetch_channel(1000871581658124368)
            await channel.send(embed=discord.Embed(description='```' + traceback.format_exc(limit=1990) + '```'))
        except Exception:
            pass


async def load_hit_stats(match_id: int, num_atks_loaded: int = 0, war: TrackedWar = None) -> None:
    lock = None
    match = None
    # fetch necessary info
    try:
        now = datetime.datetime.now()
        # fetch all possible clans for this match
        try:
            match = await Match().from_id(match_id)
        except Exception as e:
            log_scheduled_tasks.error(f"{type(e)}:\n{traceback.format_exc()}")
            return
        if match.scored:
            return
        try:
            [match_id_new] = await db.fetchrow(
                    'Select m.match_id from match_results mr1 join matches m on m.match_id = '
                    'mr1.match_id '
                    'JOIN match_results mr2 on m.match_id = mr2.match_id and '
                    'mr1.result_id != mr2.result_id where $1 = mr1.clan_tag and m.prep_start = $2 '
                    'and $3 = mr2.clan_tag order by m.match_id',
                    match.team_a_clantag, match.prep_start, match.team_b_clantag)
            if match_id_new != match_id:
                await db.execute('UPDATE matches set active = False where match_id = $1', match_id)
                match_id = match_id_new
                match = await Match().from_id(match_id)
                if match.scored:
                    return
        except Exception as es:
            log_scheduled_tasks.error(traceback.format_exc())
            raise es
        t1 = await match.team_a if match.team_a_id else await UnkownTeam().from_clantag(match.team_a_clantag)
        t2 = await match.team_b if match.team_b_id else await UnkownTeam().from_clantag(match.team_b_clantag)
        war_check = [match.team_a_clantag, match.team_b_clantag]
        switch = False
        still_trackable = 0
        match_end = match.prep_start + match.prep_duration + match.war_duration
        if not war:
            for c, tag in enumerate(war_check):
                try:
                    war: TrackedWar = await bot.fast_clash_client.get_clan_war(tag, cls=TrackedWar)
                except coc.errors.PrivateWarLog:
                    continue
                except coc.errors.Maintenance:
                    # retry in 5 minutes
                    next_run = now + datetime.timedelta(seconds=300)
                    scheduler.add_job(load_hit_stats, 'date', run_date=next_run, args=(match_id, num_atks_loaded),
                                      id=f'atks-{match_id}', name=f'atks-{match_id}', misfire_grace_time=600,
                                      replace_existing=True)
                    return
                if war.state != 'notInWar' and war.opponent.tag in war_check and tz.withtz(
                        war.preparation_start_time.time,
                        tz.UTC) == tz.withtz(
                        match.prep_start, tz.UTC):
                    switch = True
                    break
                if war.state != 'notInWar' and war.preparation_start_time.time > match_end:
                    still_trackable += 1 * 10 ** c
        else:
            match_id_by_war = await war.check_match_id()
            if match_id == match_id_by_war:
                switch = True
            else:
                if match.team1_clantag in (war.clan.tag, war.opponent.tag) \
                        and match.team2_clantag in (war.clan.tag, war.opponent.tag) \
                        and match.prep_start == war.preparation_start_time.time:
                    switch = True
        if not war or not switch:
            if still_trackable == 11:
                return
            raise ValueError('War not found')
        teams = []
        result_id1 = result_id2 = None
        for cln in [war.clan, war.opponent]:
            # check for unrostered players
            
            if cln.tag in [match.team_a_clantag]:
                teams.append(t1)
                team_id = t1.team_id
            elif cln.tag in [match.team_b_clantag]:
                teams.append(t2)
                team_id = t2.team_id
            else:
                team_id = None
        
        # store all attacks and result we have
        await war.save_results()
        log_scheduled_tasks.info(
                f'Saved {num_atks_loaded} attacks and score {war.clan.stars}:{war.opponent.stars} for match '
                f'{match_id}\n war end time: {war.end_time.time}\nwar state: {war.state}')
        utcnow = datetime.datetime.utcnow()
        # decide if we want to pull it every 5 seconds until the end of the war or if we reschedule it
        if war.end_time.time - datetime.timedelta(minutes=1) > utcnow:
            # the war end is in the future, reschedule function
            scheduler.add_job(load_hit_stats, 'date',
                              run_date=tz.to_naive(tz.withtz(war.end_time.time, tz.UTC).astimezone(tz.VMT)),
                              args=(match_id, num_atks_loaded), id=f'atks-{match_id}', name=f'atks-{match_id}',
                              misfire_grace_time=600, replace_existing=True, max_instances=2)
            log_scheduled_tasks.info(f'match {match_id}\nwar state: {war.state}\n war end: '
                                     f'{war.end_time.time}\ntrack_war scheduled: '
                                     f'{tz.to_naive(tz.withtz(war.end_time.time, tz.UTC).astimezone(tz.VMT))}')
            log_scheduled_tasks.info(f'track_war finished for match {match_id} with {num_atks_loaded} attacks loaded')
            return
        # the war is about to end
        war_over = war.state == 'warEnded'
        if not war_over:
            # the war is not over yet, call the api every 5 seconds and store everything
            nowutc = datetime.datetime.utcnow()
            kill_while = False
            while tz.to_naive(nowutc) < tz.to_naive(tz.withtz(war.end_time.time, tz.UTC)) + datetime.timedelta(
                    minutes=5):
                await asyncio.sleep(5)
                log_scheduled_tasks.info(f'match {match_id}\nrepeating {nowutc} {num_atks_loaded}')
                new_war = await bot.fast_clash_client.get_clan_war(war.clan.tag, realtime=True, cls=TrackedWar)
                if new_war.end_time and new_war.preparation_start_time.time == war.preparation_start_time.time:
                    war = new_war
                    if new_war.state == 'warEnded':
                        log_scheduled_tasks.info(f'war end tracked for match {match_id}\n'
                                                 f'breaking {nowutc} {war.end_time.time}')
                        kill_while = True
                else:
                    try:
                        new_war = await bot.fast_clash_client.get_clan_war(war.opponent.tag, realtime=True,
                                                                           cls=TrackedWar)
                    except coc.errors.PrivateWarLog:
                        log_scheduled_tasks.error(f'break repeating for match {match_id}\n'
                                                  f'{nowutc} {num_atks_loaded} due to private war')
                        break
                    if new_war.end_time and new_war.preparation_start_time.time == war.preparation_start_time.time:
                        war = new_war
                        if new_war.state == 'warEnded':
                            log_scheduled_tasks.info(f'war end tracked for match {match_id}\n'
                                                     f'breaking {nowutc} {war.end_time.time}')
                            kill_while = True
                    else:
                        log_scheduled_tasks.error(f'break repeating for match {match_id}\n'
                                                  f'{nowutc} {num_atks_loaded} due to other war')
                        break
                await war.save_results()
                log_scheduled_tasks.info(
                        f'Saved {num_atks_loaded} attacks and score {war.clan.stars}:{war.opponent.stars} for match '
                        f'{match_id}\n war end time: {war.end_time.time}\nwar state: {war.state}')
                if kill_while:
                    break
        if war.state != 'warEnded':
            log_scheduled_tasks.error(f'War not ended, but impossible to find {match_id}\n{war.state=}{war._raw_data=}')
            return
        log_scheduled_tasks.info(f'{match_id}\nwar end time: {war.end_time.time}\n'
                                 f'atks loaded: {num_atks_loaded}')
        events = await match.check_construct()
        
        cancelled = False
        scrim = False
        if not events:
            await db.execute('UPDATE matches SET active = FALSE WHERE match_id = $1', match_id)
        if len(war.attacks) > 10 and not events:
            scrim = True
        if len(war.attacks) < 9 and not events:
            cancelled = True
        if any([1 < max(len(m.attacks), len(m.defenses)) for m in war.members]) and not events:
            scrim = True
        if cancelled or scrim:
            await db.execute('UPDATE matches SET suitable = FALSE WHERE match_id = $1', match_id)
        await db.execute('UPDATE matches SET scored = TRUE where match_id = $1', match_id)
        await db.execute('INSERT INTO matches_to_discord(match_id, pending) VALUES($1, True) '
                         'ON CONFLICT DO NOTHING', match_id)
        log_scheduled_tasks.info(f'track_war finished for match {match_id} with {num_atks_loaded} attacks loaded')
    except Exception:
        try:
            match = await Match().from_id(match_id)
        except errors.NotFoundException:
            try:
                match_str = f'{match.team_a_clantag} vs {match.team_b_clantag} {match.prep_start}'
            except Exception:
                match_str = ''
            
            log_scheduled_tasks.error(traceback.format_exc() + match_str)
        
        match_end = match.prep_start + match.prep_duration + match.war_duration
        if match_end + datetime.timedelta(hours=48) < datetime.datetime.utcnow():
            next_run = now + datetime.timedelta(seconds=120)
            scheduler.add_job(load_hit_stats, 'date', run_date=next_run, args=(match_id, num_atks_loaded),
                              id=f'atks-{match_id}', name=f'atks-{match_id}', misfire_grace_time=600,
                              replace_existing=True)
        else:
            log_scheduled_tasks.error('STOPPED TRACKING WAR DUE TO ERROR' + traceback.format_exc() + f'{match_id=}')
            return
        log_scheduled_tasks.error(traceback.format_exc() + f'{match_id=}, {num_atks_loaded=}, {war=}')
    finally:
        if lock and isinstance(lock, asyncio.Lock) and lock.locked():
            lock.release()

async def finish_matches():
    """Finish a match"""
    log_scheduled_tasks.info('Starting match finisher')
    async with db.db.acquire() as con:
        async with con.transaction():
            try:
                matches = await con.fetch('SELECT * from matches_to_discord where pending FOR UPDATE SKIP '
                                          'LOCKED ')
            except Exception:
                log_scheduled_tasks.error(traceback.format_exc())
            if not matches:
                matches = []
            log_scheduled_tasks.info('found match finisher')
            for match in matches:
                if match and match.get('pending'):
                    match_id = match.get('match_id')
                    await con.execute('UPDATE matches_to_discord set pending = False where match_id = $1', match_id)
                    try:
                        await match_to_discord(match_id)
                    except Exception as e:
                        log_scheduled_tasks.error(traceback.format_exc()+f'{match_id=}')
                        await con.execute('UPDATE matches_to_discord set pending = True where match_id = $1', match_id)
                #try:
                #	await con.execute('DELETE From matches_to_discord where match_id = $1 and not pending', match_id)
                #except Exception:
                #	log_scheduled_tasks.error(traceback.format_exc()+f'\n{match_id=}')
                elif match and not match.get('pending'):
                    log_scheduled_tasks.info(f'Match {match_id} already finished')
#try:
#	await db.execute('DELETE From matches_to_discord where not pending')
#except Exception:
#	log_scheduled_tasks.error(traceback.format_exc() + f'\n{match_id=}')




async def match_to_discord(match_id: int):
    """Post match results to discord"""
    match = await Match().from_id(match_id)
    if not match.active or not match.scored:
        return
    t1 = await match.team_a
    t2 = await match.team_b
    # check if match belongs to a tournament
    elo_log = await bot.client.fetch_channel(1068855288133853264)
    embeds = []
    try:
        if t1.active and isinstance(t1, Team):
            elo_diff_team_a = await match.elo_change_team_a
            new_elo_a = await match.elo_team_a_after
            embed = discord.Embed(title=f"Elo change for team {t1.name}",
                                  description=f"Elo change: `{str(elo_diff_team_a).rjust(10)}`\n"
                                              f"New elo score: `{str(new_elo_a).rjust(7)}`\n"
                                              f"Match ID: `{match.id}`\n"
                                              f"Opponent: `{t2.name}`\n"
                                              f"Battle Modifier: `{match.battle_modifier}`",
                                  color=discord.Color.green() if elo_diff_team_a > 0 else discord.Color.red())
            if t1.logo_path:
                embed.set_thumbnail(url=t1.logo_url)
            embeds.append(embed)
    except Exception:
        log_scheduled_tasks.error(traceback.format_exc())
    try:
        if t2.active and isinstance(t2, Team):
            elo_diff_team_b = await match.elo_change_team_b
            new_elo_b = await match.elo_team_b_after
            embed = discord.Embed(title=f"Elo change for team {t2.name}",
                                  description=f"Elo change: `{str(elo_diff_team_b).rjust(10)}`\n"
                                              f"New elo score: `{str(new_elo_b).rjust(7)}`\n"
                                              f"Match ID: `{match.id}`\n"
                                              f"Opponent: `{t1.name}`\n"
                                              f"Battle Modifier: `{match.battle_modifier}`",
                                  color=discord.Color.green() if elo_diff_team_b > 0 else discord.Color.red())
            if t2.logo_path:
                embed.set_thumbnail(url=t2.logo_url)
            embeds.append(embed)
    except Exception:
        log_scheduled_tasks.error(traceback.format_exc())
    if embeds:
        try:
            await elo_log.send(embeds=embeds)
        except Exception as e:
            log_scheduled_tasks.error(traceback.format_exc() + f'\n{e=} {match_id=}')
    season = await match.season
    if match.battle_modifier and match.battle_modifier == 'hardMode':
        battle_mode = ' (Hard Mode)'
    else:
        battle_mode = ''
    if season:
        channel = await bot.client.fetch_channel(bot.config.match_result_output)
        image_url = None
        count = 0
        while image_url is None and count < 5:
            await generate_match_result(match_id)
            if pathlib.Path(f'image_cache/{season.id}/match_result_{match_id}.png').exists():
                image_url = f'https://competitiveclash.network/results/{season.id}/match_result_{match_id}.png'
            else:
                await asyncio.sleep(1)
                count += 1
        match = await Match().from_id(match_id)
        winner = await match.winner
        loser = await match.loser
        if winner and isinstance(winner, str):
            winner = await match.team_a
            loser = await match.team_b
        elo_diff_team_a = await match.elo_change_team_a
        elo_diff_team_b = await match.elo_change_team_b
        elo_team_a = await match.elo_team_a_before
        elo_team_b = await match.elo_team_b_before
        if elo_diff_team_a > 0:
            elo_diff_team_a = f'{bot.custom_emojis.plus}{abs(elo_diff_team_a)})'
        elif elo_diff_team_a < 0:
            elo_diff_team_a = f'{bot.custom_emojis.minus}{abs(elo_diff_team_a)})'
        else:
            elo_diff_team_a = f'{bot.custom_emojis.neutral}{abs(elo_diff_team_a)})'
        if elo_diff_team_b > 0:
            elo_diff_team_b = f'{bot.custom_emojis.plus}{abs(elo_diff_team_b)})'
        elif elo_diff_team_b < 0:
            elo_diff_team_b = f'{bot.custom_emojis.minus}{abs(elo_diff_team_b)})'
        else:
            elo_diff_team_b = f'{bot.custom_emojis.neutral}{abs(elo_diff_team_b)})'
        if not isinstance(winner, str) and winner.id == match.team_b_id:
            winner_elo_change = (f"({elo_team_b}" + elo_diff_team_b) if match.team_b_id else "(Not registered)"
            loser_elo_change = (f"({elo_team_a}" + elo_diff_team_a) if match.team_a_id else "(Not registered)"
        else:
            winner_elo_change = (f"({elo_team_a}" + elo_diff_team_a) if match.team_a_id else "(Not registered)"
            loser_elo_change = (f"({elo_team_b}" + elo_diff_team_b) if match.team_b_id else "(Not registered)"
        if winner == '-':
            winner = t1
            loser = t2
        
        embed = discord.Embed(title=f"Match {match_id}{battle_mode}",
                              description=f"{winner_elo_change} **"
                                          f"{discord.utils.escape_markdown(winner.name)}** vs "
                                          f"**{discord.utils.escape_markdown(loser.name)}** "
                                          f"{loser_elo_change}",
                              colour=discord.Color.dark_orange())
        try:
            events = await db.fetch('SELECT * from match_construct_new mc '
                                    'JOIN constructs t on mc.construct_id = t.construct_id '
                                    'WHERE match_id = $1', match.id)
        except errors.NotFoundException:
            events = []
        embed_thumbnail = False
        for event in events:
            t = await Event().from_id(event.get('construct_id'))
            embed.add_field(name="Match related to Event",
                            value=discord.utils.escape_markdown(t.name))
            if t.logo_url and not embed_thumbnail:
                embed.set_thumbnail(url=t.logo_url)
        embed.set_image(url=image_url)
        msg1 = await channel.send(embed=embed)
        try:
            await msg1.publish()
        except Exception:
            pass
    try:
        [event_channel] = await db.fetchrow('SELECT result_channel from constructs c join match_construct_new mc on '
                                            'c.construct_id = mc.construct_id where match_id = $1', match_id)
        if event_channel:
            event_channel = await bot.client.fetch_channel(event_channel)
            image_url = None
            counts = 0
            while image_url is None and counts < 5:
                if match.season_id and pathlib.Path(f'image_cache/{season.id}/match_result_{match_id}.png').exists():
                    image_url = f'https://competitiveclash.network/results/{season.id}/match_result_{match_id}.png'
                elif not match.season_id and pathlib.Path(f'image_cache/{1}/match_result_{match_id}.png').exists():
                    image_url = f'https://competitiveclash.network/results/1/match_result_{match_id}.png'
                elif not match.season_id and not pathlib.Path(f'image_cache/{1}/match_result_{match_id}.png').exists():
                    await generate_match_result(match_id, 1)
                    image_url = f'https://competitiveclash.network/results/1/match_result_{match_id}.png'
                elif match.season_id and not pathlib.Path(f'image_cache/{season.id}/match_result_{match_id}.png').exists():
                    await generate_match_result(match_id)
                    image_url = f'https://competitiveclash.network/results/{match.season_id}/match_result_{match_id}.png'
                if not pathlib.Path(f'/root/ccn_bot/image_cache'
                                    f'/{match.season_id if match.season_id else 1}/match_result'
                                    f'_{match_id}.png').exists():
                    image_url = None
                    await asyncio.sleep(1)
                    counts += 1
            
            
            
            match = await Match().from_id(match_id)
            winner = await match.winner
            loser = await match.loser
            if winner and  isinstance(winner, str):
                winner = await match.team_a
                loser = await match.team_b
            elo_diff_team_a = await match.elo_change_team_a
            elo_diff_team_b = await match.elo_change_team_b
            elo_team_a = await match.elo_team_a_before
            elo_team_b = await match.elo_team_b_before
            if elo_diff_team_a > 0:
                elo_diff_team_a = f'{bot.custom_emojis.plus}{abs(elo_diff_team_a)})'
            elif elo_diff_team_a < 0:
                elo_diff_team_a = f'{bot.custom_emojis.minus}{abs(elo_diff_team_a)})'
            else:
                elo_diff_team_a = f'{bot.custom_emojis.neutral}{abs(elo_diff_team_a)})'
            if elo_diff_team_b > 0:
                elo_diff_team_b = f'{bot.custom_emojis.plus}{abs(elo_diff_team_b)})'
            elif elo_diff_team_b < 0:
                elo_diff_team_b = f'{bot.custom_emojis.minus}{abs(elo_diff_team_b)})'
            else:
                elo_diff_team_b = f'{bot.custom_emojis.neutral}{abs(elo_diff_team_b)})'
            if winner.id == match.team_b_id:
                winner_elo_change = (
                        f"({elo_team_b}" + elo_diff_team_b) if match.team_b_id else "(Not registered)"
                loser_elo_change = (
                        f"({elo_team_a}" + elo_diff_team_a) if match.team_a_id else "(Not registered)"
            else:
                winner_elo_change = (
                        f"({elo_team_a}" + elo_diff_team_a) if match.team_a_id else "(Not registered)"
                loser_elo_change = (
                        f"({elo_team_b}" + elo_diff_team_b) if match.team_b_id else "(Not registered)"
            embed = discord.Embed(title=f"Match {match_id}{battle_mode}",
                                  description=f"{winner_elo_change} **"
                                              f"{discord.utils.escape_markdown(winner.name)}** vs "
                                              f"**{discord.utils.escape_markdown(loser.name)}** "
                                              f"{loser_elo_change}",
                                  colour=discord.Color.dark_orange())
            try:
                events = await db.fetch('SELECT * from match_construct_new mc '
                                        'JOIN constructs t on mc.construct_id = t.construct_id '
                                        'WHERE match_id = $1', match.id)
            except errors.NotFoundException:
                events = []
            embed_thumbnail = False
            for event in events:
                t = await Event().from_id(event.get('construct_id'))
                embed.add_field(name="Match related to Event",
                                value=discord.utils.escape_markdown(t.name))
                if t.logo_url and not embed_thumbnail:
                    embed.set_thumbnail(url=t.logo_url)
            if image_url:
                embed.set_image(url=image_url)
            msg1 = await event_channel.send(embed=embed)
    except errors.NotFoundException:
        pass
    try:
        await run_match_scored(match_id)
    except Exception:
        log_scheduled_tasks.error(traceback.format_exc())


async def read_rush_stuff(event_id, ctx):
    team_id_mapping = {}
    team_names = []
    team_name_id = {}
    with open("clash_mayhem.txt") as file:
        for line in file.readlines():
            if line.lower().startswith('username'):
                continue
            line = line.strip().replace('\n', '')
            tmp = line.split('|#')
            name = tmp[0].strip()
            tag = coc.utils.correct_tag(tmp[1].strip())
            
            try:
                [team_id] = await db.fetchrow(
                        "SELECT t.team_id from team_clan tc JOIN team t on tc.team_id = "
                        "t.team_id where clantag = $1",
                        tag)
            except errors.NotFoundException:
                try:
                    [team_id] = await db.fetchrow("SELECT team_id from team where team_name ilike $1 LIMIT 1",
                                                  name)
                except Exception:
                    print(f'{name=} {tag=}')
                    continue
            await db.execute('INSERT INTO clash_mayhem_teams(team_id, clantag, api_name) VALUES($1,$2,$3)',
                             team_id, tag, name)
    
    file.close()

async def get_rosters(ctx, sg_id, qualifier_id):
    
    async def process_roster(roster):
        id = roster.get("id")
        name = roster.get("name")
        externalIDS = roster.get("externalIds", [])
        if not externalIDS:
            log_scheduled_tasks.error(f'No external ID found for {name}')
            return
        tag = externalIDS[0].get('value')
        try:
            [roster_team_id] = await db.fetchrow('SELECT team_id from qualifiers_2024_teams where clan_tag = $1 and '
                                                 'qualifier_id = $2 and api_id = $3',
                                                 tag, qualifier_id, id)
        except errors.NotFoundException:
            roster_team_id = None
        if roster_team_id:
            await db.execute('INSERT INTO construct_teams(construct_id, team_id) VALUES(1692, $1) ON CONFLICT DO '
                             'NOTHING',
                             roster_team_id)
        try:
            [team_id_by_clan] = await db.fetchrow('SELECT team_id from team_clan where clantag = $1', tag)
        except errors.NotFoundException:
            team_id_by_clan = None
        if not team_id_by_clan:
            try:
                [team_id_by_name] = await db.fetchrow('SELECT team_id from team where team_name = $1', name)
            except errors.NotFoundException:
                team_id_by_name = None
            if not team_id_by_name:
                [team_id] = await db.fetchrow('INSERT INTO team(team_name, active, has_image) '
                                              'VALUES($1, True, False) RETURNING team_id', name)
                await db.execute('INSERT INTO elo_transaction(team_id, reason, elo_change, datetime) VALUES ($1, $2, '
                                 '1500, $3)', team_id, 'Initial', datetime.datetime.utcnow() - datetime.timedelta(days=1))
                if team_id:
                    team_id_by_clan = team_id
            else:
                team_id_by_clan = team_id_by_name
        try:
            await db.execute('INSERT INTO team_clan(team_id, clantag) VALUES($1,$2)', team_id_by_clan, tag)
        except asyncpg.exceptions.UniqueViolationError:
            pass
        except Exception as e:
            log_scheduled_tasks.error(traceback.format_exc())
            raise e
        try:
            await db.execute('UPDATE team set active = true where team_id = $1', team_id_by_clan)
        except Exception as e:
            log_scheduled_tasks.error(traceback.format_exc())
            raise e
        try:
            [roster_team_id] = await db.fetchrow('SELECT team_id from qualifiers_2024_teams where clan_tag = $1 and '
                                                 'qualifier_id = $2 and api_id = $3',
                                                 tag, qualifier_id, id)
        except errors.NotFoundException:
            roster_team_id = None
        if team_id_by_clan and not roster_team_id:
            try:
                await db.execute('INSERT INTO qualifiers_2024_teams(qualifier_id, team_id, clan_tag, '
                                 'api_name, api_id) '
                                 'VALUES($1,'
                                 '$2,$3, $4, $5)', qualifier_id, team_id_by_clan, tag, name, id)
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc())
                raise e
        elif roster_team_id:
            await db.execute('UPDATE qualifiers_2024_teams set api_id = $1, api_name = $2, team_id = $5 '
                             'where clan_tag = $3 and qualifier_id = $4', id,
                             name, tag, qualifier_id, team_id_by_clan)
    
    last = current = 1
    url = (f"https://cc-api.athlos.gg/api/rosters/1.0/rosters?filter["
           f"segment]={sg_id}&filter[status]=ready&page[size]=50&sort=name&page[number]=")
    async with aiohttp.ClientSession(headers={'X-Origin': f'supercell/clashofclans/season4/{qualifier_id}'}) as session:
        while current <= last:
            async with session.get(url + f'{current}') as response:
                try:
                    data = await response.json()
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                    data = None
                rosters = data['data']
                meta = data['meta']['page']
                await ctx.channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
                current = meta.get('number') + 1
                last = meta.get("last") + 1
                for roster in rosters:
                    await process_roster(roster)
    await ctx.channel.send('finished')



async def get_standings(channel = None):
    qualifier_id = 'july'
    url = ("https://cc-api.athlos.gg/api/standings/1.0/segments/f8c68292-3253-11ef-b666-0242ac1d0004/standings"
           "?page[size]=200&sort=placement&page[number]=")
    current = 1
    pages = -1
    async with aiohttp.ClientSession(headers={'X-Origin': f'supercell/clashofclans/season4/{qualifier_id}'}) as session:
        while current <= pages or pages == -1:
            async with session.get(url + f'{current}') as response:
                try:
                    data = await response.json()
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                    data = None
                now = datetime.datetime.utcnow()
                rosters = data['data']
                meta = data['meta']['page']
                if channel:
                    await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
                else:
                    log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
                pages = (meta.get("last", 0) or 0) + 1
                for team in rosters:
                    rank = team.get("placement")
                    matches = team.get("matches")
                    played = matches.get('played')
                    won = (matches.get('won', 0) or 0) + (matches.get('forfeitWon', 0) or 0)
                    lost = (matches.get('lost', 0) or 0) + (matches.get('forfeitLost', 0) or 0)
                    tied = matches.get('tied', 0)
                    points = team.get("points", {}).get("amount")
                    id = team.get('roster', {}).get('id')
                    name = team.get('roster', {}).get('name')
                    periode = json.dumps(team.get("periodsStats",{}))
                    try:
                        [team_id] = await db.fetchrow('SELECT team_id from qualifiers_2024_teams '
                                                      'where api_id = $1', id)
                    except errors.NotFoundException:
                        try:
                            [team_id] = await db.fetchrow('SELECT team_id from qualifiers_2024_teams '
                                                          'where api_name = $1 and qualifier_id = $2', name,
                                                          qualifier_id)
                        except errors.NotFoundException:
                            if channel:
                                await channel.send(f'{id=} {name=} {matches=} {points=}')
                            log_scheduled_tasks.error(f'{id=} {name=} {matches=} {points=}\n' + traceback.format_exc())
                            continue
                    await db.execute('insert into qualifiers_2024_standings '
                                     'values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
                                     now, team_id, rank, played, won, lost, tied, points, id,
                                     periode, qualifier_id)
    if channel:
        await channel.send('finished')




async def generate_qualified(team_id: int, event_id: int, c_matches: int = 6, only_wins: bool = True):
    if c_matches not in [3,5,6]:
        raise ValueError(f'{c_matches} not supported')
    
    team = await Team().from_id(team_id)
    event = await Event().from_id(event_id)
    data = {'event_name': event.name, 'team_logo': team.logo_path if team.logo_path else
    "assets/teamlogos/default.png"}
    try:
        matches = await db.fetch('SELECT mc.match_id, team.stars as t_stars, team.perc as t_perc, team.duration as '
                                 't_duration, '
                                 'opp.stars as o_stars, opp.perc as o_perc, opp.duration as o_duration, opp.team_id as '
                                 'o_team_id, '
                                 'get_team_elo('
                                 'opp.team_id, now()::timestamp) as opp_elo from match_construct mc join '
                                 'match_result team on team.match_id = mc.match_id '
                                 'and team.team_id = $2 JOIN match_result opp on opp.match_id = mc.match_id and '
                                 'opp.team_id != $2 '
                                 ' where '
                                 "mc.event_id = $1 and CASE WHEN $3 = 'only_wins' THEN team.result = 'win' ELSE True END "
                                 "ORDER BY opp_elo DESC, team.stars-opp.stars DESC, team.perc-opp.perc DESC LIMIT $4",
                                 event_id, team_id, 'only_wins' if only_wins else '', c_matches)
    except errors.NotFoundException:
        matches = []
    if len(matches) != c_matches:
        for o in [3,5,6]:
            if len(matches) <= o:
                c_matches = o
                break
    template_name = f'top_{c_matches}_matches'
    for i in range(len(matches), c_matches):
        matches.append({})
    for i, m in enumerate(matches):
        if m.get('o_team_id'):
            opp = await Team().from_id(m.get('o_team_id'))
            opp_name = opp.name
            if opp.logo_path:
                opp_logo = opp.logo_path
            else:
                opp_logo = "assets/teamlogos/default.png"
        elif not m.get('o_team_id') and m.get('t_stars'):
            opp_logo = "assets/teamlogos/default.png"
            [opp_name] = await db.fetchrow('SELECT clan_name from lineup_clan where match_id = $1 and team_id != $2',
                                           m.get('match_id'), team_id)
        else:
            opp_logo = opp_name = None
        data[f'logo_team{i+1}'] = data['team_logo'] if m.get('t_stars') else None
        data[f'name_team{i+1}'] = team.name if m.get('t_stars') else None
        data[f'logo_opp{i+1}'] = opp_logo
        data[f'name_opp{i+1}'] = opp_name
        data[f'stars_team{i+1}'] = str(m.get('t_stars')) if m.get('t_stars') else None
        data[f'perc_team{i + 1}'] = (str(round(m.get('t_perc'),2)) + '%') if m.get('t_perc') else None
        data[f'duration_team{i + 1}'] = f'{(m.get("t_duration",0) or 0)//60}:{(m.get("t_duration",0) or 0)%60}' if m.get('t_duration') else None
        data[f'stars_opp{i + 1}'] = str(m.get('o_stars')) if m.get('o_stars') else None
        data[f'perc_opp{i + 1}'] = (str(round(m.get('o_perc'),2)) + '%') if m.get('o_perc') else None
        data[f'duration_opp{i + 1}'] = f'{(m.get("o_duration", 0) or 0) // 60}:{(m.get("o_duration", 0) or 0) % 60}' if m.get('o_duration') else None
    
    im = await gfx.create_graphic(template_name, data)
    return im


async def generate_worlds_h2h(ctx):
    world_teams = [2, 9, 11, 33, 49, 80, 147]
    for x in world_teams:
        team = await Team().from_id(x)
        data = {'event_name': 'World Finalists', 'team_logo': team.logo_path if team.logo_path else
        "assets/teamlogos/default.png"}
        try:
            matches = await db.fetch("""SELECT
			mr2.team_id as o_team_id,
			t.team_name as o_name,
       count(*) filter ( where mr1.result = 'win' ) as t_stars,
       count(*) filter ( where mr2.result = 'win' ) as o_stars,
       round(avg(mr1.stars),2) as t_perc,
       round(avg(mr2.stars),2) as o_perc,
       round(avg(mr1.perc),2) as t_duration,
       round(avg(mr2.perc),2) as o_duration
from match m
JOIN match_result mr1 on m.match_id = mr1.match_id and mr1.team_id = $1
JOIN match_result mr2 on m.match_id = mr2.match_id and mr2.team_id != $1
JOIN team t on mr2.team_id = t.team_id
where mr2.team_id != $1 and m.active and m.scored and m.team_id1 in (2, 9, 11, 33, 49, 80, 147) and m.team_id2 in (2, 9, 11, 33, 49, 80, 147)
group by mr2.team_id, t.team_name order by t.team_name;""",
                                     team.team_id)
        except errors.NotFoundException:
            matches = []
        template_name = f'worlds_h2h'
        for i, m in enumerate(matches):
            if m.get('o_team_id'):
                opp = await Team().from_id(m.get('o_team_id'))
                opp_name = opp.name
                if opp.logo_path:
                    opp_logo = opp.logo_path
                else:
                    opp_logo = "assets/teamlogos/default.png"
            else:
                opp_logo = opp_name = None
            data[f'logo_team{i+1}'] = data['team_logo'] if m.get('t_stars') else None
            data[f'name_team{i+1}'] = team.name if m.get('t_stars') else None
            data[f'logo_opp{i+1}'] = opp_logo
            data[f'name_opp{i+1}'] = opp_name
            data[f'stars_team{i+1}'] = str(m.get('t_stars')) if m.get('t_stars') else None
            data[f'perc_team{i + 1}'] = (str(round(m.get('t_perc'),2))) if m.get('t_perc') else None
            data[f'duration_team{i + 1}'] = f'{m.get("t_duration")}'
            data[f'stars_opp{i + 1}'] = str(m.get('o_stars')) if m.get('o_stars') else None
            data[f'perc_opp{i + 1}'] = (str(round(m.get('o_perc'), 2))) if m.get('o_perc') else None
            data[f'duration_opp{i + 1}'] = f'{m.get("o_duration")}'
        
        im = await gfx.create_graphic(template_name, data)
        with io.BytesIO() as img_binary:
            im.save(img_binary, 'PNG')
            img_binary.seek(0)
            file = discord.File(img_binary,
                                filename=f'{template_name}-{team.name}.png')
            await ctx.respond(file=file)
            im.close()



async def process_match(data: dict, channel = None, stop_iter: bool = False ):
    api_id = data.get("id")
    created_at = get_datetime(data.get("createdAt", ""))
    match_date = get_datetime(data.get("date", ""))
    comps = data.get("competitors",[])
    comp_data = {}
    clantags = []
    war_duration = datetime.timedelta(minutes=30)
    prep_duration = datetime.timedelta(minutes=5)
    for comp in comps:
        temp = {}
        comp_id = comp.get("id", "")
        tmp = comp.get("roster", {})
        temp['api_id'] = tmp.get("id")
        temp['api_name'] = tmp.get("name")
        temp['comp_id'] = comp_id
        try:
            [team_id, clantag, api_name] = await db.fetchrow('SELECT team_id, clantag, api_name from '
                                                             'clash_mayhem_teams where '
                                                             'api_id = '
                                                             '$1', tmp.get('id'))
        except Exception as e:
            raise e
        temp['team_id'] = team_id
        temp['tag'] = clantag
        temp['name'] = api_name
        clantags.append(clantag)
        comp_data[comp_id] = temp
    # print(comp_data)
    if len(comp_data) > 2:
        log_scheduled_tasks.error('too much competitiors' + json.dumps(data))
        return
    matches = data.get('matches')
    if len(matches) == 0 and not stop_iter:
        await get_old_matches_match(api_id, channel, stop_iter=True)
    if len(matches) == 0:
        return
    if not matches:
        log_scheduled_tasks.error('too many matches' + json.dumps(data))
        return
    for m in matches:
        prep_start = get_datetime(m.get('createdAt'))
        periods = m.get('periods')
        for p in periods:
            attr = p.get("name")
            if attr == 'destruction':
                attr = 'perc'
            for c in p.get("competitors"):
                comp_data[c.get("id")][attr] = c.get('result', {}).get('score', 0) or 0
        # check for entry in clash_mayhem
        [t1, t2] = comp_data.values()
        try:
            await db.fetchrow('SELECT clash_id from qualifiers_2024_matches where clash_id = $1', api_id)
        except errors.NotFoundException:
            # doesnt exist, create one
            await db.execute('INSERT INTO qualifiers_2024_matches(updated_at, match_series_id, created_at, match_date, '
                             'clan1_tag, clan2_tag) VALUES ($1, $2, $3, $4, $5, $6)',
                             created_at+datetime.timedelta(minutes=35), api_id, created_at, match_date, *clantags)
        # check for match
        try:
            match = await db.fetchrow("SELECT * from match where $1 in (clantag_team1, clantag_team2) and $2 "
                                      "in (clantag_team1, clantag_team2) and $3 between prep_start - interval '5 "
                                      "min' and prep_start + interval '5 min'",
                                      t1['tag'], t2['tag'], prep_start)
            match_id = match.get('match_id')
            # deal with the special case where the match already exists but has no score!
            if match.get("stars_team1", 0) and match.get("stars_team2", 0) and \
                    match.get("duration_team1", 0) == 0 and match.get("duration_team2", 0):
                await db.execute('UPDATE match set stars_team1 = $2, stars_team2 = $3, perc_team1 = $4, perc_team2 = '
                                 '$5, duration_team1 = $6, duration_team2 = $7, active = true, scored = true where '
                                 'match_id = $1',
                                 match_id, t1['stars'], t2['stars'], t1['perc'], t2['perc'], t1['duration'],
                                 t2['duration'])
            else:
                if not stop_iter:
                    await get_old_matches_match(api_id, channel, stop_iter=True)
                return
        except errors.NotFoundException:
            # match entry doesnt exist, create it
            
            # check for listed teams
            try:
                [season_id] = await db.fetchrow('SELECT stw.season_id from season_teamtw stw join season s on '
                                                'stw.season_id = s.season_id where s.season_start <= $1 and s.season_end '
                                                '> $1 and team_id in ($2,$3)',
                                                prep_start, t1['team_id'], t2['team_id'])
            except errors.NotFoundException:
                try:
                    [season_id] = await db.fetchrow('SELECT stw.season_id from season_ranking stw join season s on '
                                                    'stw.season_id = s.season_id where s.season_start <= $1 and s.season_end '
                                                    '> $1 and team_id in ($2,$3)',
                                                    prep_start, t1['team_id'], t2['team_id'])
                except errors.NotFoundException:
                    season_id = None
            try:
                [match_id] = await db.fetchrow('INSERT INTO match(season_id, team_id1, clantag_team1, stars_team1,'
                                               ' perc_team1, duration_team1, team_id2, clantag_team2, stars_team2, perc_team2,'
                                               ' duration_team2, prep_start, prep_duration, war_duration, active, '
                                               'scored) VALUES ($1, $2, $3,$4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, '
                                               '$15, $16) RETURNING match_id',
                                               season_id, t1['team_id'], t1['tag'], t1['stars'], t1['perc'], t1['duration'],
                                               t2['team_id'], t2['tag'], t2['stars'], t2['perc'], t2['duration'], prep_start,
                                               prep_duration, war_duration, True, True)
            except Exception as e:
                log_scheduled_tasks.error(f'KEY ERROR STARS {api_id=} {t1=} {t2=} {prep_start=} {created_at=} {data=}'
                                          f'{traceback.format_exc()}')
                continue
        
        # insert match_construct
        await db.execute('INSERT INTO match_construct(match_id, event_id, tournament_id) VALUES ($1,$2,$3) '
                         'ON CONFLICT DO NOTHING ', match_id, 50, 91)
        # make lineup entry
        for t in [t1, t2]:
            await db.execute('INSERT into lineup_clan VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING ',
                             match_id, t['tag'], t['api_name'], t['team_id'])
        try:
            if t1['stars'] == t2['stars'] == 0:
                log_scheduled_tasks.error(f'STARS ARE 0 \n{api_id=} {t1=} {t2=} {match_id=} {prep_start=}'
                                          f' {created_at=}')
            else:
                # make sure match is active
                await db.execute('UPDATE match set active = True and scored = True where match_id = $1 and prep_start + '
                                 "prep_duration + war_duration + interval '5 minutes' < now()",
                                 match_id)
        except KeyError:
            log_scheduled_tasks.error(f'KEY ERROR STARS {api_id=} {t1=} {t2=} {match_id=} {prep_start=} {created_at=}')
        await db.execute('UPDATE clash_mayhem set match_id = $1 where match_series_id = $2', match_id, api_id)

async def get_stage2_teams(ctx):
    url = ("https://cc-api.gfinityesports.com/api/rosters/1.0/rosters?filter[segment]=e1533324-03b7-11ee-8272-0242ac140004"
           "&page[size]=100&sort=name")
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={'X-Origin': 'supercell/clashofclans/season3'}) as response:
            try:
                data = await response.json()
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                data = {}
                raise e
            rosters = data.get('data', [])
            await ctx.channel.send(f'{len(rosters)} teams found')
            for team in rosters:
                api_id = team.get('id')
                api_name = team.get('name')
                clantag = team.get('externalIds',[{}])[0].get('value','')
                if not clantag or not api_name or not api_id:
                    await ctx.channel.send(embed=discord.Embed(title=f'Team info error',description=f'{api_id=}\n'
                                                                                                    f'{api_name=}\n'
                                                                                                    f'{clantag=}'))
                    continue
                try:
                    t = await Team().from_metadata(clantag)
                except errors.NotFoundException:
                    await ctx.channel.send(embed=discord.Embed(title=f'Team id error', description=f'{api_id=}\n'
                                                                                                   f'{api_name=}\n'
                                                                                                   f'{clantag=}'))
                    continue
                await db.execute('INSERT INTO clash_mayhem_teams VALUES ($1,$2,$3,$4)', t.id, clantag,api_id,api_name)
                await db.execute('INSERT INTO event_team VALUES ($1,$2,$3)', 52, t.id, clantag)

async def get_old_matches(channel = None, start: int = 1):
    url = ("https:///cc-api.athlos.gg/api/matches/1.0/match-series?filter["
           "segment]=f8c68292-3253-11ef-b666-0242ac1d0004&page[size]=100&sort=-date&filter[state]=finished&page["
           "number]=")
    async with (aiohttp.ClientSession() as session):
        async with session.get(url + f'{start}', headers={'X-Origin': 'supercell/clashofclans/season4/july'}) as response:
            try:
                data = await response.json()
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                data = None
            now = datetime.datetime.utcnow()
            rosters = data['data']
            meta = data['meta']['page']
            if channel:
                await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
            else:
                log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
            last = meta.get("last") + 1
            for team in rosters:
                await process_match(team)
        for i in range(start +1, last):
            async with session.get(url + f'{i}',
                                   headers={'X-Origin': 'supercell/clashofclans/season4/july'}) as response:
                try:
                    data = await response.json()
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                    data = None
                    continue
                now = datetime.datetime.utcnow()
                try:
                    rosters = data['data']
                    meta = data['meta']['page']
                    if channel:
                        await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
                    else:
                        log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
                except Exception as e:
                    traceback.print_exc()
                    log_scheduled_tasks.error(str(data) + '\n' + traceback.format_exc())
                    raise e
                for team in rosters:
                    await process_match(team)
    await channel.send('finished')

async def get_old_matches_team(team_api_id, channel = None, start: int = 1):
    url = ("https://cc-api.gfinityesports.com/api/matches/1.0/match-series?filter["
           f"segment]=ce69197a-03b4-11ee-89d7-0242ac140004&filter[roster]={team_api_id}&page[size]=100&sort=-date&filter["
           "state]=finished&page["
           "number]=")
    async with aiohttp.ClientSession() as session:
        async with session.get(url + f'{start}', headers={'X-Origin': 'supercell/clashofclans/season3'}) as response:
            try:
                data = await response.json()
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                data = None
            now = datetime.datetime.utcnow()
            rosters = data['data']
            meta = data['meta']['page']
            if channel:
                await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
            else:
                log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
            last = meta.get("last") + 1
            for team in rosters:
                await process_match(team, channel)
        for i in range(start +1, last):
            async with session.get(url + f'{i}', headers={'X-Origin': 'supercell/clashofclans/season3'}) as response:
                try:
                    data = await response.json()
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                    data = None
                    continue
                now = datetime.datetime.utcnow()
                try:
                    rosters = data['data']
                    meta = data['meta']['page']
                    if channel:
                        await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
                    else:
                        log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
                except Exception as e:
                    traceback.print_exc()
                    log_scheduled_tasks.error(str(data) + '\n' + traceback.format_exc())
                    raise e
                for team in rosters:
                    await process_match(team, channel)
    await channel.send('finished')


async def get_old_matches_match(team_api_id, channel = None, start: int = 1, stop_iter: bool = True):
    url = ("https://cc-api.gfinityesports.com/api/matches/1.0/match-series?filter["
           f"segment]=ce69197a-03b4-11ee-89d7-0242ac140004&filter[id]={team_api_id}&page[size]=100&sort=-date&filter["
           "state]=finished&page["
           "number]=")
    async with aiohttp.ClientSession() as session:
        async with session.get(url + f'{start}', headers={'X-Origin': 'supercell/clashofclans/season3'}) as response:
            try:
                data = await response.json()
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                data = None
            now = datetime.datetime.utcnow()
            rosters = data['data']
            meta = data['meta']['page']
            if channel:
                await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
            else:
                log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
            last = meta.get("last") + 1
            for team in rosters:
                await process_match(team, channel, stop_iter)
        for i in range(start +1, last):
            async with session.get(url + f'{i}', headers={'X-Origin': 'supercell/clashofclans/season3'}) as response:
                try:
                    data = await response.json()
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n')
                    data = None
                    continue
                now = datetime.datetime.utcnow()
                try:
                    rosters = data['data']
                    meta = data['meta']['page']
                    if channel:
                        await channel.send(f'Requested page {meta.get("number")}/{meta.get("last")}')
                    else:
                        log_scheduled_tasks.warning(f'Requested page {meta.get("number")}/{meta.get("last")}')
                except Exception as e:
                    traceback.print_exc()
                    log_scheduled_tasks.error(str(data) + '\n' + traceback.format_exc())
                    raise e
                for team in rosters:
                    await process_match(team,channel, stop_iter)
    await channel.send('finished')




async def test_elo_table(ctx):
    data = []
    title = ""
    with open("elo_table.csv") as file:
        for line in file.readlines():
            if not title:
                title = line.strip().replace('\n', '')
                continue
            line = line.strip().replace('\n', '')
            tmp = line.split(';')
            weight = 1
            [elo_winner, elo_loser, winner_stars, loser_stars, winner_perc, loser_perc, elo_change_winner,
             elo_change_loser] = tmp
            winner_stars = int(winner_stars)
            loser_stars = int(loser_stars)
            elo_winner = int(elo_winner)
            elo_loser = int(elo_loser)
            winner_perc = int(winner_perc) if winner_perc else 0
            loser_perc = int(loser_perc) if loser_perc else 0
            d_stars = winner_stars - loser_stars
            d_perc = (winner_perc - loser_perc) / 100
            [app_score_winner] = await db.fetchrow("SELECT get_scaling_factor_v3($1,$2,$3,$4)::numeric as app_score",
                                                   d_stars, loser_stars, d_perc, True)
            [app_score_loser] = await db.fetchrow("SELECT get_scaling_factor_v3($1,$2,$3,$4)::numeric as app_score",
                                                  d_stars, loser_stars, d_perc, False)
            exp_score_winner = 1 / (1 + pow(1.85, (elo_loser - elo_winner) / 20))
            exp_score_loser = 1 / (1 + pow(1.85, (elo_winner - elo_loser) / 20))
            elo_change_winner = round((float(app_score_winner) - float(exp_score_winner)) * weight * 10)
            elo_change_loser = round((float(app_score_loser) - float(exp_score_loser)) * weight * 10)
            data.append(f", ".join([str(x) for x in [elo_winner, elo_loser, winner_stars, loser_stars, winner_perc,
                                                     loser_perc, elo_change_winner, elo_change_loser]]))
    file.close()
    content = title + "\n" + "\n".join(data)
    stream = io.BytesIO(content.encode('utf-8'))
    await ctx.respond('here is your result', file=discord.File(stream, filename='result.csv'))

async def elo_decay(today: datetime.datetime = None):
    default = 1500
    if not today:
        today = datetime.datetime.utcnow()
        today = today.replace(minute=59, hour=23, second=59, microsecond=0)
    try:
        teams = await db.fetch("SELECT t.team_id, ($1 - max(m.prep_start))::interval as since_last from "
                               "team t "
                               "LEFT OUTER join match_results mr on "
                               "t.team_id = mr.team_id "
                               "LEFT OUTER JOIN matches m on m.match_id = mr.match_id group by t.team_id having count("
                               "*) filter ("
                               "where m.prep_start > $1 - interval '30 days') = 0", today)
    except errors.NotFoundException:
        return
    except Exception as e:
        raise e
    for team in teams:
        [elo] = await db.fetchrow('SELECT get_team_elo($1, $2)', team.get('team_id'), today)
        if elo == default:
            continue
        elo_change = round(float(elo - default) * float(0.05))
        if elo_change == 0:
            elo_change = 1 if elo > default else -1
        await db.execute('INSERT INTO elo_transaction (team_id, datetime, elo_change, reason) '
                         'VALUES ($1, $2, $3, $4) ON CONFLICT ON Constraint elo_transaction_pkey DO UPDATE set elo_change = $3, '
                         'reason = $4 ',
                         team.get('team_id'),
                         today - datetime.timedelta(
                                 minutes=1),
                         -elo_change, f'elo decay from {elo} to {default} (day {team.get("since_last")})')

async def elo_rank_history():
    end_time = datetime.datetime.utcnow()
    now = end_time - datetime.timedelta(days=1)
    now = now.replace(minute=59, hour=23, second=59, microsecond=0)
    while now < end_time:
        await elo_decay(now)
        teams = await db.fetch("""WITH data as (SELECT et.team_id,
	                     get_team_elo(et.team_id, $1) as elo,
	                     get_team_elo_classic(et.team_id, $1) as elo_classic
	              FROM elo_transaction et join team t on t.team_id = et.team_id where match_id is not null and datetime
	              < $1 and t.active GROUP by et.team_id)
	SELECT team_id, dense_rank() OVER (ORDER BY elo DESC) as elo_rank, percent_rank() OVER (ORDER BY elo DESC) as
	    elo_rank_perc, elo, dense_rank() OVER (ORDER BY elo_classic
	    DESC)
	    as elo_rank_classic,percent_rank() OVER (ORDER BY elo_classic DESC) as
	    elo_rank_classic_perc, elo_classic FROM data;""", now)
        for team in teams:
            await db.execute('INSERT INTO elo_rank_history VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON conflict on constraint '
                             'elo_rank_history_pkey do update set elo_rank = $3, elo_points = $4, elo_rank_classic = $5, '
                             'elo_points_classic = $6, elo_rank_perc = $7, elo_rank_perc_classic = $8',
                             team.get('team_id'), now,
                             team.get('elo_rank'), team.get('elo'), team.get('elo_rank_classic'),
                             team.get('elo_classic'), team.get('elo_rank_perc'), team.get('elo_rank_classic_perc'))
        now += datetime.timedelta(hours=24)

async def elo_rank_history_manual(start_date: datetime.datetime = None):
    end_time = datetime.datetime.utcnow()
    if not start_date:
        now = end_time - datetime.timedelta(days=7)
    else:
        now = start_date
    now = now.replace(minute=59, hour=23, second=59, microsecond=0)
    while tz.to_naive(now) < end_time:
        await elo_decay(now)
        # elo decay
        try:
            teams = await db.fetch("""WITH data as (SELECT t.team_id,
		                     get_team_elo(t.team_id, $1) as elo,
		                     get_team_elo_classic(t.team_id, $1) as elo_classic
		              FROM elo_transaction et join team t on et.team_id = t.team_id where match_id is not null and
		              datetime < $1 GROUP by t.team_id)
		SELECT team_id, dense_rank() OVER (ORDER BY elo DESC) as elo_rank, percent_rank() OVER (ORDER BY elo DESC) as
		    elo_rank_perc, elo, dense_rank() OVER (ORDER BY elo_classic
		    DESC)
		    as elo_rank_classic,percent_rank() OVER (ORDER BY elo_classic DESC) as
		    elo_rank_classic_perc, elo_classic
		     FROM data;""", tz.to_naive(now))
        except errors.NotFoundException:
            teams = []
        for team in teams:
            await db.execute('INSERT INTO elo_rank_history VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON conflict on constraint '
                             'elo_rank_history_pkey do update set elo_rank = $3, elo_points = $4, elo_rank_classic = $5, '
                             'elo_points_classic = $6, elo_rank_perc = $7, elo_rank_perc_classic = $8',
                             team.get('team_id'), tz.to_naive(now),
                             team.get('elo_rank'), team.get('elo'), team.get('elo_rank_classic'),
                             team.get('elo_classic'), team.get('elo_rank_perc'), team.get('elo_rank_classic_perc'))
        now += datetime.timedelta(hours=24)


async def test():
    @dataclass
    class EloTeam:
        elo: int = 1500
        id: int = None
        name: str = ""
        wins: int = 0
        ties: int = 0
        loses: int = 0
    
    try:
        teams_r = await db.fetch('SELECT t.team_id, t.team_name from event_team et '
                                 'JOIN team t on t.team_id = et.team_id where event_id = 38')
        teams = {r[0]: EloTeam(id=r[0], name=r[1]) for r in teams_r}
    except Exception:
        teams = {}
    matches = await db.fetch('SELECT m.match_id from match_construct mc join match m on mc.match_id = m.match_id '
                             'where event_id = 38  order by match_id')
    for m in matches:
        m = await Match().from_id(m[0])
        t1_elo = teams.get(m.team_a_id)
        if not t1_elo:
            teams[m.team_a_id] = EloTeam(id=m.team_a_id)
            t1_elo = teams[m.team_a_id].elo
        else:
            t1_elo = t1_elo.elo
        t2_elo = teams.get(m.team_b_id)
        if not t2_elo:
            teams[m.team_b_id] = EloTeam(id=m.team_b_id)
            t2_elo = teams[m.team_b_id].elo
        else:
            t2_elo = t2_elo.elo
        t1_exp = float(1 / (1 + pow(1.6, (t2_elo - t1_elo) / 20.0)))
        t2_exp = float(1 / (1 + pow(1.6, (t1_elo - t2_elo) / 20.0)))
        winner = await m.winner
        [t1_app, t2_app] = await db.fetchrow(
                'SELECT get_scaling_factor_v3($1, $2, $3, $4), get_scaling_factor_v3($1, $2, $3, $5)',
                abs(m.result[0] - m.result[3]), min(m.result[0], m.result[3]), abs(m.result[1] - m.result[2]) / 100,
                                                                               winner.id == m.team_a_id,
                                                                               winner.id == m.team_b_id)
        if 9 in (m.team_a_id, m.team_b_id):
            t1 = await m.team_a
            t2 = await m.team_b
            elo_t1 = teams[t1.id]
            elo_t2 = teams.get(t2.id, EloTeam())
            print(f'{t1.name} ({elo_t1.elo}) [{int(round((float(t1_app) - t1_exp) * 10, 0))}] vs ['
                  f'{int(round((float(t2_app) - t2_exp) * 10, 0))}]({elo_t2.elo}) {t2.name} \n{winner.name}\n')
        # update team1 elo
        if m.team_a_id and m.team_a_id in teams:
            teams[m.team_a_id].elo += round((float(t1_app) - t1_exp) * 10, 0)
            if winner.id == m.team_a_id:
                teams[m.team_a_id].wins += 1
                if not teams[m.team_a_id].name:
                    teams[m.team_a_id].name = winner.name
            else:
                teams[m.team_a_id].loses += 1
        if m.team_b_id and m.team_b_id in teams:
            teams[m.team_b_id].elo += round((float(t2_app) - t2_exp) * 10, 0)
            if winner.id == m.team_b_id:
                teams[m.team_b_id].wins += 1
                if not teams[m.team_b_id].name:
                    teams[m.team_b_id].name = winner.name
            else:
                teams[m.team_b_id].loses += 1
    teams_sorted = sorted(teams, key=lambda x: teams[x].elo, reverse=True)
    data = []
    ranges = []
    for c, t in enumerate(teams_sorted):
        t = teams[t]
        if not t.id:
            continue
        if not t.name:
            t.name = (await Team().from_id(t.id)).name
        data.append([[str(c + 1), t.name, str(t.elo), str(t.wins + t.loses + t.ties), str(t.wins), str(t.loses),
                      str(t.ties)]])
        ranges.append(f"'CWL Ladder ranking'!S{c + 2}:Y{c + 2}")
    await gsheets.write("1W-Xa8JAuH0T2tX91Oo4Y5lIqdOKTSvbLmQ1kfo53USM", ranges, values=data)


async def get_nationality(tags: list[list[str]] = None):
    import motor.motor_asyncio
    client = motor.motor_asyncio.AsyncIOMotorClient(
            "mongodb://readonly:prodreadonly@db.clashking.xyz/?authSource=admin")
    new_looper = client.new_looper
    mongo_db = new_looper.player_stats
    locations = await bot.clash_client.search_locations()
    locs = {l.name: l.country_code for l in locations if l.is_country}
    if not tags:
        try:
            tags = await db.fetch("SELECT player_tag from player_names where location is null ORDER BY player_tag")
        except Exception as e:
            traceback.print_exc()
            return
    counter = 0
    for tag in tags:
        tag = tag[0]
        try:
            data = await mongo_db.find_one({'tag': tag})
        except Exception as e:
            print(tag)
            traceback.print_exc()
            print(counter)
            return
        if not data:
            continue
        try:
            country = data.get('country_code')
            name = data.get('name')
            if country:
                try:
                    await db.execute('INSERT INTO player_names(player_tag, player_name, location) VALUES ($1, $2, $3)',
                                     tag, name, country)
                
                except asyncpg.exceptions.UniqueViolationError:
                    await db.execute('UPDATE player_names set location = $1 where player_tag = $2',
                                     country, tag)
                counter += 1
        except Exception as e:
            print(tag)
            print(data)
            traceback.print_exc()
            print(counter)
            return


async def get_cwl_ladder_wars(ctx):
    log_scheduled_tasks.error('Starting fetching')
    data = await gsheets.read('17VgRSmKyMSygeigIYa0MVFLwuHJxOWhHoH0IM_xYojc', 'ESportsxWars!A6:K3078')
    log_scheduled_tasks.error(f'Finished fetching')
    await ctx.channel.send('Finished fetching')
    # war tags index 3, 7, war start 10
    for c, d in enumerate(data):
        if not d[10] or not d[3] or not d[7]:
            continue
        try:
            match_date = datetime.datetime.strptime(d[10], "%m/%d/%Y %H:%M:%S")
            match_date_min = match_date - datetime.timedelta(minutes=6)
            match_date_max = match_date + datetime.timedelta(minutes=2)
            matches = await db.fetch('SELECT match_id, active, scored from match WHERE $1 in (clantag_team1, '
                                     'clantag_team2) and $2 in (clantag_team1, clantag_team2) and '
                                     'prep_start > $3 and prep_start < $4', d[3], d[7], match_date_min, match_date_max)
            if len(matches) > 1:
                log_scheduled_tasks.error(f'{c+6} multiple matches found: {matches=}')
                print(f'{c+6} multiple matches found: {matches=}')
                await ctx
                continue
            match = matches[0]
            await db.execute('UPDATE match set active = True where match_id = $1', match.get('match_id'))
            await db.execute('INSERT INTO match_construct(match_id, event_id, tournament_id) VALUES($1,$2,$3'
                             ') ON CONFLICT DO '
                             'NOTHING',
                             match.get('match_id'), 38, 53)
        except errors.NotFoundException:
            log_scheduled_tasks.error(f'{c + 6} no match found')
            print(f'{c + 6}no match found')
        except Exception as e:
            log_scheduled_tasks.error(traceback.format_exc())
            traceback.print_exc()
            return

async def check_clash_mstrs_roster(ctx, filename):
    import csv
    p_tags = {}
    c_tags = {}
    lines = []
    output = ""
    with open(filename, encoding='utf-8') as file:
        rows = csv.reader(file, delimiter=",")
        total_tags = wrong_tags = affected_teams = wrong_th_level = wrong_clan_tag = counter = d_clans = d_players = 0
        for c, l in enumerate(rows):
            if c == 0:
                lines.append([*l])
                continue
            [team_name, clan_name, clan_tag] = l[0:3]
            counter += 1
            line = [*l]
            tags = [7, 12, 17, 22, 27]
            total_tags += len(tags)
            wrong_tags_per_team = wrong_th_per_team = d_players_per_team = 0
            try:
                clan = await bot.clash_client.get_clan(clan_tag)
                line[3] = clan.name
                c_d_count = c_tags.get(clan.tag, 0)
                if c_d_count:
                    clan_correct = False
                    d_clans += 1
                c_tags[clan.tag] = c_d_count + 1
                clan_correct = True
            except Exception as e:
                wrong_clan_tag += 1
                clan_correct = False
                c_d_count = 0
            duplicates = []
            wrong_ths = []
            wrong_acc_tags = []
            for t in tags:
                try:
                    p = await bot.clash_client.get_player(l[t])
                    line[t+1] = p.name
                    line[t+2] = str(p.town_hall)
                    p_d_count = p_tags.get(p.tag, 0)
                    if p_d_count:
                        duplicates.append(p.tag)
                        d_players_per_team += 1
                    p_tags[p.tag] = p_d_count + 1
                except Exception as e:
                    wrong_tags_per_team += 1
                    wrong_acc_tags.append(l[t])
                    continue
                if p and p.town_hall != 16:
                    wrong_ths.append(p.tag)
                    wrong_th_per_team += 1
            lines.append(line)
            if wrong_tags_per_team or wrong_th_per_team or not clan_correct or d_players_per_team:
                temp = f"## {team_name} (Row {c+1})"
                if wrong_tags_per_team:
                    temp += f"\n{wrong_tags_per_team} wrong tags: {', '.join(wrong_acc_tags)}"
                if wrong_th_per_team:
                    temp += f"\n{wrong_th_per_team} wrong th: {', '.join(wrong_ths)}"
                if d_players_per_team:
                    temp += f"\n{d_players_per_team} duplicate tags: {', '.join(duplicates)}"
                if not clan_correct:
                    temp += f"\n{clan_tag} wrong clantag"
                if c_d_count:
                    temp += f"\nclantag duplicate #{c_d_count}"
                temp += "\n"
                print(temp)
                output += temp
                wrong_tags += wrong_tags_per_team
                wrong_th_level += wrong_th_per_team
                d_players += d_players_per_team
                affected_teams += 1
        content = '\n'.join('; '.join(str(el) for el in row) for row in lines)
        stream = io.BytesIO(content.encode('utf-8'))
        with open("output.md", "w") as file:
            file.write(output)
        await ctx.respond('here is your result', files=[discord.File(stream, filename='result.csv'), discord.File("output.md")])
        print(f"Total {total_tags}: {wrong_tags} wrong tag, {wrong_th_level} wrong thlevel, {wrong_clan_tag} wrong "
              f"clantags, playertag duplicates {d_players}, clantags duplicates {d_clans}")
        print(f"total affected teams {affected_teams}/{counter}")


async def check_for_streams():
    pass




async def tracker_routine():
    clans = await get_clans_to_track()

async def print_stats():
    now = datetime.datetime.utcnow()
    stats_today = await db.fetchrow("SELECT count(match_id) as matches,"
                                    " sum((scored and active)::int) as relevant,"
                                    " sum((scored)::int) as scored,"
                                    " sum((active)::int) as active from matches where date_trunc('day', "
                                    "prep_start::timestamp) = "
                                    "date_trunc('day', $1::timestamp)", now)
    stats_yesterday = await db.fetchrow("SELECT count(match_id) as matches,"
                                        " sum((scored and active)::int) as relevant,"
                                        " sum((scored)::int) as scored,"
                                        " sum((active)::int) as active from matches where date_trunc('day', "
                                        "prep_start::timestamp) = "
                                        "date_trunc('day', $1::timestamp)", now-datetime.timedelta(days=1))
    elo_stats_today = await db.fetchrow("SELECT count(match_id) as entries,"
                                        " sum((match_id is not null and active)::int) as relevant,"
                                        " sum(CASE WHEN match_id is not null and active and elo_change > 0 THEN elo_change "
                                        "ELSE 0 END) "
                                        "as plus,"
                                        " sum(CASE WHEN match_id is not null and active and elo_change < 0 THEN elo_change "
                                        "ELSE 0 END) "
                                        "as negative from elo_transaction where date_trunc('day', "
                                        "datetime::timestamp) = "
                                        "date_trunc('day', $1::timestamp)", now)
    elo_stats_yesterday = await db.fetchrow("SELECT count(match_id) as entries,"
                                            " sum((match_id is not null and active)::int) as relevant,"
                                            " sum(CASE WHEN match_id is not null and active and elo_change > 0 THEN elo_change "
                                            "ELSE 0 END) "
                                            "as plus,"
                                            " sum(CASE WHEN match_id is not null and active and elo_change < 0 THEN elo_change "
                                            "ELSE 0 END) "
                                            "as negative from elo_transaction where date_trunc('day', "
                                            "datetime::timestamp) = "
                                            "date_trunc('day', $1::timestamp)", now - datetime.timedelta(days=1))
    embed = discord.Embed(title='CCN tracking stats',
                          timestamp=now.replace(tzinfo=datetime.timezone.utc))
    embed.add_field(name='Match stats today',
                    value=f'- Matches found {stats_today.get("matches")}\n'
                          f'- {stats_today.get("active")} active & {stats_today.get("scored")} scored matches\n'
                          f'- {stats_today.get("relevant")} matches tracked', inline=False)
    embed.add_field(name='Elo stats today',
                    value=f'- {elo_stats_today.get("relevant")} changes\n'
                          f'- {elo_stats_today.get("plus")} elo points gained\n'
                          f'- {elo_stats_today.get("negative")} elo points lost', inline=False)
    embed.add_field(name='Match stats yesterday',
                    value=f'- Matches found {stats_yesterday.get("matches")}\n'
                          f'- {stats_yesterday.get("active")} active & {stats_yesterday.get("scored")} scored matches\n'
                          f'- {stats_yesterday.get("relevant")} matches tracked', inline=False)
    embed.add_field(name='Elo stats yesterday',
                    value=f'- {elo_stats_yesterday.get("relevant")} changes\n'
                          f'- {elo_stats_yesterday.get("plus")} elo points gained\n'
                          f'- {elo_stats_yesterday.get("negative")} elo points lost', inline=False)
    channel = await bot.client.fetch_channel(1144540461395095762)
    msg = await channel.fetch_message(1144540930590916688)
    await msg.edit(embed=embed)


async def print_mayhen_stats():
    now = datetime.datetime.utcnow()
    [number_clans] = await db.fetchrow("SELECT count(*) from (SELECT distinct clan1_tag as clantag from clash_mayhem "
                                       "UNION DISTINCT SELECT "
                                       "distinct clan2_tag as clantag from clash_mayhem) as d")
    [number_matches] = await db.fetchrow("SELECT count(*) from clash_mayhem where request_date < "
                                         "match_date + interval '35 minutes'")
    [number_tracked_matches] = await db.fetchrow("SELECT count(*) from match_construct where tournament_id = 91")
    [number_current_matches] = await db.fetchrow("SELECT count(*) from match_construct mc JOIN match m on mc.match_id = m.match_id WHERE tournament_id = 91 and m.scored is false")
    stats_today = await db.fetchrow("SELECT count(m.match_id) as matches,"
                                    " sum((scored and active)::int) as relevant,"
                                    " sum((scored)::int) as scored,"
                                    " sum((active)::int) as active from match m "
                                    "join match_construct mc on m.match_id = mc.match_id and mc.tournament_id = 91 "
                                    "where "
                                    "date_trunc('day', prep_start::timestamp) = "
                                    "date_trunc('day', $1::timestamp)", now)
    stats_yesterday = await db.fetchrow("SELECT count(m.match_id) as matches,"
                                        " sum((scored and active)::int) as relevant,"
                                        " sum((scored)::int) as scored,"
                                        " sum((active)::int) as active from match m "
                                        "join match_construct mc on m.match_id = mc.match_id and mc.tournament_id = 91 "
                                        "where date_trunc('day', prep_start::timestamp) = "
                                        "date_trunc('day', $1::timestamp)", now-datetime.timedelta(days=1))
    stats_beginning = await db.fetchrow("SELECT count(m.match_id) as matches,"
                                        " sum((scored and active)::int) as relevant,"
                                        " sum((scored)::int) as scored,"
                                        " sum((active)::int) as active from match m "
                                        "join match_construct mc on m.match_id = mc.match_id "
                                        "where mc.tournament_id = 91 ")
    
    elo_stats_today = await db.fetchrow("SELECT count(et.match_id) as entries,"
                                        " sum((et.match_id is not null and active)::int) as relevant,"
                                        " sum(CASE WHEN et.match_id is not null and active and elo_change > 0 THEN "
                                        "elo_change "
                                        "ELSE 0 END) "
                                        "as plus,"
                                        " sum(CASE WHEN et.match_id is not null and active and elo_change < 0 THEN "
                                        "elo_change "
                                        "ELSE 0 END) "
                                        "as negative from elo_transaction et "
                                        "join match_construct mc on et.match_id = mc.match_id and mc.tournament_id = "
                                        "91  where date_trunc('day', "
                                        "datetime::timestamp) = "
                                        "date_trunc('day', $1::timestamp)", now)
    elo_stats_yesterday = await db.fetchrow("SELECT count(et.match_id) as entries,"
                                            " sum((et.match_id is not null and active)::int) as relevant,"
                                            " sum(CASE WHEN et.match_id is not null and active and elo_change > 0 THEN "
                                            "elo_change "
                                            "ELSE 0 END) "
                                            "as plus,"
                                            " sum(CASE WHEN et.match_id is not null and active and elo_change < 0 THEN "
                                            "elo_change "
                                            "ELSE 0 END) "
                                            "as negative from elo_transaction et "
                                            "join match_construct mc on et.match_id = mc.match_id and mc.tournament_id = "
                                            "91  where date_trunc('day', "
                                            "datetime::timestamp) = "
                                            "date_trunc('day', $1::timestamp)", now- datetime.timedelta(days=1))
    elo_stats_beginning = await db.fetchrow("SELECT count(et.match_id) as entries,"
                                            " sum((et.match_id is not null and active)::int) as relevant,"
                                            " sum(CASE WHEN et.match_id is not null and active and elo_change > 0 THEN "
                                            "elo_change "
                                            "ELSE 0 END) "
                                            "as plus,"
                                            " sum(CASE WHEN et.match_id is not null and active and elo_change < 0 THEN "
                                            "elo_change "
                                            "ELSE 0 END) "
                                            "as negative from elo_transaction et "
                                            "join match_construct mc on et.match_id = mc.match_id and mc.tournament_id = "
                                            "91")
    
    embed = discord.Embed(title='CCN Worlds Qualifier tracking stats',
                          description=f'- {number_clans} clans watched\n'
                                      f'- {number_current_matches} matches ongoing\n'
                                      f'- {number_matches} matches since start\n'
                                      f'- {number_tracked_matches} matches tracked since start',
                          timestamp=now.replace(tzinfo=datetime.timezone.utc))
    embed.add_field(name='Match stats today',
                    value=f'- Matches found {stats_today.get("matches")}\n'
                          f'- {stats_today.get("active")} active & {stats_today.get("scored")} scored matches\n'
                          f'- {stats_today.get("relevant")} matches tracked', inline=False)
    embed.add_field(name='Elo stats today',
                    value=f'- {elo_stats_today.get("relevant")} changes\n'
                          f'- {elo_stats_today.get("plus")} elo points gained\n'
                          f'- {elo_stats_today.get("negative")} elo points lost', inline=False)
    embed.add_field(name='Match stats yesterday',
                    value=f'- Matches found {stats_yesterday.get("matches")}\n'
                          f'- {stats_yesterday.get("active")} active & {stats_yesterday.get("scored")} scored matches\n'
                          f'- {stats_yesterday.get("relevant")} matches tracked', inline=False)
    embed.add_field(name='Elo stats yesterday',
                    value=f'- {elo_stats_yesterday.get("relevant")} changes\n'
                          f'- {elo_stats_yesterday.get("plus")} elo points gained\n'
                          f'- {elo_stats_yesterday.get("negative")} elo points lost', inline=False)
    embed.add_field(name='Match stats total',
                    value=f'- Matches found {stats_beginning.get("matches")}\n'
                          f'- {stats_beginning.get("active")} active & {stats_beginning.get("scored")} scored matches\n'
                          f'- {stats_beginning.get("relevant")} matches tracked', inline=False)
    embed.add_field(name='Elo stats total',
                    value=f'- {elo_stats_beginning.get("relevant")} changes\n'
                          f'- {elo_stats_beginning.get("plus")} elo points gained\n'
                          f'- {elo_stats_beginning.get("negative")} elo points lost', inline=False)
    channel = await bot.client.fetch_channel(1144540461395095762)
    msg = await channel.fetch_message(1149603125683027968)
    await msg.edit(embed=embed)



async def fetch_clash_mayhem(**kwargs):
    url = 'https://mbuqmjkflh.execute-api.eu-west-2.amazonaws.com/prod/coc-data-getter/matches/unreported'
    if 'test' in kwargs:
        data = json.loads(kwargs['test'])
    else:
        async with aiohttp.ClientSession() as session:
            auth = {'Authorization': f'kC4I9qw4tBJQi2YViY7xGWW2Yz51H6lXOxrGBO6k67xL6PbwuUTrlFSZbEFHklm',
                    'Content-Type': 'application/json'}
            kwargs['headers'] = auth
            log_scheduled_tasks.info(f'Request {url=}, {kwargs=}')
            async with session.request('GET', url, **kwargs) as response:
                log_scheduled_tasks.info(f'Requested {url=}, {kwargs=}\n{response.status=}; {response.content_type=}; '
                                         f'{response.content_length=}')
                try:
                    data = await response.json()
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc() + f'\n{url=}\n{kwargs=}')
                    data = None
    now = datetime.datetime.utcnow()
    try:
        await db.execute('INSERT INTO clash_mayhem_requests VALUES ($1,$2)', now, json.dumps(data))
    except Exception as e:
        try:
            await db.execute('INSERT INTO clash_mayhem_requests VALUES ($1,$2)', now, str(data))
        except Exception as e:
            log_scheduled_tasks.error(traceback.format_exc()+f'\n{data=}\n{now=}')
            raise e
    for d in data:
        created_at = get_datetime(d.get('created_at'))
        match_date = get_datetime(d.get('match_date'))
        result_last_checked = get_datetime(d.get('result_last_checked'))
        result_next_check_at = get_datetime(d.get('result_next_check_at'))
        try:
            resp = await db.fetchrow('SELECT match_series_id from clash_mayhem where match_series_id = $1',
                                     d['match_series_id'])
            await db.execute('UPDATE clash_mayhem set request_date = $1, id = $2, created_at = $3, match_date = $4, '
                             'clan1_tag = $5, clan2_tag = $6, result_status = $7, result_last_checked = $8, '
                             'result_checks_numer = $9, result_next_check_at = $10, score1 = $11, score2 = $12 where '
                             'match_series_id = $13', now, d.get('id', None), created_at,
                             match_date, d.get('clan1_tag'), d.get('clan2_tag'), d.get('result_status'),
                             result_last_checked,
                             d.get('result_checks_number'), result_next_check_at,
                             str(d.get('score1')), str(d.get('score2')), d.get('match_series_id'))
        except errors.NotFoundException:
            try:
                await db.execute('INSERT INTO clash_mayhem VALUES($1,NULL,$2,$13, $3, $4, $5, $6, $7, $8, $9, $10, $11, '
                                 '$12)', now, d.get('id', None), created_at, match_date, d.get('clan1_tag'),
                                 d.get('clan2_tag'), d.get('result_status'),
                                 result_last_checked,
                                 d.get('result_checks_number'), result_next_check_at,
                                 str(d.get('score1')), str(d.get('score2')), d.get('match_series_id'))
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc() + f'\n{d=}')
                continue
        except Exception as e:
            log_scheduled_tasks.error(traceback.format_exc()+ f'\n{d=}')
            continue

async def check_clan(clantag: str):
    try:
        record = await db.fetchrow('SELECT * from tracked_clans WHERE clantag = $1',
                                   clantag)
        record = {k:v for k,v in record.items()}
    except errors.NotFoundException:
        return
    now = datetime.datetime.utcnow()
    next_run_time = now
    war = None
    state = 'notinwar'
    match_id = None
    team_id = None
    team2_id = None
    # force update team_id
    try:
        [team_id] = await db.fetchrow('SELECT coalesce(et.team_id, tmc.team_id) as team_id FROM tracked_clans tc '
                                      'LEFT OUTER '
                                      'JOIN event_team et on '
                                      'tc.clantag = '
                                      'et.clantag '
                                      'LEFT OUTER JOIN event_slots ''es on et.event_id = es.event_id LEFT OUTER JOIN '
                                      'team_clan tmc on tc.clantag = tmc.clantag WHERE tc.clantag = $1 and ('
                                      '(et.team_id is not null and $2 between slot_start AND slot_end) or et.team_id '
                                      'is nULL)', clantag, now)
        if team_id:
            record['team_id'] = int(team_id)
            team_id = int(team_id)
        if str(team_id) != str(record.get('team_id', '')):
            await db.execute('UPDATE tracked_clans set team_id = $1::int where clantag = $2', team_id, clantag)
    except errors.NotFoundException:
        pass
    except Exception as e:
        log_scheduled_tasks.error(traceback.format_exc()+ f'\n{record=}')
    try:
        war: Optional[coc.ClanWar] = await bot.fast_clash_client.get_current_war(record.get('clantag'),cls=TrackedWar)
        last_checked = datetime.datetime.utcnow()
    except coc.errors.PrivateWarLog:
        next_run_time += datetime.timedelta(minutes=10)
        last_checked = datetime.datetime.utcnow()
        state = 'privatelog'
    except Exception:
        next_run_time += datetime.timedelta(minutes=2)
        last_checked = datetime.datetime.utcnow()
        state = 'privatelog'
    if war and (war.type != 'friendly' or war.team_size != 5 or not war.preparation_start_time or
                any([m.town_hall not in (15, 16) for m in war.members])):
        # not a war of interest for us
        if not war.preparation_start_time:
            state = 'notinwar'
            next_run_time += datetime.timedelta(minutes=4)
        elif war.state == 'warEnded':
            state = 'warended'
            next_run_time += datetime.timedelta(minutes=4)
        elif war.preparation_start_time and war.preparation_start_time.time < now < war.start_time.time:
            state = 'prepstarted'
            next_run_time = war.end_time.time + datetime.timedelta(minutes=5)
        elif war.preparation_start_time and war.start_time.time < now < war.end_time.time:
            state = 'warstarted'
            next_run_time = war.end_time.time + datetime.timedelta(minutes=5)
        else:
            next_run_time += datetime.timedelta(minutes=5)
    elif war:
        try:
            [match_id, scored] = await db.fetchrow('Select match_id, scored from '
                                                   'match where '
                                                   '$1 in (clantag_team1,'
                                                   'clantag_team2) and '
                                                   'prep_start = $2 and $3 in (clantag_team1,clantag_team2)', war.clan.tag,
                                                   war.preparation_start_time.time, war.opponent.tag)
        except errors.NotFoundException:
            match_id = None
            scored = False
        if not match_id:
            now = datetime.datetime.utcnow()
            try:
                [season_id] = await db.fetchrow('SELECT season_id from season where season_start <= $1 and season_end '
                                                '>= '
                                                '$1', now)
            except errors.NotFoundException:
                season_id = None
            try:
                team1 = await Team().from_id(record.get('team_id'))
                try:
                    t_1_rank = await team1.rank(season_id)
                except Exception:
                    t_1_rank = None
                team1_id = team1.id
            except errors.NotFoundException:
                team1_id = None
                t_1_rank = None
            try:
                [team2_id] = await db.fetchrow(
                        'SELECT coalesce(et.team_id, tmc.team_id) as team_id FROM tracked_clans tc '
                        'LEFT OUTER '
                        'JOIN event_team et on '
                        'tc.clantag = '
                        'et.clantag '
                        'LEFT OUTER JOIN event_slots es on et.event_id = es.event_id LEFT OUTER JOIN '
                        'team_clan tmc on tc.clantag = tmc.clantag WHERE tc.clantag = $1 and (('
                        'et.team_id is not null and $2 between slot_start AND slot_end) or et.team_id is null)',
                        war.opponent.tag, now)
                team2 = await Team().from_id(team2_id)
                try:
                    t_2_rank = await team2.rank(season_id)
                except Exception:
                    t_2_rank = None
            except errors.NotFoundException:
                team2_id = None
                t_2_rank = None
            if season_id:
                try:
                    teams_to_watch = await db.fetch(
                            'SELECT team_id from season_teamtw WHERE season_id = $1 and (team_id '
                            '= $2 or team_id = $3)', season_id, team1_id if team1_id else 0,
                            team2_id if team2_id else 0)
                except errors.NotFoundException:
                    teams_to_watch = []
            else:
                teams_to_watch = []
            try:
                [match_id] = await db.fetchrow('INSERT INTO match(season_id, team_id1, clantag_team1, '
                                               'team_id2, clantag_team2, prep_start, prep_duration, war_duration, active,'
                                               ' scored) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING '
                                               'match_id',
                                               season_id if t_1_rank or t_2_rank or teams_to_watch else None,
                                               team1_id, war.clan.tag, team2_id, war.opponent.tag,
                                               war.preparation_start_time.time,
                                               war.start_time.time - war.preparation_start_time.time,
                                               war.end_time.time - war.start_time.time, True, False)
                for cln, tid in zip([war.clan, war.opponent], [team1_id, team2_id]):
                    lu = [m for m in cln.members]
                    await db.execute('INSERT INTO lineup_player VALUES ($1,$2,$3,$4,$5, $6) ON CONFLICT DO NOTHING',
                                     [(match_id, cln.tag, x.tag, False, x.name, tid) for x in lu])
                    await db.execute('INSERT INTO lineup_clan VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING ',
                                     match_id, cln.tag, cln.name, tid)
                run_time = tz.to_naive(tz.withtz(war.end_time.time, tz.UTC).astimezone(tz.VMT))
                try:
                    match = await Match.by_id(match_id)
                    await match.check_construct()
                except errors.NotFoundException:
                    pass
                except Exception as e:
                    log_scheduled_tasks.error(traceback.format_exc())
                if datetime.datetime.now() > run_time:
                    await load_hit_stats(match_id, 0, war)
                else:
                    scheduler.add_job(load_hit_stats, 'date', run_date=run_time, args=(match_id,),
                                      id=f'atks-{match_id}',
                                      name=f'atks-{match_id}', misfire_grace_time=600, replace_existing=True)
            except Exception as e:
                log_scheduled_tasks.error(traceback.format_exc())
        
        new_atks = [(
            match_id, atk.order, atk.attacker_tag, atk.attacker.town_hall, atk.defender_tag,
            atk.defender.town_hall,
            atk.stars, atk.destruction, atk.is_fresh_attack, atk.duration) for atk in war.attacks]
        try:
            await db.execute(
                    '''INSERT INTO attack VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING ''',
                    new_atks)
            m = await Match.by_id(match_id)
            await save_result(match_id, war)
        except Exception as e:
            log_scheduled_tasks.error(str(e) + traceback.format_exc())
        if not war.preparation_start_time:
            state = 'notinwar'
            next_run_time += datetime.timedelta(minutes=4)
        elif war.state == 'warEnded':
            state = 'warended'
            if not scored:
                await load_hit_stats(match_id, 0, war=war)
            next_run_time += datetime.timedelta(minutes=4)
        elif war.preparation_start_time and war.preparation_start_time.time < now < war.start_time.time:
            state = 'prepstarted'
            next_run_time = war.start_time.time + datetime.timedelta(minutes=5)
            run_time = tz.to_naive(tz.withtz(war.end_time.time, tz.UTC).astimezone(tz.VMT))
            if datetime.datetime.now() > run_time:
                await load_hit_stats(match_id, 0, war)
            else:
                if not scheduler.get_job(f'atks-{match_id}'):
                    scheduler.add_job(load_hit_stats, 'date', run_date=run_time, args=(match_id,),
                                      id=f'atks-{match_id}',
                                      name=f'atks-{match_id}', misfire_grace_time=600, replace_existing=True)
        elif war.preparation_start_time and war.start_time.time < now < war.end_time.time:
            state = 'warstarted'
            run_time = tz.to_naive(tz.withtz(war.end_time.time, tz.UTC).astimezone(tz.VMT))
            if datetime.datetime.now() > run_time:
                await load_hit_stats(match_id, 0, war)
            else:
                if not scheduler.get_job(f'atks-{match_id}'):
                    scheduler.add_job(load_hit_stats, 'date', run_date=run_time, args=(match_id,),
                                      id=f'atks-{match_id}',
                                      name=f'atks-{match_id}', misfire_grace_time=600, replace_existing=True)
            next_run_time += datetime.timedelta(minutes=5)
        else:
            next_run_time += datetime.timedelta(minutes=4)
    await db.execute('UPDATE tracked_clans set state = $1,api_state=$1, last_checked = $2, next_check = $3, match_id = '
                     '$4 '
                     'where '
                     'clantag = $5', state, last_checked, next_run_time, match_id, clantag)
    if state not in ('notinwar', 'warended') and war:
        await db.execute('UPDATE tracked_clans set match_id = $1, state = $2 where clantag = $3',
                         match_id, state, war.opponent.tag)
    scheduler.add_job(check_clan, 'date', run_date=max(next_run_time, datetime.datetime.utcnow() +
                                                       datetime.timedelta(minutes=1)), timezone='Etc/UTC',
                      args=(clantag,),
                      id=f'clanwatcher'
                         f'-{clantag}',
                      name=f'clanwatcher-{clantag}', replace_existing=True, coalesce=True, misfire_grace_time=600 )




async def get_clans_to_track():
    try:
        raw_team_clans = await db.fetch('SELECT tc.clantag, t.team_id FROM team t JOIN team_clan tc on t.team_id = '
                                        'tc.team_id WHERE t.active ORDER BY clantag')
    except Exception as e:
        raise e
    clans = {t[0]: t[1] for t in raw_team_clans}
    for k,v in clans.items():
        await db.execute('INSERT INTO tracked_clans(clantag) VALUES($1) ON CONFLICT DO NOTHING ', k)
    for k in clans.keys():
        await check_clan(k)


async def master_event_poster(ctx: discord.ApplicationContext,
                              event=None,
                              poster_type=None,
                              until=None,
                              minimum=None,
                              sorting=None,
                              location=None,
                              force_seperation=None):
    """Create a top 10 poster for an event"""
    if location:
        try:
            query = 'SELECT distinct full_name, short_code from locations WHERE short_code ilike $1 order by ' \
                    'full_name, short_code LIMIT 1'
            records = await db.fetch(query, location)
        except errors.NotFoundException:
            await ctx.respond(embed=discord.Embed(
                    title="Invalid location.",
                    color=bot.colors.red))
            return
        except Exception as e:
            raise e
        if len(records) > 1:
            await ctx.respond(embed=discord.Embed(
                    title="Not unique location given.",
                    color=bot.colors.red))
            return
        location = records[0]
    if until:
        try:
            until = datetime.datetime.strptime(until, '%Y-%m-%d %H:%M')
        except Exception:
            await ctx.respond(embed=discord.Embed(title=f'Invalid date format', color=bot.colors.red))
            return
    else:
        until = datetime.datetime.utcnow().replace(year=2999)
    try:
        if poster_type == 'Top 10 Attackers':
            data = {'title': 'Top 10 Attackers', 'subtitle': 'World Finalists', 'attr1': 'HITS', 'attr2': 'HR'}
            template_name = 'top_10_player'
            player1_img = None
            player2_img = None
            if location:
                query = f"""WITH DATA as (SELECT
    pn.player_name,
    t.team_id,
    a.attacker_tag,
    count(*) as attacks,
    sum((a.stars = 3)::int) as triples,
    sum((a.stars = 2)::int) as twos,
    sum((a.stars = 1)::int) as ones,
    sum((a.stars = 0)::int) as zeros,
    avg(a.stars)  as avg_stars,
    avg(a.perc) as avg_perc,
    avg(a.duration) as avg_duration
from attack a
    JOIN lineup_player lp on lp.player_tag = a.attacker_tag and a.match_id = lp.match_id
    JOIN team t on lp.team_id = t.team_id
    JOIN match m on a.match_id = m.match_id
    JOIN match_construct mc on m.match_id = mc.match_id and mc.event_id in (51, 52, 53, 54, 42, 43, 29, 30, 7, 10)
    JOIN master_events_event mee on mc.event_id = mee.event_id
    JOIN player_names pn on pn.player_tag = a.attacker_tag
WHERE m.scored and m.active and m.prep_start < $1 and location = $2 and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
GROUP BY pn.player_name, t.team_id, a.attacker_tag Having count(*) >= $4) SELECT player_name as name, team_id as team,
attacker_tag as tag,
attacks,
                                                             triples, twos, ones, zeros,
                                                             round(avg_stars*100)/100 as avg_stars,
                                                             round(avg_perc*100)/100 as avg_perc,
                                                             round(avg_duration*100)/100 as avg_duration,
                                                             round(triples::numeric/greatest(attacks,1)::numeric*10000)
                                                             /100
                                                                 as
                                                                 hitrate from DATA ORDER BY
                                                                 triples::numeric/greatest(attacks,1)::numeric
                                                                 {'*triples' if sorting != 'Rate' else ''} DESC,
                                                                 attacks
                                                                 desc, avg_stars desc, avg_perc desc, avg_duration
                                                                 asc LIMIT 10;"""
                args = ( until, location.get('short_code'), minimum)
                data['location_background'] = {
                    'coords': [293, -41, 293+282, -41+269],
                    'anchor': 'mm',
                    'type': 'image',
                    'data': str(pathlib.Path(f"assets/player_hit_poster_location.png")),
                    'geometry': None,
                    'name': 'location_background',
                    'layer': 1
                }
                data['location_name'] = {
                    'coords'       : [362, 176, 362 + 144, 176 + 27],
                    'anchor'       : 'mm',
                    'type'         : 'text',
                    'data'         : location.get('full_name').upper(),
                    'font'         : "Manufaktur-Black.otf",
                    'font_min_size': 1,
                    'font_max_size': 250,
                    'color'        : "#1e1e1e",
                    'geometry'     : None,
                    'name'         : 'location_name',
                    'layer'        : 2
                }
                data['location_flag'] = {
                    'coords'  : [371, 54, 371 + 125, 54 + 81],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/flags/{location.get('short_code').lower()}.png")),
                    'geometry': None,
                    'name'    : 'location_flag',
                    'layer'   : 3
                }
            else:
                query = f"""WITH DATA as (SELECT
    pn.player_name,
    mode() within group (order by t.team_id) as team_id,
    a.attacker_tag,
    count(*) as attacks,
    sum((a.stars = 3)::int) as triples,
    sum((a.stars = 2)::int) as twos,
    sum((a.stars = 1)::int) as ones,
    sum((a.stars = 0)::int) as zeros,
    avg(a.stars)  as avg_stars,
    avg(a.perc) as avg_perc,
    avg(a.duration) as avg_duration
from attack a
    JOIN lineup_player lp on lp.player_tag = a.attacker_tag and a.match_id = lp.match_id
    JOIN team t on lp.team_id = t.team_id
    JOIN match m on a.match_id = m.match_id
    JOIN player_names pn on pn.player_tag = a.attacker_tag
WHERE m.scored and m.active and m.prep_start < $1 and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
GROUP BY pn.player_name, a.attacker_tag having count(*) >= $2) SELECT player_name as name, team_id as team,
attacker_tag as tag,
attacks,
                                                             triples, twos, ones, zeros,
                                                             round(avg_stars*100)/100 as avg_stars,
                                                             round(avg_perc*100)/100 as avg_perc,
                                                             round(avg_duration*100)/100 as avg_duration,
                                                             round(triples::numeric/greatest(attacks,1)::numeric*10000)
                                                             /100
                                                                 as
                                                                 hitrate from DATA ORDER BY
                                                                 triples::numeric/greatest(attacks,1)::numeric DESC, attacks
                                                                 desc, avg_stars desc, avg_perc desc, avg_duration
                                                                 asc LIMIT 10;"""
                args = (until, minimum)
            try:
                players = await db.fetch(query, *args)
            except errors.NotFoundException:
                return await ctx.respond('No attacks found.')
            for i, player in enumerate(players):
                player_picture = None
                try:
                    p = await Person.by_meta(player.get('tag'))
                    player_name = p.name
                    if p.picture_public and p.picture_path:
                        player_picture = p.picture_path
                except Exception:
                    player_name = player.get('name')
                    player_picture = None
                if player_picture and not player1_img:
                    data['player1_picture'] = player_picture
                    player1_img = player_name
                elif player_picture and not player2_img:
                    data['player2_picture'] = player_picture
                    player2_img = player_name
                if pathlib.Path(f"assets/teamlogos/t-{player.get('team')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{player.get('team')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f'p{i + 1}_logo'] = team_logo
                data[f'p{i + 1}_name'] = player_name
                data[f'p{i + 1}_attr1'] = f"{player.get('triples',0)}/{player.get('attacks',0)}"
                data[f'p{i + 1}_attr2'] = str(round(player.get('triples')/max(player.get('attacks',0),1)*100,2)) + '%'
            
            for i in range(len(players),10):
                data[f'p{i + 1}_logo'] = None
                data[f'p{i + 1}_name'] = None
                data[f'p{i + 1}_attr1'] = None
                data[f'p{i + 1}_attr2'] = None
            if player1_img:
                data['player_bg'] = {
                    'coords'  : [33, 938, 22 + 843, 938 + 94],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/player_hit_poster_name_background.png")),
                    'geometry': None,
                    'name'    : 'player1_bg',
                    'layer'   : 999999997
                }
                if player2_img:
                    data['player2_name'] = {
                        'coords'       : [228, 990, 228 + 493, 990 + 33],
                        'anchor'       : 'rm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color': "#1e1e1e",
                        'data'         : player2_img,
                        'geometry'     : None,
                        'name'         : 'player2_name',
                        'layer'        : 999999998
                    }
                    data['player1_name'] = {
                        'coords'       : [188, 946, 188+493, 946+33],
                        'anchor'       : 'lm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color': "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                else:
                    data['player1_name'] = {
                        'coords'       : [208, 957, 208 + 493, 957 + 56],
                        'anchor'       : 'mm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color': "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                    data['player2_picture'] = None
            else:
                data['player1_picture'] = None
                data['player2_picture'] = None
        elif poster_type == 'Top 10 Attackers 60d':
            data = {'title': 'Top 10 Attackers', 'subtitle': 'World Finalists', 'attr1': 'HITS', 'attr2': 'HR'}
            template_name = 'top_10_player'
            player1_img = None
            player2_img = None
            if location:
                query = f"""WITH DATA as (SELECT
    pn.player_name,
    t.team_id,
    a.attacker_tag,
    count(*) as attacks,
    sum((a.stars = 3)::int) as triples,
    sum((a.stars = 2)::int) as twos,
    sum((a.stars = 1)::int) as ones,
    sum((a.stars = 0)::int) as zeros,
    avg(a.stars)  as avg_stars,
    avg(a.perc) as avg_perc,
    avg(a.duration) as avg_duration
from attack a
    JOIN lineup_player lp on lp.player_tag = a.attacker_tag and a.match_id = lp.match_id
    JOIN team t on lp.team_id = t.team_id
    JOIN match m on a.match_id = m.match_id
    JOIN match_construct mc on m.match_id = mc.match_id and mc.event_id in (51, 52, 53, 54, 42, 43, 29, 30, 7, 10)
    JOIN master_events_event mee on mc.event_id = mee.event_id
    JOIN player_names pn on pn.player_tag = a.attacker_tag
WHERE m.scored and m.active and m.prep_start < $1 and location = $2 and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
GROUP BY pn.player_name, t.team_id, a.attacker_tag Having count(*) >= $4) SELECT player_name as name, team_id as team,
attacker_tag as tag,
attacks,
                                                             triples, twos, ones, zeros,
                                                             round(avg_stars*100)/100 as avg_stars,
                                                             round(avg_perc*100)/100 as avg_perc,
                                                             round(avg_duration*100)/100 as avg_duration,
                                                             round(triples::numeric/greatest(attacks,1)::numeric*10000)
                                                             /100
                                                                 as
                                                                 hitrate from DATA ORDER BY
                                                                 triples::numeric/greatest(attacks,1)::numeric
                                                                 {'*triples' if sorting != 'Rate' else ''} DESC,
                                                                 attacks
                                                                 desc, avg_stars desc, avg_perc desc, avg_duration
                                                                 asc LIMIT 10;"""
                args = ( until, location.get('short_code'), minimum)
                data['location_background'] = {
                    'coords': [293, -41, 293+282, -41+269],
                    'anchor': 'mm',
                    'type': 'image',
                    'data': str(pathlib.Path(f"assets/player_hit_poster_location.png")),
                    'geometry': None,
                    'name': 'location_background',
                    'layer': 1
                }
                data['location_name'] = {
                    'coords'       : [362, 176, 362 + 144, 176 + 27],
                    'anchor'       : 'mm',
                    'type'         : 'text',
                    'data'         : location.get('full_name').upper(),
                    'font'         : "Manufaktur-Black.otf",
                    'font_min_size': 1,
                    'font_max_size': 250,
                    'color'        : "#1e1e1e",
                    'geometry'     : None,
                    'name'         : 'location_name',
                    'layer'        : 2
                }
                data['location_flag'] = {
                    'coords'  : [371, 54, 371 + 125, 54 + 81],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/flags/{location.get('short_code').lower()}.png")),
                    'geometry': None,
                    'name'    : 'location_flag',
                    'layer'   : 3
                }
            else:
                query = f"""WITH DATA as (SELECT
    pn.player_name,
    mode() within group (order by t.team_id) as team_id,
    a.attacker_tag,
    count(*) as attacks,
    sum((a.stars = 3)::int) as triples,
    sum((a.stars = 2)::int) as twos,
    sum((a.stars = 1)::int) as ones,
    sum((a.stars = 0)::int) as zeros,
    avg(a.stars)  as avg_stars,
    avg(a.perc) as avg_perc,
    avg(a.duration) as avg_duration
from attack a
    JOIN lineup_player lp on lp.player_tag = a.attacker_tag and a.match_id = lp.match_id
    JOIN team t on lp.team_id = t.team_id
    JOIN match m on a.match_id = m.match_id
    JOIN player_names pn on pn.player_tag = a.attacker_tag
WHERE m.scored and m.active and m.prep_start < $1 and m.prep_start > now() at time zone 'utc' - interval '60 days'
and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
GROUP BY pn.player_name, a.attacker_tag having count(*) >= $2) SELECT player_name as name, team_id as team,
attacker_tag as tag,
attacks,
                                                             triples, twos, ones, zeros,
                                                             round(avg_stars*100)/100 as avg_stars,
                                                             round(avg_perc*100)/100 as avg_perc,
                                                             round(avg_duration*100)/100 as avg_duration,
                                                             round(triples::numeric/greatest(attacks,1)::numeric*10000)
                                                             /100
                                                                 as
                                                                 hitrate from DATA ORDER BY
                                                                 triples::numeric/greatest(attacks,1)::numeric DESC, attacks
                                                                 desc, avg_stars desc, avg_perc desc, avg_duration
                                                                 asc LIMIT 10;"""
                args = (until, minimum // 10)
            try:
                players = await db.fetch(query, *args)
            except errors.NotFoundException:
                return await ctx.respond('No attacks found.')
            for i, player in enumerate(players):
                player_picture = None
                try:
                    p = await Person.by_meta(player.get('tag'))
                    player_name = p.name
                    if p.picture_public and p.picture_path:
                        player_picture = p.picture_path
                except Exception:
                    player_name = player.get('name')
                    player_picture = None
                if player_picture and not player1_img:
                    data['player1_picture'] = player_picture
                    player1_img = player_name
                elif player_picture and not player2_img:
                    data['player2_picture'] = player_picture
                    player2_img = player_name
                if pathlib.Path(f"assets/teamlogos/t-{player.get('team')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{player.get('team')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f'p{i + 1}_logo'] = team_logo
                data[f'p{i + 1}_name'] = player_name
                data[f'p{i + 1}_attr1'] = f"{player.get('triples',0)}/{player.get('attacks',0)}"
                data[f'p{i + 1}_attr2'] = str(round(player.get('triples')/max(player.get('attacks',0),1)*100,2)) + '%'
            
            for i in range(len(players),10):
                data[f'p{i + 1}_logo'] = None
                data[f'p{i + 1}_name'] = None
                data[f'p{i + 1}_attr1'] = None
                data[f'p{i + 1}_attr2'] = None
            if player1_img:
                data['player_bg'] = {
                    'coords'  : [33, 938, 22 + 843, 938 + 94],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/player_hit_poster_name_background.png")),
                    'geometry': None,
                    'name'    : 'player1_bg',
                    'layer'   : 999999997
                }
                if player2_img:
                    data['player2_name'] = {
                        'coords'       : [228, 990, 228 + 493, 990 + 33],
                        'anchor'       : 'rm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color': "#1e1e1e",
                        'data'         : player2_img,
                        'geometry'     : None,
                        'name'         : 'player2_name',
                        'layer'        : 999999998
                    }
                    data['player1_name'] = {
                        'coords'       : [188, 946, 188+493, 946+33],
                        'anchor'       : 'lm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color': "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                else:
                    data['player1_name'] = {
                        'coords'       : [208, 957, 208 + 493, 957 + 56],
                        'anchor'       : 'mm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color': "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                    data['player2_picture'] = None
            else:
                data['player1_picture'] = None
                data['player2_picture'] = None
        elif poster_type == 'Top 10 Defenders':
            data = {'title': 'Top 10 Defenders', 'subtitle': 'World Finalists', 'attr1': 'HITS', 'attr2': 'DR'}
            template_name = 'top_10_player'
            player1_img = None
            player2_img = None
            if location:
                query = f"""WITH DATA as (SELECT
			    pn.player_name,
			    t.team_id,
			    a.defender_tag,
			    count(*) as attacks,
			    sum((a.stars = 3)::int) as triples,
			    sum((a.stars != 3)::int) as not_triples,
			    sum((a.stars = 2)::int) as twos,
			    sum((a.stars = 1)::int) as ones,
			    sum((a.stars = 0)::int) as zeros,
			    avg(a.stars)  as avg_stars,
			    avg(a.perc) as avg_perc,
			    avg(a.duration) as avg_duration
			from attack a
			    JOIN lineup_player lp on lp.player_tag = a.defender_tag and a.match_id = lp.match_id
			    JOIN team t on lp.team_id = t.team_id
			    JOIN match m on a.match_id = m.match_id
			    JOIN match_construct mc on m.match_id = mc.match_id and mc.event_id in (51, 52, 53, 54, 42, 43, 29, 30, 7, 10)
			    JOIN player_names pn on pn.player_tag = a.attacker_tag
			    JOIN master_events_event mee on mc.event_id = mee.event_id
			WHERE m.scored and m.active and m.prep_start < $1 and location = $2 and mee.master_event_id in (3, 5, 7,
			8) and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
			GROUP BY pn.player_name, t.team_id, a.defender_tag having count(*)>=$3) SELECT player_name as name,
			team_id as
			team,
			defender_tag as tag, attacks,
			                                                             not_triples, twos, ones, zeros,
			                                                             round(avg_stars*100)/100 as avg_stars,
			                                                             round(avg_perc*100)/100 as avg_perc,
			                                                             round(avg_duration*100)/100 as avg_duration,
			                                                             round(not_triples::numeric/greatest(attacks,
			                                                             1)::numeric*10000)
			                                                             /100
			                                                                 as
			                                                                 hitrate from DATA ORDER BY not_triples::numeric/greatest(attacks,1)::numeric
                                                                 {'*not_triples' if sorting != 'Rate' else ''} DESC,
                                                                 attacks
			                                                                 desc, avg_stars asc, avg_perc asc,
			                                                                 avg_duration
			                                                                 desc;"""
                args = ( until, location.get('short_code'),minimum)
                data['location_background'] = {
                    'coords'  : [293, -41, 293 + 282, -41 + 269],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/player_hit_poster_location.png")),
                    'geometry': None,
                    'name'    : 'location_background',
                    'layer'   : 1
                }
                data['location_name'] = {
                    'coords'       : [362, 176, 362 + 144, 176 + 27],
                    'anchor'       : 'mm',
                    'type'         : 'text',
                    'data'         : location.get('full_name').upper(),
                    'font'         : "Manufaktur-Black.otf",
                    'font_min_size': 1,
                    'font_max_size': 250,
                    'color'        : "#1e1e1e",
                    'geometry'     : None,
                    'name'         : 'location_name',
                    'layer'        : 2
                }
                data['location_flag'] = {
                    'coords'  : [371, 54, 371 + 125, 54 + 81],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/flags/{location.get('short_code').lower()}.png")),
                    'geometry': None,
                    'name'    : 'location_flag',
                    'layer'   : 3
                }
            else:
                query = f"""WITH DATA as (SELECT
			    pn.player_name,
			    mode() within group (order by t.team_id) as team_id,
			    a.defender_tag,
			    count(*) as attacks,
			    sum((a.stars = 3)::int) as triples,
			    sum((a.stars != 3)::int) as not_triples,
			    sum((a.stars = 2)::int) as twos,
			    sum((a.stars = 1)::int) as ones,
			    sum((a.stars = 0)::int) as zeros,
			    avg(a.stars)  as avg_stars,
			    avg(a.perc) as avg_perc,
			    avg(a.duration) as avg_duration
			from attack a
			    JOIN lineup_player lp on lp.player_tag = a.defender_tag and a.match_id = lp.match_id
			    JOIN team t on lp.team_id = t.team_id
			    JOIN match m on a.match_id = m.match_id
			    JOIN player_names pn on pn.player_tag = a.defender_tag
			WHERE m.scored and m.active and m.prep_start < $1 and t.team_id
			in (2, 9,
			11, 33, 49, 80, 147, 5394)
			GROUP BY pn.player_name, a.defender_tag having count(*) >= $2) SELECT player_name as name,
			team_id as
			team,
			defender_tag as tag, attacks,
			                                                             not_triples, twos, ones, zeros,
			                                                             round(avg_stars*100)/100 as avg_stars,
			                                                             round(avg_perc*100)/100 as avg_perc,
			                                                             round(avg_duration*100)/100 as avg_duration,
			                                                             round(not_triples::numeric/greatest(attacks,
			                                                             1)::numeric*10000)
			                                                             /100
			                                                                 as
			                                                                 hitrate from DATA
			                                                                 ORDER BY not_triples::numeric/greatest(attacks,1)::numeric DESC, attacks
			                                                                 desc, avg_stars asc, avg_perc asc,
			                                                                 avg_duration
			                                                                 desc LIMIT 10;"""
                args = (until, minimum)
            try:
                players = await db.fetch(query, *args)
            except errors.NotFoundException:
                return await ctx.respond('No attacks found.')
            for i, player in enumerate(players):
                player_picture = None
                try:
                    p = await Person.by_meta(player.get('tag'))
                    player_name = p.name
                    if p.picture_public and p.picture_path:
                        player_picture = p.picture_path
                except Exception:
                    player_name = player.get('name')
                    player_picture = None
                if player_picture and not player1_img:
                    data['player1_picture'] = player_picture
                    player1_img = player_name
                elif player_picture and not player2_img:
                    data['player2_picture'] = player_picture
                    player2_img = player_name
                if pathlib.Path(f"assets/teamlogos/t-{player.get('team')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{player.get('team')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f'p{i + 1}_logo'] = team_logo
                data[f'p{i + 1}_name'] = player_name
                data[f'p{i + 1}_attr1'] = f"{player.get('not_triples', 0)}/{player.get('attacks', 0)}"
                data[f'p{i + 1}_attr2'] = str(round(player.get('not_triples') / max(player.get('attacks', 0), 1) * 100,
                                                    2)) + '%'
            for i in range(len(players), 10):
                data[f'p{i + 1}_logo'] = None
                data[f'p{i + 1}_name'] = None
                data[f'p{i + 1}_attr1'] = None
                data[f'p{i + 1}_attr2'] = None
            if player1_img:
                data['player_bg'] = {
                    'coords'  : [33, 938, 22 + 843, 938 + 94],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/player_hit_poster_name_background.png")),
                    'geometry': None,
                    'name'    : 'player1_bg',
                    'layer'   : 999999997
                }
                if player2_img:
                    data['player2_name'] = {
                        'coords'       : [228, 990, 228 + 493, 990 + 33],
                        'anchor'       : 'rm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color'        : "#1e1e1e",
                        'data'         : player2_img,
                        'geometry'     : None,
                        'name'         : 'player2_name',
                        'layer'        : 999999998
                    }
                    data['player1_name'] = {
                        'coords'       : [188, 946, 188 + 493, 946 + 33],
                        'anchor'       : 'lm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color'        : "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                else:
                    data['player1_name'] = {
                        'coords'       : [208, 957, 208 + 493, 957 + 56],
                        'anchor'       : 'mm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color'        : "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                    data['player2_picture'] = None
            else:
                data['player1_picture'] = None
                data['player2_picture'] = None
        elif poster_type == 'Top 10 Defenders 60d':
            data = {'title': 'Top 10 Defenders', 'subtitle': 'World Finalists', 'attr1': 'HITS', 'attr2': 'DR'}
            template_name = 'top_10_player'
            player1_img = None
            player2_img = None
            if location:
                query = f"""WITH DATA as (SELECT
				    pn.player_name,
				    t.team_id,
				    a.defender_tag,
				    count(*) as attacks,
				    sum((a.stars = 3)::int) as triples,
				    sum((a.stars != 3)::int) as not_triples,
				    sum((a.stars = 2)::int) as twos,
				    sum((a.stars = 1)::int) as ones,
				    sum((a.stars = 0)::int) as zeros,
				    avg(a.stars)  as avg_stars,
				    avg(a.perc) as avg_perc,
				    avg(a.duration) as avg_duration
				from attack a
				    JOIN lineup_player lp on lp.player_tag = a.defender_tag and a.match_id = lp.match_id
				    JOIN team t on lp.team_id = t.team_id
				    JOIN match m on a.match_id = m.match_id
				    JOIN match_construct mc on m.match_id = mc.match_id and mc.event_id in (51, 52, 53, 54, 42, 43, 29, 30, 7, 10)
				    JOIN player_names pn on pn.player_tag = a.attacker_tag
				    JOIN master_events_event mee on mc.event_id = mee.event_id
				WHERE m.scored and m.active and m.prep_start < $1 and location = $2 and mee.master_event_id in (3, 5, 7,
				8) and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
				GROUP BY pn.player_name, t.team_id, a.defender_tag having count(*)>=$3) SELECT player_name as name,
				team_id as
				team,
				defender_tag as tag, attacks,
				                                                             not_triples, twos, ones, zeros,
				                                                             round(avg_stars*100)/100 as avg_stars,
				                                                             round(avg_perc*100)/100 as avg_perc,
				                                                             round(avg_duration*100)/100 as avg_duration,
				                                                             round(not_triples::numeric/greatest(attacks,
				                                                             1)::numeric*10000)
				                                                             /100
				                                                                 as
				                                                                 hitrate from DATA ORDER BY not_triples::numeric/greatest(attacks,1)::numeric
	                                                                 {'*not_triples' if sorting != 'Rate' else ''} DESC,
	                                                                 attacks
				                                                                 desc, avg_stars asc, avg_perc asc,
				                                                                 avg_duration
				                                                                 desc;"""
                args = (until, location.get('short_code'), minimum)
                data['location_background'] = {
                    'coords'  : [293, -41, 293 + 282, -41 + 269],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/player_hit_poster_location.png")),
                    'geometry': None,
                    'name'    : 'location_background',
                    'layer'   : 1
                }
                data['location_name'] = {
                    'coords'       : [362, 176, 362 + 144, 176 + 27],
                    'anchor'       : 'mm',
                    'type'         : 'text',
                    'data'         : location.get('full_name').upper(),
                    'font'         : "Manufaktur-Black.otf",
                    'font_min_size': 1,
                    'font_max_size': 250,
                    'color'        : "#1e1e1e",
                    'geometry'     : None,
                    'name'         : 'location_name',
                    'layer'        : 2
                }
                data['location_flag'] = {
                    'coords'  : [371, 54, 371 + 125, 54 + 81],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/flags/{location.get('short_code').lower()}.png")),
                    'geometry': None,
                    'name'    : 'location_flag',
                    'layer'   : 3
                }
            else:
                query = f"""WITH DATA as (SELECT
				    pn.player_name,
				    mode() within group (order by t.team_id) as team_id,
				    a.defender_tag,
				    count(*) as attacks,
				    sum((a.stars = 3)::int) as triples,
				    sum((a.stars != 3)::int) as not_triples,
				    sum((a.stars = 2)::int) as twos,
				    sum((a.stars = 1)::int) as ones,
				    sum((a.stars = 0)::int) as zeros,
				    avg(a.stars)  as avg_stars,
				    avg(a.perc) as avg_perc,
				    avg(a.duration) as avg_duration
				from attack a
				    JOIN lineup_player lp on lp.player_tag = a.defender_tag and a.match_id = lp.match_id
				    JOIN team t on lp.team_id = t.team_id
				    JOIN match m on a.match_id = m.match_id
				    JOIN player_names pn on pn.player_tag = a.defender_tag
				WHERE m.scored and m.active and m.prep_start < $1 and m.prep_start > now() at time zone 'utc' - interval '60 days' and t.team_id
				in (2, 9,
				11, 33, 49, 80, 147, 5394)
				GROUP BY pn.player_name, a.defender_tag having count(*) >= $2) SELECT player_name as name,
				team_id as
				team,
				defender_tag as tag, attacks,
				                                                             not_triples, twos, ones, zeros,
				                                                             round(avg_stars*100)/100 as avg_stars,
				                                                             round(avg_perc*100)/100 as avg_perc,
				                                                             round(avg_duration*100)/100 as avg_duration,
				                                                             round(not_triples::numeric/greatest(attacks,
				                                                             1)::numeric*10000)
				                                                             /100
				                                                                 as
				                                                                 hitrate from DATA
				                                                                 ORDER BY not_triples::numeric/greatest(attacks,1)::numeric DESC, attacks
				                                                                 desc, avg_stars asc, avg_perc asc,
				                                                                 avg_duration
				                                                                 desc LIMIT 10;"""
                args = (until, minimum // 10)
            try:
                players = await db.fetch(query, *args)
            except errors.NotFoundException:
                return await ctx.respond('No attacks found.')
            for i, player in enumerate(players):
                player_picture = None
                try:
                    p = await Person.by_meta(player.get('tag'))
                    player_name = p.name
                    if p.picture_public and p.picture_path:
                        player_picture = p.picture_path
                except Exception:
                    player_name = player.get('name')
                    player_picture = None
                if player_picture and not player1_img:
                    data['player1_picture'] = player_picture
                    player1_img = player_name
                elif player_picture and not player2_img:
                    data['player2_picture'] = player_picture
                    player2_img = player_name
                if pathlib.Path(f"assets/teamlogos/t-{player.get('team')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{player.get('team')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f'p{i + 1}_logo'] = team_logo
                data[f'p{i + 1}_name'] = player_name
                data[f'p{i + 1}_attr1'] = f"{player.get('not_triples', 0)}/{player.get('attacks', 0)}"
                data[f'p{i + 1}_attr2'] = str(
                        round(player.get('not_triples') / max(player.get('attacks', 0), 1) * 100,
                              2)) + '%'
            for i in range(len(players), 10):
                data[f'p{i + 1}_logo'] = None
                data[f'p{i + 1}_name'] = None
                data[f'p{i + 1}_attr1'] = None
                data[f'p{i + 1}_attr2'] = None
            if player1_img:
                data['player_bg'] = {
                    'coords'  : [33, 938, 22 + 843, 938 + 94],
                    'anchor'  : 'mm',
                    'type'    : 'image',
                    'data'    : str(pathlib.Path(f"assets/player_hit_poster_name_background.png")),
                    'geometry': None,
                    'name'    : 'player1_bg',
                    'layer'   : 999999997
                }
                if player2_img:
                    data['player2_name'] = {
                        'coords'       : [228, 990, 228 + 493, 990 + 33],
                        'anchor'       : 'rm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color'        : "#1e1e1e",
                        'data'         : player2_img,
                        'geometry'     : None,
                        'name'         : 'player2_name',
                        'layer'        : 999999998
                    }
                    data['player1_name'] = {
                        'coords'       : [188, 946, 188 + 493, 946 + 33],
                        'anchor'       : 'lm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color'        : "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                else:
                    data['player1_name'] = {
                        'coords'       : [208, 957, 208 + 493, 957 + 56],
                        'anchor'       : 'mm',
                        'type'         : 'text',
                        'font'         : "Manufaktur-Black.otf",
                        'font_min_size': 1,
                        'font_max_size': 250,
                        'color'        : "#1e1e1e",
                        'data'         : player1_img,
                        'geometry'     : None,
                        'name'         : 'player1_name',
                        'layer'        : 999999999
                    }
                    data['player2_picture'] = None
            else:
                data['player1_picture'] = None
                data['player2_picture'] = None
        elif poster_type == 'Top 10 Team W/T/L':
            query = f"""with data as (SELECT t.team_id,
                     t.team_name,
                     count(*) as matches,
                     count(*) filter ( where mr.result = 'win' ) as wins,
                     count(*) filter ( where mr.result = 'loss' ) as loses,
                     avg(mr.stars) as avg_stars,
                     avg(mr.perc) as avg_perc,
                     avg(mr.duration) as avg_duration
FROM lineup_clan lp JOIN match_result mr on mr.team_id = lp.team_id and lp.match_id = mr.match_id JOIN match m on
mr.match_id = m.match_id
    JOIN team t on lp.team_id = t.team_id
WHERE m.prep_start < $1 and m.scored and m.active and
t.team_id
in (
2, 9,
11, 33,
49, 80, 147,
5394) GROUP BY
t.team_id, t.team_name
 having
count(
mr.match_id) >=
$2)
SELECT
	data.team_id,
    data.team_name,
	data.matches,
	data.wins,
	data.loses,
	round(data.wins::numeric/data.matches::numeric*100,2) as winrate
    from data ORDER BY wins::numeric/greatest(matches,1)::numeric DESC,
    matches desc, avg_stars desc, avg_perc desc, avg_duration LIMIT 10;"""
            result = await db.fetch(query, until, minimum)
            template_name = 'top_10_teams'
            data = {'title': 'Top 10 Teams', 'subtitle': 'World Finalists', 'attr1': 'W/T/L', 'attr2': 'WR'}
            for i, team in enumerate(result):
                if pathlib.Path(f"assets/teamlogos/t-{team.get('team_id')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{team.get('team_id')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f'p{i + 1}_logo'] = team_logo
                data[f'p{i + 1}_name'] = team.get('team_name')
                wins = team.get('wins', 0) if team.get('wins', 0) else 0
                loses = team.get('loses', 0) if team.get('loses', 0) else 0
                matches = team.get('matches', 0) if team.get('matches') else 0
                data[f'p{i + 1}_attr1'] = (f"{wins}/"
                                           f"{matches-wins-loses}/"
                                           f"{loses}")
                data[f'p{i + 1}_attr2'] = str(round(wins / max(matches, 1) * 100,
                                                    2)) + '%'
            for i in range(len(result), 10):
                data[f'p{i + 1}_logo'] = None
                data[f'p{i + 1}_name'] = None
                data[f'p{i + 1}_attr1'] = None
                data[f'p{i + 1}_attr2'] = None
        elif poster_type == 'Top 10 Team W/T/L 60d':
            query = f"""with data as (SELECT t.team_id,
		                     t.team_name,
		                     count(*) as matches,
		                     count(*) filter ( where mr.result = 'win' ) as wins,
		                     count(*) filter ( where mr.result = 'loss' ) as loses,
		                     avg(mr.stars) as avg_stars,
		                     avg(mr.perc) as avg_perc,
		                     avg(mr.duration) as avg_duration
		FROM lineup_clan lp JOIN match_result mr on mr.team_id = lp.team_id and lp.match_id = mr.match_id JOIN match m on
		mr.match_id = m.match_id
		    JOIN team t on lp.team_id = t.team_id
		WHERE m.prep_start < $1 and m.prep_start > now() at time zone 'utc' - interval '60 days' and m.scored and m.active and
		t.team_id
		in (
		2, 9,
		11, 33,
		49, 80, 147,
		5394) GROUP BY
		t.team_id, t.team_name
		 having
		count(
		mr.match_id) >=
		$2)
		SELECT
			data.team_id,
		    data.team_name,
			data.matches,
			data.wins,
			data.loses,
			round(data.wins::numeric/data.matches::numeric*100,2) as winrate
		    from data ORDER BY wins::numeric/greatest(matches,1)::numeric desc,
		    matches desc, avg_stars desc, avg_perc desc, avg_duration LIMIT 10;"""
            result = await db.fetch(query, until, minimum// 10)
            template_name = 'top_10_teams'
            data = {'title': 'Top 10 Teams', 'subtitle': 'World Finalists', 'attr1': 'W/T/L', 'attr2': 'WR'}
            for i, team in enumerate(result):
                if pathlib.Path(f"assets/teamlogos/t-{team.get('team_id')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{team.get('team_id')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f'p{i + 1}_logo'] = team_logo
                data[f'p{i + 1}_name'] = team.get('team_name')
                wins = team.get('wins', 0) if team.get('wins', 0) else 0
                loses = team.get('loses', 0) if team.get('loses', 0) else 0
                matches = team.get('matches', 0) if team.get('matches') else 0
                data[f'p{i + 1}_attr1'] = (f"{wins}/"
                                           f"{matches - wins - loses}/"
                                           f"{loses}")
                data[f'p{i + 1}_attr2'] = str(round(wins / max(matches, 1) * 100,
                                                    2)) + '%'
            for i in range(len(result), 10):
                data[f'p{i + 1}_logo'] = None
                data[f'p{i + 1}_name'] = None
                data[f'p{i + 1}_attr1'] = None
                data[f'p{i + 1}_attr2'] = None
        elif poster_type == 'Top 5 Teams Hits':
            template_name = 'top_5_teams'
            data = {'title': 'World Finalists'}
            query = f"""with data as (SELECT t.team_id,
			                     t.team_name,
			                     count(distinct a.match_id) as matches,
			    count(*) as attacks,
			    sum((a.stars = 3)::int) as triples,
			    sum((a.stars = 2)::int) as twos,
			    sum((a.stars = 1)::int) as ones,
			    sum((a.stars = 0)::int) as zeros,
			    avg(a.stars)  as avg_stars,
			    avg(a.perc) as avg_perc,
			    avg(a.perc) filter ( where a.stars != 3 ) as avg_perc_not_triples,
			    avg(a.duration) as avg_duration
			FROM team t JOIN lineup_player lp on t.team_id = lp.team_id
			JOIN Attack a on lp.match_id = a
			        .match_id and lp.player_tag = a.attacker_tag JOIN match m on a.match_id = m.match_id
			WHERE m.prep_start < $1 and m.scored and m.active and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
			 GROUP BY t.team_id, team_name having count(*) >=
			$2)
			SELECT
			data.team_id,
			    data.team_name,
				data.matches,
			    attacks,
			    triples,
			    twos,
			    ones,
			    zeros,
			    round(avg_stars,2) as avg_stars,
			    round(avg_perc,2) as avg_perc,
			    round(avg_perc_not_triples,2) as avg_perc_not_triples,
			    round(avg_duration,2) as avg_duration
			    from data ORDER BY triples::numeric/greatest(attacks,1)::numeric

			    DESC,
			    matches desc, attacks desc,
			                               avg_stars desc, avg_perc desc, avg_duration LIMIT 5;"""
            result = await db.fetch(query, until, minimum)
            
            for i, team in enumerate(result):
                if pathlib.Path(f"assets/teamlogos/t-{team.get('team_id')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{team.get('team_id')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f't{i + 1}_l_logo'] = team_logo
                data[f't{i + 1}_l_name'] = team.get('team_name')
                data[f't{i + 1}_l_attr1'] = f"{team.get('triples')}"
                data[f't{i + 1}_l_attr2'] = f"{team.get('attacks')}"
                data[f't{i + 1}_l_attr3'] = str(round(team.get('triples') / max(team.get('attacks', 0), 1) * 100,
                                                      2)) + '%'
            for i in range(len(result), 5):
                data[f't{i + 1}_l_logo'] = None
                data[f't{i + 1}_l_name'] = None
                data[f't{i + 1}_l_attr1'] = None
                data[f't{i + 1}_l_attr2'] = None
                data[f't{i + 1}_l_attr3'] = None
            query = f"""with data as (SELECT t.team_id,
						                     t.team_name,
						                     count(distinct a.match_id) as matches,
						    count(*) as attacks,
						    sum((a.stars != 3)::int) as triples,
						    sum((a.stars = 2)::int) as twos,
						    sum((a.stars = 1)::int) as ones,
						    sum((a.stars = 0)::int) as zeros,
						    avg(a.stars)  as avg_stars,
						    avg(a.perc) as avg_perc,
						    avg(a.perc) filter ( where a.stars != 3 ) as avg_perc_not_triples,
						    avg(a.duration) as avg_duration
						FROM team t JOIN lineup_player lp on t.team_id = lp.team_id
						    JOIN Attack a on
						    lp.match_id = a
						        .match_id and lp.player_tag = a.defender_tag JOIN match m on a.match_id = m.match_id
						        
						WHERE m.prep_start < $1 and m.scored and m.active and t.team_id in (2, 9, 11, 33, 49, 80,
						147, 5394) GROUP BY t.team_id, team_name having count(*) >=
						$2)
						SELECT
						data.team_id,
						    data.team_name,
							data.matches,
						    attacks,
						    triples,
						    twos,
			    ones,
			    zeros,
			    round(avg_stars,2) as avg_stars,
			    round(avg_perc,2) as avg_perc,
			    round(avg_perc_not_triples,2) as avg_perc_not_triples,
			    round(avg_duration,2) as avg_duration
						    from data ORDER BY triples::numeric/greatest(attacks,1)::numeric
						    DESC,
						    matches desc, attacks desc,
						                               avg_stars asc, avg_perc asc, avg_duration desc LIMIT 5;"""
            result = await db.fetch(query,  until, minimum)
            
            for i, team in enumerate(result):
                if pathlib.Path(f"assets/teamlogos/t-{team.get('team_id')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{team.get('team_id')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f't{i + 1}_r_logo'] = team_logo
                data[f't{i + 1}_r_name'] = team.get('team_name')
                data[f't{i + 1}_r_attr1'] = f"{team.get('triples')}"
                data[f't{i + 1}_r_attr2'] = f"{team.get('attacks')}"
                data[f't{i + 1}_r_attr3'] = str(round(team.get('triples') / max(team.get('attacks', 0), 1) * 100,
                                                      2)) + '%'
            for i in range(len(result), 5):
                data[f't{i + 1}_r_logo'] = None
                data[f't{i + 1}_r_name'] = None
                data[f't{i + 1}_r_attr1'] = None
                data[f't{i + 1}_r_attr2'] = None
                data[f't{i + 1}_r_attr3'] = None
            data['player_l'] = None
            data['player_r'] = None
        elif poster_type == 'Top 5 Teams Hits 60d':
            template_name = 'top_5_teams'
            data = {'title': 'World Finalists'}
            query = f"""with data as (SELECT t.team_id,
			                     t.team_name,
			                     count(distinct a.match_id) as matches,
			    count(*) as attacks,
			    sum((a.stars = 3)::int) as triples,
			    sum((a.stars = 2)::int) as twos,
			    sum((a.stars = 1)::int) as ones,
			    sum((a.stars = 0)::int) as zeros,
			    avg(a.stars)  as avg_stars,
			    avg(a.perc) as avg_perc,
			    avg(a.perc) filter ( where a.stars != 3 ) as avg_perc_not_triples,
			    avg(a.duration) as avg_duration
			FROM team t JOIN lineup_player lp on t.team_id = lp.team_id
			JOIN Attack a on lp.match_id = a
			        .match_id and lp.player_tag = a.attacker_tag JOIN match m on a.match_id = m.match_id
			WHERE m.prep_start < $1 and m.prep_start > now() at time zone 'utc' - interval '60 days' and m.scored and m.active and t.team_id in (2, 9, 11, 33, 49, 80, 147, 5394)
			 GROUP BY t.team_id, team_name having count(*) >=
			$2)
			SELECT
			data.team_id,
			    data.team_name,
				data.matches,
			    attacks,
			    triples,
			    twos,
			    ones,
			    zeros,
			    round(avg_stars,2) as avg_stars,
			    round(avg_perc,2) as avg_perc,
			    round(avg_perc_not_triples,2) as avg_perc_not_triples,
			    round(avg_duration,2) as avg_duration
			    from data ORDER BY triples::numeric/greatest(attacks,1)::numeric

			    DESC,
			    matches desc, attacks desc,
			                               avg_stars desc, avg_perc desc, avg_duration LIMIT 5;"""
            result = await db.fetch(query, until, minimum // 10)
            
            for i, team in enumerate(result):
                if pathlib.Path(f"assets/teamlogos/t-{team.get('team_id')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{team.get('team_id')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f't{i + 1}_l_logo'] = team_logo
                data[f't{i + 1}_l_name'] = team.get('team_name')
                data[f't{i + 1}_l_attr1'] = f"{team.get('triples')}"
                data[f't{i + 1}_l_attr2'] = f"{team.get('attacks')}"
                data[f't{i + 1}_l_attr3'] = str(round(team.get('triples') / max(team.get('attacks', 0), 1) * 100,
                                                      2)) + '%'
            for i in range(len(result), 5):
                data[f't{i + 1}_l_logo'] = None
                data[f't{i + 1}_l_name'] = None
                data[f't{i + 1}_l_attr1'] = None
                data[f't{i + 1}_l_attr2'] = None
                data[f't{i + 1}_l_attr3'] = None
            query = f"""with data as (SELECT t.team_id,
						                     t.team_name,
						                     count(distinct a.match_id) as matches,
						    count(*) as attacks,
						    sum((a.stars != 3)::int) as triples,
						    sum((a.stars = 2)::int) as twos,
						    sum((a.stars = 1)::int) as ones,
						    sum((a.stars = 0)::int) as zeros,
						    avg(a.stars)  as avg_stars,
						    avg(a.perc) as avg_perc,
						    avg(a.perc) filter ( where a.stars != 3 ) as avg_perc_not_triples,
						    avg(a.duration) as avg_duration
						FROM team t JOIN lineup_player lp on t.team_id = lp.team_id
						    JOIN Attack a on
						    lp.match_id = a
						        .match_id and lp.player_tag = a.defender_tag JOIN match m on a.match_id = m.match_id

						WHERE m.prep_start < $1 and m.prep_start > now() at time zone 'utc' - interval '60 days' and m.scored and m.active and t.team_id in (2, 9, 11, 33, 49, 80,
						147, 5394) GROUP BY t.team_id, team_name having count(*) >=
						$2)
						SELECT
						data.team_id,
						    data.team_name,
							data.matches,
						    attacks,
						    triples,
						    twos,
			    ones,
			    zeros,
			    round(avg_stars,2) as avg_stars,
			    round(avg_perc,2) as avg_perc,
			    round(avg_perc_not_triples,2) as avg_perc_not_triples,
			    round(avg_duration,2) as avg_duration
						    from data ORDER BY triples::numeric/greatest(attacks,1)::numeric
						    DESC,
						    matches desc, attacks desc,
						                               avg_stars asc, avg_perc asc, avg_duration desc LIMIT 5;"""
            result = await db.fetch(query, until, minimum // 10)
            
            for i, team in enumerate(result):
                if pathlib.Path(f"assets/teamlogos/t-{team.get('team_id')}.png").is_file():
                    team_logo = f"assets/teamlogos/t-{team.get('team_id')}.png"
                else:
                    team_logo = f"assets/teamlogos/default.png"
                data[f't{i + 1}_r_logo'] = team_logo
                data[f't{i + 1}_r_name'] = team.get('team_name')
                data[f't{i + 1}_r_attr1'] = f"{team.get('triples')}"
                data[f't{i + 1}_r_attr2'] = f"{team.get('attacks')}"
                data[f't{i + 1}_r_attr3'] = str(round(team.get('triples') / max(team.get('attacks', 0), 1) * 100,
                                                      2)) + '%'
            for i in range(len(result), 5):
                data[f't{i + 1}_r_logo'] = None
                data[f't{i + 1}_r_name'] = None
                data[f't{i + 1}_r_attr1'] = None
                data[f't{i + 1}_r_attr2'] = None
                data[f't{i + 1}_r_attr3'] = None
            data['player_l'] = None
            data['player_r'] = None
        else:
            return await ctx.respond('Not implemented yet')
    except errors.NotFoundException:
        return await ctx.respond('Nothing to display!')
    try:
        im = await gfx.create_graphic(template_name, data)
    except Exception as e:
        traceback.print_exc()
        raise e
    with io.BytesIO() as img_binary:
        im.save(img_binary, 'PNG')
        img_binary.seek(0)
        file = discord.File(img_binary,
                            filename=f'{event.name} {poster_type} '
                                     f'{location.get("short_code") if location else ""} '
                                     f'{int(until.timestamp()) if until else ""}.png')
        await ctx.respond(file=file)
        im.close()

async def calculate_dates(ctx: discord.ApplicationContext):
    # fetch all tournaments
    try:
        tournaments = await db.fetch("""SELECT * FROM tournaments where match_start is null or tournament_end is null
		order by tournament_id""")
    except errors.NotFoundException:
        await ctx.respond('No tournaments found.')
        return
    for t in tournaments:
        msg = f"**{t.get('tournament_name')}**\n"
        start = None
        end = None
        if not t.get('match_start'):
            # missing tournament start
            # fetch first match
            try:
                [start] = await db.fetchrow("""SELECT prep_start FROM match m join match_construct mc on mc.match_id =
				m.match_id where tournament_id = $1 and prep_start is not null order by prep_start limit 1""",
                                            t.get('tournament_id'))
                start = tz.withtz(start, tz.UTC)
                start = start.date()
                msg += f"Match start: {start.strftime('%Y-%m-%d')}\n"
            except errors.NotFoundException:
                msg += f"Can't determine match_start\n"
        if not t.get('tournament_end'):
            # missing tournament end
            # fetch last match
            try:
                [end] = await db.fetchrow("""SELECT prep_start FROM match m join match_construct mc on mc.match_id =
				m.match_id where tournament_id = $1 and prep_start is not null order by prep_start desc limit 1""",
                                          t.get('tournament_id'))
                end = tz.withtz(end, tz.UTC)
                end = end.date() + datetime.timedelta(days=1)
                msg += f"Match end: {end.strftime('%Y-%m-%d')}\n"
            except errors.NotFoundException:
                pass
        if not t.get('tournament_end') and not end:
            # try to find earning for end
            try:
                [end] = await db.fetchrow("""SELECT date_time FROM team_earnings where comment ilike $1 order by
				date_time desc limit 1""", t.get('tournament_name'))
                end = tz.withtz(end, tz.UTC)
                msg += f"Match end: {end.strftime('%Y-%m-%d')}\n"
            except errors.NotFoundException:
                msg += f"Can't determine match_end\n"
        if start or end:
            try:
                await db.execute("UPDATE tournaments set match_start = $1, tournament_end = $2 where tournament_id = $3",
                                 tz.to_naive(start), tz.to_naive(end), t.get('tournament_id'))
            except Exception:
                traceback.print_exc()
                print(f"{t=} {start=} {end=}")
        await ctx.respond(msg)

async def get_upcoming_matches():
    ext = ExternalSources()
    await ext.get_upcoming_matches()

async def update_upcoming_matches(um_ids: List[int] = None):
    ext = ExternalSources()
    await ext.update_upcoming_matches(um_ids=um_ids)



async def list_upcoming_matches(date_from: datetime.datetime = None,
                                date_to: datetime.datetime = None,
                                top_x: int = 50):
    channel = await bot.client.fetch_channel(1172803369480499301)
    if channel is None:
        return
    if not date_from:
        date_from = datetime.datetime.utcnow()
    if not date_to:
        date_to = date_from + datetime.timedelta(days=1)
    now = datetime.datetime.utcnow()
    # date_from = date_from.replace(hour=0, minute=0, second=0, microsecond=0)
    # date_to = date_to.replace(hour=0, minute=0, second=0, microsecond=0)
    if not top_x:
        sql = '''SELECT * from upcoming_matches um
			where match_date >= $1 and match_date < $2 order by match_date, tournament_name, week_name'''
        args = (date_from, date_to)
    else:
        sql = '''SELECT * from upcoming_matches um
			where match_date >= $1 and match_date < $2 and (team1_elo_rank <= $3 or team2_elo_rank <= $3 )order by
			match_date, tournament_name, week_name'''
        args = (date_from, date_to, top_x)
    try:
        matches = await db.fetch(sql, *args)
    except errors.NotFoundException:
        
        main_embed = discord.Embed(title=f"No upcoming matches",
                                   color=bot.colors.yellow,timestamp=now.replace(tzinfo=datetime.timezone.utc))
        main_embed.description = "No matches found."
        embeds = [main_embed]
        matches = []
    
    if len(matches) == 0:
        main_embed = discord.Embed(title=f"No upcoming matches",
                                   color=bot.colors.yellow, timestamp=now.replace(tzinfo=datetime.timezone.utc))
        main_embed.description = "No matches found."
        embeds = [main_embed]
    else:
        main_embed = discord.Embed(title=f"Upcoming matches",
                                   color=bot.colors.green, timestamp=now.replace(tzinfo=datetime.timezone.utc))
        
        match_informations = []
        for idx, match in enumerate(matches):
            # get team1
            team1 = None
            team1_name = match.get('team1_name')
            team2 = None
            team2_name = match.get('team2_name')
            prep_start: datetime.datetime
            e = discord.utils.escape_markdown
            name = f'{idx + 1}. {e(team1_name)} VS {e(team2_name)}'
            try:
                [count_streams] = await db.fetchrow("""SELECT count(*) from um_streams where um_id = $1""", match.get('um_id'))
            except errors.NotFoundException:
                count_streams = 0
            match_start = match.get('match_date')
            if match_start:
                match_start = match_start.replace(tzinfo=tz.UTC)
            hard_mode = False
            has_restrictions = False
            try:
                [construct_id] = await db.fetchrow('SELECT construct_id from constructs c join um_table um on '
                                                   'um.source_key = c.source_key where '
                                                   'um.um_id = '
                                                   '$1',
                                                   match.get('um_id'))
                event = await Event().from_id(construct_id)
                if event.battle_modifier and event.battle_modifier.lower() != 'none':
                    hard_mode = True
                restrictions = event.restriction
                if restrictions:
                    has_restrictions = True
                if not has_restrictions:
                    try:
                        res = await event.restrictions
                    except errors.NotFoundException:
                        res = []
                    if res:
                        has_restrictions = True
                event_name = event.name
            except errors.NotFoundException:
                event_name = match.get('tournament_name')
            
            value = (f"__**Tournament**__: {e(match.get('tournament_name'))}\n"
                     f"__**Week**__: {e(match.get('week_name'))}\n"
                     f"<t:{int(match_start.timestamp())}:f>\n")
            if hard_mode:
                value += f"{bot.custom_emojis.hard_mode} Hard mode\n"
            if has_restrictions:
                value += f"🏴 Additional Restrictions\n"
            value += f"__**Live Streamed**__\n" if count_streams > 0 else ""
            match_informations.append((name, value))
        
        embeds = message_utils.embed_add_fields(main_embed=main_embed, field_data=match_informations, inline=False)
        log_scheduled_tasks.info(f"Found {len(embeds)} matches")
    old_messages = channel.history(oldest_first=True)
    counter = 0
    async for o_msg in old_messages:
        try:
            await o_msg.edit(embed=embeds[counter], content="")
            counter += 1
        except IndexError:
            try:
                await o_msg.delete()
            except Exception as e:
                await channel.send(str(e))
        except Exception as e:
            await channel.send(str(e))
    if len(embeds) > counter:
        for c in range(counter, len(embeds)):
            await channel.send(embed=embeds[c])


async def list_upcoming_streams(date_from: datetime.datetime = None,
                                date_to: datetime.datetime = None,
                                top_x: int = None):
    channel = await bot.client.fetch_channel(1172803762264490064)
    now = datetime.datetime.utcnow()
    if channel is None:
        return
    if not date_from:
        date_from = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    if not date_to:
        date_to = date_from + datetime.timedelta(days=3, hours=1)
    if not top_x:
        sql = '''SELECT * from upcoming_matches um JOIN um_streams ums on um.um_id = ums.um_id
			where match_date >= $1 and match_date < $2 order by match_date limit 40'''
        args = (date_from, date_to)
    else:
        sql = '''SELECT * from upcoming_matches um JOIN um_streams ums on um.um_id = ums.um_id
			where match_date >= $1 and match_date < $2 and (team1_elo_rank <= $3 or team2_elo_rank <= $3 ) order by
			match_date limit 40'''
        args = (date_from, date_to, top_x)
    # date_from = date_from.replace(hour=0, minute=0, second=0, microsecond=0)
    # date_to = date_to.replace(hour=0, minute=0, second=0, microsecond=0)
    
    prpcessed = []
    try:
        matches = await db.fetch(sql, *args)
    except errors.NotFoundException:
        await channel.purge()
        main_embed = discord.Embed(title=f"No upcoming streams",
                                   color=bot.colors.yellow, timestamp=now.replace(tzinfo=datetime.timezone.utc))
        main_embed.description = "No matches found."
        embeds = [main_embed]
        matches = []
    
    if len(matches) == 0:
        main_embed = discord.Embed(title=f"No upcoming streams",
                                   color=bot.colors.yellow,timestamp=now.replace(tzinfo=datetime.timezone.utc))
        main_embed.description = "No streams found."
        embeds = [main_embed]
    else:
        main_embed = discord.Embed(title=f"Upcoming streams",
                                   color=bot.colors.green,timestamp=now.replace(tzinfo=datetime.timezone.utc))
        
        match_informations = []
        for idx, match in enumerate(matches):
            if match.get('um_id') in prpcessed:
                continue
            # get team1
            team1 = None
            team1_name = match.get('team1_name')
            team2 = None
            team2_name = match.get('team2_name')
            prep_start: datetime.datetime
            e = discord.utils.escape_markdown
            name = f'{len(prpcessed) + 1}. {e(team1_name)} VS {e(team2_name)}'
            try:
                [count_streams] = await db.fetchrow("""SELECT count(*) from um_streams where um_id = $1""", match.get('um_id'))
            except errors.NotFoundException:
                count_streams = 0
            match_start = match.get('match_date')
            if match_start:
                match_start = match_start.replace(tzinfo=tz.UTC)
            
            value = (f"__**Tournament**__: {e(match.get('tournament_name'))}\n"
                     f"__**Week**__: {e(match.get('week_name'))}\n"
                     f"<t:{int(match_start.timestamp())}:f>")
            streams = await db.fetch('SELECT * from um_streams where um_id = $1 ORDER BY stream_start, streamer_name, url',
                                     match.get('um_id'))
            p_streams = {}
            for s in streams:
                stream_start = s.get('stream_start')
                if stream_start:
                    stream_start = stream_start.replace(tzinfo=tz.UTC)
                    p_stream_start = f"<t:{int(stream_start.timestamp())}:R>"
                else:
                    p_stream_start = "Unknown"
                key = f"{p_stream_start} - {e(s.get('streamer_name'))}"
                value1 = p_streams.get(key, "")
                if value1:
                    pass
                else:
                    value1 += f"[{e(s.get('platform'))}]({e(s.get('url'))})"
                p_streams[key] = value1
            v_old = value
            for key, value1 in p_streams.items():
                if len(value) + len(f"\n{key}: {value1}") > 1000:
                    prpcessed.append((name, value))
                    value = v_old
                value += f"\n{key}: {value1}"
            match_informations.append((name, value))
            prpcessed.append(match.get('um_id'))
        
        embeds = message_utils.embed_add_fields(main_embed=main_embed, field_data=match_informations, inline=False)
    old_messages = channel.history(oldest_first=True)
    counter = 0
    async for o_msg in old_messages:
        try:
            await o_msg.edit(embed=embeds[counter], content="")
            counter += 1
        except IndexError:
            try:
                await o_msg.delete()
            except Exception as e:
                await channel.send(str(e))
        except Exception as e:
            await channel.send(str(e))
    if len(embeds) > counter:
        for c in range(counter, len(embeds)):
            await channel.send(embed=embeds[c])

async def check_free_agents():
    now = datetime.datetime.utcnow()
    await db.execute("""UPDATE person set free_agent = null
	where free_agent is not null and free_agent < $1""", now)

async def check_mc(ctx):
    matches = await db.fetch('SELECT mc.* from match_construct_new mc '
                             'left outer join um_match ium on mc.match_id = ium.match_id '
                             'where ium.match_id is null and mc.construct_id is not null;')
    await ctx.send(f'{len(matches)}')
    counter = 0
    for data in matches:
        try:
            [match_id] = await db.fetchrow(
                    'SELECT um_id from um_table umt join tournaments t on t.lu_tournament_id = umt.tournament_id where '
                    't.tournament_id = '
                    '$1 '
                    'and week_name = $2 '
                    'and team1_name in ($3, $4) and team2_name in ($3, $4)',
                    data.get('tournament_id'), data.get('tournament_week_name'),
                    data.get('tournament_team1_name'), data.get('tournament_team2_name'))
        except errors.NotFoundException:
            match_id = None
        except Exception:
            bot.logger.error(traceback.format_exc())
            match_id = None
        if match_id:
            try:
                await db.execute('INSERT INTO um_match(match_id, um_id) VALUES($1,$2) ON CONFLICT DO NOTHING',
                                 data.get('match_id'), match_id)
                counter += 1
            except Exception:
                bot.logger.error(traceback.format_exc())
    await ctx.send(f'{counter}/{len(matches)}')

async def check_um(ctx):
    matches = await db.fetch(
            'SELECT * from um_table umt left outer join '
            'tournaments t on '
            't.lu_tournament_id = '
            'umt.tournament_id left '
            'outer join '
            'um_match '
            'umm on umt.um_id = '
            'umm.um_id where  t.tournament_id = 99')
    await ctx.send(f'{len(matches)}')
    counter = 0
    for data in matches:
        try:
            [match_id] = await db.fetchrow('SELECT match_id from match_construct where tournament_id = $1 and '
                                           'tournament_week_name = $2 and (tournament_team1_name in ($3, '
                                           '$4) or tournament_team2_name in ($3, $4))',
                                           data.get('tournament_id'), data.get('week_name'),
                                           data.get('team_name1'), data.get('team_name2'))
        except errors.NotFoundException:
            match_id = None
        except Exception:
            bot.logger.error(traceback.format_exc())
            match_id = None
        if match_id:
            try:
                await db.execute('INSERT INTO um_match(match_id, um_id) VALUES($1,$2) ON CONFLICT DO NOTHING',
                                 match_id, data.get('um_id'))
                counter += 1
            except Exception:
                bot.logger.error(traceback.format_exc())
    await ctx.send(f'{counter}/{len(matches)}')

async def fetch_old_ums(ctx):
    tournaments = await db.fetch('SELECT * from tournaments where tournaments.lu_tournament_id is not null')
    await ctx.send(f'{len(tournaments)}')
    counter = 0
    try:
        clans = await db.fetch("""SELECT tc.clantag FROM team_clan tc join team t on t.team_id = tc.team_id
		where
		t.active""")
    except errors.NotFoundException:
        return
    clans = [c.get('clantag') for c in clans]
    async with asyncpg.create_pool(**bot.config.database_utils.__dict__) as db_lu:
        async with db_lu.acquire() as conn:
            for t in tournaments:
                m_count = 0
                try:
                    matches = await conn.fetch("""WITH data as (SELECT distinct on (match_id) m.tournament_id,
									tournament, tournament_name,
							                                            schedule_id, schedule_name, match_date, match_id, match_type, team_id1,
							                                            team1, t1.clan_id as team1_clan1, t1.event_clan_id as team1_clan2,
							                                            team_id2, team2, t2.clan_id as team2_clan1, t2.event_clan_id as
							                                            team2_clan2 from
							                                            bot.match_detail m
							    JOIN bot.team t1 on m.team_id1 = t1.team_id
							    JOIN bot.team t2 on m.team_id2 = t2.team_id
							    JOIN bot.tournament_parameter tp on m.tournament_id = tp.tournament_id
							WHERE m.match_date is not null and m.tournament_id = $2 and (t1.clan_id in (
							SELECT UNNEST($1::varchar[])) or t1.event_clan_id in (
							SELECT UNNEST($1::varchar[])) or t2.clan_id in (
							SELECT UNNEST($1::varchar[])) or t2.event_clan_id in (
							SELECT UNNEST($1::varchar[]))))
							SELECT * from data ORDER BY match_date;""", clans, t.get('lu_tournament_id'))
                    if not matches:
                        matches = []
                except Exception as e:
                    raise e
                await ctx.send(f'{len(matches)}')
                for m in matches:
                    source = 'lu'
                    try:
                        m_old = await db.fetchrow('SELECT * from um_table where match_id = $1 and source = $2',
                                                  m.get('match_id'), source)
                        um_id = m_old.get('um_id')
                    except errors.NotFoundException:
                        # new match
                        [um_id] = await db.fetchrow("""INSERT INTO um_table(source, match_id, match_date, team1_name,
						team1_id,
									team1_clan1, team1_clan2,
									 team2_name, team2_id, team2_clan1, team2_clan2, tournament_id, tournament_name, week_id, week_name,
									 match_type)
									 VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
									 $16) RETURNING um_id""",
                                                    source, m.get('match_id'), m.get('match_date'), m.get('team1'),
                                                    m.get('team_id1'),
                                                    m.get('team1_clan1'), m.get('team1_clan2'),
                                                    m.get('team2'), m.get('team_id2'), m.get('team2_clan1'), m.get('team2_clan2'),
                                                    m.get('tournament_id'), m.get('tournament_name'),
                                                    m.get('schedule_id'), m.get('schedule_name'), m.get('match_type'))
                        m_count += 1
                    else:
                        # update existing
                        await db.execute("""UPDATE um_table set match_date = $1, team1_name = $2, team1_id = $3, team2_name =
									$4,
									team2_id = $5, week_name = $6, team1_clan1 = $9, team1_clan2 = $10, team2_clan1 = $11, team2_clan2 = $12 WHERE
									source = $7 and match_id = $8""",
                                         m.get('match_date'),
                                         m.get('team1'),
                                         m.get('team_id1'), m.get('team2'), m.get('team_id2'), m.get('schedule_name'),
                                         source, m.get('match_id'), m.get('team1_clan1'), m.get('team1_clan2'),
                                         m.get('team2_clan1'),
                                         m.get('team2_clan2'))
                    
                    try:
                        um = await db.fetchrow("""SELECT * from um_table where um_id = $1""", um_id)
                    except errors.NotFoundException:
                        continue
                    streams_2_stay = []
                    # fetch streams
                    if um.get('source') == 'lu':
                        try:
                            streams = await conn.fetch("""SELECT * from bot.stream s join bot.streamer st on
							s.streamer =
							st.discord_id JOIN bot.streamer_platform sp on st.discord_id = sp.discord_id where match_id = $1
							ORDER BY st.discord_id, sp.url""",
                                                       um.get('match_id'))
                            if not streams:
                                streams = []
                        except errors.NotFoundException:
                            streams = []
                    else:
                        streams = []
                    for stream in streams:
                        try:
                            old_stream = await db.fetchrow(
                                    'SELECT * from um_streams where um_id = $1 and streamer_id = '
                                    '$2 and '
                                    'url = $3', um_id, stream.get('discord_id'), stream.get('url'))
                        except errors.NotFoundException:
                            [um_stream_id] = await db.fetchrow("""INSERT INTO um_streams(um_id, stream_start, streamer_id,
							streamer_lang,
							streamer_name, platform, url) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING um_stream_id""",
                                                               um_id,
                                                               stream.get(
                                                                       'stream_start'),
                                                               stream.get('discord_id'),
                                                               stream.get('streamer_lang'),
                                                               stream.get('name'),
                                                               stream.get('platform'), stream.get('url'))
                            streams_2_stay.append(um_stream_id)
                        else:
                            await db.execute("""UPDATE um_streams set stream_start = $1 where um_stream_id = $2""",
                                             stream.get('stream_start'), old_stream.get('um_stream_id'))
                            streams_2_stay.append(old_stream.get('um_stream_id'))
                if matches:
                    await ctx.send(f'{m_count}/{len(matches)}')
                    counter += 1
    await ctx.send(f'{counter}/{len(tournaments)}')


async def import_wc_picks():
    async def save_picture(person_id: int, picture: Image):
        t_size = (400, 400)
        t_dpi = (72, 72)
        im = picture.convert('RGBa')
        im = im.crop(gfx.getbbox(im))
        im = im.convert('RGBA')
        im.save(f"assets/persons/p-{person_id}.png")
        im = gfx.smart_rescale(im, t_size, 'q')
        im.save(f"assets/persons/p-{person_id}-sized.png", dpi=t_dpi)
        await db.execute("UPDATE person set has_image = true where person_id = $1", person_id)
        print(f'Processed {person_id}')
        try:
            im.close()
            picture.close()
        except Exception:
            pass
        return True
    
    for f in pathlib.Path('assets/processed').glob('*.png'):
        source = str(f)
        filename = '#' + f.stem
        try:
            [person_id] = await db.fetchrow('SELECT person_id from person_player_names where player_tag = $1',
                                            filename)
        except errors.NotFoundException:
            print(f'Not found {filename}')
            continue
        try:
            result = await save_picture(person_id, Image.open(source))
        except Exception:
            print(f'Error {filename}')
            traceback.print_exc()
            continue
        if result:
            print(f'Processed {filename} {person_id}')

#scheduler.add_job(check_free_agents, 'interval', hours=1, id='check_free_agents', replace_existing=True)
async def fix():
    for t in ['#Y8C802CY', '#Y8C802CY', '#2YRPPG8QV', '#2LVQJ298Q',]:
        w = await bot.clash_client.get_clan_war(t, cls=TrackedWar)
        try:
            [match_id] = await db.fetchrow('SELECT m.match_id from match_results mr join matches m on m.match_id = '
                                           'mr.match_id where m.prep_start = $1 and mr.clan_tag = $2',
                                           w.preparation_start_time.time, t)
        except errors.NotFoundException:
            print(f'Not found {t} {w.preparation_start_time.time}')
            continue
        try:
            team = await Team().from_metadata(w.opponent.tag)
            team_id = team.team_id
        except errors.NotFoundException:
            team_id = None
        try:
            [result_id] = (await db.fetchrow('INSERT INTO match_results(match_id, clan_tag, clan_name, stars, perc'
                                             ', duration, team_id, result) VALUES($1, $2,$3,$4, $5, $6,$7, $8) RETURNING result_id',
                                             match_id, w.opponent.tag, w.opponent.name, w.opponent.stars,
                                             w.opponent.destruction,
                                             w.opponent.average_attack_duration, team_id, 'ongoing'))
        except errors.NotFoundException:
            result_id = None
        except Exception:
            traceback.print_exc()
            continue

async def get_player_by_id(id, token):
    base_url = "https://open.faceit.com/data/v4/"
    try:
        player = await db.fetchrow('SELECT * from esl_players where player_id = $1', id)
    except errors.NotFoundException:
        player = None
    if not player:
        # fetch from faceit api
        async with aiohttp.ClientSession(headers={'Authorization': f'Bearer {token}'}) as session:
            url = f"{base_url}players/{id}"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return
                try:
                    data = await resp.json()
                except Exception:
                    log_scheduled_tasks.error(f'{url=}\n{resp=}\n{await resp.text()}')
                    return
                player_id = data.get('player_id')
                player_name = data.get('nickname')
                if data.get('games') and data.get('games',{}).get('clash-of-clans'):
                    player_tag = data.get('games',{}).get('clash-of-clans').get('game_player_id')
                    if player_tag:
                        await db.execute('INSERT INTO esl_players(player_id, player_name, player_tag) '
                                         'VALUES($1, $2, $3)',player_id, player_name, player_tag)
                        try:
                            player = await db.fetchrow('SELECT * from esl_players where player_tag = $1',
                                                       player_tag)
                        except errors.NotFoundException:
                            player = None
    return player
async def find_esl_claimed_player(championship_id: str):
    token = "1349ab9e-2680-46b1-b9ea-b3640cc9a7ae"
    q1_teams = {'temp': {}}
    #channel = await bot.client.fetch_channel(1252352740500439050)
    #channel2 = await bot.client.fetch_channel(1000761963711823912)
    found = []
    try:
        stats = await db.fetchrow('Select count(*) as players, count(distinct player_tag) as distinct_tags, '
                                  'count(distinct player_id) as distinct_players from esl_players')
    except errors.NotFoundException:
        stats = {}
    try:
        stats2 = await db.fetchrow('Select count(*) as p_ts, count(*) filter (where current_qualifier = true) as active_pts,'
                                   'count(distinct player_id) as distinct_players, count(distinct player_id) filter (where current_qualifier = true) as distinct_active_players '
                                   'from esl_player_teams')
    except errors.NotFoundException:
        stats2 = {}
    print(f'players: {stats.get("players")}\ndistinct tags: {stats.get("distinct_tags")}\n'
          f'distinct players: {stats.get("distinct_players")}')
    print(f"p_ts: {stats2.get('p_ts')}\nactive pts: {stats2.get('active_pts')}\n"
          f"distinct_players: {stats2.get('distinct_players')}\na distinct players: {stats2.get('distinct_active_players')}")
    claimed = await db.fetch("SELECT p.discord_id, pp.player_tag from person p join person_player pp on p.person_id = "
                             "pp.person_id "
                             "order by pp.player_tag")
    url_player = f"https://open.faceit.com/data/v4/players?game=clash-of-clans&game_player_id=%23"
    url_player_teams = f"https://open.faceit.com/data/v4/players/{{player_id}}"
    url_teams = (f"https://open.faceit.com/data/v4/championships/{{championship_id}}/subscriptions"
                 f"?offset={{offset}}&limit=10")
    players = {}
    async with aiohttp.ClientSession(headers={'Authorization': f'Bearer {token}',
                                              'accept': "application/json"}) as session:
        current_page_length = 10
        current_page = 0
        len_last_iter = len(q1_teams) - 1
        counter = 0
        err_counter = 0
        while len_last_iter != len(q1_teams) or err_counter < 20:
            url = url_teams.format(offset=str(10 * current_page), championship_id=championship_id)
            
            async with session.get(url) as r:
                counter += 1
                if r.status == 200:
                    data = await r.json()
                    
                    current_page += 1
                    len_last_iter = len(q1_teams)
                    for item in data.get('items'):
                        team = item.get('team')
                        await db.execute('UPDATE esl_player_teams set current_qualifier = true,'
                                         'championship_id=$2 where team_id = $1',
                                         team.get('team_id'), championship_id)
                        q1_teams[team.get('team_id')] = {'name': team.get('name'), 'team_id': team.get('team_id')}
                        for mem in team.get('members', []):
                            players[mem.get('user_id')] = {'team_id': team.get('team_id'), 'team_name': team.get(
                                    'name'), 'nickname': mem.get('nickname')}
                            try:
                                p1 = await get_player_by_id(mem.get('user_id'), token)
                                await db.execute('INSERT INTO esl_player_teams (player_id, team_id, '
                                                 'current_qualifier, championship_id) '
                                                 'VALUES($1, $2, TRUE, $4) ON CONFLICT DO NOTHING',
                                                 mem.get('user_id'), team.get('team_id'), championship_id)
                            except Exception:
                                player = None
                    if len_last_iter == len(q1_teams):
                        err_counter += 1
                else:
                    print(f"Error {current_page_length} {len(q1_teams)} {len_last_iter} {r.status}")
                if counter % 5 == 0:
                    print(f"Found {len(q1_teams)} teams, {counter=}")
                if r.status != 200:
                    data = None
                    try:
                        data = await r.json()
                    except Exception:
                        pass
                    try:
                        data = await r.text()
                    except Exception:
                        pass
                    print(f"Error {r.status} {data=}")
                    break
        print(f"Found {len(q1_teams)} teams, checking now {len(claimed)} players.")
        try:
            stats = await db.fetchrow('Select count(*) as players, count(distinct player_tag) as distinct_tags, '
                                      'count(distinct player_id) as distinct_players from esl_players')
        except errors.NotFoundException:
            stats = {}
        try:
            stats2 = await db.fetchrow(
                    'Select count(*) as p_ts, count(*) filter (where current_qualifier = true) as active_pts,'
                    'count(distinct player_id) as distinct_players, count(distinct player_id) filter (where current_qualifier = true) as distinct_active_players '
                    'from esl_player_teams')
        except errors.NotFoundException:
            stats2 = {}
        print(f'players: {stats.get("players")}\ndistinct tags: {stats.get("distinct_tags")}\n'
              f'distinct players: {stats.get("distinct_players")}')
        print(f"p_ts: {stats2.get('p_ts')}\nactive pts: {stats2.get('active_pts')}\n"
              f"distinct_players: {stats2.get('distinct_players')}\na distinct players: {stats2.get('distinct_active_players')}")
        counter = 0
        return None
    # for c in claimed:
    # 	counter += 1
    # 	player_tag = c.get('player_tag')
    # 	url = f"{url_player}{player_tag.replace('#', '')}"
    # 	player_id = acc_nick_name = None
    # 	async with session.get(url) as r:
    # 		if r.status == 200:
    # 			data = await r.json()
    # 			player_id = data.get('player_id')
    # 			acc_nick_name = data.get('nickname')
    # 	if not player_id:
    # 		continue
    # 	try:
    # 		p = players.get(player_id)
    # 		if p:
    # 			await channel.send(f"Found: {acc_nick_name} `{player_tag}` playing for `{p.get('team_name')}` "
    # 							   f"belongs to <@{c.get('discord_id')}> `{c.get('discord_id')}`")
    # 	except Exception:
    # 		traceback.print_exc()
    # 	if counter % 10 == 0:
    # 		await channel2.send(f"Checked {counter} players")


async def count_esl_matches(qualifier_id: str, token: str = "1349ab9e-2680-46b1-b9ea-b3640cc9a7ae"):
    """Count the number of matches per team"""
    qualifier_teams = {}
    limit = 10
    page_counter = 0
    err_counter = 0
    no_change_in_teams = 0
    url_teams = (f"https://open.faceit.com/data/v4/championships/{{championship_id}}/subscriptions"
                 f"?offset={{offset}}&limit={{limit}}")
    url_matches = (f"https://open.faceit.com/data/v4/championships/{{championship_id}}/matches"
                   f"?offset={{offset}}&limit={{limit}}")
    async with aiohttp.ClientSession(headers={'Authorization': f'Bearer {token}',
                                              'accept': "application/json"}) as session:
        len_last_iter = -1
        while err_counter < 20 and no_change_in_teams < 2:
            url = url_teams.format(offset=str(limit * page_counter), limit=limit, championship_id=qualifier_id)
            page_counter += 1
            async with session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    len_last_iter = len(qualifier_teams)
                    for item in data.get('items'):
                        team = item.get('team')
                        qualifier_teams[team.get('team_id')] = {'name': team.get('name'),
                                                                'id': team.get('team_id'),
                                                                'match_count': 0,
                                                                'bye_count': 0,
                                                                'matches': []}
                    if len_last_iter == len(qualifier_teams):
                        no_change_in_teams += 1
        # get matches for championship
        page_counter = 0
        err_counter = 0
        no_change_in_matches = 0
        match_counter = 0
        bye_counter = 0
        while err_counter < 20 and no_change_in_matches < 2:
            url = url_matches.format(offset=str(limit * page_counter), limit=limit, championship_id=qualifier_id)
            page_counter += 1
            async with session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    len_last_iter = match_counter
                    for item in data.get('items'):
                        match_counter += 1
                        data_dict = {}
                        fraction1 = item.get('teams',{}).get('faction1',{})
                        fraction2 = item.get('teams',{}).get('faction2',{})
                        for k, v in item.items():
                            if k in ['faceit_url']:
                                data_dict[k] = v.replace('/{lang}/', '/en/')
                            if k in ['match_id', 'chat_room_id', 'best_of', 'round', 'group']:
                                data_dict[k] = v
                            if k in ['scheduled_at', 'started_at', 'finished_at']:
                                try:
                                    timestamp = v
                                except Exception:
                                    timestamp = v
                                data_dict[k] = timestamp
                        team1 = {'id': fraction1.get('faction_id'), 'name': fraction1.get('name')}
                        team2 = {'id': fraction2.get('faction_id'), 'name': fraction2.get('name')}
                        data_dict['team1'] = team1
                        data_dict['team2'] = team2
                        if team1['id'] == 'bye' or team2['id'] == 'bye':
                            bye_counter += 1
                        # deal with result
                        result = item.get('results',{})
                        if result.get('winner') == 'faction1':
                            data_dict['winner'] = team1['id']
                        elif result.get('winner') == 'faction2':
                            data_dict['winner'] = team2['id']
                        else:
                            data_dict['winner'] = result.get('winner')
                        if team1['id'] in qualifier_teams:
                            qualifier_teams[team1['id']]['match_count'] += 1
                            qualifier_teams[team1['id']]['matches'].append(data_dict)
                            if team2['id'] == 'bye':
                                qualifier_teams[team1['id']]['bye_count'] += 1
                        if team2['id'] in qualifier_teams:
                            qualifier_teams[team2['id']]['match_count'] += 1
                            qualifier_teams[team2['id']]['matches'].append(data_dict)
                            if team1['id'] == 'bye':
                                qualifier_teams[team2['id']]['bye_count'] += 1
                    if len_last_iter == match_counter:
                        no_change_in_matches += 1
        # dump to json
        with open(f'{qualifier_id}.json', 'w') as f:
            json.dump(qualifier_teams, f)
            f.flush()
        # dump to csv
        data_str = 'Id\tName\tMatch Count\tBye Count\n'
        for k, v in qualifier_teams.items():
            data_str += f"{v['id']}\t{v['name']}\t{v['match_count']}\t{v['bye_count']}\n"
        with open(f'{qualifier_id}.csv', 'w') as f:
            f.write(data_str)
        print(f"Found {len(qualifier_teams)} teams, {match_counter} matches, {bye_counter} byes")
        return qualifier_teams







async def kofi2rts():
    # fetch all kofi roles
    guild = bot.client.get_guild(791076875464081418) or await bot.client.fetch_guild(791076875464081418)
    for name in ['Champion', 'Legend', 'Titan', 'Team Pass', 'Team pass']:
        role = discord.utils.get(guild.roles, name=name)
        if role:
            for member in role.members:
                try:
                    [user_id] = await db.fetchrow('SELECT user_id from rts.user where discord_id = $1',
                                                  str(member.id))
                except errors.NotFoundException:
                    [user_id] = await db.fetchrow('Insert into rts.user(discord_id, user_name) VALUES($1, $2) '
                                                  'RETURNING user_id', str(member.id), member.name)
                try:
                    spending = await db.fetchrow('SELECT * from rts.user_spending where user_id = $1 and tier_id = $2',
                                                 user_id, 1)
                    if spending.get('active_until') < datetime.datetime.utcnow() + datetime.timedelta(days=2):
                        await db.execute('UPDATE rts.user_spending set active_until = $1 where user_id = $2 and tier_id = $3',
                                         datetime.datetime.utcnow()+datetime.timedelta(days=2), user_id, 1)
                except errors.NotFoundException:
                    await db.execute('INSERT INTO rts.user_spending(user_id, tier_id, active_until) '
                                     'VALUES($1, $2, $3)',
                                     user_id, 1, datetime.datetime.utcnow()+datetime.timedelta(days=2))
        
        else:
            print(f'Not found {name}')

async def check_eos_trophies_player(player: dict, season: str):
    """Take a player and check the amount of trophies"""
    # fetch from clashking api
    async with aiohttp.ClientSession() as session:
        url = f"https://api.clashking.xyz/player/%23{player['tag'].replace('#', '')}/legends?season={season}"
        async with session.get(url) as resp:
            if resp.status != 200:
                log_scheduled_tasks.warning(f'{url=}\n{resp=}\n{await resp.text()}\n{resp.status=}\n{player=}')
                await db.execute("INSERT INTO eos_invest(player_tag, season, reported_trophies, tracked_trophies, "
                                 "location, global_rank, attacks_won, defenses_won) "
                                 "VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                                 player['tag'], season, player['trophies'], -1, '', player['rank'],
                                 player['attack_wins'],
                                 player['defense_wins'])
                return
            try:
                data = await resp.json()
            except Exception as e:
                log_scheduled_tasks.warning(f'{url=}\n{resp=}\n{await resp.text()}\n{resp.status=}\n{player=}')
                await db.execute("INSERT INTO eos_invest(player_tag, season, reported_trophies, tracked_trophies, "
                                 "location, global_rank, attacks_won, defenses_won) "
                                 "VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                                 player['tag'], season, player['trophies'], -1, '', player['rank'],
                                 player['attack_wins'],
                                 player['defense_wins'])
                return
            tracked_attacks = 0
            tracked_defenses = 0
            tracked_attacks_won = 0
            tracked_defenses_won = 0
            trophies_sum = 0
            reported_tracked_attacks = 0
            latest_attack_timestamp = 0
            latest_attack_trophies = 0
            latest_defense_timestamp = 0
            latest_defense_trophies = 0
            for day in data.get('legends', {}).values():
                reported_tracked_attacks += day.get('num_attacks', 0)
                # iterate over attacks
                for a in day.get('new_attacks', []):
                    # figure out how many attacks this was
                    tracked_attacks += max(1, ceil(a.get('change',0) / 40) )
                    tracked_attacks_won += max(1, ceil(a.get('change',0) / 40) ) if a.get('change',0) > 0 else 0
                    if a.get('time', 0) > latest_attack_timestamp:
                        latest_attack_timestamp = a.get('time', 0)
                        latest_attack_trophies = a.get('trophies', 0)
                    trophies_sum += a.get('change', 0)
                # iterate over defenses
                for d in day.get('new_defenses', []):
                    tracked_defenses += max(1, ceil(d.get('change',0) / 40))
                    tracked_defenses_won += 1 if d.get('change',0) == 0 else 0
                    if d.get('time', 0) > latest_defense_timestamp:
                        latest_defense_timestamp = d.get('time', 0)
                        latest_defense_trophies = d.get('trophies', 0)
                    trophies_sum -= d.get('change', 0)
            # get location
            try:
                [location] = await db.fetchrow('SELECT location from player_names p where player_tag = $1',
                                               player['tag'])
            except errors.NotFoundException:
                # try to get it from clashking
                try:
                    await get_nationality([[player['tag']]])
                    [location] = await db.fetchrow('SELECT location from player_names p where player_tag = $1',
                                                   player['tag'])
                except errors.NotFoundException:
                    location = ''
            location = location or ''
            # get final trophies
            if latest_attack_timestamp > latest_defense_timestamp:
                final_trophies = latest_attack_trophies
            elif latest_defense_timestamp > latest_attack_timestamp:
                final_trophies = latest_defense_trophies
            elif latest_defense_timestamp == latest_attack_timestamp and latest_defense_trophies == latest_attack_trophies:
                final_trophies = latest_defense_trophies
            else:
                final_trophies = -1
            latest_attack_timestamp = datetime.datetime.utcfromtimestamp(latest_attack_timestamp)
            latest_defense_timestamp = datetime.datetime.utcfromtimestamp(latest_defense_timestamp)
            
            await db.execute("INSERT INTO eos_invest(player_tag, season, reported_trophies, tracked_trophies, "
                             "location, global_rank, local_rank, attacks_won, defenses_won, tracked_attacks_won, "
                             "tracked_attacks, tracked_defenses_won, tracked_defenses, last_defense_trophies,"
                             "last_defense_timestamp, last_attack_trophies, last_attack_timestamp, tracked_trophies_sum) "
                             "VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)",
                             player['tag'], season, player['trophies'], final_trophies, location, player['rank'], -1,
                             player['attack_wins'], player['defense_wins'], tracked_attacks_won, tracked_attacks,
                             tracked_defenses_won, tracked_defenses, latest_defense_trophies, latest_defense_timestamp,
                             latest_attack_trophies, latest_attack_timestamp, trophies_sum)

async def worker(name, queue):
    print(f'Worker {name} started')
    counter = 0
    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(2)
            continue
        try:
            await check_eos_trophies_player(item, item.get('season'))
        except Exception as e:
            player = item
            season = item.get('season')
            await db.execute("INSERT INTO eos_invest(player_tag, season, reported_trophies, tracked_trophies, "
                             "location, global_rank, attacks_won, defenses_won) "
                             "VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                             player['tag'], season, player['trophies'], -2, '', player['rank'],
                             player['attack_wins'],
                             player['defense_wins'])
            log_scheduled_tasks.error(f'{item=}\n{e=}\n{traceback.format_exc()}')
        
        queue.task_done()
        counter += 1
        #print(f'Worker {name} done {item.get("tag")} {item.get("season")} {item.get("rank")}')
        await asyncio.sleep(0.5)
    print(f'Worker {name} finished {counter=}')


async def check_eos_trophies(season: str = '2024-08', debug: bool = False):
    """Check the amount of trophies for players"""
    base_url = "https://api.clashofclans.com/v1"
    league_id = 29000022
    season_id = season
    kwargs = {"limit": 20}
    after = 'init'
    background_tasks = set()
    counter = 0
    scheduled = 0
    queue = asyncio.Queue()
    tasks = []
    
    def cleanup(x):
        background_tasks.discard(x)
        print(f'Cleanup {x.get_name()}')
    
    for i in range(10):
        task = asyncio.create_task(worker(f'worker-{season}-{i}', queue),
                                   name=f'worker-{season}-{i}')
        
        task.add_done_callback(lambda x: cleanup(x))
        background_tasks.add(task)
        tasks.append(task)
    
    while after:
        route = Route("GET",
                      base_url,
                      "/leagues/{}/seasons/{}".format(league_id, season_id),
                      **kwargs)
        try:
            ranked_players = await bot.event_clash_client.http.request(route)
        except Exception as e:
            bot.logger.error(traceback.format_exc()+"\n"+str(route.url) )
            break
        
        players = [coc.RankedPlayer(data=n, client=bot.event_clash_client) for n in ranked_players.get("items", [])]
        if ranked_players.get('paging', {}).get('cursors', {}).get('after'):
            kwargs["after"] = ranked_players.get('paging', {}).get('cursors', {}).get('after')
            after = ranked_players.get('paging', {}).get('cursors', {}).get('after')
        # create asyncio tasks to call check_eos_trophies_player for every player
        for player in players:
            p = {'tag': player.tag, 'trophies': player.trophies, 'rank': player.rank, 'attack_wins': player.attack_wins,
                 'defense_wins': player.defense_wins, 'season': season}
            queue.put_nowait(p)
            scheduled += 1
        #task.add_done_callback(background_tasks.discard)
        
        counter += 1
        
        if (counter > 0 and counter % 5 == 0) or debug:
            c_tasks = asyncio.all_tasks()
            task_counter = {'done': 0, 'cancelled': 0, 'pending': 0, 'total': 0}
            for t in c_tasks:
                if f'worker-{season}' in t.get_name():
                    task_counter['total'] += 1
                    if t.done():
                        task_counter['done'] += 1
                    elif t.cancelled():
                        task_counter['cancelled'] += 1
                    else:
                        task_counter['pending'] += 1
            task_counter['scheduled_set'] = len(background_tasks)
            task_counter['scheduled_list'] = len(tasks)
            task_counter['created'] = scheduled
            task_counter = json.dumps(task_counter, indent=4)
            log_scheduled_tasks.error(f"{task_counter}\n{counter=}")
        if debug and counter > 3:
            break
    log_scheduled_tasks.error(f"Finished Queuing {counter=} {scheduled=} {queue.qsize()=} {season=}")
    c_tasks = asyncio.all_tasks()
    task_counter = {'done': 0, 'cancelled': 0, 'pending': 0, 'total': 0}
    for t in c_tasks:
        if f'worker-{season}-' in t.get_name():
            task_counter['total'] += 1
            if t.done():
                task_counter['done'] += 1
            elif t.cancelled():
                task_counter['cancelled'] += 1
            else:
                task_counter['pending'] += 1
    task_counter['scheduled_set'] = len(background_tasks)
    task_counter['scheduled_list'] = len(tasks)
    task_counter['created'] = scheduled
    if task_counter['pending'] == 0:
        for i in range(10):
            task = asyncio.create_task(worker(f'worker-{i}-{season}', queue), name=f'worker-{season}-{i}')
            
            task.add_done_callback(lambda x: cleanup(x))
            task.set_exception()
            background_tasks.add(task)
            tasks.append(task)
    
    started_at = time.monotonic()
    iter = 0
    if debug:
        while queue.qsize() > 0 and iter < 10:
            log_scheduled_tasks.error(f"Queue size {queue.qsize()=} {season=} {iter=}")
            await asyncio.sleep(60)
            iter += 1
    if not debug and queue.qsize() > 0:
        log_scheduled_tasks.error(f"Join Queue size {queue.qsize()=} {season=}")
        await queue.join()
    total_slept_for = time.monotonic() - started_at
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    print(f'Finished {total_slept_for=}')


async def debug_eos_invest():
    tasks = asyncio.all_tasks()
    task_counter = {'done': 0, 'cancelled': 0, 'pending': 0, 'total': 0}
    for t in tasks:
        if 'check_eos_trophies_player' in t.get_name():
            task_counter['total'] += 1
            if t.done():
                task_counter['done'] += 1
            elif t.cancelled():
                task_counter['cancelled'] += 1
            else:
                task_counter['pending'] += 1
    task_counter = json.dumps(task_counter, indent=4)
    log_scheduled_tasks.error(f"{task_counter}")


async def run_uc_matches(user_id, team_id):
    """Run UC matches webhook for a user and a team"""
    # get webhook
    try:
        [webhook_id] = await db.fetchrow('SELECT webhook_id from public.webhooks w where user_id = $1 and '
                                         'team_id = $2 and webhook_type::text = $3 and webhook_active = true',
                                         user_id, team_id, 'uc_matches')
    except errors.NotFoundException:
        return
    webhook = await Webhook.by_id(webhook_id)
    # get user
    try:
        rts_user = await db.fetchrow('SELECT * from rts.user where discord_id::bigint = $1', user_id)
    except errors.NotFoundException:
        return
    try:
        [rts_active] = await db.fetchrow('SELECT count(*) from rts.user_spending where user_id = $1 and '
                                         'active_until > $2', rts_user.get('user_id'), datetime.datetime.utcnow())
    except errors.NotFoundException:
        return
    if not rts_active:
        return
    # get team
    try:
        team = await Team().from_id(team_id)
    except errors.NotFoundException:
        return
    embeds = []
    after = Datetime.utcnow() - datetime.timedelta(minutes=50)
    before = Datetime.utcnow() + datetime.timedelta(days=10)
    show_only_streamed = "No"
    try:
        clans = await db.fetch('SELECT clantag from team_clan where team_id = $1', team.team_id)
        clans = [c.get('clantag') for c in clans]
    except errors.NotFoundException:
        clans = []
    title = f"Upcoming matches of team {team.name}"
    if after:
        title += f" after {after.strftime('%Y-%m-%d %H:%M')}"
    else:
        after = Datetime.utcnow()
    query = """SELECT * from um_table left outer join constructs c on c.source_key = um_table.source_key
    where match_date > $1 and
    (team1_clan1 in (SELECT UNNEST($2::varchar[])) or
    team1_clan2 in (SELECT UNNEST($2::varchar[])) or
    team2_clan1 in (SELECT UNNEST($2::varchar[])) or
    team2_clan2 in (SELECT UNNEST($2::varchar[])))  """
    args = [after, clans]
    if before:
        title += f" before {before.strftime('%Y-%m-%d %H:%M')}"
        args.append(before)
        query += f" and match_date < ${len(args)}"
    
    if show_only_streamed == "Yes":
        query += " and (SELECT count(*) from um_streams ums where ums.um_id = um_table.um_id) > 0"
        title += " (only streamed matches)"
    query += " order by match_date"
    try:
        schedules = await db.fetch(query, *args)
    except errors.NotFoundException:
        schedules = []
    except Exception as e:
        log_scheduled_tasks.error(f'{traceback.format_exc()}\n{query=}\n{args=}')
        schedules = []
    if not schedules:
        await webhook.send([])
    embeds = []
    embed = discord.Embed(title=title,
                          color=discord.Color.from_rgb(221, 183, 118), timestamp=datetime.datetime.now(tz=tz.UTC))
    if team.logo_path:
        embed.set_thumbnail(url=team.u_logo_url)
    for m in schedules:
        opponent_elo = 'N/A'
        event_logo = None
        hard_mode = False
        has_restrictions = False
        try:
            [construct_id] = await db.fetchrow('SELECT construct_id from constructs c where c.source_key = $1',
                                               m.get('source_key'))
            
            event = await Event().from_id(construct_id)
            if event.battle_modifier and event.battle_modifier.lower() != 'none':
                hard_mode = True
            restrictions = event.restriction
            if restrictions:
                has_restrictions = True
            if not has_restrictions:
                try:
                    res = await event.restrictions
                except errors.NotFoundException:
                    res = []
                if res:
                    has_restrictions = True
            event_name = event.name
        except errors.NotFoundException:
            event_name = m.get('tournament_name')
        if m.get('team2_clan1') not in clans:
            opponent_name = m.get('team2_name')
            opponent_clans = [m.get('team2_clan1'), m.get('team2_clan2')]
            opponent_logo = m.get('team2_logo')
        elif m.get('team1_clan1') not in clans:
            opponent_name = m.get('team1_name')
            opponent_clans = [m.get('team1_clan1'), m.get('team1_clan2')]
            opponent_logo = m.get('team1_logo')
        elif m.get('team2_clan2') and m.get('team2_clan2') not in clans:
            opponent_name = m.get('team2_name')
            opponent_clans = [m.get('team2_clan1'), m.get('team2_clan2')]
            opponent_logo = m.get('team2_logo')
        elif m.get('team1_clan2') and m.get('team1_clan2') not in clans:
            opponent_name = m.get('team1_name')
            opponent_clans = [m.get('team1_clan1'), m.get('team1_clan2')]
            opponent_logo = m.get('team1_logo')
        else:
            opponent_name = m.get('team1_name')
            opponent_clans = [m.get('team1_clan1'), m.get('team1_clan2')]
            opponent_logo = m.get('team1_logo')
        opponent_id = None
        opponent = None
        for c in opponent_clans:
            if c not in clans:
                try:
                    opponent = await Team().from_metadata(c)
                    if opponent.logo_path:
                        opponent_logo = opponent.logo_path
                    opponent_name = opponent.name
                    opponent_id = opponent.id
                    opponent_elo = str(await opponent.get_elo(datetime.datetime.utcnow()))
                    break
                except errors.NotFoundException:
                    opponent_id = None
        match_date: Datetime = Datetime.fromtimestamp(int(m.get('match_date').timestamp()))
        desc = f"{bot.custom_emojis.mantelpiece_clock} <t:{match_date.t_stamp}:f>\n"
        desc += f"🏆 {discord.utils.escape_markdown(event_name)}\n"
        desc += f"🗓️ {discord.utils.escape_markdown(m.get('week_name'))}\n"
        if hard_mode:
            desc += f"{bot.custom_emojis.hard_mode} Hard mode\n"
        if has_restrictions:
            desc += f"🏴 Additional Restrictions\n"
        desc += f'🌐 [Website](https://competitiveclash.network/matches/upcoming/{m.get("um_id")})'
        if (not embed.fields or len(embed.fields) < 25) and len(embed) < 5500:
            embed.add_field(name=f"Match vs {discord.utils.escape_markdown(opponent_name)}",
                            value=desc,
                            inline=False)
        else:
            embeds.append(embed)
            embed = discord.Embed(title=title, color=discord.Color.from_rgb(221, 183, 118),
                                  timestamp=datetime.datetime.now(tz=tz.UTC))
            if team.logo_path:
                embed.set_thumbnail(url=team.u_logo_url)
            embed.add_field(name=f"Match vs {discord.utils.escape_markdown(opponent_name)}",
                            value=desc,
                            inline=False)
    if embed.fields:
        embeds.append(embed)
    await webhook.send(embeds)


async def run_uc_streams(user_id, team_id):
    """Run UC matches webhook for a user and a team"""
    # get webhook
    try:
        [webhook_id] = await db.fetchrow('SELECT webhook_id from public.webhooks w where user_id = $1 and '
                                         'team_id = $2 and webhook_type::text = $3 and webhook_active = true',
                                         user_id, team_id, 'uc_streams')
    except errors.NotFoundException:
        return
    webhook = await Webhook.by_id(webhook_id)
    # get user
    try:
        rts_user = await db.fetchrow('SELECT * from rts.user where discord_id::bigint = $1', user_id)
    except errors.NotFoundException:
        return
    try:
        [rts_active] = await db.fetchrow('SELECT count(*) from rts.user_spending where user_id = $1 and '
                                         'active_until > $2', rts_user.get('user_id'), datetime.datetime.utcnow())
    except errors.NotFoundException:
        return
    if not rts_active:
        return
    # get team
    try:
        team = await Team().from_id(team_id)
    except errors.NotFoundException:
        return
    embeds = []
    after = Datetime.utcnow() - datetime.timedelta(minutes=50)
    before = Datetime.utcnow() + datetime.timedelta(days=10)
    sql = '''SELECT * from upcoming_matches um JOIN um_streams ums on um.um_id = ums.um_id
            where match_date >= $1 and match_date < $2 and $3 in (team1_id, team2_id) order by match_date limit 40'''
    args = (after, before, team.id)
    # date_from = date_from.replace(hour=0, minute=0, second=0, microsecond=0)
    # date_to = date_to.replace(hour=0, minute=0, second=0, microsecond=0)
    embeds = []
    prpcessed = []
    try:
        matches = await db.fetch(sql, *args)
    except errors.NotFoundException:
        matches = []
    
    main_embed = discord.Embed(title=f"Upcoming streams",
                               color=discord.Color.from_rgb(221, 183, 118),
                               timestamp=datetime.datetime.now(tz=tz.UTC))
    
    match_informations = []
    title = (f"Streams of upcoming matches of team {team.name} from {after.strftime('%Y-%m-%d %H:%M')} to "
             f"{before.strftime('%Y-%m-%d %H:%M')}")
    embed = discord.Embed(title=title, color=discord.Color.from_rgb(221, 183, 118),
                          timestamp=datetime.datetime.now(tz=tz.UTC))
    if team.logo_path:
        embed.set_thumbnail(url=team.u_logo_url)
    embeds = []
    for idx, match in enumerate(matches):
        if match.get('um_id') in prpcessed:
            continue
        # get team1
        team1 = None
        team1_name = match.get('team1_name')
        team2 = None
        team2_name = match.get('team2_name')
        if match.get('team1_id') == team.id:
            team1 = None
            team1_name = match.get('team1_name')
            team2 = None
            opponent_name = match.get('team2_name')
        elif match.get('team2_id') == team.id:
            team1 = None
            team1_name = match.get('team2_name')
            team2 = None
            opponent_name = match.get('team1_name')
        else:
            team1 = None
            team1_name = match.get('team1_name')
            team2 = None
            opponent_name = match.get('team2_name')
        prep_start: datetime.datetime
        e = discord.utils.escape_markdown
        hard_mode = False
        has_restrictions = False
        try:
            [construct_id] = await db.fetchrow('SELECT construct_id from constructs c where c.source_key = $1',
                                               match.get('source_key'))
            
            event = await Event().from_id(construct_id)
            if event.battle_modifier and event.battle_modifier.lower() != 'none':
                hard_mode = True
            restrictions = event.restriction
            if restrictions:
                has_restrictions = True
            if not has_restrictions:
                try:
                    res = await event.restrictions
                except errors.NotFoundException:
                    res = []
                if res:
                    has_restrictions = True
            event_name = event.name
        except errors.NotFoundException:
            event_name = match.get('tournament_name')

        opponent_id = None
        opponent = None

        try:
            [count_streams] = await db.fetchrow("""SELECT count(*) from um_streams where um_id = $1""",
                                                match.get('um_id'))
        except errors.NotFoundException:
            count_streams = 0
        match_start = match.get('match_date')
        if match_start:
            match_start = match_start.replace(tzinfo=tz.UTC)
        match_title = f"Match vs {discord.utils.escape_markdown(opponent_name)}"
        match_date: Datetime = Datetime.fromtimestamp(int(match.get('match_date').timestamp()))
        desc = f"{bot.custom_emojis.mantelpiece_clock} <t:{match_date.t_stamp}:f>\n"
        desc += f"🏆 {discord.utils.escape_markdown(match.get('tournament_name'))}\n"
        desc += f"🗓️ {discord.utils.escape_markdown(match.get('week_name'))}\n"
        if hard_mode:
            desc += f"{bot.custom_emojis.hard_mode} Hard mode\n"
        if has_restrictions:
            desc += f"🏴 Additional Restrictions\n"
        desc += f'🌐 [Website](https://competitiveclash.network/matches/upcoming/{match.get("um_id")})'
        


        streams = await db.fetch(
            'SELECT * from um_streams where um_id = $1 ORDER BY stream_start, streamer_name, url',
            match.get('um_id'))
        p_streams = {}
        for s in streams:
            stream_start = s.get('stream_start')
            if stream_start:
                stream_start = stream_start.replace(tzinfo=tz.UTC)
                p_stream_start = f"<t:{int(stream_start.timestamp())}:R>"
            else:
                p_stream_start = "Unknown"
            key = f"{p_stream_start} - {e(s.get('streamer_name'))}"
            value1 = p_streams.get(key, "")
            if value1:
                pass
            else:
                value1 += f"[{e(s.get('platform'))}]({e(s.get('url'))})"
            p_streams[key] = value1
        v_old = desc
        for key, value1 in p_streams.items():
            if len(desc) + len(f"\n{key}: {value1}") > 1000:
                match_informations.append((match_title, desc))
                desc = v_old
            desc += f"\n{key}: {value1}"
        match_informations.append((match_title, desc))
        prpcessed.append(match.get('um_id'))
    for match_title, desc in match_informations:
        if (not embed.fields or len(embed.fields) < 25) and len(embed) < 5500:
            embed.add_field(name=match_title,
                            value=desc,
                            inline=False)
        else:
            embeds.append(embed)
            embed = discord.Embed(title=title, color=discord.Color.from_rgb(221, 183, 118),
                                  timestamp=datetime.datetime.now(tz=tz.UTC))
            if team.logo_path:
                embed.set_thumbnail(url=team.u_logo_url)
            embed.add_field(name=match_title,
                            value=desc,
                            inline=False)
    if embed.fields:
        embeds.append(embed)
    await webhook.send(embeds)


async def run_match_scored(match_id):
    try:
        match = await Match().from_id(match_id)
    except errors.NotFoundException:
        return
    if not match.scored:
        return
    if not match.active:
        return
    # get webhook
    webhooks = []
    # find webhooks of interest
    try:
        raw_webhooks = await db.fetch('SELECT webhook_id from public.webhooks w where '
                                         'team_id in ($1, $2) and webhook_type::text = $3 and webhook_active = true',
                                      match.team1_id, match.team2_id, 'uc_streams')
    except errors.NotFoundException:
        return
    for rw in raw_webhooks:
        webhook = await Webhook.by_id(rw.get('webhook_id'))
        user_id = webhook.user_id
        # get user
        try:
            rts_user = await db.fetchrow('SELECT * from rts.user where discord_id::bigint = $1', user_id)
        except errors.NotFoundException:
            await webhook.close()
            continue
        try:
            [rts_active] = await db.fetchrow('SELECT count(*) from rts.user_spending where user_id = $1 and '
                                             'active_until > $2', rts_user.get('user_id'), datetime.datetime.utcnow())
        except errors.NotFoundException:
            await webhook.close()
            continue
        if not rts_active:
            await webhook.close()
            continue
        webhooks.append(webhook)
    # get team
    for wh in webhooks:
        if match.battle_modifier and match.battle_modifier == 'hardMode':
            battle_mode = ' (Hard Mode)'
        else:
            battle_mode = ''
        season = (await match.season) or (await Season().from_id(1))
        image_url = None
        count = 0
        while image_url is None and count < 5:
            await generate_match_result(match_id)
            if pathlib.Path(f'image_cache/{season.id}/match_result_{match_id}.png').exists():
                image_url = f'https://competitiveclash.network/results/{season.id}/match_result_{match_id}.png'
            else:
                await asyncio.sleep(1)
                count += 1
        match = await Match().from_id(match_id)
        winner = await match.winner
        loser = await match.loser
        if winner and isinstance(winner, str):
            winner = await match.team_a
            loser = await match.team_b
        elo_diff_team_a = await match.elo_change_team_a
        elo_diff_team_b = await match.elo_change_team_b
        elo_team_a = await match.elo_team_a_before
        elo_team_b = await match.elo_team_b_before
        if elo_diff_team_a > 0:
            elo_diff_team_a = f'{bot.custom_emojis.plus}{abs(elo_diff_team_a)})'
        elif elo_diff_team_a < 0:
            elo_diff_team_a = f'{bot.custom_emojis.minus}{abs(elo_diff_team_a)})'
        else:
            elo_diff_team_a = f'{bot.custom_emojis.neutral}{abs(elo_diff_team_a)})'
        if elo_diff_team_b > 0:
            elo_diff_team_b = f'{bot.custom_emojis.plus}{abs(elo_diff_team_b)})'
        elif elo_diff_team_b < 0:
            elo_diff_team_b = f'{bot.custom_emojis.minus}{abs(elo_diff_team_b)})'
        else:
            elo_diff_team_b = f'{bot.custom_emojis.neutral}{abs(elo_diff_team_b)})'
        if not isinstance(winner, str) and winner.id == match.team_b_id:
            winner_elo_change = (f"({elo_team_b}" + elo_diff_team_b) if match.team_b_id else "(Not registered)"
            loser_elo_change = (f"({elo_team_a}" + elo_diff_team_a) if match.team_a_id else "(Not registered)"
        else:
            winner_elo_change = (f"({elo_team_a}" + elo_diff_team_a) if match.team_a_id else "(Not registered)"
            loser_elo_change = (f"({elo_team_b}" + elo_diff_team_b) if match.team_b_id else "(Not registered)"
        if winner == '-':
            winner = await match.team_a
            loser = await match.team_b
        
        embed = discord.Embed(title=f"Match {match_id}{battle_mode}",
                              description=f"{winner_elo_change} **"
                                          f"{discord.utils.escape_markdown(winner.name)}** vs "
                                          f"**{discord.utils.escape_markdown(loser.name)}** "
                                          f"{loser_elo_change}",
                              colour=discord.Color.from_rgb(221, 183, 118), timestamp=datetime.datetime.now(tz=tz.UTC))
        try:
            events = await db.fetch('SELECT * from match_construct_new mc '
                                    'JOIN constructs t on mc.construct_id = t.construct_id '
                                    'WHERE match_id = $1', match.id)
        except errors.NotFoundException:
            events = []
        embed_thumbnail = False
        for event in events:
            t = await Event().from_id(event.get('construct_id'))
            embed.add_field(name="Match related to Event",
                            value=discord.utils.escape_markdown(t.name))
            if t.logo_url and not embed_thumbnail:
                embed.set_thumbnail(url=t.logo_url)
        embed.set_image(url=image_url)
        embeds = [embed]
        await wh.send(embeds)


async def schedule_webhooks():
    try:
        webhooks = await db.fetch('SELECT webhook_id from public.webhooks w where webhook_active = true')
    except errors.NotFoundException:
        return
    for webhook in webhooks:
        w = await Webhook.by_id(webhook.get('webhook_id'))
        user_id = w.user_id
        # get user
        try:
            rts_user = await db.fetchrow('SELECT * from rts.user where discord_id::bigint = $1', user_id)
        except errors.NotFoundException:
            await w.close()
            continue
        try:
            [rts_active] = await db.fetchrow('SELECT count(*) from rts.user_spending where user_id = $1 and '
                                             'active_until > $2', rts_user.get('user_id'), datetime.datetime.utcnow())
        except errors.NotFoundException:
            await w.close()
            continue
        if not rts_active:
            await w.close()
            continue
        if w.type == 'uc_matches':
            scheduler.add_job(run_uc_matches,
                              args=[w.user_id, w.team_id],
                              trigger='interval', minutes=15,
                              id=f'uc_matches_{w.id}',
                              name=f'uc_matches_{w.id}',
                              replace_existing=True,
                              misfire_grace_time=60,
                              coalesce=True)
            print(f"scheduled run_uc_matches-{w.id}")
        elif w.type == 'uc_streams':
            scheduler.add_job(run_uc_streams,
                              args=[w.user_id, w.team_id],
                              trigger='interval', minutes=15,
                              id=f'uc_streams_{w.id}',
                              name=f'uc_streams_{w.id}',
                              replace_existing=True,
                              misfire_grace_time=60,
                              coalesce=True)
            print(f"scheduled run_uc_streams-{w.id}")
        else:
            pass
        await w.close()


async def get_players_qualification(
                                    circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16'], ) -> List[str]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    if 'TH15' in circuits:
        ids.extend(['#2CGJYVCP', '#GJGQ0JG0', '#2LPPPU0UG', '#QRUU9QGU', '#GR9V9YU'])
        ids.extend(['#Y0GPGPRP', '#RYL89PVR', '#CU2L2RUG', '#8LYY08YR', '#9288YY0J'])
        ids.extend(['#8JR282YGY', '#YL9VYQV0', '#YVQCP9LR', '#RCC0LV0P', '#28RC89JPV'])
        ids.extend(['#JUQ00YYY', '#RLU99P89', '#QP9PGV', '#2PQJGLGV', '#2G8QPCY8'])
        ids.extend(['#LLVCG200', '#Y8JR0PCR', '#PJ0QLCQL', '#Q22RLPUP', '#LYVU00UL'])
        ids.extend(['#9JG8UVL0', '#LGJUY8909', '#PPVYC88R', '#Y2QJVLCL', '#UQUPQ9GG'])
        ids.extend(['#8J2YCG008', '#ULJY9LV9', '#V9P9LCLP', '#PPCRRR0', '#LR82GYQJ', '#9U2VU0L2'])
    if 'TH16' in circuits:
        ids.extend(['#YVQCP9LR', '#QRUU9QGU', '#20YJQVRL', '#J8PRJ0YC', '#QYRV9Q2G', '#28RC89JPV'])
        ids.extend(['#YCCLY8CR', '#LV0P2C0PV', '#8J2YCG008', '#2LPPPU0UG', '#2JJVP9G2'])
        ids.extend(['#Y8JR0PCR', '#8JR282YGY', '#QCY9PYRV', '#GLQYCL2', '#8CVGGPJY'])
        ids.extend(['#PPVYC88R', '#PPCRRR0', '#ULJY9LV9', '#V9P9LCLP', '#LR82GYQJ'])
        ids.extend(['#RYL89PVR', '#Y0GPGPRP', '#CU2L2RUG', '#8LYY08YR', '#9288YY0J', '#PY9UGL9L'])
        ids.extend(['#2R0YCPUC0', '#PUC0CQQ9', '#2RY89CPRJ', '#2V2LRCL2U', '#QU9VCGYY'])
    return ids

async def get_players_finals_accounts(circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16'], ) -> List[str]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    if 'TH15' in circuits:
        ids.extend(['#2CGJYVCP', '#GJGQ0JG0', '#2LPPPU0UG', '#QRUU9QGU', '#GR9V9YU'])
        ids.extend(['#Y0GPGPRP', '#RYL89PVR', '#CU2L2RUG', '#8LYY08YR', '#9288YY0J'])
        ids.extend(['#8JR282YGY', '#YL9VYQV0', '#YVQCP9LR', '#RCC0LV0P', '#28RC89JPV'])
        ids.extend(['#JUQ00YYY', '#RLU99P89', '#QP9PGV', '#2PQJGLGV', '#2G8QPCY8'])
        ids.extend(['#LLVCG200', '#Y8JR0PCR', '#PJ0QLCQL', '#Q22RLPUP', '#LYVU00UL'])
        ids.extend(['#9JG8UVL0', '#LGJUY8909', '#PPVYC88R', '#Y2QJVLCL', '#UQUPQ9GG'])
        ids.extend(['#8J2YCG008', '#ULJY9LV9', '#V9P9LCLP', '#PPCRRR0', '#LR82GYQJ', '#9U2VU0L2'])
    if 'TH16' in circuits:
        ids.extend(['#YVQCP9LR', '#QRUU9QGU', '#20YJQVRL', '#J8PRJ0YC', '#QYRV9Q2G', '#28RC89JPV'])
        ids.extend(['#YCCLY8CR', '#LV0P2C0PV', '#8J2YCG008', '#2LPPPU0UG', '#2JJVP9G2'])
        ids.extend(['#Y8JR0PCR', '#8JR282YGY', '#QCY9PYRV', '#GLQYCL2', '#8CVGGPJY'])
        ids.extend(['#PPVYC88R', '#PPCRRR0', '#ULJY9LV9', '#V9P9LCLP', '#LR82GYQJ'])
        ids.extend(['#RYL89PVR', '#Y0GPGPRP', '#CU2L2RUG', '#8LYY08YR', '#9288YY0J', '#PY9UGL9L'])
        ids.extend([
            '#GLQ2QLQGV', '#QRYGL8GC9', '#G92QPGLU9',
                     '#QVG90RJ09', '#G2QVGUL08', '#QCR9LL2P2', '#G9LGV89RL', '#QCR28822R', '#GL8GG9QVQ', '#QCGQL8VUV', '#G2GVJJVU2',
                     '#QRPLQP2RG', '#G8GRQ02PP', '#QCJGJJ0LQ', '#GYCGYRG2V', '#G09YYGR9J', '#GLQ8PU8YV', '#QVV2JUVQG', '#QJC98QVRQ',
                     '#G09RLG0GC', '#G092RR82C', '#G2GVC90JR', '#QRYGY8UC9', '#G2G028GP2', '#G09JVVRUQ', '#G9LGURLPG', '#QYUUGVP80',
                     '#GP9YGPUR8', '#QCQGGV2PR', '#G9VYYLYJY', '#QJ89GRRLV', '#G0PUGYQQ8', '#GP0VVVUGJ', '#GYGYY9CG2', '#GYRY880JC',
                     '#GP9J0V2RY', '#G9Q2J2J08', '#G90LL2VYY', '#G9JY0U00C', '#G09800GQL', '#GYCGQ0RUQ', '#QCR22J092', '#G9C0PJC02',
                     '#QCL89QJR0', '#G9ULQ0Y0Q', '#QVY8P8V2Y', '#QU8YUG9L9', '#QJ8QJ8UG8', '#G0PVQRV2V', '#QVG8VURUQ', '#G0PVQYR0C',
                     '#QCJ2Q2V89', '#G2GJJ289Y', '#QPQCG8QL2', '#LJULJYYV9', '#QYUUG9PYL', '#GP9YQC0V8', '#QJC0CR2QR', '#G2QVG0RL8',
                     '#QCL9L8L80'
        ])
    return ids


async def get_teams_qualification(
                                  circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16'], ) -> List[int]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    if 'TH15' in circuits:
        ids.extend([2, 9, 11, 33, 49, 80, 147])
    if 'TH16' in circuits:
        ids.extend([11, 5468, 6, 147, 9, 754])
    return ids


async def get_players_qualification_events(
                                           player_tag: str,
                                           circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16'], ) -> List[int]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    tag = coc.utils.correct_tag(player_tag)
    if 'TH15' in circuits:
        if tag in ['#2CGJYVCP', '#GJGQ0JG0', '#2LPPPU0UG', '#QRUU9QGU', '#GR9V9YU']:
            ids.extend([95, 336, 338, 339])
        elif tag in ['#Y0GPGPRP', '#RYL89PVR', '#CU2L2RUG', '#8LYY08YR', '#9288YY0J']:
            ids.extend([324, 325, 326, 327])
        elif tag in ['#8JR282YGY', '#YL9VYQV0', '#YVQCP9LR', '#RCC0LV0P', '#28RC89JPV']:
            ids.extend([40, 41, 42, 43])
        elif tag in ['#JUQ00YYY', '#RLU99P89', '#QP9PGV', '#2PQJGLGV', '#2G8QPCY8']:
            ids.extend([95, 336, 338, 339])
        elif tag in ['#LLVCG200', '#Y8JR0PCR', '#PJ0QLCQL', '#Q22RLPUP', '#LYVU00UL']:
            ids.extend([95, 336, 338, 339])
        elif tag in ['#9JG8UVL0', '#LGJUY8909', '#PPVYC88R', '#Y2QJVLCL', '#UQUPQ9GG']:
            ids.extend([95, 336, 338, 339])
        elif tag in ['#8J2YCG008', '#ULJY9LV9', '#V9P9LCLP', '#PPCRRR0', '#LR82GYQJ', '#9U2VU0L2']:
            ids.extend([5, 6, 7, 320])
    if 'TH16' in circuits:
        if tag in ['#YVQCP9LR', '#QRUU9QGU', '#20YJQVRL', '#J8PRJ0YC', '#QYRV9Q2G', '#28RC89JPV']:  # Synchronic Gaming
            ids.extend([1637])  # April Qualifier
        elif tag in ['#YCCLY8CR', '#LV0P2C0PV', '#8J2YCG008', '#2LPPPU0UG', '#2JJVP9G2']:  # VM Legacy/STMN Esports
            ids.extend([1658])  # May Qualifier
        elif tag in ['#Y8JR0PCR', '#8JR282YGY', '#QCY9PYRV', '#GLQYCL2', '#8CVGGPJY']:  # Millesime MG
            ids.extend([1675])  # June Qualifier
        elif tag in ['#PPVYC88R', '#PPCRRR0', '#ULJY9LV9', '#V9P9LCLP', '#LR82GYQJ']:  # NAVI
            ids.extend([1692])  # July Qualifier
        elif tag in ['#RYL89PVR', '#Y0GPGPRP', '#CU2L2RUG', '#8LYY08YR', '#9288YY0J', '#PY9UGL9L']:  # Tribe Gaming
            ids.extend([1690, 1712])  # Snapdragon Pro Series 5, LCQ
        elif tag in ['#2V2LRCL2U', '#QU9VCGYY', '#2RY89CPRJ', '#PUC0CQQ9', '#2R0YCPUC0']: # MTFY
            ids.extend([1742]) # Qualified in China, no data
    return ids


async def get_teams_qualification_events(
                                         team_id: int,
                                         circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16'], ) -> List[int]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    if 'TH15' in circuits:
        if team_id == 2:
            ids.extend([95, 336, 338, 339])
        elif team_id == 9:
            ids.extend([324, 325, 326, 327])
        elif team_id == 11:
            ids.extend([40, 41, 42, 43])
        elif team_id == 33:
            ids.extend([95, 336, 338, 339])
        elif team_id == 49:
            ids.extend([95, 336, 338, 339])
        elif team_id == 80:
            ids.extend([95, 336, 338, 339])
        elif team_id == 147:
            ids.extend([5, 6, 7, 320])
    if 'TH16' in circuits:
        if team_id == 11:  # Synchronic Gaming
            ids.extend([1637])  # April Qualifier
        elif team_id == 5468:  # VM Legacy/STMN Esports
            ids.extend([1658])  # May Qualifier
        elif team_id == 6:  # Millesime MG
            ids.extend([1675])  # June Qualifier
        elif team_id == 147:  # NAVI
            ids.extend([1692])  # July Qualifier
        elif team_id == 9:  # Tribe Gaming
            ids.extend([1690, 1712])  # Snapdragon Pro Series 5, LCQ
        elif team_id == 754: # MTFY
            ids.extend([1742]) # Qualified in China, no data
    return ids


async def get_official_events(circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16']) -> \
List[int]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    if 'TH15' in circuits:
        ids.extend([95, 336, 338, 339, 324, 325, 326, 327, 40, 41, 42, 43])
    if 'TH16' in circuits:
        ids.extend([1637, 1655, 1658, 1675, 1692, 1690, 1712, 1741, 1742])
    return ids


async def get_community_events(circuits: List[Literal['TH14', 'TH15', 'TH16']] = ['TH16']) -> \
List[int]:
    if not circuits:
        circuits = ['TH16']
    ids = []
    if 'TH15' in circuits:
        try:
            c_ids = await db.fetch('SELECT construct_id from ccn_bot.public.constructs c '
                                    'where construct_id not in (95, 336, 338, 339, 324, 325, 326, 327, 40, 41, 42, '
                                    '43) and circuit = \'TH15\'')
        except errors.NotFoundException:
            c_ids = []
        if c_ids:
            ids.extend([c.get('construct_id') for c in c_ids])
    if 'TH16' in circuits:
        try:
            c_ids = await db.fetch('SELECT construct_id from ccn_bot.public.constructs c '
                                    'where construct_id not in (1637,  1658, 1675, 1692, 1712, 1741, 1742) '
                                    'and circuit = \'TH16\'')
        except errors.NotFoundException:
            c_ids = []
        if c_ids:
            ids.extend([c.get('construct_id') for c in c_ids])
    return ids
    

async def parse_esport_stats_row(record: Union[asyncpg.Record, dict], mode_rep: dict[str, str]) -> list[str]:
    """Parse a row of the esport stats table"""
    row = []
    for k, v in record.items():
        if isinstance(v, Decimal):
            v = float(v)
        elif v is None:
            v = ""
        if isinstance(v, (float, int)) and ('rate' in k or 'Avg' in k):
            value = f"{v:.2f}"
        elif isinstance(v, (float, int)):
            value = f"{v:.0f}"
        else:
            value = str(v)
        if str(k).lower().replace('"', '') == 'mode':
            value = mode_rep.get(value, str(value))
        row.append(value)
    return row


async def fill_esports_stats(sheet_id: str = "1f_DAP7c5yuuTcpZHwLlpsZ7ndRXX6BtHwbnKc03bb5s",
                             circuit: Literal['TH14', 'TH15', 'TH16'] = 'TH16'):
    """Fill the esport stats sheet"""
    # construct team stats
    stats_rows = []
    stats_ranges = []
    qualified_teams = await get_teams_qualification(circuits=[circuit])
    hard_mode_date = datetime.datetime(2024, 9, 9, 12, 0, 0)
    for side_mode in ['offense', 'defense']:
        # get overall stats offense
        try:
            records = await db.fetch(
                    """SELECT * from public.get_team_esports_data($1::int[], $2, battle_modifiers := $3::text[])""",
                    qualified_teams, side_mode, [])
        except errors.NotFoundException:
            records = []
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Normal', 'hardMode': 'Hard', 'All': 'All'})
            stats_rows.append(row)
        # get hard1 stats
        try:
            records = await db.fetch(
                    """SELECT * from public.get_team_esports_data($1::int[], $2, battle_modifiers := $3::text[],
                    end_date := $4)
                    where "Mode" = 'hardMode'""",
                    qualified_teams, side_mode, ['hardMode'], hard_mode_date)
        except errors.NotFoundException:
            records = []
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Normal', 'hardMode': 'Hard1', 'All': 'All'})
            stats_rows.append(row)
        # get hard2 stats
        try:
            records = await db.fetch(
                    """SELECT * from public.get_team_esports_data($1::int[], $2, battle_modifiers := $3::text[],
                    start_date := $4)
                    where "Mode" = 'hardMode'""",
                    qualified_teams, side_mode, ['hardMode'], hard_mode_date)
        except errors.NotFoundException:
            records = []
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Normal', 'hardMode': 'Hard2', 'All': 'All'})
            stats_rows.append(row)
        
        # get the qualification stats
        for t_id in qualified_teams:
            construct_ids = await get_teams_qualification_events(t_id, circuits=[circuit])
            # get stats
            try:
                records = await db.fetch(
                        """SELECT * from public.get_team_esports_data($1::int[], $2, battle_modifiers := $3::text[],
                        constructs := $4::int[]) where "Mode" = 'All'""",
                        [t_id], side_mode, [], construct_ids)
            except errors.NotFoundException:
                records = []
            except Exception as e:
                log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{t_id=}\n{side_mode=}')
                bot.logger.error(f'{traceback.format_exc()}\n{e=}\n{t_id=}\n{side_mode=}')
                print(f'{traceback.format_exc()}\n{e=}\n{t_id=}\n{side_mode=}')
            for record in records:
                row = await parse_esport_stats_row(record, {'none': 'Quali', 'hardMode': 'Quali', 'All': 'Quali'})
                stats_rows.append(row)
        # get the world finals stats
        try:
            records = await db.fetch(
                    """SELECT * from public.get_team_esports_data($1::int[], $2, battle_modifiers := $3::text[],
                    constructs := $4::int[]) where "Mode" = 'All'""",
                    qualified_teams, side_mode, [], [1741])
        except errors.NotFoundException:
            records = []
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Finals', 'hardMode': 'Finals', 'All': 'Finals'})
            stats_rows.append(row)
    stats_ranges.append(f"TeamStats!A2:AC{len(stats_rows) + 1}")
    try:
        await gsheets.write(sheet_id, stats_ranges, values=[stats_rows])
    except Exception as e:
        log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{stats_ranges=}\n{stats_rows=}')
    stats_ranges = []
    stats_rows = []
    # construct player stats
    qualified_players = await get_players_qualification(circuits=[circuit])
    for side_mode in ['offense', 'defense']:
        # get overall stats offense
        try:
            records = await db.fetch(
                    """SELECT * from public.get_player_esports_data($1::text[], $2, battle_modifiers := $3::text[])""",
                    qualified_players, side_mode, [])
        except errors.NotFoundException:
            records = []
        except Exception as e:
            log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
            bot.logger.error(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
            print(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Normal', 'hardMode': 'Hard', 'All': 'All'})
            stats_rows.append(row)
        # get hard1 stats
        try:
            records = await db.fetch(
                    """SELECT * from public.get_player_esports_data($1::text[], $2, battle_modifiers := $3::text[],
                    end_date := $4)
                    where "Mode" = 'hardMode'""",
                    qualified_players, side_mode, ['hardMode'], hard_mode_date)
        except errors.NotFoundException:
            records = []
        except Exception as e:
            log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
            bot.logger.error(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
            print(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Normal', 'hardMode': 'Hard1', 'All': 'All'})
            stats_rows.append(row)
        # get hard2 stats
        try:
            records = await db.fetch(
                    """SELECT * from public.get_player_esports_data($1::text[], $2, battle_modifiers := $3::text[],
                    start_date := $4)
                    where "Mode" = 'hardMode'""",
                    qualified_players, side_mode, ['hardMode'], hard_mode_date)
        except errors.NotFoundException:
            records = []
        except Exception as e:
            log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
            bot.logger.error(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
            print(f'{traceback.format_exc()}\n{e=}\n{qualified_players=}\n{side_mode=}')
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Normal', 'hardMode': 'Hard2', 'All': 'All'})
            stats_rows.append(row)
        
        # get the qualification stats
        for t_id in qualified_players:
            construct_ids = await get_players_qualification_events(t_id, circuits=[circuit])
            # get stats
            try:
                records = await db.fetch(
                        """SELECT * from public.get_player_esports_data($1::text[], $2, battle_modifiers := $3::text[],
                        constructs := $4::int[]) where "Mode" = 'All'""",
                        [t_id], side_mode, [], construct_ids)
            except errors.NotFoundException:
                records = []
            except Exception as e:
                log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{t_id=}\n{side_mode=}\n{construct_ids=}')
                bot.logger.error(f'{traceback.format_exc()}\n{e=}\n{t_id=}\n{side_mode=}\n{construct_ids=}')
                print(f'{traceback.format_exc()}\n{e=}\n{t_id=}\n{side_mode=}\n{construct_ids=}')
            for record in records:
                row = await parse_esport_stats_row(record, {'none': 'Quali', 'hardMode': 'Quali', 'All': 'Quali'})
                stats_rows.append(row)
        # get the world finals stats
        qualified_players = await get_players_finals_accounts(circuits=[circuit])
        try:
            records = await db.fetch(
                    """SELECT * from public.get_player_esports_data($1::text[], $2, battle_modifiers := $3::text[],
                    constructs := $4::int[]) where "Mode" = 'All'""",
                    qualified_players, side_mode, [], [1741])
        except errors.NotFoundException:
            records = []
        for record in records:
            row = await parse_esport_stats_row(record, {'none': 'Finals', 'hardMode': 'Finals', 'All': 'Finals'})
            stats_rows.append(row)
    stats_ranges.append(f"PlayerStats!A2:AB{len(stats_rows) + 1}")
    try:
        await gsheets.write(sheet_id, stats_ranges, values=[stats_rows])
    except Exception as e:
        log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{stats_ranges=}\n{stats_rows=}')
    
    
async def fill_esports_details(sheet_id: str = "1f_DAP7c5yuuTcpZHwLlpsZ7ndRXX6BtHwbnKc03bb5s",
                             circuit: Literal['TH14', 'TH15', 'TH16'] = 'TH16'):
    data_ranges = []
    data_data = []
    try:
        [date_from, date_to] = await db.fetchrow("SELECT c.start as date_from, c.end as date_to from public.circuits c where circuit = $1",
                                                 circuit)
    except errors.NotFoundException:
        return
    date_from = Datetime.by_dt(date_from)
    date_to = Datetime.by_dt(date_to)
    # get qualified teams
    qualified_teams = await get_teams_qualification(circuits=[circuit])
    # get qualified players
    qualified_players = await get_players_qualification(circuits=[circuit])
    for t_id in qualified_teams:
        team = await Team().from_id(t_id)
        data = {'name': team.name, 'tag': "", 'logo': team.logo_url if team.logo_path else "", "id": team.id,
                'twitter': team.twitter, "contact": team.contact}
        try:
            quali_events = await get_teams_qualification_events(t_id, circuits=[circuit])
        except errors.NotFoundException:
            quali_events = []
        qualified_in = []
        for qe in quali_events:
            event = await Event().from_id(qe)
            qualified_in.append(event.name)
        if len(qualified_in) > 1:
            data['qualified_in'] = (", ".join(qualified_in[:-1]) + " & " + qualified_in[-1])
        elif len(qualified_in) == 1:
            data['qualified_in'] = qualified_in[0]
        else:
            data['qualified_in'] = "Chinese"
        # get the price money and placements
        # get first place finishes
        try:
            team_firsts = await db.fetchrow("SELECT count(*) filter (where te.placement = '1st') as firsts,"
                                            "count(*) filter (where te.placement = '2nd') as second,"
                                            "count(*) filter (where te.placement = '3rd') as third, "
                                            "sum(te.earned) as earnings "
                                            "FROM team_earnings te "
                                            "WHERE te.team_id = $1 AND te.date_time BETWEEN $2 AND $3",
                                            team.id, date_from, date_to)
        except errors.NotFoundException:
            team_firsts = {}
        except Exception as e:
            raise e
        data['firsts'] = str(team_firsts.get('firsts', 0) or 0)
        data['second'] = str(team_firsts.get('second', 0) or 0)
        data['third'] = str(team_firsts.get('third', 0) or 0)
        data['earnings'] = f"${team_firsts.get('earnings', 0) or 0:,.0f}"
        # get event/tournaments played
        try:
            team_events = await db.fetchrow("SELECT count(distinct mc.construct_id) as events  "
                                            " from match_results mr join match_construct_new mc on mc.match_id = "
                                            "mr.match_id JOIN matches m on mc.match_id = m.match_id and "
                                            "m.scored and m.active "
                                            "WHERE $1 = mr.team_id AND m.prep_start BETWEEN $2 AND $3",
                                            team.id, date_from, date_to)
        except errors.NotFoundException:
            team_events = {}
        except Exception as e:
            raise e
        data['tournament_count'] = str((team_events.get('tournaments', 0) or 0) + (team_events.get('events', 0) or 0))
        data_data.append(await parse_esport_stats_row(data, {}))
    data_ranges.append(f"TeamDetails!A2:L{len(data_data) + 1}")
    try:
        await gsheets.write(sheet_id, data_ranges, values=[data_data])
    except Exception as e:
        log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{data_ranges=}\n{data_data=}')
    data_ranges = []
    data_data = []
    for p_id in qualified_players:
        try:
            player = await db.fetchrow("SELECT * from person_player_names ppn left outer join person p on ppn.person_id = p.person_id "
                                       "WHERE ppn.tag = $1", p_id)
        except errors.NotFoundException:
            continue
        if player.get('person_id') is None:
            person = None
        else:
            person = await Person.by_id(player.get('person_id'))
        
        data = {'name': player.get("name"), 'tag': player.get('tag'),
                'account_name': player.get('player_name'),
                'team': "",
                'logo': "", "id": "",
                'twitter': "",
                'location': ""}
        if person:
            data['name'] = person.name
            data['twitter'] = person.twitter
            data['logo'] = person.picture_url if person.picture_path else ""
            data['id'] = person.discord_id or ""
        try:
            [location] = await db.fetchrow("SELECT l.full_name from player_names pn left outer "
            "join "
                                                                          "locations l on "
            "pn.location = "
            "l.short_code where pn.player_tag = $1", p_id)
            data['location'] = location
        except errors.NotFoundException:
            pass
            
        # get the team
        try:
            [team] = await db.fetchrow("SELECT team_name from public.player_teams pt "
                                     "WHERE player_tag = $1 ORDER BY month desc, rnk, matches desc, last desc limit 1", p_id)
            data['team'] = team
        except errors.NotFoundException:
            pass
        data_data.append(await parse_esport_stats_row(data, {}))
    data_ranges.append(f"PlayerDetails!A2:H{len(data_data) + 1}")
    try:
        await gsheets.write(sheet_id, data_ranges, values=[data_data])
    except Exception as e:
        log_scheduled_tasks.error(f'{traceback.format_exc()}\n{e=}\n{data_ranges=}\n{data_data=}')
        
    
    

print(f'{__file__} imported at {datetime.datetime.now()}')
