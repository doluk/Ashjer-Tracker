import json
import logging
import datetime
import os
import traceback
from decimal import Decimal
from typing import Optional, Union

import aiohttp
import asyncio

import asyncpg
import coc
import discord
from custom_dataclasses import bot, Datetime, Player

from . import db, errors, gsheets
from .independent.automation import scheduler

LOG = bot.logger.getChild(__name__)


def geta(player: coc.Player, name: str):
    ach = player.get_achievement(name)
    if ach:
        return ach.value
    else:
        for ach in player.achievements:
            if ach.name.lower() == name.lower():
                return ach.value
        return 0


def parse_player(player: coc.Player):
    result = {}
    result['builder_hall_level'] = player.builder_hall
    result['builder_base_trophies'] = player.builder_base_trophies
    result['best_builder_base_trophies'] = player.best_builder_base_trophies
    result['builder_base_league'] = player.builder_base_league.id if player.builder_base_league else None
    try:
        result['best_builder_base_season_id'] = player.legend_statistics.best_builder_base_season.id
    except:
        result['best_builder_base_season_id'] = None
    try:
        result['best_builder_base_season_rank'] = player.legend_statistics.best_builder_base_season.rank
    except:
        result['best_builder_base_season_rank'] = None
    try:
        result['best_builder_base_season_trophies'] = player.legend_statistics.best_builder_base_season.trophies
    except:
        result['best_builder_base_season_trophies'] = None
    try:
        result['previous_builder_base_season_id'] = player.legend_statistics.previous_builder_base_season.id
    except:
        result['previous_builder_base_season_id'] = None
    try:
        result['previous_builder_base_season_rank'] = player.legend_statistics.previous_builder_base_season.rank
    except:
        result['previous_builder_base_season_rank'] = None
    try:
        result['previous_builder_base_season_trophies'] = player.legend_statistics.previous_builder_base_season.trophies
    except:
        result['previous_builder_base_season_trophies'] = None
    result['builder_base_halls_destroyed'] = geta(player, 'Un-Build It')
    result['builder_base_trophies_achievement'] = geta(player, 'Champion Builder')
    return result

def parse_lb_player(player: coc.RankedPlayer):
	result = {}
	result['current_rank'] = player.rank
	result['previous_rank'] = player.previous_rank
	result['builder_base_trophies'] = player.builder_base_trophies
	return result


def parse_delta(old: dict, new: dict):
    result = {}
    messed_up_record = False
    for k in new.keys():
        if not isinstance(new.get(k), int):
            continue
        elif new.get(k) == old.get(k, 0):
            result[k] = new[k] - old.get(k, 0)
        elif new.get(k) > old.get(k, 0):
            result[k] = new[k] - old.get(k, 0)
        elif new.get(k) < old.get(k, 0) and k in ['trophies', 'bb_trophies']:
            result[k] = new[k] - old.get(k, 0)
        elif new.get(k) < old.get(k, 0) and k not in ['donations_requested', 'donations_received', 'cc_contributions']:
            result[k] = old.get(k, 0)
            messed_up_record = True
        elif new.get(k) < old.get(k, 0):
            result[k] = new[k]
        else:
            result[k] = 0
    if messed_up_record:
        for k in result.keys():
            result[k] = 0
    result['messed_up_record'] = "messed" if messed_up_record else None
    return result


async def track_player(player_tag: str):
	try:
		request_sent = datetime.datetime.now(tz=datetime.timezone.utc)
		data = await bot.clash_client.http.request(
				coc.http.Route("GET", bot.clash_client.http.base_url, "/players/{}".format(player_tag)))
		if isinstance(data, list) and len(data) == 2:
			response = data[1]
			data = data[0]
		player = coc.Player(data=data, client=bot.clash_client, load_game_data=False)
		response_received = datetime.datetime.now(tz=datetime.timezone.utc)
	except coc.errors.NotFound:
		raise errors.NotFoundException(f'Player {player_tag} not found')
	requested_at = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=max((60-player._response_retry) or 5, 5))
	cache_age = player._response_retry if isinstance(player._response_retry, int) else -1
	data_dict = {}
	raw_data = data
	for k, v in raw_data.items():
		if k in ['league', 'clan', 'playerHouse', 'labels', 'troops', 'heroes', 'heroEquipment', 'spells']:
			continue
		data_dict[k] = v
	data_dict['cache-control'] = cache_age
	request_data = json.dumps(data_dict)
	
	# try:
	# 	await db.execute('INSERT INTO carbon.player_tracking_log as ptl (player_tag, requested_send, response_received, requested_at, '
	# 					 'cache_age, response) VALUES($1, $2, $3, $4, $5, $6)',
	# 					 player.tag, request_sent, response_received, requested_at, cache_age, request_data)
	# except Exception as e:
	# 	raise e
	player_new = parse_player(player)
	try:
		player_old = await db.fetchrow('SELECT * from public.account_tracking where account_tag = $1 order by requested_at DESC LIMIT 1',
									   player.tag)
	except errors.NotFoundException:
		player_old = {}
	try:
		await db.execute("INSERT INTO account_tracking_v2 (account_tag, account_name, requested_at, "
						 "builder_base_trophies) VALUES ($1, $2, date_trunc('min', $3::timestamptz)::timestamptz, $4)",
						 player.tag, player.name, requested_at, player.builder_base_trophies)
	except asyncpg.exceptions.UniqueViolationError:
		pass
	except Exception as e:
		bot.logger.error(f'Error inserting leaderboard player {player.tag}\n{traceback.format_exc()}')
	delta = parse_delta(player_old, player_new)
	if any([x != 0 for x in delta.values() if isinstance(x, int)]) or delta.get('messed_up_record'):
		try:
			await db.execute('INSERT INTO account_tracking(account_tag, first_observed, requested_at, builder_hall_level, '
							 'builder_base_trophies,'
							 'best_builder_base_trophies, builder_base_league, best_builder_base_season_id, best_builder_base_season_rank,'
							 'best_builder_base_season_trophies, previous_builder_base_season_id, previous_builder_base_season_rank,'
							 'previous_builder_base_season_trophies, builder_base_trophies_achievement, builder_base_halls_destroyed,'
							 'times_observed) '
							 'VALUES ($1,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ',
							 player.tag, requested_at, requested_at, player_new.get('builder_hall_level'),
							 player_new.get('builder_base_trophies'),
							 player_new.get('best_builder_base_trophies'), player_new.get('builder_base_league'),
							 player_new.get('best_builder_base_season_id'), player_new.get('best_builder_base_season_rank'),
							 player_new.get('best_builder_base_season_trophies'), player_new.get('previous_builder_base_season_id'),
							 player_new.get('previous_builder_base_season_rank'), player_new.get('previous_builder_base_season_trophies'),
							 player_new.get('builder_base_trophies_achievement'), player_new.get('builder_base_halls_destroyed'), 1)
		except Exception as e:
			raise e
	else:
		await db.execute('UPDATE account_tracking set requested_at = $3,'
						 'times_observed = times_observed + 1 where account_tag = $1 and requested_at = $2',
						 player.tag, player_old.get('requested_at', requested_at - datetime.timedelta(minutes=1)), requested_at)
	# update players
	try:
		await db.execute('UPDATE accounts set last_updated = $2, account_name = $3 where account_tag = $1',
						 player.tag, requested_at, player.name)
	except Exception as e:
		bot.logger.error(f'Error updating player {player.tag}\n{traceback.format_exc()}')
		raise e

async def track_test():
	print('test')
	try:
		print('Testing db')
		now = await db.fetchrow("SELECT now()")
		print(now)
	except Exception as e:
		bot.logger.error(f'Error getting now\n{traceback.format_exc()}')
		traceback.print_exc()
	try:
		print('Testing clash client')
		player = await bot.clash_client.get_player('#Y0URPG2G8')
		print(player.name)
	except Exception as e:
		bot.logger.error(f'Error tracking test player\n{traceback.format_exc()}')
		traceback.print_exc()
	print('test done')

async def get_leaderboard():
	try:
		leaderboard = await bot.clash_client.get_location_players_builder_base()
		requested_at = datetime.datetime.now(tz=datetime.timezone.utc)
	except Exception as e:
		LOG.error(f'Error getting leaderboard\n{traceback.format_exc()}')
		return
	for player in leaderboard:
		try:
			await db.execute('INSERT INTO leaderboards (account_tag, account_name, requested_at, current_rank, previous_rank, '
							 'builder_base_trophies) VALUES ($1, $2, $3, $4, $5, $6)',
							 player.tag, player.name, requested_at, player.rank, player.previous_rank, player.builder_base_trophies)
		except Exception as e:
			bot.logger.error(f'Error inserting leaderboard player {player.tag}\n{traceback.format_exc()}')
	await post_process_leaderboard()

async def post_process_leaderboard():
	# SELECT all the winners with their trophy change compared to the previous requested leaderboard
	query = """WITH requested as (SELECT date_trunc('min',requested_at) as requested_at from account_tracking_v2 l group by date_trunc(
	'min', requested_at)
	order by date_trunc('min',requested_at)
	DESC LIMIT 2 ),
    winner_raw as (SELECT l.* from account_tracking_v2 l join requested r on l.requested_at = date_trunc('min',r.requested_at)),
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
	order by w.winner_tag, l.loser_tag, w.winner_diff"""
	try:
		matches = await db.fetch(query)
	except errors.NotFoundException:
		return
	print(f'Found {len(matches)} leaderboard matches')
	output = {}
	for m in matches:
		w_name = discord.utils.escape_markdown(m.get('winner_name', "") or "")
		l_name = discord.utils.escape_markdown(m.get('loser_name', "") or "")
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
		output[key_str] = temp
	print(f"{len(output)} leaderboard matches processed\n{output=}")
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
	print(f"{len(embeds)} leaderboard embeds created")
	# sendout embeds to webhook
	async with aiohttp.ClientSession() as session:
		webhook = discord.Webhook.from_url(os.getenv('DISCORD_WEBHOOK_LB_LOG_URL'), session=session)
		# send the embeds in batches of 5
		for i in range(0, len(embeds), 5):
			await webhook.send(embeds=embeds[i:i + 5])



async def get_tracked_players():
	try:
		players = await db.fetch('SELECT * from accounts where tracking_active ORDER BY account_tag')
	except errors.NotFoundException:
		players = []
	tasks = [track_player(p['account_tag']) for p in players]
	try:
		await asyncio.gather(*tasks)
	except Exception as e:
		bot.logger.error(f'Error tracking players\n{traceback.format_exc()}')
		raise e
	await post_process_leaderboard()
	
# try:
# 	scheduler.add_job(get_tracked_players, 'interval', minutes=1, id='track_players', replace_existing=True, misfire_grace_time=60,
#	coalesce=True)
# except Exception as e:
# 	bot.logger.error(f'Error scheduling player tracking\n{traceback.format_exc()}')
# 	raise e

def p4gsheet(v)->str:
	if isinstance(v, datetime.datetime):
		return v.strftime('%Y-%m-%d %H:%M:%S %Z')
	elif isinstance(v, str):
		return v
	elif isinstance(v, int):
		return f'{v}'
	elif isinstance(v, (float, Decimal)):
		return f'{round(v,2)}'
	else:
		return ''
	
def calc_range(row_count, col_count):
	# 1-indexed
	row = row_count
	col = col_count
	result = []
	while col > 26:
		result.append(chr(64 + col % 26))
		col = col // 26
	result.append(chr(64 + col))
	result.reverse()
	return ''.join(result) + str(row)

async def write_to_gsheet():
	rows = []
	ranges = []
	try:
		players = await db.fetch('SELECT p.player_name, ptm.* from carbon.player_tracking_meta ptm join carbon.players p on '
								 'ptm.player_tag = p.player_tag '
								 'ORDER BY p.player_name')
	except errors.NotFoundException:
		players = []
	header = ['Name', 'Tag', 'Last Updated', 'Trophies', 'Attack Wins', 'Defense Wins', 'War Stars',
			  'CC donated', 'CC received', 'BB Trophies', 'Clan Capital Contribution', 'Obstacles Removed', 'Gold Looted', 'Elixir Looted',
			  'Dark Looted', 'Walls destroyed', 'TH Destroyed', 'Builder huts Destroyed', 'Attacks Won', 'Defenses Won',
			  'CC Donations', 'Mortars Destroyed', 'XBows Destroyed', 'Infernos Destroyed', 'War Loot', 'Eagles Destroyed',
			  'Spells Donated', 'BB Destroyed', 'CG Points', 'CWL Stars', 'Seasonal Challenge Points', 'Scatters Destroyed',
			  'Weapon TH Destroyed', 'Sieges Donated', 'Capital Gold Obtained', 'Capital Gold Donated', 'Number of Attacks',
			  'Number of 3*', '3* Rate',
			  '1* Rate', 'War Defenses', '# of 99% 1*', '# of 99% 2*']
	rows.append(header)
	war_query = """SELECT *
	from carbon.war_attacks wa join carbon.war_lineup wl on wa.attacker = wl.lineup_id where player_tag = $1"""
	war_query2 = """SELECT count(*) filter (where stars != 3) as defenses from carbon.war_attacks wa
	join carbon.war_lineup wl on wa.defender = wl.lineup_id where player_tag = $1"""
	
	for player in players:
		row = []
		for k, v in player.items():
			row.append(p4gsheet(v))
		
		try:
			war_stats = await db.fetchrow(war_query, player.get('player_tag'))
		except errors.NotFoundException:
			war_stats = {}
		row.append(p4gsheet(war_stats.get('attacks', 0)))
		row.append(p4gsheet(war_stats.get('three_stars', 0)))
		row.append(p4gsheet(war_stats.get('three_star_rate', 0)))
		row.append(p4gsheet(war_stats.get('one_star_rate', 0)))
		try:
			war_stats2 = await db.fetchrow(war_query2, player.get('player_tag'))
		except errors.NotFoundException:
			war_stats2 = {}
		row.append(p4gsheet(war_stats2.get('defenses', 0)))
		row.append(p4gsheet(war_stats.get('stars1', 0)))
		row.append(p4gsheet(war_stats.get('stars2', 0)))
		rows.append(row)
	ranges.append(f'A1:{calc_range(len(rows), len(header))}')
	try:
		await gsheets.write('1ro2JSJggjhvVZQAVgNjN2E-1qPnzaE7oK3nrnQ4s9KE', ranges, [rows])
	except Exception as e:
		bot.logger.error(f'Error writing to gsheet\n{traceback.format_exc()}')
		raise e
