import asyncio
import datetime
import traceback
from math import ceil
from typing import Iterable, List, Optional, Tuple, Union

import discord
import numpy as np
from discord.ui import InputText, Modal
import logging
log = logging.getLogger(f'ccn.utils.message_utils')

from . import db, errors



ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])

def get_category(guild: discord.Guild, category_id: int = None, category_name: str = None) -> discord.CategoryChannel:
	"""
	Fetch category for a specific guild and return this category if found

	:param guild: guild to look for category
	:param category_id: id of the category to obtain
	:param category_name: name of the category to obtain
	:return: category: Category you wanted
	"""

	for cat in guild.categories:
		if category_id == cat.id or category_name.lower() == cat.name.lower():
			return cat
	raise errors.NotFoundException


def embed_add_fields(main_embed: discord.Embed,
                     field_data: List[Tuple[str, str]],
                     copy_thumbnail: bool = False,
                     **kwargs) -> List[discord.Embed]:
	"""
	Function to create multiple Embeds if the number of fields or the total length of the Embed
	is too high for a single one

	:param copy_thumbnail: whether to use the main_embed thumbnail for all additional Embeds
	:param main_embed: The main Embed. Its color will be used for all additional Embeds
	:param field_data: List of tuples containing the name and value for each field of the embed
	:param kwargs: any other (not name or value) arguments with keyword to pass into Embed.add_field()
	:return: List of Embeds containing all the fields that were specified
	"""
	embeds: List[discord.Embed] = []

	num_fields = len(field_data)
	if num_fields == 0:
		return [main_embed]

	num_embeds = ceil(num_fields / 14)
	too_small = True
	
	embeds: List[discord.Embed] = []
	num_fields_per_embed = ceil(num_fields / num_embeds)
	
	color = main_embed.color
	if copy_thumbnail:
		thumbnail = main_embed.thumbnail.url
	else:
		thumbnail = None
	current_embed = main_embed
	try:
		for page_idx in range(0, num_embeds):
			for i in range(0, num_fields_per_embed):
				try:
					name, value = field_data[page_idx * num_fields_per_embed + i]
					if len(current_embed) + len(name) + len(value) <= 5000 and len(current_embed.fields) < 25:
						current_embed.add_field(name=name, value=value, **kwargs)
					else:
						embeds.append(current_embed)
						current_embed = discord.Embed(color=color, timestamp=main_embed.timestamp)
						if copy_thumbnail:
							current_embed.set_thumbnail(url=thumbnail)
						current_embed.add_field(name=name, value=value, **kwargs)
				except IndexError:
					pass
	
			embeds.append(current_embed)
			current_embed = discord.Embed(color=color, timestamp=main_embed.timestamp)
			if copy_thumbnail:
				current_embed.set_thumbnail(url=thumbnail)
	except asyncio.TimeoutError:
		print(f"Splitting into {num_embeds} embeds")
		print(f", ".join([f"{len(embed)}: {len(embed.fields)}" for embed in embeds]))


	return embeds


def add_local_thumbnail(embed: discord.Embed, filepath: str):
	"""
	Function to create a thumbnail from a local image

	:param embed: Embed, to which a thumbnail should be added
	:param filepath: Path of the image assets/filepath
	:return: file to send with embed
	"""
	file = discord.File(f"{filepath}", filename="thumbnail.png")
	embed.set_thumbnail(url="attachment://thumbnail.png")
	return file


def add_local_image(embed: discord.Embed, filepath: str):
	"""
	Function to create a thumbnail from a local image

	:param embed: Embed, to which a thumbnail should be added
	:param filepath: Path of the image assets/filepath
	:return: file to send with embed
	"""
	file = discord.File(f"{filepath}", filename="image.png")
	embed.set_image(url="attachment://image.png")
	return file


async def dm_embed(users: Iterable[Union[discord.User, discord.Member]], *args, **kwargs):
	for user in users:
		await user.send(*args, **kwargs)


class Paginator:
	"""a class to enable interactive pagination in Discord messages
	Attributes
	----------
		elements: list of discord.Embed
			the embeds to switch between
		num_elems: integer
			the total number of elements
		index: integer
			the index of the currently displayed element
		ctx: discord.ApplicationContext
			the context the paginator was called from
		timeout: integer
			number of seconds before the paginator becomes unresponsive
		REACTION_EMOJIS: list of string
			string representation of the allowed emojis
		message: discord.Message
			the pagination message

	Methods
	-------
		run
			start the interactive pagination
	"""

	def __init__(self, elements: List[discord.Embed], ctx: discord.ApplicationContext, timeout: int = 180, file=None):
		from custom_dataclasses import bot
		self.num_elems = len(elements)
		self.elements = elements
		self.index = 0
		self.ctx = ctx
		self.timeout = timeout
		self.REACTION_EMOJIS = ['⏮️','⏪','⏩','⏭️','⏹️']
		self.REACTIONS = ['first', 'previous', 'next', 'last', 'exit']
		self.message = None
		self.file = file

	async def build_selector_options(self):
		options = set()
		i = 1
		if self.num_elems < 27:
			options = [j for j in range(0, self.num_elems) if j != self.index]
			return options
		while len(options) < 25 and len(options) < self.num_elems - 1 and (self.index - i >= 0 or self.index + i <
		                                                                   self.num_elems):
			if self.index - i >= 0:
				if self.index - i not in options:
					options.add(self.index - i)
				else:
					for j in range(self.index - i - 1, -1, -1):
						if j not in options:
							options.add(j)
							break
			if len(options) < 25 and self.index + i < self.num_elems:
				if self.index + i not in options:
					options.add(self.index + i)
				else:
					for j in range(self.index + i + 1, self.num_elems):
						if j not in options:
							options.add(j)
							break
			i += 1
		options_t = sorted(options, key=lambda x: x)
		return options_t if len(options_t) < 26 else options_t[:25]

	async def run(self):
		from custom_dataclasses import bot
		"""send the pagination message and wait for user interactions until the timeout is reached.
		The timeout refreshes with each valid interaction
		"""

		# break early if the buffer has only one element
		if self.num_elems == 1 and self.file:
			await self.ctx.respond(embed=self.elements[0], file=self.file)
			return
		elif self.num_elems == 1 and not self.file:
			await self.ctx.respond(embed=self.elements[0])
			return
		options = await self.build_selector_options()
		# build pagination components
		view = discord.ui.View()
		view.add_item(discord.ui.Select(custom_id='select', placeholder=f'Page {self.index+1}/{self.num_elems}',
		                                options=[discord.SelectOption(label=f'Page {i+1}/{self.num_elems}',
		                                                              value=f'{i}') for i in options]))
		for emoji, reaction in zip(self.REACTION_EMOJIS, self.REACTIONS):
			view.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary, custom_id=reaction, emoji=emoji))

		# initialize pagination message
		if self.file:
			self.message = await self.ctx.respond(embed=self.elements[0], view=view, file=self.file)
		else:
			self.message = await self.ctx.respond(embed=self.elements[0], view=view)
		# process user input
		try:
			task = asyncio.create_task(self._process_interactions(), name='GCC Bot Paginator')
			await task
		except asyncio.TimeoutError:
			# remove buttons once expired
			try:
				await self.message.edit(view=None)
			except discord.errors.NotFound:
				pass
			except Exception as e:
				log.error(traceback.format_exc())
		except asyncio.exceptions.CancelledError:
			# remove buttons once expired
			try:
				await self.message.edit(view=None)
			except discord.errors.NotFound:
				pass
			except Exception as e:
				log.error(traceback.format_exc())
		except Exception as e:
			log.error(traceback.format_exc())

	def _check(self, i: discord.Interaction):
		"""check if an interaction event is valid
		"""
		return i.message and i.message.id == self.message.id and i.user.id == self.ctx.user.id

	async def _process_interactions(self):
		from custom_dataclasses import bot
		"""process the user reactions, updating the embed to show the first, next, previous or
		last embed, depending on which interaction has occurred
		"""
		# wait for user input
		interaction: discord.Interaction = await bot.client.wait_for('interaction', check=self._check,
		                                                             timeout=self.timeout)
		await interaction.response.defer()
		# process interaction
		if str(interaction.data['custom_id']) == 'first':
			self.index = 0
		elif str(interaction.data['custom_id']) == 'next':
			self.index = (self.index + 1) % self.num_elems
		elif str(interaction.data['custom_id']) == 'previous':
			self.index = (self.index + self.num_elems - 1) % self.num_elems
		elif str(interaction.data['custom_id']) == 'last':
			self.index = self.num_elems - 1
		elif str(interaction.data['custom_id']) == 'select':
			self.index = int(interaction.data['values'][0])
		else:  # i.e. == nok
			await interaction.edit_original_response(view=None)
			# terminate prematurely
			return
		options = await self.build_selector_options()
		view = discord.ui.View()
		view.add_item(discord.ui.Select(custom_id='select', placeholder=f'Page {self.index + 1}/{self.num_elems}',
		                                options=[discord.SelectOption(label=f'Page {i + 1}/{self.num_elems}',
		                                                              value=f'{i}') for i in options]))
		for emoji, reaction in zip(self.REACTION_EMOJIS, self.REACTIONS):
			view.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary, custom_id=reaction, emoji=emoji))
		if self.index == 0:
			view.children[1].disabled = True
		elif self.index == self.num_elems - 1:
			view.children[4].disabled = True
		try:
			await interaction.edit_original_response(embed=self.elements[self.index], view=view)
		except discord.errors.NotFound as e:
			log.error(traceback.format_exc())
			return
		except Exception as e:
			log.error(traceback.format_exc())
		# refresh timeout
		await self._process_interactions()


class Confirmator:
	"""a class to handle easy confirmation messages
	Attributes
	----------
		ctx: discord.ApplicationContext
			the context the paginator was called from
		timeout: integer
			number of seconds before the paginator becomes unresponsive
		confirmer: integer
			discord id of the user who should confirm
		text: string
			text to display for the confirmation message
		message: discord.Message
			the pagination message
		view: discord.View
			view of the confirmation message
		user_response: boolean
			user confirmation or deny

	"""

	def __init__(self, ctx: discord.ApplicationContext, timeout: int = 60, confirmer: int = 0, text: str = ""):
		from custom_dataclasses import bot
		self.ctx = ctx
		self.timeout = timeout
		self.text = text
		self.message = None
		if confirmer == 0:
			self.confirmer = self.ctx.author.id
		else:
			self.confirmer = confirmer
		self.user_response = None
		view = discord.ui.View()
		view.add_item(discord.ui.Button(style=discord.ButtonStyle.green, custom_id='1',
		                                emoji=bot.custom_emojis['green_checkmark']))
		view.add_item(
			discord.ui.Button(style=discord.ButtonStyle.red, custom_id='0', emoji=bot.custom_emojis['red_checkmark']))
		self.view = view

	async def run(self):
		"""send the confirmation message and wait for user interaction"""
		try:
			if not self.message:
				if not self.text and self.confirmer:
					self.text = f'<@{self.confirmer}>'
				elif not self.text and not self.confirmer:
					self.text = "Please confirm the action"
				self.message = await self.ctx.respond(self.text, view=self.view)
			try:
				await self._process_interactions()
			except asyncio.TimeoutError:
				# remove buttons once expired
				self.user_response = False
				pass
			await self.message.edit('already confirmed/denied', view=None)
		except Exception as e:
			log.error(traceback.format_exc())
		return self.user_response

	def _check(self, i):
		"""check if an interaction event is valid"""
		if self.confirmer:
			check = i.user.id == self.confirmer
		else:
			check = True
		return i.message and i.message.id == self.message.id and check

	async def _process_interactions(self):
		"""process the user reaction"""
		# wait for user
		from custom_dataclasses import bot
		interaction = await bot.client.wait_for('interaction', check=self._check, timeout=self.timeout)

		# process
		if str(interaction.data['custom_id']) == '1':
			self.user_response = True
		else:
			self.user_response = False
		self.view.stop()
		try:
			await interaction.response.edit_message(f"Action was {'confirmed' if self.user_response else 'denied'}")
		except Exception as e:
			log.error(traceback.format_exc())


class Selector:
	"""a class to enable interactive selection from Discord menus
	Attributes
	----------
		prompt: string
			the prompt the user should see with the selector
		options: dictionary
			the options (label -> value) the user can choose from (up to 25)
		callback: coroutine
			the callback associated with a selection from the menu
		ctx: utils.commands.CommandContext
			the context the selector was called from
		timeout: integer
			number of seconds before the selector becomes unresponsive
		message: discord.Message
			the selection message
		delete_after_use: boolean
			Delete message after usage

	Methods
	-------
		run
			start the interactive selection
	"""

	def __init__(self,
	             prompt: str,
	             options: dict,
	             callback,
	             ctx,
	             timeout: int = 120,
	             min_options: int = 1,
	             max_options: int = 1,
	             allow_multiple_selections: bool = False,
	             sep: str = '\n\n',
	             text_wrap: str = '```',
	             descriptions: list = [],
	             delete_afterwards=False):
		from custom_dataclasses import bot
		self.callback = callback
		self.ctx = ctx
		self.timeout = timeout
		self.num_pages = (max(len(descriptions), len(options)) - 1) // 25 + 1
		self.multiselect = allow_multiple_selections
		self.delete_after_use = delete_afterwards
		select_options = [discord.SelectOption(label=lbl, value=val) for lbl, val in options.items()]

		# build pages
		if self.num_pages > 1:  # do we need multiple select components?
			# evenly distribute items over pages
			items_per_page = max(len(descriptions), len(options)) // self.num_pages
			diff = max(len(descriptions), len(options)) - self.num_pages * items_per_page
			breaks = [0] + [items_per_page + 1] * diff + [items_per_page] * (self.num_pages - diff)
			breaks = np.cumsum(breaks)

			# build UI elements
			reaction_emojis = {em: bot.get_emoji(em) for em in ('lleft', 'left', 'right', 'rright', 'nok')}
			buttons = [
				discord.ui.Button(style=discord.ButtonStyle.secondary, custom_id=emoji, emoji=reaction_emojis[emoji])
				for emoji in reaction_emojis]
			embeds = [discord.Embed(title=prompt, description=(text_wrap + sep.join(
					descriptions[breaks[i]:breaks[i + 1]]) + text_wrap) if descriptions else None,
			                        color=bot.colors.yellow) for i in range(self.num_pages)]
			for i, e in enumerate(embeds, 1):
				e.set_footer(text=f'[{i}/{self.num_pages}]')
			views = [discord.ui.View(
					discord.ui.Select(placeholder=prompt, options=select_options[breaks[i]:breaks[i + 1]],
							custom_id='select', min_values=min_options,
							max_values=min(max_options, len(select_options[breaks[i]:breaks[i + 1]]))), *buttons) for i
				in range(self.num_pages)]

			self.pages = [{'view': view, 'embed': embed} for view, embed in zip(views, embeds)]
		else:
			# build UI elements
			select = discord.ui.Select(placeholder=prompt, options=select_options, custom_id='select',
			                           min_values=min_options, max_values=min(max_options, len(select_options)))
			button = discord.ui.Button(style=discord.ButtonStyle.secondary, custom_id='0',
			                           emoji=bot.custom_emojis['red_checkmark'])
			msg = (text_wrap + sep.join(descriptions) + text_wrap) if descriptions else None

			self.pages = [{'view' : discord.ui.View(select, button),
			               'embed': discord.Embed(title=prompt, description=msg, color=bot.colors.yellow)}]

		self.message = None
		self.index = 0

	async def run(self):
		from custom_dataclasses import bot
		"""send the selection message and wait for user interactions until the timeout is reached.
		The timeout refreshes with each pagination interaction
		"""

		# initialize select message
		self.message = await self.ctx.send(**self.pages[0])

		# process user input
		try:
			task = asyncio.create_task(self._process_interactions(), name='GCC Bot Selector')
			await task
		except (asyncio.TimeoutError, asyncio.CancelledError):
			# remove view once expired or cancelled by shutdown
			if not self.delete_after_use:
				await self.message.edit(view=None, embed=discord.Embed(title='finished', color=bot.colors.yellow))
			else:
				await self.message.delete()

	def _check(self, i):
		"""check if an interaction event is valid
		"""

		return i.message and i.message.id == self.message.id and i.user.id == self.ctx.author.id

	async def _process_interactions(self):
		"""process the user reactions, updating the embed to show the first, next, previous or
		last embed, depending on which interaction has occurred
		"""
		from custom_dataclasses import bot

		# wait for user input
		interaction = await bot.client.wait_for('interaction', check=self._check, timeout=self.timeout)

		# process interaction
		if str(interaction.data['custom_id']) == 'select':
			await self.callback(interaction.data['values'])

			# maybe terminate prematurely
			if not self.multiselect:
				raise asyncio.TimeoutError
		elif str(interaction.data['custom_id']) == 'lleft':
			self.index = 0
		elif str(interaction.data['custom_id']) == 'right':
			self.index = (self.index + 1) % self.num_pages
		elif str(interaction.data['custom_id']) == 'left':
			self.index = (self.index + self.num_pages - 1) % self.num_pages
		elif str(interaction.data['custom_id']) == 'rright':
			self.index = self.num_pages - 1
		else:  # i.e. == nok
			# terminate prematurely
			raise asyncio.TimeoutError
		await self.message.edit(**self.pages[self.index])

		# refresh timeout
		await self._process_interactions()


async def get_missing_value(ctx, value_title, value_old):

	class Missing_Value_Modal(Modal):
		def __init__(self, *args, **kwargs) -> None:
			self.values = []
			self.interaction = None
			super().__init__(*args, timeout=60, **kwargs)
			self.add_item(InputText(label=value_title, value=value_old,
			                        placeholder="This token will be refreshed after usage.",
			                        style=discord.InputTextStyle.multiline))
			self.values = []
			self.interaction = None

		async def callback(self, minteraction: discord.Interaction):
			await minteraction.response.defer()
			self.values = [c.value for c in self.children if c]
			self.stop()
			self.interaction = minteraction

	custom_id = f'{ctx.user.id}-{int(datetime.datetime.utcnow().timestamp())}'
	def _check(self, i: discord.Interaction):
		"""check if an interaction event is valid
		"""
		return i.custom_id and i.custom_id == custom_id and i.user.id == self.ctx.user.id

	m = Missing_Value_Modal(title="You forgot to enter the new value!",
	                        custom_id=custom_id)
	await ctx.send_modal(m)
	try:
		await m.wait()
	except Exception as e:
		log.error(traceback.format_exc())
	return [m.values, m.interaction]





