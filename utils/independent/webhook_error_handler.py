#  This file is copied from https://github.com/bijij/Ditto/tree/master/ditto/utils
#  It was published under the following license:
"""
The MIT License (MIT)

Copyright (c) 2018-2021 Bijij

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""

import asyncio
import aiohttp
import datetime
import logging

import discord

from discord.ext import tasks


ZWSP = "\N{ZERO WIDTH SPACE}"

def codeblock(text, *, language: str = "") -> str:
	return f"```{language}\n{text}\n```"

class EmbedWebhookLogger:
	_to_log: list[discord.Embed]

	def __init__(self, webhook_url: str, *, loop: asyncio.BaseEventLoop = None) -> None:
		self.loop = loop or asyncio.get_event_loop()
		self._webhook_url = webhook_url
		self._to_log = []

		# setup loop
		self._loop.start()

	def log(self, embed: discord.Embed) -> None:
		self._to_log.append(embed)

	@tasks.loop(seconds=5)
	async def _loop(self) -> None:
		async with aiohttp.ClientSession() as session:
			webhook = discord.Webhook.from_url(self._webhook_url, session=session)
			while self._to_log:
				embed = self._to_log.pop(0)
				await webhook.send(embed=embed)


class WebhookHandler(logging.Handler):
	_colours = {
		logging.DEBUG: discord.Colour.light_grey(),
		logging.INFO: discord.Colour.gold(),
		logging.WARNING: discord.Colour.orange(),
		logging.ERROR: discord.Colour.red(),
		logging.CRITICAL: discord.Colour.dark_red(),
	}

	def __init__(self, webhook_url: str, level: int = logging.NOTSET) -> None:
		super().__init__(level)
		self._webhook_logger = EmbedWebhookLogger(webhook_url)

	def emit(self, record: logging.LogRecord) -> None:
		"""
        Emit record
		:param record:
		"""
		self.format(record)

		message = f'{record.message}\n{record.exc_text or ""}'
		message = message[:4000] + "..." if len(message) > 4000 else message
		embed = discord.Embed(
				colour=self._colours.get(record.levelno, discord.Colour.default()),
				title=record.name,
				description=codeblock(message, language="py"),
				timestamp=datetime.datetime.fromtimestamp(record.created),
		)
		embed.add_field(name=ZWSP, value=f"{record.filename}:{record.lineno}")
		self._webhook_logger.log(embed)
