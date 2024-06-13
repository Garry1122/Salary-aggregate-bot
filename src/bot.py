import asyncio
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.utils import executor

from src.config import BOT_TOKEN
from src.db import db
from src.aggregation import aggregate_salaries


import logging

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.reply("Welcome! Send me a JSON with dt_from, dt_upto, and group_type.")

@dp.message_handler()
async def handle_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data["dt_from"]
        dt_upto = data["dt_upto"]
        group_type = data["group_type"]

        logging.info(f"Received data: dt_from={dt_from}, dt_upto={dt_upto}, group_type={group_type}")

        result = await aggregate_salaries(db, dt_from, dt_upto, group_type)
        logging.info(f"Aggregation result: {result}")
        await message.reply(json.dumps(result))

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        await message.reply(f"Error: {str(e)}")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(executor.start_polling(dp, skip_updates=True))
    loop.run_forever()


