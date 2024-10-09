"""
This script is designed to monitor the activity of specific instruments in a database, specifically checking if they have been updated within the last 5 minutes. If an instrument has not been updated, it sends an alert message to a specified Telegram chat.

The script uses the `clickhouse_connect` library to interact with the ClickHouse database, `pandas` for data manipulation, and the `telegram` library to send messages via Telegram.

The script is structured to run asynchronously, using Python's asyncio library, allowing it to perform other tasks while waiting for database queries or sleep intervals.
"""

import asyncio
import logging
import os
import subprocess
from datetime import datetime

import clickhouse_connect as cc
import pandas as pd
from telegram import Bot
from telegram.error import TelegramError

LOG_DIRECTORY = "~/bot/logs"
log_filename = os.path.join(LOG_DIRECTORY, "bot.log")
os.makedirs(name=LOG_DIRECTORY, exist_ok=True)

logger = logging.getLogger(name=__name__)
logger.setLevel(level=logging.INFO)

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
c_format = logging.Formatter(fmt=LOG_FORMAT)
f_format = logging.Formatter(fmt=LOG_FORMAT)

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=log_filename)
c_handler.setLevel(level=logging.INFO)
f_handler.setLevel(level=logging.INFO)
c_handler.setFormatter(fmt=c_format)
f_handler.setFormatter(fmt=f_format)

logger.addHandler(hdlr=c_handler)
logger.addHandler(hdlr=f_handler)

TOKEN = ""
CHAT_ID = ""

client = cc.get_client(host="localhost", username="default")

instruments = \
    {f"{inst} ({segm})" : f"Binance{segm}_{inst}_Binance_{segm}_PERPETUAL"
        for inst, segm in
     {
        "ADAUSDT"   : "FutT",
        # "ALGOUSDT"  : "FutT",
        # "APTUSDT"   : "FutT",
        # "ATAUSDT"   : "FutT",
        # "AVAXUSDT"  : "FutT",
        # "BCHUSDT"   : "FutT",
        "BNBUSDT"   : "FutT",
        # "BNXUSDT"   : "FutT",
        "BTCUSDT"   : "FutT",
        "DOGEUSDT"  : "FutT",
        # "DOTUSDT"   : "FutT",
        # "ETCUSDT"   : "FutT",
        "ETHUSDT"   : "FutT",
        # "FILUSDT"   : "FutT",
        "FTMUSDT"   : "FutT",
        # "GALAUSDT"  : "FutT",
        # "HBARUSDT"  : "FutT",
        "LINKUSDT"  : "FutT",
        # "LTCUSDT"   : "FutT",
        # "MATICUSDT" : "FutT",
        "NEARUSDT"  : "FutT",
        # "NEOUSDT"   : "FutT",
        # "ONTUSDT"   : "FutT",
        # "OPUSDT"    : "FutT",
        "SOLUSDT"   : "FutT",
        "XRPUSDT"   : "FutT",
        "WIFUSDT"   : "FutT",
        "NOTUSDT"   : "FutT",
        "SUIUSDT"   : "FutT",
        "RAREUSDT"  : "FutT",

        # "BNBUSD_PERP"   : "FutC",
        # "BTCUSD_PERP"   : "FutC",
        # "ETHUSD_PERP"   : "FutC",
        # "FTMUSD_PERP"   : "FutC",
        # "LINKUSD_PERP"  : "FutC",
        # "SOLUSD_PERP"   : "FutC",
        # "XRPUSD_PERP"   : "FutC",
        # "WIFUSD_PERP"   : "FutC",
    }.items()}

instruments.update(
    {f"{inst} (Fut)" : f"BitMEX_{inst}_BitMEX_PERP"
     for inst in [
        "XBTUSDT",
        "ETHUSDT",
        "SOLUSDT"
    ]}
)

instruments.update(
    {f"{inst} ({segm})" :
        f"Crypto_{inst}_Crypto_{segm}_{'SPOT' if segm == 'Spt' else 'PERP'}"
        for inst, segm in
     {
       "BTCUSD_PERP"    : "Fut",
       "ETHUSD_PERP"    : "Fut",
       "SOLUSD_PERP"    : "Fut",
       "XRPUSD_PERP"    : "Fut",
       "BTC_USD"        : "Spt",
       "ETH_USD"        : "Spt",
       "SOL_USD"        : "Spt",
       "XRP_USD"        : "Spt",
    }.items()}
)


def escape_markdown(text: str) -> str:
    """
    Escapes special characters in a string to be used in Telegram's MarkdownV2 mode.

    This function is necessary because Telegram's MarkdownV2 mode interprets certain characters as special formatting characters. By escaping these characters, we ensure that the text is displayed as intended without any unintended formatting.

    Parameters:
    - text (str): The text to escape.

    Returns:
    - str: The escaped text.
    """

    SPECIAL_CHARS = [
        "\\",
        "_",
        "*",
        "[",
        "]",
        "(",
        ")",
        "~",
        "`",
        ">",
        "<",
        "&",
        "#",
        "+",
        "-",
        "=",
        "|",
        "{",
        "}",
        ".",
        "!",
    ]

    return "".join(f"\\{char}" if char in SPECIAL_CHARS else char for char in text)


async def alert_if_instrument_inactive(bot: Bot) -> None:
    """
    Checks if each instrument in the predefined list has been updated in the last 10 minutes.

    If an instrument has not been updated, it sends an alert message to the specified Telegram chat. The alert message is formatted using Telegram's MarkdownV2 mode for better readability.

    Parameters:
    - bot (Bot): The Telegram Bot instance used to send messages.
    """

    failed_instruments = list()

    for instrument, table in instruments.items():
        query_df = client.query_df(query=f"SELECT max(o_ts_exch) FROM {table}")

        logger.info(msg=f"{table}: {query_df.iloc[0, 0]}")

        datetime_minutes_ago = datetime.now() - pd.Timedelta(minutes=10)
        has_passed_minutes = query_df.iloc[0, 0] < datetime_minutes_ago

        if has_passed_minutes:
            failed_instruments.append(instrument)

    if failed_instruments:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=f"⚠️ Alert: {len(failed_instruments)} instruments {', '.join([f'*{escape_markdown(text=failed_instrument)}*' for failed_instrument in failed_instruments])} stopped updating",
            parse_mode="MarkdownV2",
        )

        try:
            subprocess.run(
                args=["bash", "~/services_clickhouse/services_restart.sh"],
                check=True,
            )
            logger.info(msg="Bash script executed successfully")
        except subprocess.CalledProcessError as e:
            logger.error(msg=f"Failed to execute bash script: {e}")


async def main() -> None:
    """
    The main entry point for the script. Initializes the Telegram bot, logs the bot's initialization, and starts the monitoring process. The monitoring process runs indefinitely, checking the activity of the specified instruments at regular intervals.

    If an error occurs during the initialization of the bot or during the monitoring process, it logs the error and stops execution.
    """

    logger.info(msg="Initializing the Telegram bot")
    bot = Bot(token=TOKEN)

    try:
        bot_info = await bot.get_me()
        text = f"Bot {escape_markdown(text=bot_info.username)} initialized successfully"
        logger.info(msg=text)
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode="MarkdownV2",
        )
    except TelegramError as e:
        logger.error(msg=f"An error occurred initializing the bot: {e}")
        return

    logger.info(msg="Starting the monitoring bot")

    try:
        while True:
            start_time = asyncio.get_running_loop().time()

            await alert_if_instrument_inactive(bot=bot)

            seconds = 60 * 10
            next_run_time = start_time + seconds
            sleep_time = max(0, next_run_time - asyncio.get_running_loop().time())

            if sleep_time:
                logger.info(msg=f"Sleeping for {sleep_time} seconds")
                await asyncio.sleep(delay=sleep_time)
    except asyncio.CancelledError:
        logger.info("Monitoring bot was cancelled")
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)


if __name__ == "__main__":
    asyncio.run(main=main())
