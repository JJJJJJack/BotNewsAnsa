from telegram.ext import Updater, CallbackContext, CommandHandler, MessageHandler, Filters
from rssparser import RssParser
from dotenv import load_dotenv
from database import Database
from telegram import Update
from lxml import etree
import telegram.ext
import asyncio
import sqlite3
import time
import sys
import os


class Bot:
    _token: str
    updater: Updater
    DB: Database
    urls: list
    id_categories: dict

    def __init__(self, token):
        self._token = token
        self.updater = Updater(token=self._token, use_context=True)
        self.DB = Database("bot.db")
        self.urls = self._fetch_urls()
        self.id_categories = self._init_cat_id_dict()

    @staticmethod
    def _init_cat_id_dict() -> dict:
        """ Initialize dict of keys = category id, value = category name """
        return {
            "Abruzzo": 1, "Basilicata": 2, "Calabria": 3, "Campania": 4, "Emilia Romagna": 5,
            "Friuli Venezia Giulia": 6, "Lazio": 7, "Liguria": 8, "Lombardia": 9, "Marche": 10,
            "Molise": 11, "Piemonte": 12, "Puglia": 13, "Sardegna": 14, "Sicilia": 15,
            "Toscana": 16, "Trentino Alto Adige/Suedtirol": 17, "Umbria": 18, "Valle d'Aosta": 19, "Veneto": 20,
            "Canale Europa": 21, "Europarlamento 2019": 22, "La tua Europa": 23, "AgriUe": 24, "Europa delle Regioni": 25,
            "Proprietà intellettuale": 26, "Homepage": 27, "Cronaca": 28, "Politica": 29, "Mondo": 30,
            "Economia": 31, "Calcio": 32, "Sport": 33, "Cinema": 34, "Cultura": 35,
            "Tecnologia": 36, "Ultima Ora": 37, "English News": 38, "Foto": 39, "Video": 40,
            "Ambiente&Energia": 41, "Canale Motori": 42, "Canale Terra&Gusto": 43,
            "Canale Salute&Benessere": 44, "Canale Scienza&Tecnica": 45,
            "Canale Nuova Europa (IT)": 46, "Canale Nuova Europa (EN)": 47, "ANSA Viaggiart": 48, "Lifestyle": 49
        }  # cat_id:cat_name

# async thread functions from here

    async def update_and_publish_rss(self) -> None:
        """ Initialize instance of RssParser for given database and list of urls(rss)
            and fetch news until a match of database pubdate or the last xml item found
            thus post them into every chat that enabled the news's category """
        rss_parser = RssParser(self.DB, self.urls)
        try:
            while True:
                try:
                    new_feeds = await rss_parser.parse_feed()
                    if not new_feeds:
                        print(f'No news to be found...')
                        await asyncio.sleep(600)
                        continue
                    tasks = (self._get_channels_by_category_id(cat_id, cat_post)
                             for cat_id, cat_post in new_feeds.items())
                    await asyncio.gather(*tasks)
                    print(f'Updating done and posted! See you in 1 minute <3')
                except telegram.error.BadRequest as e:
                    print(f'{e.args} telegram.error.BadRequest occurred, ignoring this route of update')
                    continue
                except etree.XMLSyntaxError as e:
                    print(f'{e} etree.XMLSyntaxError occurred, ignoring this route of update')
                except etree.Error as e:
                    print(f'{e} etree.Error occurred, ignoring this route of update')
        except KeyboardInterrupt:
            raise

    async def _get_channels_by_category_id(self, cat_id: int, cat_post: list) -> None:
        """ Get all the channels that have cat_id enabled and return a 'channel_id : post' dict """
        self.DB.exec("SELECT channel_id FROM channel_categories WHERE category_id = ?",
                     [cat_id])
        fetched_cursor = self.DB.cursor.fetchall()
        channels_post = {x[0]: cat_post for x in fetched_cursor}
        await self._spread_news(channels_post)

    async def _spread_news(self, channels_post: dict) -> None:
        """ Async HTTP GET requests to send messages into chats """
        tasks = (self._separate_news(channel_id, cat_post)
                 for channel_id, cat_post in channels_post.items())
        await asyncio.gather(*tasks)

    async def _separate_news(self, channel_id: int, cat_post: list):
        """ Separate the cat_post string into each news link
            so that the messages are sent every 3 seconds
            (telegram bots must send 20 messages max in 1 minute!) """
        category_name = cat_post[0]
        catid = self.id_categories[category_name]
        news_list = cat_post[1:]
        news_list = list(reversed(news_list))  # reverse list so posts get picked from the least recent
        for title_descr_img_link in news_list:
            if title_descr_img_link == news_list[-1]:
                await self._send_message(channel_id, title_descr_img_link, catid, last_one=True)
            else:
                await self._send_message(channel_id, title_descr_img_link, catid)
            await asyncio.sleep(3)

    async def _send_message(self, channel_id: int, title_descr_img_link: tuple, catid: int, last_one: bool = False) -> None:
        """ Separating 'send_message' so that it can be called by '_spread_news'
            as many async tasks in the same time and wait the last one
            (4096 is the max length of a telegram message) """
        try:
            title, descr, img, link = title_descr_img_link
            if last_one:
                if not self.DB.check_last_news(channel_id, title, catid):
                    self.updater.bot.send_photo(chat_id=channel_id, photo=img,
                                                caption=f'{title}{descr[:-6]}\n[Read more]({link})',
                                                parse_mode='markdown')
        except telegram.error.Unauthorized:
            await self._remove_chat(channel_id)
        except telegram.error.TimedOut as e:
            print(f'{e.args} occurred trying to send photo update')
            raise
        except telegram.error.BadRequest as e:
            print(f'{e.args}: printing self.updater.bot.send_photo args:\n'
                  f'channel id: {channel_id}\n'
                  f'image: {img}\n'
                  f'description: {descr}')
            raise
        except telegram.error.RetryAfter as e:
            print(f'{e.args} occurred trying to send photo update, not sending photo, nor updating epoch')
            raise

    async def _remove_chat(self, chat_id: int) -> None:
        self.DB.exec("DELETE FROM channels WHERE channel_id = ?", [chat_id])
        self.DB.commit()

    def _fetch_urls(self) -> list:
        self.DB.exec("SELECT feed FROM categories")
        return [x[0] for x in self.DB.cursor.fetchall()]

# functions for the bot commands from now on

    def list_categories(self, update: Update, context: CallbackContext) -> None:
        """ Use a provided HtmlParser instance to return a dict of categories and rss URLs"""
        self.DB.exec("SELECT category_id, name FROM categories")
        catid_catname = self.DB.cursor.fetchall()
        lista = '\n'.join([f'{x[0]}){x[1]}' for x in catid_catname])
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'To enable feeds, use /enable + [category_id]\n'
                                      f'{lista}')

    def active_categories(self, update: Update, context: CallbackContext) -> None:
        active = self.DB.cat_list(update.effective_chat.id)
        active = "\n".join(active)
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'Feed attivi:\n{active}')

    def add_command(self, cmd: str, handler):
        self.updater.dispatcher.add_handler(CommandHandler(cmd, handler))

    def enable(self, update: Update, context: CallbackContext) -> None:
        self.add_chat_group(update)
        cats_ids = context.args
        if not cats_ids:
            context.bot.send_message(chat_id=update.effective_chat.id, text='At least one argument is needed!')
            return
        added = self.DB.enable_cat(update.effective_chat.id, cats_ids)
        added = " ".join(added)
        context.bot.send_message(chat_id=update.effective_chat.id, text=f"You are gonna receive feeds from {added}")

    def disable(self, update: Update, context: CallbackContext) -> None:
        self.add_chat_group(update)
        cats_ids = context.args
        if not cats_ids:
            context.bot.send_message(chat_id=update.effective_chat.id, text='At least one argument is needed!')
            return
        deleted = self.DB.disable_cat(update.effective_chat.id, cats_ids)
        deleted = " ".join(deleted)
        context.bot.send_message(chat_id=update.effective_chat.id, text=f"You are not gonna receive news from {deleted}")

    def add_msg_handler(self, filters: telegram.ext.filters.BaseFilter, handler):
        """ Add messages handler, filters are built bitwise with 'Filters' module """
        self.updater.dispatcher.add_handler(MessageHandler(filters, handler))

    def add_chat_group(self, update: Update):
        """ Check chat group, if needed, add new chat group in database or change the name"""
        chat_id = update.effective_chat.id
        chat_name = f'{update.effective_chat.title or update.effective_chat.username}'
        self.DB.channel_update_or_insert(chat_id, chat_name)

    def start_polling(self):
        return self.updater.start_polling()

    def idle(self):
        return self.updater.idle()

    def stop(self):
        self.DB.close()
        self.updater.stop()

    @staticmethod
    def help(update: Update, context: CallbackContext):
        help_message = f"/list shows all categories and their relative IDs to be activated with\n"\
                       f"/active list all active feeds\n" \
                       f"/enable followed by the category IDs or 'all', enable one or more feeds update (separated by a whitespace)\n" \
                       f"/disable followed by category IDs or 'all', disable one or more feeds update (separated by a whitespace)\n" \
                       f"/help shows what each command does"
        context.bot.send_message(chat_id=update.effective_chat.id, text=help_message)


def main(bot: Bot) -> None:
    bot.add_msg_handler(Filters.text & (~Filters.command), bot.add_chat_group)
    bot.add_command("list", bot.list_categories)
    bot.add_command("active", bot.active_categories)
    bot.add_command('enable', bot.enable)
    bot.add_command('disable', bot.disable)
    bot.add_command('help', Bot.help)
    bot.start_polling()
    asyncio.run(bot.update_and_publish_rss())


if __name__ == "__main__":
    load_dotenv()
    TOKEN = os.getenv("TOKEN")
    bot = Bot(TOKEN)
    try:
        main(bot)
    except KeyboardInterrupt:
        try:
            bot.stop()
        except sqlite3.Error as sqle:
            print(f'{sqle}\nError in closing DB and bot')
            raise
        else:
            sys.exit('Database closed\nBot shutted down')
    else:
        print("Something went wrong!!!")
