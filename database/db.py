from urllib import parse
from io import StringIO
from os import makedirs
from lxml import etree
import requests
import sqlite3
import logging
import time

loggers = {}


def logger_cfg(name, filename, level=logging.DEBUG,
               formatter='%(asctime)s||%(levelname)s||%(name)s||%(message)s'):
    """ Configuration function for spawning new loggers
        (needed to compare with 'loggers: dict' instances) """
    global loggers
    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        file_handler = logging.FileHandler(filename, encoding='utf-8')
        ch = logging.StreamHandler()
        formatter = logging.Formatter(formatter)
        file_handler.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.setLevel(level)
        ch.setLevel(level)
        logger.addHandler(ch)
        logger.addHandler(file_handler)
        loggers[name] = logger
        return logger


class Database:
    __slots__ = ['_db', 'cursor', 'db_logger', 'ansa_url']

    def __init__(self, name):
        self._db = sqlite3.connect(name, check_same_thread=False)
        self.db_logger = logger_cfg("DATABASE", 'db.log')
        self.ansa_url = "https://www.ansa.it/sito/static/ansa_rss.html"
        self._initialize()

    def _initialize(self):
        """ Channel_categories = for unique pairs of enabled categories in each chat (channel_id, category_id)
            categories = for storing categories and their state of feed
            channels = for storing channels list and IDs
        """
        self.cursor = self._db.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS channel_categories "
                            "(channel_id INTEGER, category_id INTEGER, last_news TEXT, UNIQUE(channel_id, category_id))")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS categories "
                            "(category_id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 0, name TEXT, feed TEXT, epoch INTEGER DEFAULT 0)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS channels "
                            "(channel_id INTEGER PRIMARY KEY NOT NULL, channel_name TEXT)")
        self.exec("SELECT feed FROM categories")
        fetched_feed = self.cursor.fetchall()
        if not fetched_feed:
            try:
                self.db_logger.info('Populating database...')
                self.populate_rss_urls()
            except sqlite3.Error as e:
                self.db_logger.warning(f'{e}: occurred while populating feeds')
            else:
                self.db_logger.info("Database feeds populated correctly")
        else:
            self._update_epoch()

    @staticmethod
    def http_request(url):
        """ Only used for 'populate_rss_feeds' """
        try:
            resp = requests.get(url)
            return resp, True
        except requests.exceptions.RequestException as e:
            print(f"Error while requesting: {url}! -> {e}")
            return None, False

    def populate_rss_urls(self):
        """ Only used to populate database 'feed' and 'epoch' columns
            with urls and current epoch in database table 'categories' """
        ansa_url = self.ansa_url
        url_parsed = parse.urlparse(ansa_url)
        lista_feed = {}
        resp, done = self.http_request(ansa_url)
        if not done:
            raise Exception("Error while requesting http page")
        parser = etree.HTMLParser()
        root = etree.parse(StringIO(resp.text), parser)
        dd_list = root.xpath("//dd")
        for dd in dd_list:
            self.get_urls_to_db(dd, url_parsed, lista_feed)
        [self.exec("INSERT INTO categories (name, feed, epoch) VALUES (?, ?, ?)",
                   [key, url, int(time.time())])
         for key, url in lista_feed.items()]
        self.commit()

    @staticmethod
    def get_urls_to_db(dd, url_parsed, lista_feed):
        """ Fill database with urls """
        li_list = dd.xpath("ul//li")
        if len(li_list) != 2:
            raise Exception("Bad li_list")
        else:
            cat_name = li_list[0].xpath("a")[0]
            texts = [t for t in cat_name.itertext()]
            link = li_list[1].xpath("a[@class='b-rss']")[0]
            lista_feed.update({texts[1]: f"{url_parsed.scheme}://{url_parsed.hostname}/{link.attrib['href']}"})

    def _update_epoch(self):
        self.exec("UPDATE categories SET epoch = ?", [int(time.time())])
        self.commit()

    def _update_last_news_into_channel_categories(self, title: str, cat_id: int, channel_id: int) -> None:
        """ Insert last news title in its category """
        self.exec("UPDATE channel_categories SET last_news = ? WHERE (category_id = ? AND channel_id = ?)",
                  [title, cat_id, channel_id])
        self.commit()

    def check_last_news(self, channel_id: int, news_title: str, cat_id: int) -> bool:
        """ Check if the news is already been published in the chat """
        self.exec("SELECT category_id  FROM channel_categories WHERE channel_id = ?",
                  [channel_id])
        fetched = self.cursor.fetchall()
        active_categories = [x[0] for x in fetched]
        self.exec("SELECT category_id, last_news FROM channel_categories")
        catid_lastnews = {x[0]: x[1] for x in self.cursor.fetchall()}
        for catid in active_categories:
            if catid_lastnews[catid] == news_title:
                return True
        self._update_last_news_into_channel_categories(news_title, cat_id, channel_id)
        return False

    def channel_update_or_insert(self, chat_id: int, chat_name: str) -> None:
        """ Add the new chat in the table or update chat name if chat_id doesn't match """
        self.exec("SELECT channel_id, channel_name FROM channels WHERE channel_id = ?", [chat_id])
        fetched = self.cursor.fetchone()
        if fetched:  # chat_id found in db
            if chat_name != fetched[1]:  # chat_name & chat_id don't match in db
                try:
                    self.db_logger.info(f'{fetched[1]} changed name into {chat_name}')
                    self.exec("UPDATE channels SET channel_name = ? WHERE channel_id = ?", [chat_name, chat_id])
                except sqlite3.Error as e:
                    logging.warning(e)
                except Exception as e:
                    logging.warning(f'{e.args} occurred while updating chat_name in db')
                else:
                    self.commit()
            else:  # chat_id & chat_name matches in db
                pass
        else:
            self.exec("INSERT INTO channels (channel_id, channel_name) VALUES (?, ?)",
                      [chat_id, chat_name])
            self.commit()

    def enable_cat(self, chat_id: int, cats_ids: list) -> list:
        """ INSERT the row that matches specified category(id)
            with the chat(id) which called the '/enable' command """
        if type(cats_ids[0]) is str and cats_ids[0] == "all":
            self.execmany("INSERT OR IGNORE INTO channel_categories (channel_id, category_id) VALUES (?, ?)",
                          [(chat_id, i) for i in range(1, 50)])
            self.commit()
            return ["all categories"]
        else:
            added = []
            for cat in cats_ids:
                self.exec("SELECT name, category_id FROM categories WHERE category_id = ?", [cat])
                name_id = self.cursor.fetchone()
                if name_id:
                    added.append(name_id[0])
                    cat_id = name_id[1]
                    self.exec("INSERT OR IGNORE INTO channel_categories (channel_id, category_id) VALUES (?, ?)", [chat_id, cat_id])
                    self.commit()
            return added

    def disable_cat(self, chat_id, cats_ids: list) -> list:
        """ DELETE the row that matches specified category(id)
            with the chat(id) which called the '/disable' command """
        if type(cats_ids[0]) is str and cats_ids[0] == "all":
            self.exec("DELETE FROM channel_categories WHERE channel_id = ?",
                      [chat_id])
            self.commit()
            return ["any category"]
        else:
            deleted = []
            for cat in cats_ids:
                self.exec("SELECT name, category_id FROM categories WHERE category_id = ?", [cat])
                name_id = self.cursor.fetchone()
                if name_id:
                    cat_name = name_id[0]
                    cat_id = name_id[1]
                    self.exec("DELETE FROM channel_categories WHERE (channel_id = ? AND category_id = ?)",
                              [chat_id, cat_id])
                    self.commit()
                    deleted.append(cat_name)
            return deleted

    def cat_list(self, chat_id: int) -> list:
        """ List all active categories feeds in the channel """
        self.exec("SELECT category_id FROM channel_categories WHERE channel_id = ?", [chat_id])
        active_categories = self.cursor.fetchall()
        fetched = [x[0] for x in active_categories]
        actives_list = []
        for cat_id in fetched:
            self.exec("SELECT name FROM categories WHERE category_id = ?",
                      [cat_id])
            cat_name = self.cursor.fetchone()
            actives_list.append(f"{cat_id}) {cat_name[0]}")
        return actives_list

    def update_epoch(self, item_epoch: int, cat_id: int) -> None:
        """ UPDATE the last feed publication date (as epoch) in database
            (the epoch is used to check whether there are news) """
        try:
            self.exec("UPDATE categories SET epoch = ? WHERE category_id = ?",
                      [item_epoch, cat_id])
        except sqlite3.Error as e:
            logging.warning(f'{e}: error in updating publish date in database')
        else:
            self.commit()

    def chat_list(self):  # for unit testing
        """ Return a list of all chats in db """
        try:
            self.exec("SELECT channel_id FROM channels")
            all_chats = self.cursor.fetchall()
        except sqlite3.Error as e:
            logging.warning(f'{e}: error in listing chats from database')
        else:
            return [x[0] for x in all_chats]

    def exec(self, query: str, *args):
        """ Execute query """
        return self.cursor.execute(query, *args)

    def execmany(self, query: str, *args):
        """ Execute many query """
        return self.cursor.executemany(query, *args)

    def commit(self):
        """ Commit changes """
        self._db.commit()

    def close(self):
        """ Close the connection with the database """
        self.cursor.close()
        self._db.close()


if __name__ == "__main__":
    pass
