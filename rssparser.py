from database import Database
from lxml import etree, html
from io import StringIO
import asyncio
import logging
import httpx
import time
import sys


class RssParser:
    __slots__ = ["urls", "bot_db", "ansa_url"]

    def __init__(self, db: Database, urls: list):
        self.urls = urls
        self.bot_db = db
        logging.basicConfig(filename="rssparser.log", level=logging.INFO)

    async def get_db_args(self) -> list:
        """ Fetch database table 'categories' into a list of tuples '(category_id: int, category_name: str, last publication link: str)' """
        # print("sono in get_db_args") for debugging purpose
        self.bot_db.exec("SELECT category_id, name, epoch FROM categories")
        catid_name_epoch_db = [x for x in self.bot_db.cursor.fetchall()]
        return catid_name_epoch_db

    async def parse_feed(self) -> dict:
        """ Setup environment variables to feed 'get_news_from_html' with
            rss_pages = list of rss pages
            catid_name_epoch_db = list of tuples from database '(category_id: int, category_name: str, last publish date: str)'
            new_feeds = dictionary to pair chat group id with the actual news '(chat_id: news)'
        """
        new_feeds = {}  # will contain 'category_id : category_news'
        async with httpx.AsyncClient() as client:  # async client for http requests
            tasks = (client.get(url, timeout=None)
                     for url in self.urls)
            reqs = await asyncio.gather(*tasks)
        parser = etree.XMLParser()
        rss_pages = [etree.parse(StringIO(req.text), parser) for req in reqs]
        catid_name_epoch_db = await self.get_db_args()
        get_news = (self.get_news_from_html(new_feeds, catid, name, db_epoch, rss_xml)
                    for (catid, name, db_epoch), rss_xml
                    in zip(catid_name_epoch_db, rss_pages))
        await asyncio.gather(*get_news)
        return new_feeds

    async def get_news_from_html(self, new_feeds: dict, catid: int, name: str, db_epoch: int,
                                 rss_xml: etree._ElementTree) -> None:
        """ The actual fetching function that parse, check, format and add news to 'new_feeds' """
        try:
            items = rss_xml.xpath("//item")
        except etree.Error as e:
            logging.warning(f'{e} occurred at {time.strftime("%d.%m.%y %I:%M:%S", time.localtime(int(time.time())))}\n'
                            f'printing xml that could have caused the issue\n'
                            f'{rss_xml}\n')
            return
        if not items:
            return
        rss_new_items = []
        rss_new_items.append(f'{name}')
        epochs_pool = 0
        for item in items:
            try:
                pubdate = time.strptime(item.xpath("pubDate")[0].text, "%a, %d %b %Y %H:%M:%S %z")
                item_epoch = int(time.mktime(pubdate))
                pubdate = time.strftime('%a, %d %b %Y %H:%M:%S %z', pubdate)
            except ValueError:
                try:
                    pubdate = time.strptime(item.xpath("pubDate")[0].text, "%d %b %Y %H:%M:%S %z")
                    struct_time = time.struct_time(pubdate)
                    item_epoch = int(time.mktime(struct_time))
                    if item_epoch != time.mktime(pubdate):
                        print(f"EPOCHS ARE DIFFERENT: catid={catid}, name={name}, db_epoch={db_epoch},  ")
                    pubdate = time.strftime('%a, %d %b %Y %H:%M:%S %z', pubdate)
                except ValueError:
                    print(f"Bad pubdate format in rss feed page, raising ValueError from\n"
                          f'{item.xpath("title")[0].text}')
                    raise
            link = item.xpath("link")[0].text
            if item_epoch > db_epoch:
                title_descr_img_link = await self.parse_link_metas(link)
                if not title_descr_img_link:
                    return
                rss_new_items.append(title_descr_img_link)
                epochs_pool = max(epochs_pool, item_epoch)
        if len(rss_new_items) > 1:
            self.bot_db.update_epoch(int(time.time()), catid)
            new_feeds[catid] = rss_new_items

    async def parse_link_metas(self, link):
        resp = httpx.get(link, follow_redirects=True)
        if resp.status_code == 301:
            logging.warning(f"This link gave 301 response: {link}")
            print(f"This link gave 301 response: {link}")
            return None
        try:
            html_root = html.parse(StringIO(resp.text))
        except etree.XMLSyntaxError:
            print(f"LINK THAT BROKE THE SCRIPT WITH XMLSyntaxError: {link}\n"
                  f"resp is = {resp}\n"
                  f"resp text = {resp.text}")
            sys.exit(-1)
        html_root = html.parse(StringIO(resp.text))
        try:
            title = html_root.xpath("//meta[@name='EdTitle']/@content")
        except AssertionError:
            print(f"link that broke the script with assertion error = {link}\n"
                  f"resp is = {resp}\n"
                  f"resp text is _ {resp.text}")
            sys.exit(-1)
        if not title:
            title = ''
        else:
            title = f'{title[0]}\n'
        descr = html_root.xpath("//meta[@name='description']/@content")
        if not descr:
            descr = title
        else:
            descr = f'{descr[0]}'
        img = self.fetch_correct_img(html_root)
        return title, descr, img, link

    @staticmethod
    def fetch_correct_img(html_root: etree._ElementTree) -> str:
        """ Search for best img candidate to publish with the news post """
        try:
            img = html_root.xpath("//meta[@name='twitter:image:src']/@content")
            if img and not img[0].endswith('.0'):
                return img[0]
            img = html_root.xpath("//meta[@property='og:image']/@content")
            if img and not img[0].endswith('.0'):
                return img[0]
            return "https://www.ansa.it/sito/img/ico/ansa-700x366-precomposed.png"
        except Exception as e:
            print(f"{e} ERROR WHILE PARSING IMG IN RSSPARSER")
