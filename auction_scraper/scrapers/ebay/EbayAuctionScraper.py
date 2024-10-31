import sys
import json
import logging
import unicodedata
import asyncio
import aiohttp
import contextlib
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from slimit import ast
from sqlalchemy_utils import Currency
import dateutil.parser
import re

from auction_scraper.abstract_scraper import AbstractAuctionScraper, SearchResult
from auction_scraper.scrapers.ebay.models import EbayAuction, EbayProfile

# Setup logging with multiple levels for more granular control
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Caching mechanism for improved performance on repetitive data access
class SimpleCache:
    def __init__(self):
        self._cache = {}

    def get(self, key):
        return self._cache.get(key)

    def set(self, key, value):
        self._cache[key] = value

    def clear(self):
        self._cache.clear()

cache = SimpleCache()

@contextlib.contextmanager
def silence_output():
    """Suppress unwanted parser output during JS parsing with slimit."""
    save_stdout, save_stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = open(sys.devnull, 'w'), open(sys.devnull, 'w')
    try:
        yield
    finally:
        sys.stdout, sys.stderr = save_stdout, save_stderr

class EbayAuctionScraper(AbstractAuctionScraper):
    """
    Asynchronous eBay scraper for auctions, profiles, and search pages.
    """
    auction_table = EbayAuction
    profile_table = EbayProfile
    base_uri = 'https://www.ebay.com'
    auction_suffix = '/itm/{}'
    profile_suffix = '/usr/{}'
    search_suffix = '/sch/i.html?_nkw={}&_pgn={}&_skc=0'
    backend_name = 'ebay'
    auction_duplicates = ['maxImageUrl', 'displayImgUrl']

    def __init__(self):
        super().__init__()

    async def _get_page(self, uri):
        """
        Fetches and parses a page asynchronously.
        """
        cache_key = f"page:{uri}"
        cached_content = cache.get(cache_key)
        if cached_content:
            logger.debug(f"Cache hit for URI: {uri}")
            return cached_content

        async with aiohttp.ClientSession() as session:
            async with session.get(uri) as response:
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                cache.set(cache_key, soup)
                return soup

    async def _scrape_auction_page(self, uri):
        soup = await self._get_page(uri)
        auction = self.__parse_auction_soup(soup)
        auction.uri = uri
        return auction, soup.prettify()

    async def _scrape_profile_page(self, uri):
        profile_id = urlparse(uri).path.split('/')[2]
        soup = await self._get_page(uri)
        profile = self.__parse_profile_soup(soup, profile_id)
        profile.uri = uri
        return profile, soup.prettify()

    async def _scrape_search_page(self, uri):
        soup = await self._get_page(uri)
        return self.__parse_search_soup(soup), soup.prettify()

    def __get_dict_value(self, dictionary, key, default=None):
        """Retrieve and convert values from dictionary with error handling."""
        value = dictionary.get(key, default)
        if value == 'true':
            return True
        if value == 'false':
            return False
        if value in ('null', None):
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            pass
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
        return value

    def __parse_auction_soup(self, soup):
        """
        Parses auction details from the page soup.
        """
        raw_values = self.__get_embedded_json(soup)

        auction = EbayAuction(id=int(raw_values.get('itemId', 0)))
        auction.title = unicodedata.normalize("NFKD", raw_values.get('it', ''))
        auction.description = self.__extract_description(soup)
        auction.seller_id = raw_values.get('entityName', '')

        auction.start_time = self.__parse_timestamp(raw_values.get('startTime'))
        auction.end_time = self.__parse_timestamp(raw_values.get('endTime'))
        auction.n_bids = self.__get_dict_value(raw_values, 'bids', 0)
        auction.currency = self.__parse_currency(raw_values.get('ccode'))
        auction.latest_price = self.__get_dict_value(raw_values, 'bidPriceDouble')
        auction.buy_now_price = self.__get_dict_value(raw_values, 'binPriceDouble')
        auction.image_urls = ' '.join(self.__get_image_urls(raw_values))

        auction.locale = raw_values.get('locale', '')
        auction.quantity = self.__get_dict_value(raw_values, 'totalQty', 1)
        auction.video_url = raw_values.get('videoUrl', '')
        auction.vat_included = raw_values.get('vatIncluded') == 'true'
        auction.domain = raw_values.get('currentDomain', '')

        return auction

    def __get_embedded_json(self, soup):
        """
        Extracts embedded JSON from JavaScript within the HTML.
        """
        cache_key = f"json:{soup}"
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.debug("Using cached JSON data")
            return cached_data

        raw_values = {}
        with silence_output():
            parser = Parser()
        for script in soup.find_all('script', src=None):
            script_text = ''.join(script.contents)
            if '$rwidgets' in script_text:
                tree = parser.parse(script_text)
                for node in nodevisitor.visit(tree):
                    if isinstance(node, ast.FunctionCall) and node.identifier.value == '$rwidgets':
                        fields = {}
                        for n in nodevisitor.visit(node):
                            if isinstance(n, ast.Assign):
                                k = getattr(n.left, 'value', '').strip('"')
                                v = getattr(n.right, 'value', '').strip('"')
                                if k in self.auction_duplicates:
                                    fields.setdefault(k, []).append(v)
                                else:
                                    fields[k] = v
                        for k, v in fields.items():
                            if k in self.auction_duplicates:
                                raw_values.setdefault(k, []).extend(v)
                            else:
                                raw_values[k] = v
        cache.set(cache_key, raw_values)
        return raw_values

    def __get_image_urls(self, raw_values):
        """Retrieve and decode image URLs from embedded JSON."""
        max_images = raw_values.get('maxImageUrl', [])
        display_images = raw_values.get('displayImgUrl', [])
        return [
            json.loads(f'"{max_img if max_img != "null" else disp_img}"')
            for max_img, disp_img in zip(max_images, display_images) if max_img or disp_img
        ]

    def __parse_timestamp(self, timestamp):
        """Converts a timestamp string to a datetime object."""
        try:
            return datetime.utcfromtimestamp(int(timestamp) / 1000)
        except (TypeError, ValueError) as e:
            logger.error(f"Error parsing timestamp: {timestamp} - {e}")
            return None

    def __parse_currency(self, currency_code):
        """Parses currency code to Currency object with error handling."""
        try:
            return Currency(currency_code)
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing currency: {currency_code} - {e}")
            return None

    def __extract_description(self, soup):
        """Extract description content from HTML soup."""
        desc_div = soup.find('div', id='desc_div')
        if desc_div and desc_div.find('iframe'):
            return desc_div.find('iframe').get_text(strip=True)
        desc_holder = soup.find('div', class_='vi_descsnpt_holder')
        return desc_holder.get_text(strip=True) if desc_holder else ''

