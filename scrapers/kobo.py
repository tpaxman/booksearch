import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd


def compose_search_url(keywords: str) -> str:
    quoted_search_string = quote_plus(keywords)
    search_url = f"https://www.kobo.com/ca/en/search?query={quoted_search_string}"
    return search_url

def parse_results(content: bytes) -> pd.DataFrame:
    soup = BeautifulSoup(content, features='html.parser')
    result_items = soup.find('ul', class_='result-items').find_all('li', class_='book')

    result_items_data = []
    for x in result_items:
        title = _get_text(x.find('h2', class_='title').find('a'))
        link = x.find('h2', class_='title').find('a')['href']
        subtitle = _get_text(x.find('p', class_='subtitle'))
        author = _get_text(x.find('span', class_='synopsis-text').find('a', class_='contributor-name'))
        price = _get_text(x.find('span', class_='price-value'))
        result_items_data.append({
            "title": title,
            "subtitle": subtitle,
            "author": author,
            "link": link,
            "price": price,
        })
    return pd.DataFrame(result_items_data).fillna('')

def _get_text(elem) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText() if elem else ''
