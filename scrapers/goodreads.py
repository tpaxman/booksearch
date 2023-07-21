import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
from typing import Callable
import tabulate

# TODO: figure out why goodreads only returns 5 things
# TODO: sort by most ratings maybe? Getting some weird values otherwise

def compose_search_url(keywords: str) -> str:
    quoted_search_string = quote_plus(keywords)
    search_url = f"https://www.goodreads.com/search?utf8=%E2%9C%93&q={quoted_search_string}&search_type=books&search%5Bfield%5D=on"
    return search_url


def parse_results(content: bytes) -> pd.DataFrame:
    soup = BeautifulSoup(content, features='html.parser')

    try:
        result_items = soup.find('table', class_='tableList').find_all('tr')
    except:
        # TODO: remove this duplication by using reindex later on
        return pd.DataFrame(columns=['title', 'author', 'avg_rating', 'num_ratings', 'item_href'])


    result_items_data = []
    for x in result_items:
        title = x.find('a', class_='bookTitle')
        author = x.find('a', class_='authorName')
        minirating = x.find('span', class_='minirating')

        avg_rating_raw, num_ratings_raw = re.search(r'(\d\.\d\d) avg rating\D+([\d|,]+) ratings*', _get_text(minirating)).groups()
        avg_rating = float(avg_rating_raw)
        num_ratings = int(num_ratings_raw.replace(',', ''))

        title_text = _get_text(title.find('span', role='heading')) if title else ''
        item_href = re.sub(r'\?.*', '', title['href']) if title else ''

        result_items_data.append({
            "title": title_text,
            "author": _get_text(author),
            "avg_rating": avg_rating,
            "num_ratings": num_ratings,
            "item_href": item_href,
        })

    data = (
        pd.DataFrame(result_items_data)
        .sort_values('num_ratings', ascending=False)
        .reset_index(drop=True)
    )

    # TODO: figure out how to implement this re-filtering stuff
    # def string_contains_all_words(string: str, words: str) -> bool:
    #     words_clean = re.findall(r'\w+', words)
    #     return all(w.strip().lower() in string.lower() for w in words_clean)
    # data = (data
    #     .loc[lambda t: t.title.apply(lambda x: string_contains_all_words(x, title))]
    #     .loc[lambda t: t.author.apply(lambda x: string_contains_all_words(x, author))]
    # )

    return data


def _get_text(elem) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText() if elem else ''
