from functools import partial
from collections.abc import Iterable
import re
import requests
from typing import Callable, Literal
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
import numpy as np
import tabulate
import pandas as pd
from tools.webscraping import refilter, get_text

# TODO: calculate waiting period for books
# TODO: make an exhaustive list of format code types
# TODO: make this more flexible to allow OR

LIBRARY_SUBDOMAIN_TYPE = Literal['epl', 'calgary']

FORMATCODE_TYPE = Literal['BK', 'AB', 'EBOOK']

FORMATCODES_TYPE = Iterable[FORMATCODE_TYPE] | FORMATCODE_TYPE


def compose_search_url(
    library_subdomain: LIBRARY_SUBDOMAIN_TYPE,
    title: str=None,
    author: str=None,
    anywhere: str=None,
    publisher: str=None,
    formatcodes: Iterable[FORMATCODE_TYPE] | FORMATCODE_TYPE = ('BK', 'EBOOK', 'AB'),
    isolanguage: str=None,
) -> pd.DataFrame:
    """
    Compose a search URL for a bibliocommons library search
    """

    if isinstance(formatcodes, str):
        formatcode = formatcodes
    else:
        formatcode = ' OR '.join(formatcodes)

    kwargs = {
        'title': title,
        'author': author,
        'anywhere': anywhere,
        'publisher': publisher,
        'formatcode': formatcode,
        'isolanguage': isolanguage,
    }

    search_string = ' AND '.join(f'{k}:({v})' for k, v in kwargs.items() if v)
    quoted_search_string = quote_plus(search_string)
    search_url = f"https://{library_subdomain}.bibliocommons.com/v2/search?query={quoted_search_string}&searchType=bl"
    return search_url


compose_search_url_epl = partial(compose_search_url, library_subdomain='epl')

compose_search_url_calgary = partial(compose_search_url, library_subdomain='calgary')


def parse_results(results_html: bytes, title_refilter: str = None, author_refilter: str = None) -> pd.DataFrame:
    soup = BeautifulSoup(results_html, features='html.parser')

    try:
        result_items = soup.find('ul', class_='results').find_all('li', class_='cp-search-result-item')
    except:
        return pd.DataFrame()

    result_items_data = []
    for x in result_items:
        title = x.find('span', class_='title-content')
        subtitle = x.find('span', class_='cp-subtitle')
        author = x.find('a', class_='author-link')

        formats = x.find_all('div', class_='manifestation-item')
        formats_data = []
        for y in formats:
            format_ = y.find('span', class_='cp-format-indicator')
            availability_status = y.find('span', class_='cp-availability-status')
            call_number = y.find('span', class_='cp-call-number')
            hold_counts = y.find('span', class_='cp-hold-counts')
            eresource_link = y.find('a', class_='cp-eresource-link')

            formats_data.append({
                "format_description": get_text(format_),
                "availability_status": get_text(availability_status),
                "call_number": get_text(call_number),
                "hold_counts": get_text(hold_counts),
                "eresource_link": get_text(eresource_link),
            })

        result_items_data.append({
            "title": get_text(title),
            "subtitle": get_text(subtitle),
            "author": get_text(author),
            "formats": formats_data
        })


    base_data = pd.DataFrame(result_items_data)
    formats = base_data.formats.explode().apply(pd.Series)
    data = base_data.drop(columns='formats').join(formats)

    data = (data
        .assign(true_format = lambda t: np.where(t.format_description.eq('eBook') & t.call_number.eq('Internet Access'), 'web-ebook', t.format_description.str.lower()))
        .replace({'true_format': {'downloadable audiobook': 'audiobook'}})
    )

    if title_refilter:
        data = data.loc[lambda t: (t['title'] + ' ' + t['subtitle']).apply(lambda x: refilter(x, title_refilter))]

    if author_refilter:
        data = data.loc[lambda t: t['author'].apply(lambda x: refilter(x, author_refilter))]

    return data


def agg_results(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results
        [['author_search', 'title_search', 'true_format']]
        .value_counts()
        .unstack()
        .reindex(['book', 'ebook', 'web-ebook', 'audiobook'], axis=1)
        .fillna(0)
        .astype('int')
        .reset_index()
    )


def create_view(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results

    return (
        results
        .reindex(['true_format', 'hold_counts', 'title', 'author'], axis=1)
        .reset_index(drop=True)
    )


def create_description(results: pd.DataFrame) -> str:
    if results.empty:
        return '<no results>'

    description = (
        results
        .reindex(['true_format', 'title', 'author', 'hold_counts'], axis=1)
        .groupby('true_format').first()
    )
    string = tabulate.tabulate(description, showindex=False)
    return string

def get_available_formats(parsed_data: pd.DataFrame) -> str:
    return (parsed_data
        .groupby('true_format')
        .first()
        .reset_index()
        .assign(description = lambda t: '[' + t.true_format + '] ' + t.title + ': ' + t.subtitle + ' (' + t.author + ')')
        .description
        .pipe('\n'.join)
    )


def extract_library_subdomain(search_url: str) -> str:
    return re.search(r'https://(\w+)', search_url).group(1)



