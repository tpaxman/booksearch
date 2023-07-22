import re
import requests
from functools import partial
from collections.abc import Iterable
from typing import Callable, Literal
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
import numpy as np
import tabulate
import pandas as pd

# TODO: calculate waiting period for books
# TODO: make an exhaustive list of format code types
# TODO: make this more flexible to allow OR

LIBRARY_SUBDOMAIN_TYPE = Literal['epl', 'calgary']

FORMATCODE_TYPE = Literal['BK', 'AB', 'EBOOK']

FORMATCODES_TYPE = Iterable[FORMATCODE_TYPE] | FORMATCODE_TYPE

def generate_compose_search_url(library_subdomain: LIBRARY_SUBDOMAIN_TYPE) -> Callable:
    def compose_search_url(
        title: str=None,
        author: str=None,
        keywords: str=None,
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
            'anywhere': keywords,
            'publisher': publisher,
            'formatcode': formatcode,
            'isolanguage': isolanguage,
        }

        search_string = ' AND '.join(f'{k}:({v})' for k, v in kwargs.items() if v)
        quoted_search_string = quote_plus(search_string)
        root_url = f"https://{library_subdomain}.bibliocommons.com/v2/search"
        search_url = f"{root_url}?query={quoted_search_string}&searchType=bl"
        return search_url
    return compose_search_url


compose_search_url_epl = generate_compose_search_url('epl')
compose_search_url_calgary = generate_compose_search_url('calgary')


def parse_results(results_html: bytes) -> pd.DataFrame:
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
            item_href = y.find('a', class_='manifestation-item-link')['href']

            formats_data.append({
                "format_description": _get_text(format_),
                "availability_status": _get_text(availability_status).strip(),
                "call_number": _get_text(call_number),
                "hold_counts": _get_text(hold_counts),
                "eresource_link": _get_text(eresource_link),
                "item_href": item_href,
            })

        result_items_data.append({
            "title": _get_text(title),
            "subtitle": _get_text(subtitle),
            "author": _get_text(author),
            "formats": formats_data
        })


    base_data = pd.DataFrame(result_items_data)
    formats = base_data.formats.explode().apply(pd.Series)
    data = base_data.drop(columns='formats').join(formats)

    data = (data
        .assign(true_format = lambda t: np.where(t.format_description.eq('eBook') & t.call_number.eq('Internet Access'), 'web-ebook', t.format_description.str.lower()))
        .replace({'true_format': {'downloadable audiobook': 'audiobook'}})
        .reset_index(drop=True)
    )

    return data

def _get_text(elem) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText() if elem else ''
