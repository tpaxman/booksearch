from functools import partial
import numpy as np
from bs4 import BeautifulSoup
import requests
from urllib.parse import quote_plus
import pandas as pd
from typing import Callable, Literal
from tools.webscraping import refilter, get_text

valid_formatcodes = Literal[
    'BK',
    'EBOOK',
    'AB'
]

valid_library_subdomains = Literal[
    'epl',
    'calgary'
]

# TODO: calculate waiting period for books
# TODO: make this exhaustive
# VALID_FORMATCODES = ['BK', 'AB', 'EBOOK']

def generate_compose_search_url_function(library_subdomain: valid_library_subdomains) -> Callable:
    # TODO: add in exhaustive set of allowable inputs
    def compose_search_url(
        title: str=None,
        author: str=None,
        anywhere: str=None,
        publisher: str=None,
        formatcode: str='BK OR EBOOK OR AB',
        isolanguage: str=None,
    ) -> pd.DataFrame:
        # TODO: make this more flexible to allow OR
        # TODO: do away with this terrible 'locals' hack
        kwargs = {k:v for k,v in locals().items() if k != 'library_subdomain'}
        search_string = ' AND '.join(f'{k}:({v})' for k, v in kwargs.items() if v)
        quoted_search_string = quote_plus(search_string)

        search_url = f"https://{library_subdomain}.bibliocommons.com/v2/search?query={quoted_search_string}&searchType=bl"
        return search_url

    return compose_search_url

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


def get_available_formats(parsed_data: pd.DataFrame) -> str:
    return (parsed_data
        .groupby('true_format')
        .first()
        .reset_index()
        .assign(description = lambda t: '[' + t.true_format + '] ' + t.title + ': ' + t.subtitle + ' (' + t.author + ')')
        .description
        .pipe('\n'.join)
    )
    #return data.true_format.drop_duplicates().to_list()


compose_search_url_epl = generate_compose_search_url_function('epl')
compose_search_url_calgary = generate_compose_search_url_function('calgary')
