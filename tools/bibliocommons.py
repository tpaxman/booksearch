from functools import partial
from bs4 import BeautifulSoup
import requests
from urllib.parse import quote_plus
import pandas as pd
from typing import Callable

# TODO: add in the exhaustive set
VALID_SEARCH_TERMS = [
    'author',
    'title',
    'anywhere',
    'publisher',
    'formatcode',
    'isolanguage',
]

# TODO: make this exhaustive
# VALID_FORMATCODES = ['BK', 'AB', 'EBOOK']


def generate_bibliocommons_search_engine(library_subdomain: str) -> Callable:

    def run_bibliocommons_search(**kwargs) -> pd.DataFrame:

        invalid_args = set(kwargs) - set(VALID_SEARCH_TERMS)
        assert not invalid_args, f"invalid parameters: {invalid_args}"

        # TODO: make this more flexible to allow OR
        search_string = ' AND '.join(f'{k}:({v})' for k, v in kwargs.items())
        quoted_search_string = quote_plus(search_string)

        search_url = f"https://{library_subdomain}.bibliocommons.com/v2/search?query={quoted_search_string}&searchType=bl"

        response = requests.get(search_url)
        search_results_html = response.content
        soup = BeautifulSoup(search_results_html, features='html.parser')

        result_items = soup.find('ul', class_='results').find_all('li', class_='cp-search-result-item')

        get_text = lambda elem: elem.getText() if elem else ''

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

        return data

    return run_bibliocommons_search


search_epl = generate_bibliocommons_search_engine('epl')
search_cpl = generate_bibliocommons_search_engine('calgary')


