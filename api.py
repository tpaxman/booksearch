import re
import requests
from typing import Literal, Callable, get_args
from collections import namedtuple
import scrapers.abebooks as abebooks
import scrapers.annas_archive as annas_archive
import scrapers.bibliocommons as bibliocommons
import scrapers.goodreads as goodreads
import scrapers.kobo as kobo

SOURCE_TYPE = Literal[
    'abebooks',
    'annas_archive',
    'calgary',
    'epl',
    'goodreads',
    'kobo',
]

def generate_compose_search_url(source: SOURCE_TYPE) -> Callable:
    """Generate function for composing search url."""
    composer_functions = {
        'abebooks': abebooks.compose_search_url,
        'annas_archive': annas_archive.compose_search_url,
        'goodreads': goodreads.compose_search_url,
        'epl': bibliocommons.generate_compose_search_url('epl'),
        'calgary': bibliocommons.generate_compose_search_url('calgary'),
        'kobo': kobo.compose_search_url,
    }
    assert set(composer_functions) == set(get_args(SOURCE_TYPE)), (
        'inconsistent set of source types listed here'
    )
    composer = composer_functions.get(source)

    def compose_search_url(author: str=None, title: str=None, keywords: str=None) -> str:
        """Compose search url."""
        if source in ('abebooks', 'epl', 'calgary'):
            url = composer(author=author, title=title, keywords=keywords)
        elif source in ('goodreads', 'annas_archive', 'kobo'):
            keywords = ' '.join(filter(bool, (author, title, keywords)))
            url = composer(keywords=keywords)
        else:
            raise ValueError('unknown "source" value {source}')
        return url

    return compose_search_url


def download(url):
    if url.startswith("https://www.abebooks.com"):
        parse_results = abebooks.parse_results
    elif url.startswith("https://annas-archive.org"):
        parse_results = annas_archive.parse_results
    elif url.startswith("https://www.goodreads.com"):
        parse_results = goodreads.parse_results
    elif ".bibliocommons.com" in url:
        parse_results = bibliocommons.parse_results
    elif url.startswith("https://www.kobo.com"):
        parse_results = kobo.parse_results
    else:
        raise ValueError

    content = requests.get(url).content
    data = parse_results(content)
    return data


def _generate_is_subset(search_string: str) -> Callable:
    def _is_subset(main_string: str) -> bool:
        main_string_parts = set(re.findall(r'\w+', main_string.lower() if main_string else ''))
        search_string_parts = set(re.findall(r'\w+', search_string.lower() if search_string else ''))
        return search_string_parts.issubset(main_string_parts)
    return _is_subset


def quick_search(source: SOURCE_TYPE, refilter: bool=True) -> Callable:
    def searcher(author: str=None, title: str=None, keywords: str=None) -> str:
        """Generate search url, get content, and parse to table."""
        compose_search_url = generate_compose_search_url(source=source)
        url = compose_search_url(author=author, title=title, keywords=keywords)
        data = download(url)
        if refilter and source in ('goodreads', 'annas_archive', 'kobo'):
        #if refilter:
            data = data.loc[
                lambda t:
                t['title'].apply(_generate_is_subset(title)) & 
                t['author'].apply(_generate_is_subset(author))
            ].reset_index(drop=True)
        return data
    return searcher


def _quick_search_all(author: str=None, title: str=None, keywords: str=None) -> dict:
    """Search all the sources."""
    return {
        source: quick_search(source)(author=author, title=title, keywords=keywords)
        for source in get_args(SOURCE_TYPE)
    }

        


