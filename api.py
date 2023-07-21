import requests
from typing import Literal
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

def compose_search_url(
    source: SOURCE_TYPE,
    author: str=None,
    title: str=None,
    keywords: str=None,
) -> str:
    """Compose search url for a selected source."""
    composer = {
        'abebooks': abebooks.compose_search_url,
        'annas_archive': annas_archive.compose_search_url,
        'goodreads': goodreads.compose_search_url,
        'epl': bibliocommons.generate_compose_search_url('epl'),
        'calgary': bibliocommons.generate_compose_search_url('calgary'),
        'kobo': kobo.compose_search_url,
    }.get(source)

    if source in ('abebooks', 'epl', 'calgary'):
        url = composer(author=author, title=title, keywords=keywords)
    elif source in ('goodreads', 'annas_archive', 'kobo'):
        keywords = ' '.join(filter(bool, (author, title, keywords)))
        url = composer(keywords=keywords)
    else:
        raise ValueError
    return url


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


def quick_search(
    source: SOURCE_TYPE,
    author: str=None,
    title: str=None,
    keywords: str=None,
) -> str:
    """Generate search url, get content, and parse to table."""
    url = compose_search_url(source=source, author=author, title=title, keywords=keywords)
    data = download(url)
    return data

    
