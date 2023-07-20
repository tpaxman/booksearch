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

def compose_search_url(source: SOURCE_TYPE, author=None, title=None, keywords=None):
    if source == 'abebooks':
        url = abebooks.compose_search_url(author=author, title=title, keywords=keywords)
    elif source == 'annas_archive':
        keywords = ' '.join(x for x in (author, title, keywords) if x)
        url = annas_archive.compose_search_url(keywords=keywords)
    elif source == 'goodreads':
        keywords = ' '.join(x for x in (author, title, keywords) if x)
        url = goodreads.compose_search_url(keywords=keywords)
    elif source in ('epl', 'calgary'):
        url = bibliocommons.generate_compose_search_url(source)(
            author=author, title=title, keywords=keywords
        )
    elif source == 'kobo':
        keywords = ' '.join(x for x in (author, title, keywords) if x)
        url = kobo.compose_search_url(keywords=keywords)
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

