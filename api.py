import typing
from collections import namedtuple
import scrapers.abebooks as abebooks
import scrapers.annas_archive as annas_archive
import scrapers.bibliocommons as bibliocommons
import scrapers.goodreads as goodreads

SOURCE_TYPE = Literal[
    'abebooks',
    'annas_archive'
    'calgary',
    'epl',
    'goodreads',
]

def compose_search_url(source: SOURCE_TYPE, author=None, title=None, keywords=None):
    if source == 'abebooks':
        url = abebooks.compose_search_url(author=author, title=title, keywords=keywords)
    elif source == 'annas_archive':
        url = annas_archive.compose_search_url(keywords=author + ' ' + title + ' ' + keywords)
    elif source == 'goodreads':
        url = goodreads.compose_search_url(keywords=author + ' ' + title + ' ' + keywords)
    elif source in ('epl', 'calgary'):
        url = bibliocommons.generate_compose_search_url(source)(
            author=author, title=title, keywords=keywords
        )
    else:
        raise ValueError
    return url


def download(url):
    content = requests.get(url).content
    if url.startswith("https://www.abebooks.com"):
        data = abebooks.parse_results(content)
    elif url.startswith("https://annas-archive.org"):
        data = annas_archive.parse_results(content)
    elif url.startswith("https://www.goodreads.com"):
        data = goodreads.parse_results(content)
    elif ".bibliocommons.com" in url:
        data = bibliocommons.parse_results(content)
    else:
        raise ValueError
    return data

