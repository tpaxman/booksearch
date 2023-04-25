import re
import requests
import pandas as pd
from urllib.parse import quote_plus
from typing import Literal, get_args

# DOCUMENTATION: https://developers.google.com/books/docs/v1/using#st_params

def search_google_books(author: str=None, title: str=None, keywords: str=None, lang: str=None) -> pd.DataFrame:
    """ 
    a wrapper for simple searches - sufficient for my purposes 
    """
    query_argument = form_query_argument(keywords=keywords, intitle=title, inauthor=author)
    search_url = form_search_url(q=query_argument, langRestrict=lang)
    results = run_search(search_url)
    clean_results = clean_search_results(results)
    refiltered_results = clean_results.loc[lambda t: 
        (t.title + ' ' + t.subtitle).apply(lambda x: refilter(x, title)) & 
        t.authors.apply(lambda x: refilter(x, author))
    ]
    return refiltered_results


def form_query_argument(
    keywords: str=None,
    intitle: str=None,
    inauthor: str=None,
    inpublisher: str=None,
    subject: str=None,
    isbn: str=None,
    lccn: str=None,
    oclc: str=None,
) -> str:
    """
    Form the argument to be supplied to the 'q' parameter of the search url
    source: https://developers.google.com/books/docs/v1/using#st_params
    """
    kwargs = {k:v for k, v in locals().items() if (k != 'keywords') and v}
    kwargs_formatted = [f'{k}:({v})' for k, v in kwargs.items()]
    query_argument = ' '.join(kwargs_formatted) + (' ' + keywords if keywords else '')
    query_argument_quoted = quote_plus(query_argument)
    return query_argument_quoted


def form_search_url(
    q: str,
    maxResults: int=40,
    langRestrict: str=None,
    orderBy: Literal["relevance", "newest"]=None,
    printType: Literal["all", "books", "magazines"]=None,
    projection: Literal["full", "lite"]=None,
    startIndex: int=None,
    volumeId: str=None,
) -> str:
    """
    Form the search url
    source: https://developers.google.com/books/docs/v1/using#st_params
    """
    kwargs = {k:v for k,v in locals().items() if v}
    kwargs_formatted = [f'{k}={v}' for k, v in kwargs.items()]
    query_argument = '&'.join(kwargs_formatted)
    search_url = f"https://www.googleapis.com/books/v1/volumes?" + query_argument
    return search_url


def run_search(search_url: str) -> pd.DataFrame:
    """ run a search on the Google Books API and return raw results """
    items = (requests
        .get(search_url)
        .json()
        .get('items')
    )
    volumes = pd.DataFrame(x.get('volumeInfo') for x in items)
    return volumes


def clean_search_results(results: pd.DataFrame) -> pd.DataFrame:
    """ clean the raw search results returned by the Google Books API """

    isbns = (results
        .industryIdentifiers
        .dropna()
        .explode()
        .apply(pd.Series)
        [['type', 'identifier']] # because the previous step may generate an empty column called 0
        .set_index('type', append=True)
        .unstack()['identifier']
        .astype('string')
        .rename(columns=str.lower)
    )

    image_links = (results
        .imageLinks
        .apply(pd.Series)
        .rename(columns={"smallThumbnail": "small_image_link", "thumbnail": "image_link"})
    )
    
    clean_results = (results
        .join(isbns)
        .join(image_links)
        .assign(
            publishedDate = lambda t: t.publishedDate.astype('string').fillna('0'), # ensure dates are strings
            author_primary = lambda t: t.authors.str[0],
            published_year = lambda t: t.publishedDate.apply(get_publication_year).astype('int'),
            authors = lambda t: t.authors.fillna('').apply(lambda x: ';'.join(x) if x else '')
        )
        .rename(columns={
            "pageCount": "page_count",
            "ratingsCount": "ratings_count",
            "averageRating": "average_rating",
            "canonicalVolumeLink": "volume_link",
        })
        .fillna({'published_year': 0})
        .fillna('')
    )

    return clean_results


def get_publication_year(published_date: str) -> int:
    """ 
    Since "publishedDate" can be either 'yyyy-mm-dd' or  'yyyy-mm' or 'yyyy' parsing is required. 
    The year is the only value of real interest anyway.
    """
    assert isinstance(published_date, str), f"published_date must be a string not {type(published_date)}"
    year_match = re.search(r'^\d{4}', published_date) if published_date else None
    year = int(year_match.group()) if year_match else 0
    return year


def refilter(full_string, search_string):
    """ ensure all words in search_string appear in full_string """
    if search_string:
        search_words = re.findall(r'\w+', search_string)
        return all(w.lower() in full_string.lower() for w in search_words)
    else:
        return True
