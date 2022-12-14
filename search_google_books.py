import re
import requests
import pandas as pd


def search_google_books(query, langRestrict="en") -> pd.DataFrame:
    """ run a search on the Google Books API and return a clean table of results """
    return _clean_search_results(_submit_search_query(query, langRestrict))


def _submit_search_query(query, langRestrict="en") -> pd.DataFrame:
    """ run a search on the Google Books API and return raw results """
    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&langRestrict={langRestrict}"
    data = requests.get(url).json()
    volumes = (x.get('volumeInfo') for x in data['items'])
    return (pd
        .DataFrame(volumes)
        .query('language == @langRestrict') # filter again because Google API doesn't do it properly for some reason
    )


def _clean_search_results(results: pd.DataFrame) -> pd.DataFrame:
    """ clean the raw search results returned by the Google Books API """

    isbns = (results
        .industryIdentifiers
        .explode()
        .apply(pd.Series)
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
            publishedDate = lambda t: t.publishedDate.astype('string'), # ensure dates are strings
            author_primary = lambda t: t.authors.str[0],
            published_year = lambda t: t.publishedDate.apply(_get_published_year).astype('int'),
        )
        .rename(columns={
            "pageCount": "page_count",
            "ratingsCount": "ratings_count",
            "averageRating": "average_rating",
            "canonicalVolumeLink": "volume_link",
        })
        .loc[:, ['title', 'author_primary', 'published_year', 'authors', 'subtitle', 
                 'language', 'isbn_10', 'isbn_13', 'description', 'publisher', 
                 'volume_link', 'small_image_link', 'image_link']
        ]
    )

    return clean_results


def _get_published_year(published_date: str) -> int:
    """ 
    Since "publishedDate" can be either 'yyyy-mm-dd' or  'yyyy-mm' or 'yyyy' parsing is required. 
    The year is the only value of real interest anyway.
    """
    assert isinstance(published_date, str), "published_date must be a string"

    year_match = re.search(r'^\d{4}', published_date) if published_date else None
    year = int(year_match.group()) if year_match else None
    return year
