import re
import requests
from collections import Counter
import pandas as pd

GOOGLE_BOOKS_VOLUMES_API_ROOT_URL = "https://www.googleapis.com/books/v1/volumes"

def search_google_books(query, langRestrict="en") -> pd.DataFrame:
    return _clean_search_results(_submit_search_query(query, langRestrict))

def _submit_search_query(query, langRestrict="en") -> pd.DataFrame:
    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&langRestrict={langRestrict}"
    data = requests.get(url).json()
    volumes = (x.get('volumeInfo') for x in data['items'])
    return (pd
        .DataFrame(volumes)
        .query('language == @langRestrict') # filter again because Google API doesn't do it properly for some reason
    )


def _clean_search_results(results: pd.DataFrame) -> pd.DataFrame:

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
            published_year = lambda t: t.publishedDate.apply(get_published_year).astype('int'),
        )
        .rename(columns={
            "pageCount": "page_count",
            "ratingsCount": "ratings_count",
            "averageRating": "average_rating",
            "canonicalVolumeLink": "volume_link",
        })
        .loc[:, ['title', 'author_primary', 'published_year', 'authors', 'subtitle', 'language', 'isbn_10', 'isbn_13', 'description', 'publisher', 'volume_link', 'small_image_link', 'image_link']]
    )

    return clean_results


def move_columns_to_front(df: pd.DataFrame, front_columns: list) -> pd.DataFrame:
    """
    Move selected columns to the front (i.e. left side) of a DataFrame

    Example:
    df = pd.DataFrame({"a": [1,2], "b": [3,4], "c": [4,5]})
    df2 = move_columns_to_front(df, ["c", "b"])

    In : df2.columns
    Out: Index(['c', 'b', 'a'], dtype='object')
    """

    # ensure front_columns has no duplicate values
    front_column_duplicates = {k for k,v in Counter(front_columns).items() if v > 1}
    assert not front_column_duplicates, f"front_columns contains {len(front_column_duplicates)} duplicate value(s): {front_column_duplicates}"

    # ensure all front_columns values are valid column names
    invalid_front_columns = set(front_columns) - set(df.columns)
    assert not invalid_front_columns, f"front_columns contains {len(invalid_front_columns)} name(s) that are not in df: {invalid_front_columns}"

    # reorder columns so that front_columns appear first
    back_columns = [x for x in df.columns if x not in front_columns]
    new_columns_order = front_columns + back_columns
    return df[new_columns_order]



def get_published_year(published_date: str) -> int:
    """ 
    Since "publishedDate" can be either 'yyyy-mm-dd' or  'yyyy-mm' or 'yyyy' parsing is required. 
    The year is the only value of real interest anyway.
    """
    assert isinstance(published_date, str), "published_date must be a string"

    year_match = re.search(r'^\d{4}', published_date) if published_date else None
    year = int(year_match.group()) if year_match else None
    return year


def select_as(df: pd.DataFrame, column_mapper: dict) -> pd.DataFrame:
    return df.rename(columns=column_mapper)[column_mapper.values()]

{
    'title': 'title',
    'author_primary': 'author_primary',
    'published_year': 'published_year',
    'pageCount': 'page_count',
    'description': 'description',
    'isbn_10': 'isbn_10',
    'isbn_13': 'isbn_13',
    'small_image_link': 'small_image_link',
    'image_link': 'image_link'
    }


publishedDate
industryIdentifiers
readingModes
printType
categories
maturityRating
allowAnonLogging
contentVersion
panelizationSummary
imageLinks
previewLink
infoLink
