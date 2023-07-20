import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
from typing import Callable
from functools import reduce
import tabulate

# TODO: figure out why goodreads only returns 5 things
# TODO: sort by most ratings maybe? Getting some weird values otherwise

COLUMN_MAPPINGS = {
    'Book Id': 'goodreads_id',
    'Title': 'title',
    'Author': ' author',
    'Author l-f': 'author_lf',
    'Additional Authors': 'additional_authors',
    'ISBN': 'isbn',
    'ISBN13': 'isbn13',
    'My Rating': 'my_rating',
    'Average Rating': 'rating_avg',
    'Publisher': 'publisher',
    'Binding': 'binding',
    'Number of Pages': 'num_pages',
    'Year Published': 'publication_year',
    'Original Publication Year': 'publication_year_original',
    'Date Read': 'date_read',
    'Date Added': 'date_added',
    'Bookshelves': 'bookshelves',
    'Bookshelves with positions': 'bookshelves_ordered',
    'Exclusive Shelf': 'exclusive_bookshelf',
    'My Review': 'my_review',
    'Spoiler': 'spoiler',
    'Private Notes': 'private_notes',
    'Read Count': 'read_count',
    'Owned Copies': 'owned_copies',
}

USED_COLUMNS = [
    'goodreads_id',
    'title',
    'author_lf',
    'rating_avg',
    'num_pages',
    'publisher',
    'publication_year_original',
    'bookshelves', 
    'exclusive_bookshelf',
]

pipe = lambda *functions: lambda seed: reduce(lambda x,f: f(x), functions, seed)

def compose_search_url(keywords: str) -> str:
    quoted_search_string = quote_plus(keywords)
    search_url = f"https://www.goodreads.com/search?utf8=%E2%9C%93&q={quoted_search_string}&search_type=books&search%5Bfield%5D=on"
    return search_url


def parse_results(content: bytes) -> pd.DataFrame:
    soup = BeautifulSoup(content, features='html.parser')

    try:
        result_items = soup.find('table', class_='tableList').find_all('tr')
    except:
        return pd.DataFrame(columns=['title', 'author', 'avg_rating', 'num_ratings', 'link'])


    result_items_data = []
    for x in result_items:
        title = x.find('a', class_='bookTitle')
        author = x.find('a', class_='authorName')
        minirating = x.find('span', class_='minirating')

        avg_rating_raw, num_ratings_raw = re.search(r'(\d\.\d\d) avg rating\D+([\d|,]+) ratings*', _get_text(minirating)).groups()
        avg_rating = float(avg_rating_raw)
        num_ratings = int(num_ratings_raw.replace(',', ''))

        title_text = _get_text(title.find('span', role='heading')) if title else ''

        result_items_data.append({
            "title": title_text,
            "author": _get_text(author),
            "avg_rating": avg_rating,
            "num_ratings": num_ratings,
            "link": "https://www.goodreads.com/" + (title['href'] if title else ''),
        })

    data = (
        pd.DataFrame(result_items_data)
        .sort_values('num_ratings', ascending=False)
    )

    # TODO: figure out how to implement this re-filtering stuff
    # def string_contains_all_words(string: str, words: str) -> bool:
    #     words_clean = re.findall(r'\w+', words)
    #     return all(w.strip().lower() in string.lower() for w in words_clean)
    # data = (data
    #     .loc[lambda t: t.title.apply(lambda x: string_contains_all_words(x, title))]
    #     .loc[lambda t: t.author.apply(lambda x: string_contains_all_words(x, author))]
    # )

    return data


def clean_library_export(goodreads_library: pd.DataFrame, expand_shelves: bool=False) -> pd.DataFrame:
    """
    clean the Goodreads Library Export data file
    """

    # functions for cleaning author name
    remove_junior_senior = lambda x: re.sub(r'(Jr|Sr)\., (.*?)\s(\w+)$', r'\3, \2', x)
    remove_other_names = lambda x: re.sub(r',.*', '', x)
    extract_author_surname = pipe(remove_junior_senior, remove_other_names)
    remove_author_punctuation = lambda author: author.replace('.', '').replace(',', '')

    # functions for cleaning title
    remove_series_info = lambda x: re.sub(r'\s+\(.*?\)', '', x)
    remove_subtitle = lambda x: re.sub(': .*', '', x)
    extract_plain_title = pipe(remove_series_info, remove_subtitle)

    goodreads_library_clean = (
        goodreads_library
        .rename(columns=COLUMN_MAPPINGS)
        .loc[:, list(USED_COLUMNS)]
        .rename(columns={'title': 'title_raw'})
        .assign(
            title = lambda t: t['title_raw'].apply(extract_plain_title),
            author_surname = lambda t: t['author_lf'].apply(extract_author_surname),
            author_unpunctuated = lambda t: t['author_lf'].apply(remove_author_punctuation)
        )
    )

    if expand_shelves:
        shelf_dummies = get_shelf_dummies(goodreads_library_clean)
        return goodreads_library_clean.merge(shelf_dummies, how='left', on='goodreads_id')
    else:
        return goodreads_library_clean


def get_shelf_dummies(goodreads_library_clean: pd.DataFrame, normalize_shelf_names: bool=False) -> pd.DataFrame:
    shelf_dummies = (
        goodreads_library_clean
            .set_index('goodreads_id')
            .melt(
                value_vars=['bookshelves', 'exclusive_bookshelf'],
                value_name='bookshelf',
                ignore_index=False
            )
            ['bookshelf']
            .dropna()
            .str.split(r'\s*,\s*', regex=True)
            .explode()
            .reset_index()
            .drop_duplicates()
            .assign(dummy=1)
            .set_index(['goodreads_id', 'bookshelf'])['dummy']
            .unstack()
            .fillna(0)
            .astype('bool')
            .reset_index()
    )
    if normalize_shelf_names:
        shelf_dummies = shelf_dummies.rename(columns=lambda x: x.replace('-', '_'))

    return shelf_dummies


def get_owned_books(goodreads_library_clean: pd.DataFrame) -> pd.DataFrame:
    return (
        goodreads_library_clean
        .pipe(lambda t: t.merge(t.pipe(get_shelf_dummies), on='goodreads_id'))
        .rename(columns={'author_surname': 'author_search', 'title': 'title_search'})
        .query('own')
        [['goodreads_id', 'author_search', 'title_search']]
    )


def _get_text(elem) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText() if elem else ''
