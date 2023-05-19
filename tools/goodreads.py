from bs4 import BeautifulSoup
import requests
from urllib.parse import quote_plus
import pandas as pd
from typing import Callable
import re
from tools.webscraping import get_text

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
    'Exclusive Shelf': 'bookshelf_exclusive',
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
    'bookshelf_exclusive',
]


def compose_search_url(query: str) -> str:
    quoted_search_string = quote_plus(query)
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

        avg_rating_raw, num_ratings_raw = re.search(r'(\d\.\d\d) avg rating\D+([\d|,]+) ratings*', get_text(minirating)).groups()
        avg_rating = float(avg_rating_raw)
        num_ratings = int(num_ratings_raw.replace(',', ''))

        title_text = get_text(title.find('span', role='heading')) if title else ''

        result_items_data.append({
            "title": title_text,
            "author": get_text(author),
            "avg_rating": avg_rating,
            "num_ratings": num_ratings,
            "link": "https://www.goodreads.com/" + (title['href'] if title else ''),
        })

    data = (
        pd.DataFrame(result_items_data)
        .sort_values('num_ratings', ascending=False)
    )

    # TODO: figure out how to implement this re-filtering stuff
    # data = (data
    #     .loc[lambda t: t.title.apply(lambda x: string_contains_all_words(x, title))]
    #     .loc[lambda t: t.author.apply(lambda x: string_contains_all_words(x, author))]
    # )

    return data


def parse_library_export(goodreads_library: pd.DataFrame) -> pd.DataFrame:
    return (goodreads_library
        .rename(columns=COLUMN_MAPPINGS)
        .loc[:, list(USED_COLUMNS)]
        .rename(columns={'title': 'title_raw'})
        .assign(
            title = lambda t: t['title_raw'].apply(extract_plain_title),
            author_surname = lambda t: t['author_lf'].apply(extract_author_surname),
            author_unpunctuated = lambda t: t['author_lf'].apply(remove_punctuation),
        )
    )

def get_shelf_dummies(goodreads_library: pd.DataFrame) -> pd.DataFrame:
    return (goodreads_library
        .set_index('Book Id')
        .melt(
            value_vars=['Bookshelves', 'Exclusive Shelf'], 
            value_name='bookshelf', 
            ignore_index=False
        )
        ['bookshelf']
        .dropna()
        .str.split(r'\s*,\s*', regex=True)
        .explode()
        .reset_index()
        .drop_duplicates()
        .set_index('Book Id')['bookshelf']
        .pipe(pd.get_dummies)
        .astype('bool')
        .reset_index()
        )


def remove_punctuation(title: str) -> str:
    return title.replace('.', '').replace(',', '')


def string_contains_all_words(string: str, words: str) -> bool:
    words_clean = re.findall(r'\w+', words)
    return all(w.strip().lower() in string.lower() for w in words_clean)



def extract_author_surname(goodreads_author_lf: str) -> str:
    remove_junior_senior = lambda x: re.sub(r'(Jr|Sr)\., (.*?)\s(\w+)$', r'\3, \2', x)
    remove_other_names = lambda x: re.sub(r',.*', '', x)
    return remove_other_names(remove_junior_senior(goodreads_author_lf))


def extract_plain_title(goodreads_title: str) -> str:
    remove_series_info = lambda x: re.sub(r'\s+\(.*?\)', '', x)
    remove_subtitle = lambda x: re.sub(': .*', '', x)
    return remove_subtitle(remove_series_info(goodreads_title))


