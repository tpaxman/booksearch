# TODO: print url out with each source as well
# TODO: add option to open all results in selenium?
# TODO: add modes: general, summary, batch, values, availability, etc.
# TODO: merge calibre and goodreads data with search results too to get various insights
# TODO: make script to find - "owned ebooks that I haven't flagged in Goodreads"
# TODO: script option - need to download or try to download
# TODO: list of books that have no ebook version
# TODO: ensure abebooks is filtering to used books by default
# TODO: split abebooks sellers into store, city, country
# TODO: add more language details and search optionality
# TODO: create a 'summary string' for each source e.g. 'abe: $10 (soft), $15 (hard), 56 copies. Edmonton: Bookseller ($15)' or something 
# TODO: remove the request part from all the formatting functions

import argparse
import requests
from typing import Callable
import tabulate
import tools.bibliocommons as biblio
from functools import reduce
import tools.abebooks as abe
import tools.annas_archive as annas
import tools.goodreads as goodreads
import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t', nargs='+', default=[])
    parser.add_argument('--author', '-a', nargs='+', default=[])
    parser.add_argument('--max_num_results', '-n', type=int)
    parser.add_argument('--sources', '-s', nargs='+', default=[])
    parser.add_argument('--width', '-w', type=int, default=30)
    args = parser.parse_args()

    AUTHOR = ' '.join(args.author)
    TITLE = ' '.join(args.title)
    QUERY = ' '.join(x for x in (TITLE, AUTHOR) if x)
    MAX_NUM_RESULTS = args.max_num_results
    SOURCES = args.sources
    WIDTH = args.width

    BIBLIOCOMMONS_COLUMN_MAPPER = {
        'title': 'Title',
        'author': 'Author',
        'true_format': 'Format',
        'hold_counts': 'Holds',
        'eresource_link': 'e-Resource',
    }

    SOURCES_DATA = {
        "goodreads": {
            "parser": goodreads.parse_results,
            "url": goodreads.compose_search_url(query=QUERY),
            "column_mapper": {
                "title": "Title",
                "author": "Author",
                "avg_rating": "Rating",
                "num_ratings": "Num Ratings",
            },
        },
        "abebooks": {
            "parser": abe.parse_results,
            "url": abe.compose_search_url(title=TITLE, author=AUTHOR),
            "column_mapper": {
                'title': 'Title',
                'author': 'Author',
                'price_description': 'Price (CAD)',
                'binding': 'Binding',
                'condition': 'Condition',
                'seller': 'Seller',
                'edition': 'Edition',
            }
        },
        "calgary": {
            "parser": biblio.parse_results,
            "url": biblio.generate_compose_search_url_function('calgary')(title=TITLE, author=AUTHOR),
            "column_mapper": BIBLIOCOMMONS_COLUMN_MAPPER,
        },
        "epl": {
            "parser": biblio.parse_results,
            "url": biblio.generate_compose_search_url_function('epl')(title=TITLE, author=AUTHOR),
            "column_mapper": BIBLIOCOMMONS_COLUMN_MAPPER,
        },
        "annas": {
            "parser": annas.parse_results,
            "url": annas.compose_search_url(query=QUERY),
            "column_mapper": {
                'title': 'Title',
                'author': 'Author',
                'filetype': 'Type',
                'filesize_mb': 'Size (MB)',
                'language': 'Language',
                'publisher': 'Publisher',
            }
        },
    }

    printable_results = []
    for k, v in SOURCES_DATA.items():

        if k in SOURCES or not SOURCES:
            url = v['url']
            parser = v['parser']
            column_mapper = v['column_mapper']
            formatter = format_results(column_mapper)
            df = pipe(lambda url: requests.get(url).content, parser, formatter)(url)

            if not df.empty:
                source = k.upper()
                df_str = clip_table(df.head(MAX_NUM_RESULTS), WIDTH)
                printable_results.append('\n' + source + '\n' + url + '\n\n' +  df_str + '\n')


    for x in printable_results:
        print(x)


def format_results(column_mapper: dict) -> Callable:
    def formatter(df_results: pd.DataFrame) -> pd.DataFrame:
        return select_rename(column_mapper)(df_results) if not df_results.empty else pd.DataFrame()
    return formatter


def pipe(*functions):
    def combined_function(seed):
        return reduce(lambda x, f: f(x), functions, seed)
    return combined_function


def select_rename(column_mapper: dict) -> Callable:
    def selector(df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[:, list(column_mapper)].rename(columns=column_mapper)
    return selector


def clip_table(df: pd.DataFrame, width: int) -> pd.DataFrame:
    df_clipped = df.assign(**{c: s.astype('string').str[:width] for c, s in df.to_dict(orient='series').items()})
    df_string = tabulate.tabulate(df_clipped, showindex=False, headers=df_clipped.columns)
    return df_string


if __name__ == '__main__':
    main()
