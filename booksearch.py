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

    sources_data = {
        "goodreads": {
            "function": pipe(get_content, goodreads.parse_results, format_results_goodreads),
            "url": goodreads.compose_search_url(query=QUERY),
        },
        "abebooks": {
            "function": pipe(get_content, abe.parse_results, format_results_abebooks),
            "url": abe.compose_search_url(title=TITLE, author=AUTHOR),
        },
        "calgary": {
            "function": pipe(get_content, biblio.parse_results, format_results_bibliocommons),
            "url": biblio.generate_compose_search_url_function('calgary')(title=TITLE, author=AUTHOR),
        },
        "epl": {
            "function": pipe(get_content, biblio.parse_results, format_results_bibliocommons),
            "url": biblio.generate_compose_search_url_function('epl')(title=TITLE, author=AUTHOR),
        },
        "annas": {
            "function": pipe(get_content, annas.parse_results, format_results_annas_archive),
            "url": annas.compose_search_url(query=QUERY),
        },
    }

    printable_results = []
    for k, v in sources_data.items():

        if k in SOURCES or not SOURCES:
            function = v['function']
            url = v['url']
            df = function(url)

            if not df.empty:
                source = k.upper()
                df_str = clip_table(df.head(MAX_NUM_RESULTS), WIDTH)
                printable_results.append('\n' + source + '\n' + url + '\n\n' +  df_str + '\n')

    for x in printable_results:
        print(x)



def get_content(url: str) -> bytes:
    return requests.get(url).content


def pipe(*functions):
    def combined_function(seed):
        return reduce(lambda x, f: f(x), functions, seed)
    return combined_function


def run_pipe(*everything):
    seed = everything[0]
    functions = everything[1:]
    return pipe(*functions)(seed)


def select_rename(column_mapper: dict) -> Callable:
    return lambda df: df.loc[:, list(column_mapper)].rename(columns=column_mapper)


def clip_table(df: pd.DataFrame, width: int) -> pd.DataFrame:
    df_clipped = df.assign(**{c: s.astype('string').str[:width] for c, s in df.to_dict(orient='series').items()})
    df_string = tabulate.tabulate(df_clipped, showindex=False, headers=df_clipped.columns)
    return df_string


def wrap_with_empty_df_return(formatter: Callable) -> Callable:
    def new_formatter(df):
        return df if df.empty else formatter(df)
    return new_formatter


def format_results_goodreads(df_results: pd.DataFrame) -> pd.DataFrame:
    df_formatted = (df_results
        .drop(columns='link')
        .rename(columns={
            "title": "Title",
            "author": "Author",
            "avg_rating": "Rating",
            "num_ratings": "Num Ratings",
        })
    )
    return df_formatted


@wrap_with_empty_df_return
def format_results_annas_archive(df_results: pd.DataFrame) -> pd.DataFrame:
    column_mapper = {
        'title': 'Title',
        'author': 'Author',
        'filetype': 'Type',
        'filesize_mb': 'Size (MB)',
        'language': 'Language',
        'publisher': 'Publisher',
    }
    return df_results.pipe(select_rename(column_mapper))


@wrap_with_empty_df_return
def format_results_bibliocommons(df_results: pd.DataFrame) -> pd.DataFrame:
    column_mapper = {
        'title': 'Title',
        'author': 'Author',
        'true_format': 'Format',
        'hold_counts': 'Holds',
        'eresource_link': 'e-Resource',
    }
    return df_results.pipe(select_rename(column_mapper))


@wrap_with_empty_df_return
def format_results_abebooks(df_results: pd.DataFrame) -> pd.DataFrame:
    column_mapper = {
        'title': 'Title',
        'author': 'Author',
        'price_description': 'Price (CAD)',
        'binding': 'Binding',
        'condition': 'Condition',
        'seller': 'Seller',
        'edition': 'Edition',
    }
    return (df_results
        .sort_values('total_price_cad')
        .pipe(select_rename(column_mapper))
    )


if __name__ == '__main__':
    main()
