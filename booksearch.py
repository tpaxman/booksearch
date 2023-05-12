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
# TODO: split up the CPL and EPL again perhaps
# TODO: create a 'summary string' for each source e.g. 'abe: $10 (soft), $15 (hard), 56 copies. Edmonton: Bookseller ($15)' or something 
import argparse
import requests
from typing import Callable
import tabulate
import tools.bibliocommons as biblio
import tools.abebooks as abe
import tools.annas_archive as annas
import tools.goodreads as goodreads
import numpy as np
import pandas as pd



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t', nargs='+', default=[])
    parser.add_argument('--author', '-a')
    parser.add_argument('--max_num_results', '-n', type=int)
    parser.add_argument('--sources', '-s', nargs='+')
    args = parser.parse_args()

    title_joined = ' '.join(args.title)
    query = ' '.join(x for x in (title_joined, args.author) if x)

    search_url_goodreads = goodreads.compose_search_url(query = query)
    search_url_abebooks = abe.compose_search_url(title=title_joined, author=args.author)
    search_url_annas_archive = annas.compose_search_url(query=query)
    search_urls_library = [biblio.generate_compose_search_url_function(library)(title=title_joined, author=args.author) 
                   for library in ('epl', 'calgary')]

    sources = [
        {
            "source": "goodreads", 
            "parser": format_results_goodreads, 
            "url": search_url_goodreads
        },
        {
            "source": "abebooks", 
            "parser": format_results_abebooks, 
            "url": search_url_abebooks
        },
        {
            "source": "library", 
            "parser": format_results_bibliocommons, 
            "url": search_urls_library
        },
        {
            "source": "annas", 
            "parser": format_results_annas_archive, 
            "url": search_url_annas_archive
        },
    ]
    selected_sources = [x for x in sources if x['source'] in user_sources] if (user_sources := args.sources) else sources
    sources_results = [x | {'df': x['parser'](x['url'])} for x in selected_sources]
    sources_results_notempty = [x for x in sources_results if not x['df'].empty]
    for x in sources_results_notempty:
        source = x['source'].upper()
        url = u if isinstance((u := x['url']), str) else '\n'.join(u)
        df = x['df'].head(args.max_num_results)
        df_str = clip_table(df, 30)
        print('\n' + source + '\n' + url + '\n\n' +  df_str + '\n')


def select_rename(column_mapper: dict) -> Callable:
    return lambda df: df.loc[:, list(column_mapper)].rename(columns=column_mapper)

# def select_rename(column_mapper: dict) -> Callable:
#     def rename(df: pd.DataFrame) -> pd.DataFrame:
#         return df.loc[list(column_mapper)].rename(columns=column_mapper)
#     return rename

def clip_table(df: pd.DataFrame, width: int) -> pd.DataFrame:
    df_clipped = df.assign(**{c: s.astype('string').str[:width] for c, s in df.to_dict(orient='series').items()})
    df_string = tabulate.tabulate(df_clipped, showindex=False, headers=df_clipped.columns)
    return df_string


def format_results_goodreads(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = goodreads.parse_results(content)
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


def format_results_annas_archive(search_url: str) -> pd.DataFrame:
    content = requests.get(search_url).content
    df_results = annas.parse_results(content)

    if df_results.empty:
        return df_results

    column_mapper = {
        'title': 'Title',
        'author': 'Author',
        'filetype': 'Type',
        'filesize_mb': 'Size (MB)',
        'language': 'Language',
        'publisher': 'Publisher',
    }

    df_formatted = select_rename(column_mapper)(df_results)

    return df_formatted


def format_results_bibliocommons(search_urls: list) -> None:
    results_tables = []
    for search_url in search_urls:
        content = requests.get(search_url).content
        df_results = biblio.parse_results(content)

        if df_results.empty:
            return df_results

        column_mapper = {
            'library': 'Library',
            'title': 'Title',
            'author': 'Author',
            'true_format': 'Format',
            'hold_counts': 'Holds',
            'eresource_link': 'e-Resource',
        }

        df_formatted = (df_results
            .assign(library = biblio.extract_library_subdomain(search_url))
            .pipe(select_rename(column_mapper))
        )

        results_tables.append(df_formatted)
    
    df_all_results = pd.concat(results_tables)
    return df_all_results


def format_results_abebooks(search_url: str) -> pd.DataFrame:
    content = requests.get(search_url).content
    df_results = abe.parse_results(content)

    if df_results.empty:
        return df_results

    column_mapper = {
        'title': 'Title',
        'author': 'Author',
        'price_description': 'Price (CAD)',
        'binding': 'Binding',
        'condition': 'Condition',
        'seller': 'Seller',
        'edition': 'Edition',
    }

    df_formatted = (df_results
        .sort_values('total_price_cad')
        .pipe(select_rename(column_mapper))
    )
    return df_formatted


if __name__ == '__main__':
    main()
