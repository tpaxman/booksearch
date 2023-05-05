import argparse
import requests
import tabulate
import tools.bibliocommons as biblio
import tools.abebooks as abe
import tools.annas_archive as annas
import tools.goodreads as goodreads
import numpy as np
import pandas as pd

from forex_python.converter import CurrencyRates
USD_TO_CAD_FACTOR = CurrencyRates().get_rate('USD', 'CAD')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t', nargs='+', default=[])
    parser.add_argument('--author', '-a')
    parser.add_argument('--max_num_results', '-n', type=int)
    #parser.add_argument('--sources', '-s', nargs='*')
    args = parser.parse_args()

    title_joined = ' '.join(args.title)
    query = ' '.join(x for x in (title_joined, args.author) if x)

    search_url_goodreads = goodreads.compose_search_url(query = query)
    search_url_abebooks = abe.compose_search_url(title=title_joined, author=args.author)
    search_url_annas_archive = annas.compose_search_url(query=query)
    search_urls_library = [biblio.generate_compose_search_url_function(library)(title=title_joined, author=args.author) 
                   for library in ('epl', 'calgary')]

    formatted_tables = {
        "goodreads": format_results_goodreads(search_url_goodreads),
        "abebooks": format_results_abebooks(search_url_abebooks),
        "library": format_results_bibliocommons(search_urls_library),
        "anna's archive": format_results_annas_archive(search_url_annas_archive),
    }

    for name, df in formatted_tables.items():
        if not df.empty:
            print(f"\n\n{name.upper()}:\n")
            print_table(df.head(args.max_num_results))

    print('\n')


def print_table(df: pd.DataFrame) -> None:
    print(tabulate.tabulate(df, showindex=False, headers=df.columns))


def format_results_goodreads(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = goodreads.parse_results(content)
    df_formatted = (df_results
        .drop(columns='link')
        .assign(
            title = lambda t: t['title'].str[:20],
            author = lambda t: t['author'].str[:30],
        )
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

    df_formatted = (
        df_results
        .assign(
            title = lambda t: t['title'].str[:20],
            author = lambda t: t['author'].str[:30], 
            filesize_mb = lambda t: t['filesize_mb'].astype('int'),
            publisher = lambda t: t['publisher'].str[:50]
        )
        [['title', 'author', 'filetype', 'filesize_mb', 'language', 'publisher']]
        .rename(columns={
            'title': 'Title',
            'author': 'Author',
            'filetype': 'Type',
            'filesize_mb': 'Size (MB)',
            'language': 'Language',
            'publisher': 'Publisher',
        })
    )

    return df_formatted



def format_results_bibliocommons(search_urls: list) -> None:
    results_tables = []
    for search_url in search_urls:
        content = requests.get(search_url).content
        df_results = biblio.parse_results(content)

        if df_results.empty:
            return df_results

        df_formatted = (
            df_results
            .assign(
                title = lambda t: t['title'].str[:20],
                author = lambda t: t['author'].str[:15], 
            )
            .assign(library = biblio.extract_library_subdomain(search_url))
            [['library', 'title', 'author', 'true_format', 'hold_counts']]
            .rename(columns={
                'library': 'Library',
                'title': 'Title',
                'author': 'Author',
                'true_format': 'Format',
                'hold_counts': 'Holds',
                'eresource_link': 'e-Resource',
            })
        )

        results_tables.append(df_formatted)
    
    df_all_results = pd.concat(results_tables)
    return df_all_results



def format_results_abebooks(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = abe.parse_results(content)

    if df_results.empty:
        return df_results

    df_formatted = (
        df_results
        .convert_dtypes()
        .fillna({
            'price_usd': 0,
            'shipping_cost_usd': 0
        })
        .assign(
            price_cad = lambda t: t.price_usd.multiply(USD_TO_CAD_FACTOR),
            shipping_cost_cad = lambda t: t.shipping_cost_usd.multiply(USD_TO_CAD_FACTOR),
        )
        .assign(
            in_edmonton = lambda t: t.seller.str.lower().str.contains('edmonton'),
            # they always list them in USD but the in-store price is the same value in CAD
            price_cad = lambda t: np.where(t.seller.str.lower().str.contains('edmonton book store'), t.price_usd, t.price_cad),
            shipping_cost_cad = lambda t: np.where(t.in_edmonton, 0.0, t.shipping_cost_cad),
        )
        .assign(
            total_price_cad = lambda t: t.price_cad + t.shipping_cost_cad,
            price_description = lambda t: (
                t.price_cad.astype('int').astype('string')
                + ' + ' + t.shipping_cost_cad.astype('int').astype('string')
                + ' = ' + t.total_price_cad.astype('int').astype('string')
                ),
        )
        .assign(
            title = lambda t: t['title'].str[:20],
            author = lambda t: t['author'].str[:15], 
            binding = lambda t: t['binding'].str.lower(),
            about = lambda t: t['about'].str[:40],
            edition = lambda t: t.edition if 'edition' in t.columns else '',
            seller = lambda t: np.where(t.in_edmonton, '* ' + t.seller, t.seller),
        )
        .sort_values('total_price_cad')
        [['title', 'author', 'price_description', 'binding', 'condition', 'seller', 'edition']]
        .rename(columns={
            'title': 'Title',
            'author': 'Author',
            'price_description': 'Price (CAD)',
            'binding': 'Binding',
            'condition': 'Condition',
            'seller': 'Seller',
            'edition': 'Edition',
        })
    )

    return df_formatted



if __name__ == '__main__':
    main()
