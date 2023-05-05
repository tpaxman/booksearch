import argparse
import requests
import tabulate
import tools.bibliocommons as biblio
import tools.abebooks as abe
import tools.annas_archive as annas
import numpy as np

from forex_python.converter import CurrencyRates
USD_TO_CAD_FACTOR = CurrencyRates().get_rate('USD', 'CAD')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t', nargs='+')
    parser.add_argument('--author', '-a')
    #parser.add_argument('--sources', '-s', nargs='*')
    args = parser.parse_args()

    title_joined = ' '.join(args.title)

    print("\nABEBOOKS")
    search_url_abebooks = abe.compose_search_url(title=title_joined, author=args.author)
    print_results_abebooks(search_url_abebooks)

    print("\nABEBOOKS (EDMONTON)")
    search_url_abebooks_edmonton = abe.compose_search_url_edmonton(title=title_joined, author=args.author)
    print_results_abebooks(search_url_abebooks_edmonton)

    print("\nANNA'S ARCHIVE")
    search_url_annas_archive = annas.compose_search_url(query=' '.join(x for x in (title_joined, args.author) if x))
    print_results_annas_archive(search_url_annas_archive)

    print("\nEPL")
    search_url_epl = biblio.compose_search_url_epl(title=title_joined, author=args.author)
    print_results_bibliocommons(search_url_epl)

    print("\nCPL")
    search_url_cpl = biblio.compose_search_url_calgary(title=title_joined, author=args.author)
    print_results_bibliocommons(search_url_cpl)


def print_results_annas_archive(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = annas.parse_results(content)


    if df_results.empty:
        print('no results')
        return

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

    print(tabulate.tabulate(df_formatted, showindex=False, headers=df_formatted.columns))


def print_results_bibliocommons(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = biblio.parse_results(content)

    if df_results.empty:
        print('no results')
        return

    df_formatted = (
        df_results
        .assign(
            title = lambda t: t['title'].str[:20],
            author = lambda t: t['author'].str[:15], 
        )
        [['title', 'author', 'true_format', 'hold_counts']]
        .rename(columns={
            'title': 'Title',
            'author': 'Author',
            'true_format': 'Format',
            'hold_counts': 'Holds',
            'eresource_link': 'e-Resource',
        })
    )

    print(tabulate.tabulate(df_formatted, showindex=False, headers=df_formatted.columns))

def checker(df):
    print(df.dtypes)
    return df

def print_results_abebooks(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = abe.parse_results(content)

    if df_results.empty:
        print('no results')
        return

    df_formatted = (
        df_results
        .convert_dtypes()
        .astype({
            'price_usd': 'float',
            'shipping_cost_usd': 'float',
        })
        .fillna({
            'price_usd': 0,
            'shipping_cost_usd': 0
        })
        .assign(
            price_cad = lambda t: t.price_usd.astype('float').multiply(USD_TO_CAD_FACTOR),
            shipping_cost_cad = lambda t: t.shipping_cost_usd.astype('float').multiply(USD_TO_CAD_FACTOR),
        )
        .assign(
            in_edmonton = lambda t: t.seller.str.lower().str.contains('edmonton'),
            # they always list them in USD but the in-store price is the same value in CAD
            price_cad = lambda t: np.where(t.seller.str.lower().str.contains('edmonton book store'), t.price_usd, t.price_cad),
            shipping_cost_cad = lambda t: np.where(t.in_edmonton, 0, t.shipping_cost_cad).astype('float'),
        )
        .assign(
            total_price_cad = lambda t: t.price_cad + t.shipping_cost_cad,
            price_description = lambda t: (
                t.price_cad.fillna(0).round(0).astype('int').astype('string')
                + ' + ' + t.shipping_cost_cad.fillna(0).round(0).astype('int').astype('string')
                + ' = ' + t.total_price_cad.fillna(0).round(0).astype('int').astype('string')
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

    print(tabulate.tabulate(df_formatted, showindex=False, headers=df_formatted.columns))



if __name__ == '__main__':
    main()
