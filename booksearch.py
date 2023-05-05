import argparse
import requests
import tabulate
import tools.bibliocommons as biblio
import tools.abebooks as abe
import tools.annas_archive as annas

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t')
    parser.add_argument('--author', '-a')
    #parser.add_argument('--sources', '-s', nargs='*')
    args = parser.parse_args()

    print("\nANNA'S ARCHIVE")
    search_url_annas_archive = annas.compose_search_url(query=' '.join(x for x in (args.title, args.author) if x))
    print_results_annas_archive(search_url_annas_archive)

    print("\nABEBOOKS")
    search_url_abebooks = abe.compose_search_url(title=args.title, author=args.author)
    print_results_abebooks(search_url_abebooks)

    print("\nABEBOOKS (EDMONTON)")
    search_url_abebooks_edmonton = abe.compose_search_url_edmonton(title=args.title, author=args.author)
    print_results_abebooks(search_url_abebooks_edmonton)

    print("\nEPL")
    search_url_epl = biblio.compose_search_url_epl(title=args.title, author=args.author)
    print_results_bibliocommons(search_url_epl)

    print("\nCPL")
    search_url_cpl = biblio.compose_search_url_calgary(title=args.title, author=args.author)
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


def print_results_abebooks(search_url: str) -> None:
    content = requests.get(search_url).content
    df_results = abe.parse_results(content)

    if df_results.empty:
        print('no results')
        return


    df_formatted = (
        df_results
        .assign(
            title = lambda t: t['title'].str[:20],
            author = lambda t: t['author'].str[:15], 
            binding = lambda t: t['binding'].str.lower(),
            price_cad = lambda t: (
                t.price_cad.astype('int').astype('string')
                + ' + ' + t.shipping_cost_cad.astype('int').astype('string')
                + ' = ' + t.total_price_cad.astype('int').astype('string')
            ),
            about = lambda t: t['about'].str[:40],
            edition = lambda t: t.edition if 'edition' in t.columns else '',
        )
        [['title', 'author', 'price_cad', 'binding', 'condition', 'seller', 'edition']]
        .rename(columns={
            'title': 'Title',
            'author': 'Author',
            'price_cad': 'Price (CAD)',
            'binding': 'Binding',
            'condition': 'Condition',
            'seller': 'Seller',
            'edition': 'Edition',
        })
    )

    print(tabulate.tabulate(df_formatted, showindex=False, headers=df_formatted.columns))


if __name__ == '__main__':
    main()
