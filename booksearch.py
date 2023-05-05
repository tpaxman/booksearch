import argparse
import run_abebooks as abe
import run_annas_archive as ann
import requests
import tabulate
from tools.bibliocommons import generate_bibliocommons_search_url_composer, parse_bibliocommons_search_results

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t')
    parser.add_argument('--author', '-a')
    #parser.add_argument('--sources', '-s', nargs='*')
    args = parser.parse_args()

    print("\nANNA'S ARCHIVE")
    ann.main_display(query=args.title + ' ' + args.author)

    print("\nABEBOOKS")
    abe.generate_main_display(edmonton_only=False)(title=args.title, author=args.author)

    print("\nABEBOOKS (EDMONTON)")
    abe.generate_main_display(edmonton_only=True)(title=args.title, author=args.author)

    print("\nEPL")
    search_url_epl = generate_bibliocommons_search_url_composer('epl')(title=args.title, author=args.author)
    print_results_bibliocommons(search_url_epl)

    print("\nCPL")
    search_url_cpl = generate_bibliocommons_search_url_composer('calgary')(title=args.title, author=args.author)
    print_results_bibliocommons(search_url_cpl)


def print_results_bibliocommons(search_url: str) -> None:
    
    content = requests.get(search_url).content
    df_results = parse_bibliocommons_search_results(content)

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

if __name__ == '__main__':
    main()
