import argparse
import tabulate
import pandas as pd
import requests
from tools.abebooks import compose_abebooks_search_url, compose_abebooks_edmonton_search_url, parse_abebooks_results_html

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t')
    parser.add_argument('--author', '-a')
    parser.add_argument('--edmonton_only', '-e', action='store_true')
    parser.add_argument('--first_edition', '-f', action='store_const', const='on')
    args = parser.parse_args()
    generate_main_display(args.edmonton_only)(args.title, args.author, args.first_edition)



def generate_main_display(edmonton_only):
    url_compose = compose_abebooks_search_url if not edmonton_only else compose_abebooks_edmonton_search_url
    def main_display(title, author, first_edition=None):
        search_url = url_compose(author=author, title=title, first_edition=first_edition)
        content = requests.get(search_url).content
        df_results = parse_abebooks_results_html(content)
        print_results(df_results)
    return main_display


def print_results(df_results: pd.DataFrame) -> pd.DataFrame:
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
