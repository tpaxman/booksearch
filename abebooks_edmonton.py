
import argparse
import requests
from typing import Callable
import tabulate
from functools import reduce
import tools.abebooks as abe
import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t', nargs='+', default=[])
    parser.add_argument('--author', '-a', nargs='+', default=[])
    parser.add_argument('--keywords', '-k', nargs='+', default=[])
    parser.add_argument('--max_num_results', '-n', type=int)
    parser.add_argument('--width', '-w', type=int, default=30)
    parser.add_argument('--table_format', '-f', default='psql')
    parser.add_argument('--output_csv', '-o')
    args = parser.parse_args()

    AUTHOR = ' '.join(args.author)
    TITLE = ' '.join(args.title)
    KEYWORDS = ' '.join(args.keywords)
    MAX_NUM_RESULTS = args.max_num_results
    WIDTH = args.width
    TABLE_FORMAT = args.table_format
    OUTPUT_CSV = args.output_csv

    SOURCES_DATA = {
        "abebooks": {
            "parser": abe.parse_results,
            "url": abe.compose_search_url_edmonton(title=TITLE, author=AUTHOR, keywords=KEYWORDS),
            "column_mapper": {
                'title': 'Title',
                'author': 'Author',
                'total_price_cad': 'Price (CAD)',
                'binding': 'Binding',
                'condition': 'Condition',
                'seller': 'Seller',
                'edition': 'Edition',
            },
        },
    }

    printable_results = []
    for k, v in SOURCES_DATA.items():

        url = v['url']
        parser = v['parser']
        column_mapper = v['column_mapper']
        formatter = format_results(column_mapper)
        df = pipe(lambda url: requests.get(url).content, parser, formatter)(url)

        if not df.empty:
            if OUTPUT_CSV:
                df.to_csv(OUTPUT_CSV, index=False)
                return
            else:
                source = k.upper()
                df_str = clip_table(df.head(MAX_NUM_RESULTS), WIDTH, TABLE_FORMAT)
                printable_results.append('\n' + source + '\n\n' + url + '\n\n' +  df_str + '\n')


    for x in printable_results:
        print(x)


def format_results(column_mapper: dict) -> Callable:
    def formatter(df_results: pd.DataFrame) -> pd.DataFrame:
        if df_results.empty:
            return pd.DataFrame()
        else:
            return (
                df_results
                .assign(seller=lambda t: t.seller.str.replace(', Edmonton, Canada', '', regex=False).str.replace('* ', '', regex=False))
                .assign(total_price_cad=lambda t: t.total_price_cad.astype('int'))
                .pipe(select_rename(column_mapper))
            )
    return formatter


def pipe(*functions):
    def combined_function(seed):
        return reduce(lambda x, f: f(x), functions, seed)
    return combined_function


def select_rename(column_mapper: dict) -> Callable:
    def selector(df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[:, list(column_mapper)].rename(columns=column_mapper)
    return selector


def clip_table(df: pd.DataFrame, width: int, tablefmt: str='simple') -> pd.DataFrame:
    df_clipped = df.assign(**{c: s.astype('string').str[:width] for c, s in df.to_dict(orient='series').items()})
    df_string = tabulate.tabulate(df_clipped, showindex=False, headers=df_clipped.columns, tablefmt=tablefmt)
    return df_string


if __name__ == '__main__':
    main()
