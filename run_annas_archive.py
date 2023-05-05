import tabulate
import argparse
import requests
import pandas as pd
from tools.annas_archive import compose_annas_archive_search_url, parse_annas_archive_results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('query')
    parser.add_argument('--filetype', '-f')
    parser.add_argument('--language', '-l')
    args = parser.parse_args()
    main_display(args.query, args.filetype, args.language)


def main_display(query, filetype=None, language=None):
    search_url = compose_annas_archive_search_url(
        query=query, 
        filetype=filetype,
        language=language,
    )
    content = requests.get(search_url).content
    df_results = parse_annas_archive_results(content)
    print_results(df_results)


def print_results(df_results: pd.DataFrame) -> pd.DataFrame:

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



if __name__ == '__main__':
    main()
