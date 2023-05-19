import argparse
import requests
from typing import Callable
import pandas as pd
import numpy as np
import tools.abebooks as abe
import tools.annas_archive as ann
import tools.bibliocommons as bib
import tools.goodreads as good
import time

VALID_SOURCES = ['abebooks', 'annas_archive', 'epl', 'calgary', 'goodreads']

FUNCTIONS = {
    'abebooks': {
        'composer': abe.compose_search_url, 
        'parser': abe.parse_results
    },
    'epl': {
        'composer': bib.generate_compose_search_url_function('epl'),
        'parser': bib.parse_results
    },
    'calgary': {
        'composer': bib.generate_compose_search_url_function('calgary'),
        'parser': bib.parse_results,
    },
    'goodreads': {
        'composer': good.compose_search_url,
        'parser': good.parse_results,
    },
    'annas_archive': {
        'composer': ann.compose_search_url,
        'parser': ann.parse_results,
    }
}

VALID_SOURCES = list(FUNCTIONS)

PARSERS = {
    'abebooks': abe.parse_results,
    'epl': bib.parse_results,
    'calgary': bib.parse_results,
    'goodreads': good.parse_results,
    'annas_archive': ann.parse_results,
}


PARSERS = {
    'abebooks': abe.parse_results,
    'epl': bib.parse_results,
    'calgary': bib.parse_results,
    'goodreads': good.parse_results,
    'annas_archive': ann.parse_results,
}




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('input_textfile')
    parser.add_argument('output_csvfile')
    parser.add_argument('--separator', '--sep', '-s', default=';')
    args = parser.parse_args()
    source = args.source
    input_textfile = args.input_textfile
    output_csvfile = args.output_csvfile
    separator = args.separator

    assert source in VALID_SOURCES, f"{source} is not a valid source"

    with open(input_textfile, encoding='utf-8') as f:
        raw_inputs = f.read()

    input_pairs = [x.strip().split(separator) for x in raw_inputs.strip().split('\n')]
    inputs = pd.DataFrame(input_pairs, columns=['author', 'title']).fillna('')

    parser = PARSERS.get(source)

    results = []
    for author, title in inputs[['author', 'title']].values:
        time.sleep(1)
        if source == 'abebooks':
            url = abe.compose_search_url(title=title, author=author)
        elif source == 'annas_archive':
            url = ann.compose_search_url(title + ' ' + author)
        elif source in ('epl', 'calgary'):
            url = bib.generate_compose_search_url_function(source)(title=title, author=author)
        elif source == 'goodreads':
            url = good.compose_search_url(title + ' ' + author)

        content = requests.get(url).content
        df_raw = parser(content)
        df = df_raw.assign(author_search=author, title_search=title)
        results.append(df)

        if source == 'abebooks':
            min_price = str(int(df.total_price_cad.min() if not df.empty else 0))
            print(min_price, author, title)
        else:
            print(author, title)

    all_results = pd.concat(results)
    all_results.to_csv(output_csvfile, index=False)


if __name__ == '__main__':
    main()
