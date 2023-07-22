import argparse
import re
import pandas as pd
from goodreads_cleaner import clean_library_export


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('input_type')
    parser.add_argument('output_file')
    args = parser.parse_args()

    input_file = args.input_file
    input_type = args.input_type
    output_file = args.output_file

    if input_type == 'dotsep':
        assert input_file.lower().endswith('.txt')
        df = from_dotsep_file(input_file)
    elif input_type == 'goodreads':
        assert input_file.lower().endswith('.csv')
        df = from_goodreads_export(input_file)
    else:
        df = pd.DataFrame(columns=['author', 'title', 'keywords'])

    df.to_csv(output_file, index=False)


def from_goodreads_export(csv_filename: str) -> pd.DataFrame:
    return (
        clean_library_export(csv_filename)
        [['author_surname', 'title']]
        .rename(columns={'author_surname': 'author'})
        .reindex(['author', 'title', 'keywords'], axis=1)
        .fillna('')
    )
        

def from_dotsep_file(text_filename: str) -> pd.DataFrame:

    with open(text_filename) as f:
        raw_text = f.read()

    lines = raw_text.strip().split('\n')
    items = [re.split(r'\s*\.\s*', x) for x in lines]
    return (
        pd.DataFrame(items)
        .reindex([0, 1, 2], axis=1)
        .rename({0: 'author', 1: 'title', 2: 'keywords'}, axis=1)
        .convert_dtypes()
        .fillna('')
    )


if __name__ == '__main__':
    main()
