from typing import get_args, Iterable
import argparse
import pathlib
import time
import pandas as pd
import api

VALID_SOURCES = get_args(api.SOURCE_TYPE)
EXPECTED_COLNAMES = {'author', 'title', 'keywords'}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_file',
        help='input CSV file containing columns: ' + _quote_join(EXPECTED_COLNAMES)
    )
    parser.add_argument(
        'source',
        help='Website to search. One of: ' + _quote_join(VALID_SOURCES)
    )
    parser.add_argument(
        'output_folder',
        help='Folder to save each search result pickle file'
    )

    args = parser.parse_args()
    input_file = args.input_file
    source = args.source
    output_folder = args.output_folder

    assert source in VALID_SOURCES, f'source must be in {VALID_SOURCES}'
    assert input_file.lower().endswith('.csv'), 'input_file must be CSV'

    df_raw = pd.read_csv(input_file)

    assert EXPECTED_COLNAMES.issubset(df_raw.columns), (
        'input_file must be CSV with columns ' + _quote_join(EXPECTED_COLNAMES)
    )

    df_clean = df_raw.astype('string').fillna('')
    total_rows = df_clean.shape[0]

    searcher = api.quick_search(source)

    output_folder = pathlib.Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    for row_index, row_values in df_clean.to_dict('index').items():
        author = row_values['author']
        title = row_values['title']
        keywords = row_values['keywords']

        save_filename = f'{source} - {author} - {title}.pkl'.lower()
        save_filepath = output_folder / save_filename

        if not save_filepath.is_file():
            try:
                result = searcher(author=author, title=title)
                result.to_pickle(save_filepath)
                prefix = 'SAVED'
                time.sleep(1)
            except:
                prefix='FAILED'
            print(f'[{row_index + 1}/{total_rows}] {prefix}: {save_filepath}')


def _quote_join(iterable: Iterable) -> str:
    return ', '.join(f"'{x}'" for x in iterable)


if __name__ == '__main__':
    main()

