import pathlib
import argparse
import time
import pandas as pd
import api
from goodreads_cleaner import clean_library_export

# input_file = 'inputs/goodreads_library_export_20230721.csv'
# source = 'abebooks'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('source')
    parser.add_argument('output_folder')
    args = parser.parse_args()

    input_file = args.input_file
    source = args.source
    output_folder = args.output_folder

    searcher = api.quick_search(source)

    df_raw = pd.read_csv(input_file).astype('string').fillna('')
    assert set(df_raw.columns) == {'author', 'title', 'keywords'}, 'wrong columns'

    output_folder_path = pathlib.Path(output_folder)
    output_folder_path.mkdir(parents=True, exist_ok=True)

    total_rows = df_raw.shape[0]

    for i, row in df_raw.to_dict('index').items():

        author = row['author']
        title = row['title']
        keywords = row['keywords']

        save_location = output_folder_path / f'{source} - {author} - {title}.pkl'.lower()
        if not save_location.is_file():
            try:
                result = searcher(author=author, title=title)
                result.to_pickle(save_location)
                prefix = 'SAVING'
                time.sleep(1)
            except:
                prefix='! FAIL'
            print(f'[{i + 1}/{total_rows}] {prefix}: {save_location}')


if __name__=='__main__':
    main()

