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
    args = parser.parse_args()

    input_file = args.input_file
    source = args.source

    searcher = api.quick_search(source)

    need = (
        pd.read_csv(input_file)
        .pipe(lambda t: clean_library_export(t, expand_shelves=True))
        .query('`to-read` and not own and not `own-epub`')
        [['author_surname', 'title']]
        .rename(columns={'author_surname': 'author'})
    )

    total_items = need.shape[1]

    for author, title in need[['author', 'title']].values:
        time.sleep(1)
        save_location = f'output/{source} - {author} - {title}.pkl'.lower()
        try:
            result = searcher(author=author, title=title)
            result.to_pickle(save_location)
            print(f'SAVING: {save_location}')
        except:
            print(f'! FAIL: {save_location}')
    

if __name__=='__main__':
    main()
