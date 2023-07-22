import argparse
import pathlib
import re
from functools import reduce
import pandas as pd

pipe = lambda *functions: lambda seed: reduce(lambda x,f: f(x), functions, seed)

COLUMN_MAPPINGS = {
    'Book Id': 'goodreads_id',
    'Title': 'title',
    'Author': ' author',
    'Author l-f': 'author_lf',
    'Additional Authors': 'additional_authors',
    'ISBN': 'isbn',
    'ISBN13': 'isbn13',
    'My Rating': 'my_rating',
    'Average Rating': 'rating_avg',
    'Publisher': 'publisher',
    'Binding': 'binding',
    'Number of Pages': 'num_pages',
    'Year Published': 'publication_year',
    'Original Publication Year': 'publication_year_original',
    'Date Read': 'date_read',
    'Date Added': 'date_added',
    'Bookshelves': 'bookshelves',
    'Bookshelves with positions': 'bookshelves_ordered',
    'Exclusive Shelf': 'exclusive_bookshelf',
    'My Review': 'my_review',
    'Spoiler': 'spoiler',
    'Private Notes': 'private_notes',
    'Read Count': 'read_count',
    'Owned Copies': 'owned_copies',
}

USED_COLUMNS = [
    'goodreads_id',
    'title',
    'author_lf',
    'rating_avg',
    'num_pages',
    'publisher',
    'publication_year_original',
    'bookshelves', 
    'exclusive_bookshelf',
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('goodreads_library_export_raw_csv')
    parser.add_argument('--output-file', '-o')
    parser.add_argument('--expand-shelves', '-e', action='store_true')
    parser.add_argument('--expand-shelves', '-e', action='store_true')
    args = parser.parse_args()

    goodreads_library_export_raw_csv = args.goodreads_library_export_raw_csv
    output_file = args.output_file
    expand_shelves = args.expand_shelves

    df_raw = pd.read_csv(goodreads_library_export_raw_csv)
    df_clean = clean_library_export(df_raw, expand_shelves=expand_shelves)

    if not output_file:
        input_path = pathlib.Path(goodreads_library_export_raw_csv)
        output_file = (input_path.parent / (input_path.stem + '_clean.csv'))
        
    df_clean.to_csv(output_file, index=False)


def clean_library_export(
    goodreads_library: pd.DataFrame,
    expand_shelves: bool=False,
    filter_to_needed: bool=False,
) -> pd.DataFrame:
    """
    clean the Goodreads Library Export data file
    """

    # functions for cleaning author name
    remove_junior_senior = lambda x: re.sub(r'(Jr|Sr)\., (.*?)\s(\w+)$', r'\3, \2', x)
    remove_other_names = lambda x: re.sub(r',.*', '', x)
    extract_author_surname = pipe(remove_junior_senior, remove_other_names)
    remove_author_punctuation = lambda author: author.replace('.', '').replace(',', '')

    # functions for cleaning title
    remove_series_info = lambda x: re.sub(r'\s+\(.*?\)', '', x)
    remove_subtitle = lambda x: re.sub(': .*', '', x)
    extract_plain_title = pipe(remove_series_info, remove_subtitle)

    goodreads_library_clean = (
        goodreads_library
        .rename(columns=COLUMN_MAPPINGS)
        .loc[:, list(USED_COLUMNS)]
        .rename(columns={'title': 'title_raw'})
        .assign(
            title = lambda t: t['title_raw'].apply(extract_plain_title),
            author_surname = lambda t: t['author_lf'].apply(extract_author_surname),
            author_unpunctuated = lambda t: t['author_lf'].apply(remove_author_punctuation)
        )
    )

    data = goodreads_library_clean
    shelf_dummies = _get_shelf_dummies(goodreads_library_clean)

    if filter_to_needed:
        needed = shelf_dummies.query('`to-read` and not own and not `own-epub`')[['goodreads_id']]
        data = data.merge(needed, how='inner', on='goodreads_id')

    if expand_shelves:
        data = data.merge(shelf_dummies, how='left', on='goodreads_id')

    return data


def _get_shelf_dummies(goodreads_library_clean: pd.DataFrame, normalize_shelf_names: bool=False) -> pd.DataFrame:
    shelf_dummies = (
        goodreads_library_clean
            .set_index('goodreads_id')
            .melt(
                value_vars=['bookshelves', 'exclusive_bookshelf'],
                value_name='bookshelf',
                ignore_index=False
            )
            ['bookshelf']
            .dropna()
            .str.split(r'\s*,\s*', regex=True)
            .explode()
            .reset_index()
            .drop_duplicates()
            .assign(dummy=1)
            .set_index(['goodreads_id', 'bookshelf'])['dummy']
            .unstack()
            .fillna(0)
            .astype('bool')
            .reset_index()
    )
    if normalize_shelf_names:
        shelf_dummies = shelf_dummies.rename(columns=lambda x: x.replace('-', '_'))

    return shelf_dummies


def get_owned_books(goodreads_library_clean: pd.DataFrame) -> pd.DataFrame:
    return (
        goodreads_library_clean
        .pipe(lambda t: t.merge(t.pipe(_get_shelf_dummies), on='goodreads_id'))
        .rename(columns={'author_surname': 'author_search', 'title': 'title_search'})
        .query('own')
        [['goodreads_id', 'author_search', 'title_search']]
    )


if __name__=='__main__':
    main()
