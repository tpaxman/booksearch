import re
import tools.google_books as goo
import pandas as pd

inputs_file = 'inputs/manual_input_all.txt'
inputs_delim = ';'
inputs_order = ['title', 'author']

with open(inputs_file, encoding='utf-8') as f:
    raw_search_strings = f.read().strip().split('\n')

df_searches = (
    pd.DataFrame(
        [[y.strip() for y in x.split(inputs_delim)] for x in raw_search_strings],
        columns=inputs_order
    )
    .fillna('')
)

df_results = (df_searches
    .head()
    .apply(lambda r: goo
        .search_google_books(title=r.title, author=r.author)
        .assign(query = r.title + ' ' + r.author)
        .iloc[0]
        , axis=1
    )
    .loc[:, ['query', 'title', 'subtitle', 'authors', 'author_primary', 'publisher', 'isbn_10', 'isbn_13', 'language', 'published_year']]
    .fillna({'published_date': 0})
    .fillna('')
)
