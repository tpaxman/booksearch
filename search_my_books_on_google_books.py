import re
import tools.google_books as goo
import pandas as pd

inputs_file = 'inputs/manual_input_all.txt'
inputs_delim = ';'
inputs_order = ['title', 'author']

with open(inputs_file, encoding='utf-8') as f:
    raw_search_strings = f.read().strip().split('\n')

def search(title, author):
    print(title, author)
    return goo.search_google_books(title=title, author=author)

df_searches = (
    pd.DataFrame(
        [[y.strip() for y in x.split(inputs_delim)] for x in raw_search_strings],
        columns=inputs_order
    )
    .fillna('')
)

df_results = (df_searches
    .head(22)
    .assign(results = lambda t: t.apply(lambda r: search(title=r.title, author=r.author).assign(q_title=r.title, q_author=r.author), axis=1))
    .loc[lambda t: t.results.apply(lambda x: not x.empty)]
    .results
    .apply(lambda x: x.iloc[0])
    .loc[:, ['q_title', 'q_author', 'title', 'subtitle', 'authors', 'author_primary', 'publisher', 'isbn_10', 'isbn_13', 'language', 'published_year']]
    .fillna({'published_date': 0})
    .fillna('')
)

