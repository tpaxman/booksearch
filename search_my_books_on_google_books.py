import re
import tools.google_books as goo
import pandas as pd

inputs_file = 'inputs/manual_input_all.txt'
inputs_delim = ';'
inputs_order = ['title', 'author']

def refilter(full_string, search_string):
    """ ensure all words in search_string appear in full_string """
    if search_string:
        search_words = re.findall(r'\w+', search_string)
        return all(w.lower() in full_string.lower() for w in search_words)
    else:
        return True

with open(inputs_file, encoding='utf-8') as f:
    raw_search_strings = f.read().strip().split('\n')

df_searches = (
    pd.DataFrame(
        [[y.strip() for y in x.split(inputs_delim)] for x in raw_search_strings],
        columns=inputs_order
    )
    .fillna('')
)

def search(title, author):
    query = ' '.join((title, author))
    df_results = goo.search_google_books(query, langRestrict=None)
    df_refiltered = df_results.loc[lambda t: t.title.apply(lambda x: refilter(x, title)) & t.authors.apply(lambda x: refilter(x, author))]
    df_output = df_refiltered.assign(query = query)
    return df_output.iloc[0] if not df_output.empty else pd.Series()

df_results = df_searches.head().apply(lambda r: search(title=r.title, author=r.author), axis=1)



