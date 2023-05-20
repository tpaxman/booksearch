import time
import requests
import pandas as pd
import tools.goodreads as goodreads
import tools.abebooks as abebooks
from functools import reduce

pipe = lambda *functions: lambda seed: reduce(lambda x,f: f(x), functions, seed)

goodreads_library_export_filepath = 'inputs/goodreads_library_export.csv'
results_export_csv = 'outputs/goodreads_to_read_unowned_edmonton_abebooks_results.csv'

# import goodreads library export
goodreads_library_raw = pd.read_csv(goodreads_library_export_filepath)
goodreads_library = goodreads.clean_library_export(goodreads_library_raw)
shelf_dummies = goodreads.get_shelf_dummies(goodreads_library, normalize_shelf_names=True)

to_read_unowned = (
    goodreads_library
    [['goodreads_id', 'author_surname', 'title']]
    .merge(shelf_dummies, on='goodreads_id')
    .query('to_read and not own')
    [['author_surname', 'title']]
)


all_results = []
for author, title in to_read_unowned[['author_surname', 'title']].values:
    time.sleep(1)
    url = abebooks.compose_search_url_edmonton(author=author, title=title)
    content = requests.get(url).content
    results = abebooks.parse_results(content).assign(author_search=author, title_search=title)
    print(author, title)
    if not results.empty:
        all_results.append(results)
        print(results[['title', 'price_description', 'seller', 'condition', 'binding']])
    
df_all_results = pd.concat(all_results)

df_all_results.to_csv(results_export_csv, index=False)


# get title and author
# create df_inputs
# run the abebooks local check

