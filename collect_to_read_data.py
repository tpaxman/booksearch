"""
Get info about want to read books - where to read them
"""
import pandas as pd
import tools.bibliocommons as bib
import tools.abebooks as abe
import tools.annas_archive as ann
import time
import pathlib
import requests
from bs4 import BeautifulSoup

# import goodreads exported data
goodreads_filepath = 'inputs/goodreads_library_export.csv'

df_goodreads = pd.read_csv(goodreads_filepath)

shelves_data = (df_goodreads
    .assign(Bookshelves=lambda t: t.Bookshelves.fillna('').str.split(r'\s*,\s*', regex=True))
    .explode('Bookshelves')
    .rename(columns={'Bookshelves': 'bookshelf'})
    .loc[lambda t: t.bookshelf.isin(['to-read', 'own', 'own-epub'])]
    .set_index(['Book Id', 'bookshelf']).assign(dummy=1)['dummy']
    .unstack()
    .fillna(0)
    .astype(bool)
    .reset_index()
)

to_read = (df_goodreads
    # remove the extra title data and subtitles
    .assign(short_title = lambda t: t.Title.str.replace('\s+\(.*?\)', '', regex=True).str.replace(': .*', '', regex=True))
    # get just author surname
    .assign(author_lf = lambda t: t['Author l-f'].str.replace(r'Jr\., (.*?)\s(\w+)$', r'\2, \1', regex=True))
    .assign(author_surname = lambda t: t['author_lf'].str.replace(',.*', '', regex=True))
    .merge(shelves_data, how='left', on='Book Id')
    .rename(columns={'Book Id': 'goodreads_id'})
    .fillna({'own': False, 'own-epub': False, 'to-read': False})
    .loc[lambda t: t['to-read']]
    [['goodreads_id', 'author_surname', 'short_title']]
)

search_urls = (to_read
    .assign(epl_search_url = lambda t: t.apply(lambda r: bib.generate_bibliocommons_search_url_composer('epl')(title=r.short_title, author=r.author_surname), axis=1))
    .assign(cpl_search_url = lambda t: t.apply(lambda r: bib.generate_bibliocommons_search_url_composer('calgary')(title=r.short_title, author=r.author_surname), axis=1))
    .assign(annas_search_url = lambda t: t.apply(lambda r: ann.compose_annas_archive_search_url(query=r.short_title + ' ' + r.author_surname), axis=1))
    .assign(abebooks_search_url = lambda t: t.apply(lambda r: abe.compose_abebooks_search_url(title=r.short_title, author=r.author_surname), axis=1))
    .assign(abebooks_edmonton_search_url = lambda t: t.apply(lambda r: abe.compose_abebooks_edmonton_search_url(title=r.short_title, author=r.author_surname), axis=1))
    .melt(
        id_vars=['goodreads_id', 'short_title', 'author_surname'],
        value_vars=['epl_search_url', 'cpl_search_url', 'annas_search_url', 'abebooks_search_url', 'abebooks_edmonton_search_url'],
        var_name='source', 
        value_name='search_url'
    )
    .assign(source = lambda t: t.source.str.replace('_search_url', '', regex=False))
    .sort_values(['goodreads_id', 'source'])
)

for goodreads_id, source, search_url, short_title, author_surname in search_urls[['goodreads_id', 'source', 'search_url', 'short_title', 'author_surname']].values:
    print(source, short_title, author_surname)
    response = requests.get(search_url)
    results_html = response.content
    soup = BeautifulSoup(results_html, features='html.parser')
    save_filename = f'outputs/raw_html/{source}_{goodreads_id}.html'
    with open(save_filename, 'w', encoding='utf-8') as f:
        f.write(str(soup))
