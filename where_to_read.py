"""
Get info about want to read books - where to read them
"""
import pandas as pd
import tools.bibliocommons as bib
import tools.abebooks as abe
import tools.annas_archive as ann
import time

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


# TODO: handle the Jr. surname error (instead of Vonnegut Jr. for example)

to_read = (df_goodreads
    # remove the extra title data and subtitles
    .assign(short_title = lambda t: t.Title.str.replace('\s+\(.*?\)', '', regex=True).str.replace(': .*', '', regex=True))
    # get just author surname
    .assign(author_surname = lambda t: t['Author l-f'].str.replace(',.*', '', regex=True))
    .merge(shelves_data, how='left', on='Book Id')
    .fillna({'own': False, 'own-epub': False, 'to-read': False})
    .loc[lambda t: t['to-read']]
)

def get_epl_formats(title, author):
    time.sleep(1)
    search_url = bib.generate_bibliocommons_search_url_composer('epl')(title=title, author=author)
    results_html = bib.get_search_results_html(search_url)
    #parsed_data = bib.parse_bibliocommons_search_results(results_html, title_refilter=title, author_refilter=author)
    parsed_data = bib.parse_bibliocommons_search_results(results_html)

    if not parsed_data.empty:
        available_formats = bib.get_available_formats(parsed_data)
    else:
        available_formats = []
    if available_formats:
        print('')
        print(title, author)
        print(available_formats)
        print('')
    else:
        print(title, author)
    return available_formats


df_results = (to_read
    .assign(epl_formats = lambda t: t.apply(lambda r: get_epl_formats(title=r.short_title, author=r.author_surname), axis=1))
)

