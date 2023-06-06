import requests
import tabulate
import pandas as pd
import re
from functools import reduce, partial
import tools.goodreads as goodreads
import tools.bibliocommons as bibliocommons
import tools.annas_archive as annas_archive
import tools.abebooks as abebooks

get = lambda url: requests.get(url).content
pipe = lambda *funcs: lambda seed: reduce(lambda x,f: f(x), funcs, seed)

def _tabulate(data: pd.DataFrame) -> str:
    return tabulate.tabulate(
        data, 
        showindex=False, 
        headers=data.columns, 
        tablefmt=''
    )

FUNCTIONS = {
    'goodreads': {
        'composer': lambda author, title: goodreads.compose_search_url(query=title + ' ' + author),
        'getter': pipe(get, goodreads.parse_results, goodreads.create_oneliner),
    },
    'annas_archive': {
        'composer': lambda author, title: annas_archive.compose_search_url(query=title + ' ' + author),
        'getter': pipe(get, annas_archive.parse_results, annas_archive.create_oneliner),
    },
    'abebooks': {
        'composer': abebooks.compose_search_url,
        'getter': pipe(get, partial(abebooks.parse_results, assume_canada=False), abebooks.create_oneliner_generic),
    },
    'epl': {
        'composer': lambda author, title: bibliocommons.compose_search_url_epl(title=title, author=author),
        'getter': pipe(get, bibliocommons.parse_results, bibliocommons.create_oneliner),
    },
    'calgary': {
        'composer': lambda author, title: bibliocommons.compose_search_url_calgary(title=title, author=author),
        'getter': pipe(get, bibliocommons.parse_results, bibliocommons.create_oneliner),
    },
}

def create_string(author, title):
    strings = []
    for source, definitions in FUNCTIONS.items():
        compose = definitions['composer']
        get_description = definitions['getter']
        url = compose(title=title, author=author)
        description = get_description(url)
        #table = _tabulate(view) if not view.empty else '<no results>'
        if description != '':
            string = source.upper() + ' - ' + description + '\n' + url
            strings.append(string)
    return '\n\n'.join(strings)

def print_off_stuff(raw_inputs: str) -> None:
    pairs = [tuple(y.strip() for y in re.split('\s*(?:\.|\?|\-)\s*', x)) for x in raw_inputs.strip().split('\n') if x]
    for author, title in pairs:
        string = create_string(author, title)
        print('_'*150 + '\n\n# ' + author.upper() + ' - ' + title.upper() + '\n\n' + string + '\n\n')
