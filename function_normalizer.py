from functools import reduce, partial
from typing import Callable, Literal, get_args
import pandas as pd
import tools.abebooks as abebooks
import tools.annas_archive as annas_archive
import tools.bibliocommons as bibliocommons
import tools.goodreads as goodreads
# import tools.google_books as google


SOURCES = Literal[
    'abebooks', 
    'annas_archive',
    'calgary',
    'epl',
    'goodreads',
    # 'google_books'
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('input_csv')
    parser.add_argument('output_csv')

    args = parser.parse_args()
    source = args.source
    input_csv = args.input_csv
    output_csv = args.output_csv

    assert source in get_args(SOURCES), f"{source} is not a valid source"
    df_inputs = pd.read_csv(input_csv).fillna('')

    results = []
    for author, title in df_inputs[['author', 'title']].values:
        time.sleep(1)
        result_set = parse_results(source=source, author=author, title=title)
        results.append(result_set)

    df_results = pd.concat(results)
    df_results.to_csv(output_csv, index=False)


def compose_search_url(source: SOURCES, title: str=None, author: str=None, keywords: str=None, language: str=None, publisher: str=None) -> str:
    # TODO: ensure language is consistent across functions
    return {
        'abebooks': abebooks.compose_search_url(title=title, author=author, keywords=keywords, publisher=publisher),
        'annas_archive': annas_archive.compose_search_url(query=' '.join(filter(bool, (title, author, keywords))), language=language),
        'calgary': bibliocommons.generate_compose_search_url_function('calgary')(title=title, author=author, publisher=publisher),
        'epl': bibliocommons.generate_compose_search_url_function('epl')(title=title, author=author, publisher=publisher, isolanguage=language),
        'goodreads': goodreads.compose_search_url(query = ' '.join(filter(bool, (title, author, keywords)))),
        # 'google_books': google.compose_search_url(q= ' '.join(filter(bool, (title, author, keywords))), langRestrict=language),
    }.get(source)


def parse_results(source: SOURCES, content: bytes) -> pd.DataFrame:
    return {
        'abebooks': abebooks.parse_results,
        'annas_archive': annas_archive.parse_results,
        'epl': bibliocommons.parse_results,
        'calgary': bibliocommons.parse_results,
        'goodreads': goodreads.parse_results,
        # 'google_books': google.parse_results,
    }.get(source)(content)


def search(source: SOURCES, **kwargs) -> pd.DataFrame:
    compose = partial(compose_search_url, source=source)
    parse = partial(parse_results, source=source)
    url = compose(**kwargs)
    content = requests.get(url).content
    results = parse(content=content)
    return results


if __name__ == '__main__':
    main()


# def _generate_compose_function(source: SOURCES) -> Callable:
#     def compose_search_url(title: str=None, author: str=None, keywords: str=None, language: str=None, publisher: str=None):
#         # TODO: ensure language is consistent across functions
#         return {
#             'abebooks': abebooks.compose_search_url(title=title, author=author, keywords=keywords, publisher=publisher),
#             'annas_archive': annas_archive.compose_search_url(query=' '.join(filter(bool, (title, author, keywords))), language=language),
#             'calgary': bibliocommons.generate_compose_search_url_function('calgary')(title=title, author=author, publisher=publisher),
#             'epl': bibliocommons.generate_compose_search_url_function('epl')(title=title, author=author, publisher=publisher, isolanguage=language),
#             'goodreads': goodreads.compose_search_url(query = ' '.join(filter(bool, (title, author, keywords)))),
#             # 'google_books': google.compose_search_url(q= ' '.join(filter(bool, (title, author, keywords))), langRestrict=language),
#         }.get(source)
#     return compose_search_url

# def _generate_parse_function(source: SOURCES) -> Callable:
#     def parse_results(content: bytes) -> pd.DataFrame:
#         return {
#             'abebooks': abebooks.parse_results,
#             'annas_archive': annas_archive.parse_results,
#             'calgary': bibliocommons.parse_results,
#             'epl': bibliocommons.parse_results,
#             'goodreads': goodreads.parse_results,
#             # 'google_books': google.parse_results,
#         }.get(source)
#     return parse_results



# functions = {x: {'composer': _generate_compose_function(x), 'parser': _generate_parse_function(x)} for x in get_args(SOURCES)}
# _pipe = lambda *functions: lambda seed: reduce(lambda x,f: f(x), functions, seed)
# _pipe_kwargs = lambda *functions: lambda **kwargs: reduce(lambda x,f: f(x), functions, **kwargs)
# 
# _generate_tabulator = lambda source: _pipe_kwargs(
#     _generate_compose_function(source),
#     lambda url: requests.get(url).content,
#     _generate_parse_function(source)
# )

"""
# abebooks
    title: str=None,
    author: str=None,
    keywords: str=None,
    binding: Literal['any', 'hardcover', 'softcover']='any',
    condition: Literal['any', 'new', 'used']='used',
    publisher: str=None,
    signed: ON_OFF_TYPE=None,
    product_type: Literal['book', 'art', 'comic', 'mag', 'ms', 'map', 'photo', 'music']='book',
    isbn: str=None,
    recentlyadded: Literal['all', '2day', '3day', '21day']='all',
    region: Literal['na', 'er']=None, # TODO: add more
    country: Literal['ca', 'us']=None, # TODO: add more
    price_low: float=None,
    price_high: float=None,
    year_low: int=None,
    year_high: int=None,
    sortby: str='total-price',
    num_results: int=100,
    first_edition: ON_OFF_TYPE=None,
    dust_jacket: ON_OFF_TYPE=None,
    rollup: ON_OFF_TYPE=None,
    boolean_search: ON_OFF_TYPE='off',
    not_print_on_demand: ON_OFF_TYPE='off',
    expand_descriptions: ON_OFF_TYPE='off',
    sellers: list=None,

# annas
    query: str,
    filetype: FILETYPES=None,
    language: VALID_LANGUAGES=None,
    content_type: CONTENT_TYPES="book_any",
    sortby: SORT_OPTIONS=None,

# goodreads
    query: str

# bibliocommons
title: str=None,
author: str=None,
anywhere: str=None,
publisher: str=None,
formatcode: str='BK OR EBOOK OR AB',
isolanguage: str=None,


# google_books
# form_search_url
q: str,
maxResults: int=40,
langRestrict: str=None,
orderBy: Literal["relevance", "newest"]=None,
printType: Literal["all", "books", "magazines"]=None,
projection: Literal["full", "lite"]=None,
startIndex: int=None,
volumeId: str=None,
"""
