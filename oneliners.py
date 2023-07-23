import argparse
from typing import get_args, Callable
import pandas as pd
import api

pd.options.display.width = 160
pd.options.display.max_columns = 20
pd.options.display.min_rows = 8
pd.options.display.max_rows = 30
pd.options.display.max_colwidth = 25

VALID_SOURCES = get_args(api.SOURCE_TYPE)

EDMONTON_STORE_ALIASES = {
    'Alhambra Books': 'alhambra',
    'Mister-Seekers Books': 'misterseeker',
    'The Bookseller': 'bookseller',
    "The Great Catsby's Rare Books": 'catsby',
    'Edmonton Book Store': 'edmonton'
}

DISPLAY_COLUMNS = {
    'abebooks': ['title', 'author', 'price', 'seller_name', 'seller_city', 'seller_country', 'binding'],
    'epl': ['title', 'author', 'true_format', 'hold_counts'],
    'calgary': ['title', 'author', 'true_format', 'hold_counts'],
    'kobo': ['title', 'author', 'reg_price', 'sale_price'],
    'goodreads': ['title', 'author', 'avg_rating', 'num_ratings'],
    'annas_archive': ['title', 'author', 'filesize_mb', 'language', 'filetype'],
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--author', '-a', nargs='+')
    parser.add_argument('--title', '-t', nargs='+')
    parser.add_argument('--keywords', '-k', nargs='+')
    parser.add_argument('--source', '-s', choices=VALID_SOURCES)
    parser.add_argument('--filter', '-f', nargs='+')
    parser.add_argument('--print-url', '-u', action='store_true')

    args = parser.parse_args()
    author = ' '.join(args.author) if args.author else None
    title = ' '.join(args.title) if args.title else None
    keywords = ' '.join(args.keywords) if args.keywords else None
    source = args.source
    _filter = args.filter
    print_url = args.print_url

    assert not (_filter and not source), 'must specify source-level view to apply filters'
    # TODO: make this flexible for printing all urls instead at smoe point
    assert not (print_url and not source), 'must specify source-level view to do url print'
    
    if source:
        if print_url:
            compose_search_url = api.generate_compose_search_url(source)
            search_url = compose_search_url(author=author, title=title, keywords=keywords)
            print(search_url)
            return
        # just look at one specific result
        display_columns = DISPLAY_COLUMNS[source]
        df_results = api.quick_search(source)(author=author, title=title, keywords=keywords)
        if _filter:
            assert all(x.count(':') == 1 for x in _filter), 'filter must have color-separated pairs (no spaces between)'
            filter_pairs = dict(x.split(':') for x in _filter)
            assert set(filter_pairs.keys()).issubset(df_results.columns), (
                'all filters must match columns in results table'
            )
            for column, filter_value in filter_pairs.items():
                df_results = df_results.loc[lambda t: t[column].str.lower().str.contains(filter_value.lower())]
        df_selected = df_results[display_columns]
        print(df_selected)

    else:

        searchers = {source: api.quick_search(source) for source in VALID_SOURCES}
        results = {
            source: searcher(author=author, title=title, keywords=keywords)
            for source, searcher in searchers.items()
        }

        results_bibliocommons = pd.concat((
            results['epl'],
            results['calgary'].query('true_format != "book"')
        ))

        oneliners = {
            'abebooks': results['abebooks'].pipe(create_abebooks_oneliner),
            'edmonton': results['abebooks'].pipe(create_abebooks_edmonton_oneliner),
            'kobo': results['kobo'].pipe(create_kobo_oneliner),
            'bibliocommons': results_bibliocommons.pipe(create_bibliocommons_oneliner),
            'annas_archive': results['annas_archive'].pipe(create_annas_archive_oneliner),
            'goodreads': results['goodreads'].pipe(create_goodreads_oneliner),
        }

        display_string = '\n'.join(
            source.upper()[:3] + ': ' + (oneliner if oneliner else '--')
            for source, oneliner in oneliners.items()
        )

        print(display_string)
            


def _skip_if_empty(oneliner_creator: Callable) -> Callable:
    def decorated_function(df_results):
        if df_results.empty:
            return None
        else:
            return oneliner_creator(df_results)
    return decorated_function



@_skip_if_empty
def create_annas_archive_oneliner(df_results):
    oneliner = ' | '.join({'epub', 'pdf'}.intersection(df_results['filetype']))
    return oneliner


@_skip_if_empty
def create_goodreads_oneliner(df_results):
    avg_rating, num_ratings = (
        df_results.loc[df_results.num_ratings.idxmax(), ['avg_rating', 'num_ratings']]
    )
    oneliner = f'{avg_rating} ({int(num_ratings)})'
    return oneliner


@_skip_if_empty
def create_abebooks_oneliner(df_results):
    min_price, max_price, num_results = (
        df_results['price'].agg(['min', 'max', 'count']).astype('int')
    )
    oneliner = f'${min_price}-{max_price} ({num_results})'
    return oneliner


@_skip_if_empty
def create_kobo_oneliner(df_results):
    min_price, max_price, num_results = (
        df_results
        .sale_price
        .fillna(df_results.reg_price)
        .round(0)
        .astype('int')
        .agg(['min', 'max', 'count'])
    )
    oneliner = f'${min_price}-{max_price} ({num_results})'
    return oneliner


@_skip_if_empty
def create_bibliocommons_oneliner(df_results):
    formats_of_interest = ['book', 'ebook', 'audiobook', 'web-ebook']
    formats_available = set(formats_of_interest).intersection(df_results['true_format'])
    oneliner = ' | '.join(sorted(formats_available, key=formats_of_interest.index))
    return oneliner


@_skip_if_empty
def create_abebooks_edmonton_oneliner(df_results):
    oneliner = (
        df_results
        .query('seller_city=="Edmonton"')
        .assign(
            store_alias=lambda t: t.seller_name.map(EDMONTON_STORE_ALIASES),
            nice_price=lambda t: t.price.round(0).astype('int').astype('string').apply(lambda x: '$' + x),
        )
        .pipe(lambda t: t.store_alias + ':' + t.nice_price)
        .pipe(' | '.join)
    )
    return oneliner



if __name__ == '__main__':
    main()
