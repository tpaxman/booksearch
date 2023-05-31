import argparse
import tabulate
from typing import Callable
import requests
import pandas as pd
from batch_search import compose_search_url, parse_results

pd.options.display.max_colwidth = 80
pd.options.display.width = 200

title = 'mccarthy'
author = 'blood meridian'

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--author', '-a', default='', nargs='+')
    parser.add_argument('--title', '-t', default='', nargs='+')
    parser.add_argument('--oneline', '-x', action='store_true')
    args = parser.parse_args()
    title = ' '.join(args.title)
    author = ' '.join(args.author)
    oneline = args.oneline
    main_process(author, title, oneline)
    
def main_process(author, title, oneline):
    
    raw_data = {source: _download_data(source=source, title=title, author=author)
                for source in ['abebooks', 'epl', 'annas_archive', 'goodreads', 'calgary']}

    if not oneline:
        views = {
            'abebooks': create_abebooks_view(raw_data.get('abebooks')),
            'abebooks_local': create_abebooks_local_view(raw_data.get('abebooks')),
            'annas_archive': create_annas_archive_view(raw_data['annas_archive']),
            'epl': create_bibliocommons_view(raw_data['epl']),
            'calgary': create_bibliocommons_view(raw_data['calgary']),
            'goodreads': create_goodreads_view(raw_data['goodreads']),
        }


        for k, v in views.items():
            if not v.empty:
                print(k.upper() + ':')
                print(_tabulate(v))
                print('')



    # TODO: make option to compress everything to one line only
    # TODO: make option to do batches (default one liners only for each)


def _tabulate(data: pd.DataFrame) -> str:
    return tabulate.tabulate(data, showindex=False, headers=data.columns)



def _download_data(source, title, author):
    url = compose_search_url(source=source, title=title, author=author)
    content = requests.get(url).content
    data = parse_results(source=source, content=content)
    return data


def _skip_empty_tables(create_view: Callable) -> Callable:
    def create_view_embellished(data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()
        else:
            return (
                create_view(data)
                .assign(author=lambda t: t['author'].str.replace(r'^(\w+\W*\w*).*', r'\1', regex=True))
                .assign(title_author=lambda t: t['author'].fillna('') + ' - ' + t['title'])
                .drop(columns=['author', 'title'])
            )
    return create_view_embellished


@_skip_empty_tables
def create_abebooks_view(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.loc[lambda t: t.groupby(['binding', 'condition'])['total_price_cad'].idxmin()]
        .sort_values('total_price_cad')
        [['binding', 'condition', 'price_description', 'seller', 'title', 'author']]
        .reset_index(drop=True)
    )

def create_abebooks_oneliner(agg_view: pd.DataFrame) -> str:
    describe_row = lambda r: f"{r.price_description} ({r.binding}, {r.condition})"
    with_descrips = agg_view.assign(summary=lambda t: t.apply(describe_row, axis=1))

    min_summary = with_descrips.loc[lambda t: t.total_price_cad.idxmin(), 'summary']
    max_summary = with_descrips.loc[lambda t: t.total_price_cad.idxmax(), 'summary']

    final_summary = f"{min_summary} / {max_summary}"
    return final_summary



@_skip_empty_tables
def create_abebooks_local_view(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data
        .astype({'in_edmonton': 'bool'})
        .query('in_edmonton')
        .assign(seller=lambda t: t['seller'].str.replace(', Edmonton, AB, Canada', '', regex=False).str.replace('\* ', '', regex=True))
        [['binding', 'condition', 'price_cad', 'seller', 'title', 'author']]
        .reset_index(drop=True)
    )

def create_abebooks_local_oneliner(agg_view: pd.DataFrame) -> str:
    return (
        agg_view
        .apply(lambda r: f"{round(r.price_cad)} ({r.seller})", axis=1)
        .pipe(' / '.join)
    )
        
    

@_skip_empty_tables
def create_bibliocommons_view(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data
        .reindex(['true_format', 'hold_counts', 'title', 'author'], axis=1)
        .reset_index(drop=True)
    )

def create_bibliocommons_oneliner(agg_view: pd.DataFrame) -> str:
    return ', '.join(set(agg_view['true_format']))


# @_skip_empty_tables
# def create_annas_archive_view(data: pd.DataFrame) -> pd.DataFrame:
#     return (
#         data
#         .reindex(['filetype', 'filesize', 'language', 'title', 'author'], axis=1)
#         .loc[lambda t: t['filetype'].isin(('epub', 'pdf', 'mobi'))]
#         .groupby('filetype').first()
#         .reset_index()
#     )

# def create_annas_archive_oneliner(agg_view: pd.DataFrame) -> str:
#     return agg_view['filetype'].drop_duplicates().pipe(', '.join)



@_skip_empty_tables
def create_goodreads_view(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data
        [['num_ratings', 'avg_rating', 'title', 'author']]
        .sort_values('num_ratings', ascending=False)
        .head(4)
    )


def create_goodreads_oneliner(agg_view: pd.DataFrame):
    return agg_view.iloc[0].apply(lambda r: f"{r.avg_rating} ({r.num_ratings}

if __name__ == '__main__':
    main()
