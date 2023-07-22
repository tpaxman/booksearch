from functools import reduce
import pathlib
def concat(source): 
    return pd.concat(
        pd.read_pickle(x).assign(searched=x.stem.replace(f'{source} - ', ''))
        for x in pathlib.Path('output/flip').glob(f'{source}*pkl')
    )
sources = ['abebooks', 'goodreads', 'annas_archive', 'kobo', 'epl', 'calgary']
details = {x: concat(x) for x in sources}

def aggregate_abebooks(df_abebooks: pd.DataFrame) -> pd.DataFrame:
    abe_agg = (
        df_abebooks
        [['searched', 'author', 'title', 'price', 'seller_name', 'seller_city']]
        .groupby('searched')
        .agg(
            num_results=('author', 'count'),
            min_price=('price', 'min'),
            max_price=('price', 'max'),
        )
        .reset_index()
    )

    abe_edm = (
        df_abebooks
        .query('seller_city=="Edmonton"')
        .replace({'seller_name': {
            'Alhambra Books': 'alhambra',
            'Mister-Seekers Books': 'mister_seekers',
            'The Bookseller': 'bookseller',
            "The Great Catsby's Rare Books": 'great_catsby',
            'Edmonton Book Store': 'edmonton_bookstore'
         }})
        .groupby(['searched', 'seller_name'])
        ['price']
        .min()
        .unstack()
        .round(0)
    )

    return (
        abe_agg
        .merge(abe_edm, how='left', on='searched')
        .set_index('searched')
    )


def aggregate_kobo(df_kobo: pd.DataFrame) -> pd.DataFrame:
    return (
        df_kobo
        .assign(current_price=lambda t: t.sale_price.fillna(t.reg_price))
        .groupby('searched')
        .agg(
            min_price=('current_price', 'min'),
            max_price=('current_price', 'max'),
            num_copies=('currency', 'count'),
        )
    )


def aggregate_bibliocommons(df_bibliocommons: pd.DataFrame) -> pd. DataFrame:
    return (
        df_bibliocommons
        .assign(holds=lambda t: t.hold_counts.str.replace('Holds: ', ''))
        .groupby(['searched', 'true_format'])
        .size()
        .unstack()
        .reindex(['book', 'ebook', 'audiobook', 'web-ebook'], axis=1)
    )


def aggregate_goodreads(df_goodreads: pd.DataFrame) -> pd. DataFrame:
    return (
        df_goodreads
        .astype({'num_ratings': 'float'})
        .sort_values('num_ratings', ascending=False)
        .groupby('searched')
        .first()
    )


def aggregate_annas_archive(df_annas_archive: pd.DataFrame) -> pd. DataFrame:
    return (
        df_annas_archive
        .groupby(['searched', 'filetype'])
        .size()
        .unstack()
        .reindex(['epub', 'pdf', 'mobi'], axis=1)
    )


aggregator_functions = {
    'abebooks': aggregate_abebooks,
    'kobo': aggregate_kobo,
    'annas_archive': aggregate_annas_archive,
    'goodreads': aggregate_goodreads,
    'epl': aggregate_bibliocommons,
    'calgary': aggregate_bibliocommons,
}

agg_data = {k: aggregator_functions.get(k)(v) for k, v in details.items()}
agg_data_renamed = {k: v.rename(columns=lambda x: k + '_' + x) for k, v in agg_data.items()}

flat_table = reduce(
    lambda df_current, df_next: df_current.join(df_next, how='left'),
    agg_data_renamed.values()
)

