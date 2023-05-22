from typing import Literal
import pandas as pd

SOURCES = Literal[
    'abebooks', 
    'annas_archive',
    'calgary',
    'epl',
    'goodreads',
]


# TODO: apply refilter stuff to make sure we are picking the right things
# TODO: use idxmin instead to preserve more data

def agg_abebooks(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results
        .groupby(['author_search', 'title_search'])
        .agg(
            total_price=('total_price_cad', 'min'), 
            price=('price_cad', 'min'), 
            n=('price_cad', 'count')
        )
        .reset_index()
    )

def agg_annas_archive(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results
        [['author_search', 'title_search', 'filetype']].value_counts()
        .unstack()
        .reindex(['epub', 'pdf'], axis=1)
        .fillna(0)
        .astype('int')
        .reset_index()
    )


def agg_goodreads(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results
        .loc[lambda t: t.groupby(['author_search', 'title_search'])['num_ratings'].idxmax()]
        .set_index(['author_search', 'title_search'])[['avg_rating', 'num_ratings']]
        .reset_index()
    )


def agg_bibliocommons(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results
        [['author_search', 'title_search', 'true_format']]
        .value_counts()
        .unstack()
        .reindex(['book', 'ebook', 'web-ebook', 'audiobook'], axis=1)
        .fillna(0)
        .astype('int')
        .reset_index()
    )


AGG_FUNCTIONS = {
    'abebooks': agg_abebooks,
    'annas_archive': agg_annas_archive,
    'epl': agg_bibliocommons,
    'calgary': agg_bibliocommons,
    'goodreads': agg_goodreads,
}

inputs_csv = 'inputs/goodreads_export_to_read.csv'

RESULTS_FILEPATHS = {
    'abebooks': 'outputs/to_read_abebooks.csv',
    'annas_archive': 'outputs/to_read_annas_archive.csv',
    'epl': 'outputs/to_read_epl.csv',
    'calgary': 'outputs/to_read_calgary.csv',
    'goodreads': 'outputs/to_read_goodreads.csv',
}

df_inputs = pd.read_csv(inputs_csv)[['author_surname', 'title']].rename(columns={'author_surname': 'author_search', 'title': 'title_search'})
results_tables = {k: pd.read_csv(v) for k, v in RESULTS_FILEPATHS.items()}
aggregated_tables = {k: AGG_FUNCTIONS.get(k)(v) for k, v in results_tables.items()}

df_final = df_inputs.copy()
for source, aggtable in aggregated_tables.items():
    a = aggtable.set_index(['author_search', 'title_search']).add_prefix(source + '_').reset_index()
    df_final = df_final.merge(a, how='left', on=['author_search', 'title_search'])

to_fill_zero = list(df_final.filter(regex='(epl|calgary|annas_archive)_').columns)
fillna_dict = dict((x,0) for x in to_fill_zero)
astype_dict = dict((x,'int') for x in to_fill_zero)

df_final = df_final.fillna(fillna_dict).astype(astype_dict)

abe_edmonton = (
    results_tables['abebooks']
    .query('in_edmonton')
    [[
        "author_search",
        "title_search",
        "author",
        "title",
        "seller",
        "price_cad",
        "binding",
        "condition",
        "edition",
        "about",
    ]]
)



