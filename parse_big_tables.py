import pandas as pd
import pathlib
import numpy as np

source_folder = 'outputs/summaries'

tables = {x.stem: pd.read_csv(x) for x in pathlib.Path(source_folder).glob('*.csv')}

search_urls = tables['search_urls']
results = {k:v for k,v in tables.items() if k != 'search_urls'}


def abebooks_summary(df):
    return (df
        .assign(comments = lambda t: (t.seller + ', ' + t.edition.fillna('')).str.strip())
        .rename(columns={'binding': 'book_format'})
        [['title', 'author', 'source', 'goodreads_id', 'book_format', 'comments', 'price_cad', 'total_price_cad']]
        .sort_values('total_price_cad')
        .groupby(['source', 'goodreads_id', 'book_format']).first().reset_index()
    )


def abebooks_edmonton_summary(df):
    return (df
        .assign(comments = lambda t: (t.seller + ', ' + t.edition.fillna('')).str.strip())
        .rename(columns={'binding': 'book_format'})
        .assign(price_cad = lambda t: np.where(t.seller.str.contains('Edmonton Book'), t.price_usd, t.price_cad))
        [['title', 'author', 'source', 'goodreads_id', 'book_format', 'comments', 'price_cad', 'total_price_cad']]
        .sort_values('total_price_cad')
    )
    """
    about
    author
    availability
    binding
    condition
    date_published
    edition
    filestem
    goodreads_id
    in_edmonton
    isbn
    priceCurrency
    price_cad
    price_usd
    publisher
    seller
    shipping_cost_cad
    shipping_cost_usd
    shipping_details
    source
    title
    total_price_cad
    """

def bibliocommons_summary(df):
    return (df
        .assign(title = lambda t: (t.title + ' ' + t.subtitle.fillna('')).str.strip())
        .assign(comments = lambda t: t.availability_status + ', ' + t.hold_counts)
        .rename(columns={'true_format': 'book_format'})
        .fillna('')
        [['title', 'author', 'source', 'goodreads_id', 'book_format', 'comments']]
    )
    """
    author
    availability_status
    call_number
    eresource_link
    filestem
    format_description
    goodreads_id
    hold_counts
    source
    subtitle
    title
    true_format
    """

def annas_archive_summary(df):
    return (df
        .assign(comments = lambda t: t.filesize_mb.astype('string') + ', ' + t.filename)
        .loc[lambda t: t.language.fillna('?').isin(('?', 'en', 'fr', 'de'))]
        .loc[lambda t: t.filetype.isin(('epub', 'pdf'))]
        .rename(columns={'filetype': 'book_format'})
        [['title', 'author', 'source', 'goodreads_id', 'book_format', 'comments']]
        .groupby(['source', 'goodreads_id', 'book_format'])
        .first()
        .reset_index()
    )
    """
    author
    filename
    filesize_mb
    filestem
    filetype
    goodreads_id
    language
    publisher
    source
    title
    """


parsers = {
    'abebooks': abebooks_summary,
    'abebooks_edmonton': abebooks_edmonton_summary,
    'annas_archive': annas_archive_summary,
    'epl': bibliocommons_summary,
    'cpl': bibliocommons_summary
}




parsed_stuff = {k: parsers.get(k)(v) for k, v in results.items()}

all_data = pd.concat(parsed_stuff.values()).sort_values(['goodreads_id', 'source'])

final_data = (search_urls
    .merge(all_data, how='left', on=['goodreads_id', 'source'])
    [['short_title', 'author_surname', 'source', 'book_format', 'title', 'author', 'comments', 'price_cad', 'total_price_cad', 'search_url', 'goodreads_id']]
    .assign(found = lambda t: t.title.notna().astype('int'))
    .set_index(['short_title', 'author_surname', 'source', 'book_format', 'found'])
    .reset_index()
)

aggregated = (final_data
    .groupby(['short_title', 'author_surname', 'goodreads_id', 'source'])['found'].agg(lambda x: sum(x) > 0)
    .reset_index()
    .pivot_table(columns='source', values='found', index=['short_title', 'author_surname', 'goodreads_id'])
    .astype('int')
)

final_data.to_clipboard(index=False)

aggregated.to_clipboard()


