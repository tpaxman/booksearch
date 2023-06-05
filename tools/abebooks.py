import re
import requests
from functools import partial
from typing import Literal
from urllib.parse import quote, quote_plus
from bs4 import BeautifulSoup
from forex_python.converter import CurrencyRates
import numpy as np
import pandas as pd

try:
    USD_TO_CAD_FACTOR = CurrencyRates().get_rate('USD', 'CAD')
except RatesNotAvailableError:
    USD_TO_CAD_FACTOR = 1.35

# TODO: add a 'strict' filter mode where the title inputs are quoted
# TODO: decide if this conversion is really necessary

ALHAMBRA_BOOKS_SELLER_ID = 3054340
EDMONTON_BOOK_STORE_SELLER_ID = 19326
THE_BOOKSELLER_SELLER_ID = 51101471

SELLERS = {
    'alhambra-books': ALHAMBRA_BOOKS_SELLER_ID,
    'edmonton-book-store': EDMONTON_BOOK_STORE_SELLER_ID,
    'the-bookseller': THE_BOOKSELLER_SELLER_ID,
}

ON_OFF_TYPE = Literal['on', 'off']


def compose_search_url(
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
) -> str:
    """
    Compose a URL to search Abebooks
    """
    # TODO: link these mappings to their associated 'Literal' argument types (if possible)
    sortby_mappings = {
        'recent': 0,
        'price-desc': 1,
        'price': 2,
        'author-desc': 4,
        'author': 5,
        'title-desc': 6,
        'title': 7,
        'seller-rating': 15,
        'total-price': 17,
        'publication-year-desc': 18,
        'publication-year': 19,
        'relevance': 20,
    }

    binding_mappings = {
        "any": "any",
        "hardcover": "h",
        "softcover": "s",
    }

    condition_mappings = {
        "new": '100121501',
        "used": '100121503',
        "any": None,
    }

    # TODO: figure out what cm_sp is
    # TODO: figure out what sts is
    arguments = {
        "cm_sp": "SearchF-_-Advs-_-Result", 
        "sts": "t", 
        "bx": boolean_search,
        "ds": num_results,
        "n": condition_mappings.get(condition),
        "fe": first_edition,
        "recentlyadded": recentlyadded,
        "rollup": rollup,
        "sortby": sortby_mappings.get(sortby),
        "xdesc": expand_descriptions,
        "xpod": not_print_on_demand,
        "kn": keywords,
        "tn": title,
        "an": author,
        "pn": publisher,
        "bi": binding_mappings.get(binding),
        "dj": dust_jacket,
        "isbn": isbn,
        "pt": product_type,
        "rgn": region,
        "cty": country,
        "yrh": year_high,
        "yrl": year_low,
        "prh": price_high,
        "prl": price_low,
        "sgnd": signed,
        "saction=allow&slist": quote_plus(' '.join(str(x) for x in sellers)) if sellers else None #TODO: handle this one differently (must have the saction=allow as well
    }
    root_url = "https://www.abebooks.com/servlet/SearchResults?"
    arguments_string = '&'.join(f'{k}={quote(str(v))}' for k, v in arguments.items() if v)
    search_url = root_url + arguments_string
    return search_url


compose_search_url_edmonton = partial(compose_search_url, sellers=SELLERS.values())


def parse_results(content: bytes, assume_canada: bool=True) -> pd.DataFrame:
    """
    Parse the response content returned by an Abebooks search request
    """

    soup = BeautifulSoup(content, features='html.parser')
    results_block = soup.find('ul', class_='result-block', id='srp-results')

    if not results_block:
        return pd.DataFrame()

    result_items = []
    for x in results_block.find_all('li', attrs={'data-cy': 'listing-item'}):

        # get all metadata
        metadata = {y.get('itemprop'): y.get('content') for y in x.find_all('meta')}
        currency = metadata['priceCurrency']
        assert currency == 'USD', f'this thing is only set up to handle USD as the source currency, (What is {currency}?)'

        # get other pieces of data
        shipping_details = x.find('a', class_='item-shipping-dest').getText().strip()
        assert shipping_details.strip().lower().endswith('canada') or not assume_canada, f'Shipping destination set to {shipping_details}'

        shipping_cost_raw = x.find('span', class_='item-shipping').getText()
        shipping_cost_raw = 'US$ 0' if 'free' in shipping_cost_raw.lower() else shipping_cost_raw
        assert 'US$' in shipping_cost_raw, f'this thing is only set up to handle USD as the source currency (dont understand {shipping_cost_raw})'
        shipping_cost_usd = (
            shipping_cost_raw
            .strip()
            .strip('US$ ')
            .strip(' Shipping')
        )

        seller_raw = x.find('div', class_='bookseller-info').getText()
        seller = re.sub(r'^\s+Seller:\s+(.*?)\n.*', r'\1', seller_raw, flags=re.DOTALL)

        other_data = {
            "seller": seller,
            "shipping_details": shipping_details,
            "shipping_cost_usd": shipping_cost_usd
        }

        all_data = metadata | other_data
        result_items.append(all_data)

    df_results = (pd
        .DataFrame(result_items)
        .fillna('')
        .rename(columns={
            "name": "title",
            "datePublished": "date_published",
            "price": "price_usd",
            "priceCurrency": "currency",
            "itemCondition": "condition",
            "bookEdition": "edition",
            "bookFormat": "binding",
        })
        .assign(condition = lambda t: t.about.apply(get_condition_description))
        .convert_dtypes()
        .astype({'price_usd': 'float', 'shipping_cost_usd': 'float'})
        # TODO: add another way to get "edition" here (in case it's non-existant) by parsing 'about' - use reindex
        .convert_dtypes()
        .fillna({
            'price_usd': 0,
            'shipping_cost_usd': 0
        })
        .assign(
            edition = lambda t: t.edition if 'edition' in t.columns else '',
        )
    )

    if assume_canada:
        return (
            df_results
            .assign(
                price_cad = lambda t: t.price_usd.multiply(USD_TO_CAD_FACTOR),
                shipping_cost_cad = lambda t: t.shipping_cost_usd.multiply(USD_TO_CAD_FACTOR),
            )
            .assign(
                in_edmonton = lambda t: t.seller.str.lower().str.contains('edmonton'),
                # they always list them in USD but the in-store price is the same value in CAD
                price_cad = lambda t: np.where(t.seller.str.lower().str.contains('edmonton book store'), t.price_usd, t.price_cad),
                shipping_cost_cad = lambda t: np.where(t.in_edmonton, 0.0, t.shipping_cost_cad),
                seller = lambda t: np.where(t.in_edmonton, '* ' + t.seller, t.seller),
            )
            .assign(
                total_price_cad = lambda t: t.price_cad + t.shipping_cost_cad,
                price_description = lambda t: (
                    t.price_cad.astype('int').astype('string')
                    + ' + ' + t.shipping_cost_cad.astype('int').astype('string')
                    + ' = ' + t.total_price_cad.astype('int').astype('string')
                ),
            )
            .convert_dtypes()
            .sort_values('total_price_cad')
        )
    else:
        return df_results
        
def agg_batch_results_generic(results: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate batch results - not for Canada 
    """

    if results.empty:
        return pd.DataFrame()

    return (
        results
        .groupby(['author_search', 'title_search'])
        .agg(
            price=('price_usd', 'min'), 
            n=('price_usd', 'count')
        )
        .reset_index()
    )


def agg_batch_results(results: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate batch results
    """

    if results.empty:
        return pd.DataFrame()

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


def create_view(results: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary view for printout
    """

    if results.empty:
        return pd.DataFrame()

    return (
        results
        .loc[lambda t: t.groupby(['binding', 'condition'])['total_price_cad'].idxmin()]
        .sort_values('total_price_cad')
        [['binding', 'condition', 'price_description', 'seller', 'title', 'author']]
        .reset_index(drop=True)
    )

def create_view_generic(results: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary view for printout - for non-canada
    """

    if results.empty:
        return pd.DataFrame()

    return (
        results
        .loc[lambda t: t.groupby(['binding', 'condition'])['price_usd'].idxmin()]
        .sort_values('price_usd')
        [['binding', 'condition', 'price_usd', 'seller', 'title', 'author']]
        .reset_index(drop=True)
    )


def create_description(results: pd.DataFrame) -> str:
    """
    Create a summary description for printout
    """

    num_results = results.shape[1]
    extremities = {
        'min': results.loc[results['total_price_cad'].idxmin()],
        'max': results.loc[results['total_price_cad'].idxmax()],
    }
    range_description = '\n'.join(f'{name.upper()}: {r.price_description} ..... "{r.title}" ({r.binding}, {r.condition}) ..... {r.seller}' for name, r in extremities.items())
    edmonton_description = (
        results
        .astype({'in_edmonton': 'bool'})
        .query('in_edmonton')
        .assign(seller = lambda t: t['seller'].str.replace(r'\* (.*?), Edmonton, AB, Canada', r'\1', regex=True))
        .apply(lambda r: f'${round(r.price_cad)}: {r.seller}', axis=1)
        .pipe('\n'.join)
    )
    count_description = f'{num_results} Results'
    all_descriptions = '\n'.join((count_description, range_description, edmonton_description))
    return all_descriptions


def get_condition_description(about: str) -> str:
    search_result = re.search(r'Condition:\s+(.*?)(\.|$)', about)
    condition = search_result.group(1).lower().strip() if search_result else ''
    return condition


# TODO: IMPLEMENT
def get_condition_rank(condition_description: str) -> int:
    {
        'poor': 1,
        'acceptable': 2,
        'fair': 2,
        'good': 3,
        'very good': 4,
        'very good+': 5,
        'near fine': 6,
        'like new': 7,
        'fine': 7,
    }.get(condition_description)


# TODO: IMPLEMENT
def display_results(df_results: pd.DataFrame) -> None:
    if not df_results.empty:

        df_results_display = (
            df_results
            .sort_values('total_price_cad', ignore_index=True)
            .assign(
                seller = lambda t: np.where(t.in_edmonton, '* ' + t.seller, t.seller),
            )
            .rename(columns={
                'price_cad': 'price',
                'shipping_cost_cad': 'shipping',
                'total_price_cad': 'total'
            })
            [['title', 'author', 'price', 'total', 'seller', 'about', 'condition', 'binding']]
        )

        print(df_results_display)
        print('')


