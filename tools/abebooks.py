from bs4 import BeautifulSoup
from typing import Literal
import requests
from urllib.parse import quote, quote_plus
from forex_python.converter import CurrencyRates
import re
import pandas as pd
import numpy as np
from functools import partial

# TODO: add a 'strict' filter mode where the title inputs are quoted

# TODO: decide if this conversion is really necessary
USD_TO_CAD_FACTOR = CurrencyRates().get_rate('USD', 'CAD')

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


def parse_results(content: bytes) -> pd.DataFrame:
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
        assert shipping_details.strip().lower().endswith('canada')

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
            "itemCondition": "condition",
            "bookEdition": "edition",
            "bookFormat": "binding",
        })
        .assign(
            in_edmonton = lambda t: t.seller.str.lower().str.contains('edmonton'),
            price_cad = lambda t: t.price_usd.astype(float).multiply(USD_TO_CAD_FACTOR).round(0).astype(int),
            shipping_cost_cad = lambda t: np.where(t.in_edmonton, 0, t.shipping_cost_usd.astype(float).multiply(USD_TO_CAD_FACTOR).round(0).astype(int)),
            condition = lambda t: t.about.apply(get_condition_description),
            total_price_cad = lambda t: t.price_cad + t.shipping_cost_cad,
        )
    )

    return df_results


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


compose_search_url_edmonton = partial(compose_search_url, sellers=SELLERS.values())
