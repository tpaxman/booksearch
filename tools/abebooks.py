from bs4 import BeautifulSoup
import requests
from urllib.parse import quote
from forex_python.converter import CurrencyRates
import re
import pandas as pd
import numpy as np
from functools import partial

USD_TO_CAD_FACTOR = CurrencyRates().get_rate('USD', 'CAD')

pd.options.display.max_colwidth = 50

SELLERS = {
    'alhambra-books': 3054340,
    'edmonton-book-store': 19326,
    'the-bookseller': 51101471,
}


def run_abebooks_search(
    author: str=None,
    title: str=None,
    keywords: str=None,
    sellers: list=None,
    publisher: str=None,
    binding: str=None,
) -> pd.DataFrame:

    static_parameters = [
        f"bx=off",
        f"cm_sp=SearchF-_-Advs-_-Result",
        f"ds=100",  # 100 seems to be the max. If you go over it, it will instead return 30
        f"n=100121503",
        f"recentlyadded=all",
        f"rollup=on",
        f"sortby=17",
        f"sts=t",
        f"xdesc=off",
        f"xpod=off",
    ]

    optional_parameters = [
        f"kn={quote(keywords)}" if keywords else None,
        f"tn={quote(title)}" if title else None,
        f"an={quote(author)}" if author else None,
        (f"saction=allow&" + (f"&slist=" + '%2B'.join(map(str, sellers)))) if sellers else None,
        f"pn={quote(publisher)}" if publisher else None,
        f"bi={binding}" if binding else "bi=0"
    ]

    all_parameters = static_parameters + [x for x in optional_parameters if x]

    search_url = (
        "https://www.abebooks.com/servlet/SearchResults?" +
        '&'.join(all_parameters)
    )

    response = requests.get(search_url)
    search_results_html = response.content
    soup = BeautifulSoup(search_results_html, features='html.parser')
    results_block = soup.find('ul', class_='result-block', id='srp-results')

    if not results_block:
        return pd.DataFrame()

    result_items = results_block.find_all('li', attrs={'data-cy': 'listing-item'})

    result_items_data = []
    for x in result_items:

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
        result_items_data.append(all_data)

    df_results = (pd
        .DataFrame(result_items_data)
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
            condition = lambda t: t.about.apply(get_condition),
            total_price_cad = lambda t: t.price_cad + t.shipping_cost_cad,
        )
    )

    return df_results


         
def get_condition(about: str) -> str:
    search_result = re.search(r'Condition:\s+(.*?)(\.|$)', about)
    condition = search_result.group(1).lower().strip() if search_result else ''

    # TODO: implement this
    # condition_special = {
    #     'poor': 1
    #     'acceptable': 2
    #     'fair': 2
    #     'good': 3
    #     'very good' 4
    #     'very good+': 5
    #     'near fine': 6
    #     'like new': 7
    #     'fine': 7
    # }

    return condition


search_edmonton_bookstore = partial(search_abebooks, sellers=[19326])
search_edmonton_stores = partial(search_abebooks, sellers=SELLERS.values())

request_search_results_in_edmonton = partial(run_abebooks_search, sellers=SELLERS.values())

def search_abebooks(**kwargs) -> pd.DataFrame:
    df_results = run_abebooks_search(**kwargs)
    display_results(df_results)


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


# TODO: implement these
def search_abebooks_advanced(
    title: str = None,
    author: str = None,
    keywords: str = None,
    binding: str = None,
    seller_ids: list = None,
    is_signed: bool = None,
) -> str:
    slist_term = (f"&slist=" + '%2B'.join(map(str, seller_ids))) if seller_ids else ''
    f"saction=allow",


