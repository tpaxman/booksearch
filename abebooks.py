from bs4 import BeautifulSoup
import requests
from urllib.parse import quote
from forex_python.converter import CurrencyRates
import re
import pandas as pd


def search_abebooks(search_term: str) -> str:
    search_term_quoted = quote(search_term)
    search_url = (
        "https://www.abebooks.com/servlet/SearchResults?" +
        '&'.join([
            f"kn={search_term_quoted}",
            f"bi=0",
            f"bx=off",
            f"cm_sp=SearchF-_-Advs-_-Result",
            f"ds=50",
            f"n=100121503",
            f"recentlyadded=all",
            f"rollup=on",
            f"sortby=17",
            f"sts=t",
            f"xdesc=off",
            f"xpod=off",
        ])
    )
    response = requests.get(search_url)
    search_results_html = response.content

    soup = BeautifulSoup(search_results_html)
    results_block = soup.find('ul', class_='result-block', id='srp-results')
    result_items = results_block.find_all('li', attrs={'data-cy': 'listing-item'})

    result_items_data = []
    for x in result_items:

        # get all metadata
        metadata = {y.get('itemprop'): y.get('content') for y in x.find_all('meta')}
        assert metadata['priceCurrency'] == 'USD', 'this thing is only set up to handle USD as the source currency'

        # get other pieces of data
        shipping_details = x.find('a', class_='item-shipping-dest').getText().strip()
        assert shipping_details.strip().lower().endswith('canada')
        #shipping_source, shipping_destination = re.search(r'From (\w.*?) to (\w.*)\s*$', shipping_details).groups()
        #assert shipping_destination.lower().startswith('can'), 'this thing expects that everything is being shipped to Canada'

        shipping_cost_raw = x.find('span', class_='item-shipping').getText()
        assert 'US$' in shipping_cost_raw, 'this thing is only set up to handle USD as the source currency'
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

    usd_to_cad_factor = CurrencyRates().get_rate('USD', 'CAD')

    df_results = (pd
        .DataFrame(result_items_data)
        .rename(columns={
            "name": "title",
            "datePublished": "date_published",
            "price": "price_usd",
            "itemCondition": "condition",
            "bookEdition": "edition",
            "bookFormat": "format",
        })
        .assign(
            price_cad = lambda t: t.price_usd.astype(float) * usd_to_cad_factor,
            shipping_cost_cad = lambda t: t.shipping_cost_usd.astype(float) * usd_to_cad_factor
        )
        [[
            "title",
            "author",
            "format",
            "price_cad",
            "shipping_cost_cad",
            "seller",
            "condition",
            "about",
            "publisher",
            "date_published",
            "edition"
        ]]
    )

    return df_results

         




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


