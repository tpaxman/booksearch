import re
from typing import Literal
from urllib.parse import quote, quote_plus
import bs4
import pandas as pd

# TODO: add a 'strict' filter mode where the title inputs are quoted

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
        #TODO: handle this one differently (must have the saction=allow as well
        "saction=allow&slist": (
            quote_plus(' '.join(str(x) for x in sellers)) if sellers else None 
        ),
    }

    root_url = "https://www.abebooks.com/servlet/SearchResults?"
    arguments_string = '&'.join(
        f'{k}={quote(str(v))}' for k, v in arguments.items() if v
    )
    search_url = root_url + arguments_string
    return search_url


def parse_results(content: bytes) -> pd.DataFrame:
    """Parse the response content returned by an Abebooks search request."""

    soup = bs4.BeautifulSoup(content, features='html.parser')
    results_block = soup.find('ul', class_='result-block', id='srp-results')

    if results_block:
        result_items = []
        for li_tag in results_block.find_all('li', attrs={'data-cy': 'listing-item'}):
            metadata = {
                y.get('itemprop'): y.get('content') for y in li_tag.find_all('meta')
            }
            div_bookseller_info = li_tag.find('div', class_='bookseller-info')
            bookseller_info = _parse_bookseller_info(div_bookseller_info)
            all_data = metadata | bookseller_info
            result_items.append(all_data)
        df_results = pd.DataFrame(result_items)
    else:
        df_results = pd.DataFrame()

    df_results_final = (
        df_results
        .reindex(columns=[
            'name',
            'author',
            'price',
            'priceCurrency',
            'seller_name',
            'seller_city',
            'seller_region',
            'seller_country',
            'about',
            'bookFormat',
            'itemCondition',
            'bookEdition',
            'isbn',
            'publisher',
            'datePublished',
            'availability',
        ]) 
        .rename(columns={
            "name": "title",
            "datePublished": "year_published",
            "priceCurrency": "currency",
            "itemCondition": "condition",
            "bookEdition": "edition",
            "bookFormat": "binding",
        })
        .convert_dtypes()
        .astype({'price': 'float', 'year_published': 'float'})
    )

    return df_results_final


def _parse_bookseller_info(div_bookseller_info: bs4.element.Tag) -> dict[str, str]:
    text_secondary = div_bookseller_info.find('p', class_='text-secondary')
    if not text_secondary:
        return (None, None)
    
    seller_link = text_secondary.find('a')
    if not seller_link:
        return (None, None)

    seller = seller_link.getText()

    # remove the tag containing the seller name to leave only the location
    seller_link.decompose()
    location_raw = text_secondary.getText()
    full_location = location_raw.replace('Seller:', '').strip().strip(',').strip()
    location_parts = full_location.split(', ')
    num_parts = len(location_parts)
    if num_parts == 3:
        city, region, country = location_parts
    elif num_parts == 2:
        city, region, country = location_parts[0], None, location_parts[1]
    else:
        city, region, country = None, None, location_parts[0]

    return {
        'seller_name': seller, 
        'seller_city': city, 
        'seller_region': region,
        'seller_country': country
    }

