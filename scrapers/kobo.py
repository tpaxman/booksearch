import requests
import bs4
from bs4 import BeautifulSoup, element
from urllib.parse import quote_plus
import pandas as pd


def compose_search_url(keywords: str) -> str:
    quoted_search_string = quote_plus(keywords)
    search_url = f"https://www.kobo.com/ca/en/search?query={quoted_search_string}"
    return search_url


def parse_results(content: bytes) -> pd.DataFrame:
    soup = BeautifulSoup(content, features='html.parser')
    results = soup.find('ul', class_='result-items')

    result_items_data = []
    if results:
        result_items = results.find_all('li', class_='book')
        for item in result_items:
            title = item.find('h2', class_='title').find('a')
            subtitle = item.find('p', class_='subtitle')
            author = item.find('a', class_='contributor-name')
            price_value = item.find('span', class_='price-value')
            try:
                reg_price = price_value.find('span', class_='was-price')
                sale_price = price_value.find('span', class_='alternate-price-style')
            except AttributeError:
                reg_price, sale_price = None, None

            reg_amount, reg_currency = (
                _extract_price_details(reg_price) if reg_price else 
                _extract_price_details(price_value) if price_value else
                (None, None)
            )
            sale_amount, sale_currency = (
                _extract_price_details(sale_price) if sale_price else (None, None)
            )

            result_items_data.append({
                "title": _get_text_stripped(title),
                "subtitle": _get_text_stripped(subtitle),
                "author": _get_text_stripped(author),
                "reg_price": reg_amount,
                "sale_price": sale_amount,
                "currency": reg_currency,
                "link": title['href'],
            })

    df_results = (
        pd
        .DataFrame(result_items_data)
        # TODO: share this with the dict above
        .reindex(['title', 'subtitle', 'author', 'reg_price', 'sale_price', 'currency', 'link'], axis=1)
        .convert_dtypes()
        .astype({'reg_price': 'float', 'sale_price': 'float'})
    )
    return df_results


def _extract_price_details(price_value: element.Tag):
    return price_value.getText().strip().strip('$').split()


def _get_text_stripped(elem: element.Tag) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText().strip() if elem else ''
