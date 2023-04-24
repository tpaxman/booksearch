import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd

# TODO: delete this temporary input line
search_string = 'joseph smith rough stone bushman'

def search_indigo(search_string: str) -> pd.DataFrame:
    quoted_search_string = quote_plus(search_string)
    search_url = f"https://www.chapters.indigo.ca/en-ca/home/search/?keywords={quoted_search_string}#internal=1"
    search_results_html = requests.get(search_url).content
    soup = BeautifulSoup(search_results_html, features='html.parser')
    result_items = soup.find('div', class_="product-list__results-container").find_all('div', class_="product-list__product product-list__product-container")

    get_text = lambda elem: elem.getText() if elem else ''

    result_items_data = []
    for x in result_items:
        title = x.find('h3', class_="product-list__product-title")
        author = x.find('p', class_="product-list__author")
        bookformat = x.find('div', class_="product-list__product-format")
        price_cad = x.find('p', class_="product-list__price--black product-list__listview-price")
        online_availability = x.find('div', attrs={"data-a8n":"search-page__online-availability-message"})
        store_availability = x.find('div', attrs={"data-a8n":"search-page__store-availability-message"})
        result_items_data.append({
            "title": get_text(title),
            "author": get_text(author),
            "bookformat": get_text(bookformat),
            "price_cad": get_text(price_cad),
            "online_availability": get_text(online_availability),
            "store_availability": get_text(store_availability),
        })

    return pd.DataFrame(result_items_data)
                    
