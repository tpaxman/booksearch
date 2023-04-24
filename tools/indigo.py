import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
import re

# TODO: delete this temporary input line
search_string = 'joseph smith rough stone bushman'

def search_indigo(author: str=None, title: str=None, keywords: str=None) -> pd.DataFrame:
    search_string = ' '.join(filter(bool, (author, title, keywords)))
    quoted_search_string = quote_plus(search_string)
    search_url = f"https://www.chapters.indigo.ca/en-ca/home/search/?keywords={quoted_search_string}#internal=1"
    print(search_url)
    search_results_html = requests.get(search_url).content
    soup = BeautifulSoup(search_results_html, features='html.parser')
    result_items = soup.find('div', class_="product-list__results-container").find_all('div', class_="product-list__product product-list__product-container")

    result_items_data = []
    for x in result_items:
        base_price = x.find('p', attrs={"data-a8n": "search-page__product-list-or-adjusted-price"})

        if base_price:
            price = base_price.getText()
            list_price = price
        else:
            price = x.find('p', attrs={"data-a8n": "search-page__product-adjusted-price"}).getText()
            list_price = x.find('p', attrs={"data-a8n":  "search-page__product-list-price"}).getText()

        get_price = lambda x: float(re.search(r'\d+\.\d+', x).group(0))

        price_cad = get_price(price)
        list_price_cad = get_price(list_price)
        discount = str(int((1 - price_cad/list_price_cad)*100)) + '%'

        result_items_data.append({
            "title": x.find('h3', class_="product-list__product-title").getText(),
            "author": x.find('p', class_="product-list__author").getText(),
            "bookformat": x.find('div', class_="product-list__product-format").getText(),
            "price_cad": price_cad,
            "discount": discount,
            "online_availability": x.find('div', attrs={"data-a8n":"search-page__online-availability-message"}).getText(),
            "store_availability": x.find('div', attrs={"data-a8n":"search-page__store-availability-message"}).getText(),
        })

    df_results = pd.DataFrame(result_items_data)
    df_refiltered = df_results.loc[lambda t: t.title.apply(lambda x: refilter(x, title)) & t.author.apply(lambda x: refilter(x, author))]
    return df_refiltered

    
                    

def refilter(full_string, search_string):
    """ ensure all words in search_string appear in full_string """
    if search_string:
        search_words = re.findall(r'\w+', search_string)
        return all(w.lower() in full_string.lower() for w in search_words)
    else:
        return True
