import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd


def search_kobo(search_string: str) -> pd.DataFrame:
    quoted_search_string = quote_plus(search_string)
    search_url = f"https://www.kobo.com/ca/en/search?query={quoted_search_string}"
    search_results_html = requests.get(search_url).content
    soup = BeautifulSoup(search_results_html, features='html.parser')
    result_items = soup.find('ul', class_='result-items').find_all('li', class_='book')
    print(len(result_items))

    result_items_data = []
    for x in result_items:
        title = x.find('h2', class_='title').find('a').getText()
        link = x.find('h2', class_='title').find('a')['href']
        subtitle = x.find('p', class_='subtitle').getText()
        author = x.find('span', class_='synopsis-text').find('a', class_='contributor-name').getText()
        price = x.find('span', class_='price-value').getText()
        result_items_data.append({
            "title": title,
            "subtitle": subtitle,
            "author": author,
            "link": link,
            "price": price,
        })

    return pd.DataFrame(result_items_data).fillna('')


