from bs4 import BeautifulSoup
import requests
from urllib.parse import quote_plus
import pandas as pd
from typing import Callable
import re

def run_goodreads_search(search_string: str) -> pd.DataFrame:

    quoted_search_string = quote_plus(search_string)
    #search_url = f"https://www.goodreads.com/search?q={quoted_search_string}"
    search_url = f"https://www.goodreads.com/search?utf8=%E2%9C%93&q={quoted_search_string}&search_type=books&search%5Bfield%5D=on"
    print(f"GOODREADS: {search_url}")
    response = requests.get(search_url)
    search_results_html = response.content
    soup = BeautifulSoup(search_results_html, features='html.parser')

    try:
        result_items = soup.find('table', class_='tableList').find_all('tr')
    except:
        return pd.DataFrame(columns=['title', 'author', 'avg_rating', 'num_ratings', 'link'])

    get_text = lambda elem: elem.getText() if elem else ''

    result_items_data = []
    for x in result_items:
        title = x.find('a', class_='bookTitle')
        author = x.find('a', class_='authorName')
        minirating = x.find('span', class_='minirating')

        avg_rating_raw, num_ratings_raw = re.search(r'(\d\.\d\d) avg rating\D+([\d|,]+) ratings*', get_text(minirating)).groups()
        avg_rating = float(avg_rating_raw)
        num_ratings = int(num_ratings_raw.replace(',', ''))

        title_text = get_text(title.find('span', role='heading')) if title else ''

        result_items_data.append({
            "title": title_text,
            "author": get_text(author),
            "avg_rating": avg_rating,
            "num_ratings": num_ratings,
            "link": "https://www.goodreads.com/" + (title['href'] if title else ''),
        })

    data = pd.DataFrame(result_items_data)

    return data





