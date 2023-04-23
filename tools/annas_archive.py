import requests
import pandas as pd
from urllib.parse import quote_plus
from bs4 import BeautifulSoup


def run_annas_archive_search(
    search_string: str,
    language: str=None,
    filetype: str=None,
    content: str=None,
) -> pd.DataFrame:

    quoted_search_string = quote_plus(search_string)

    search_url = (
        "https://annas-archive.org/search?" + 
        '&'.join([
            f"q={quoted_search_string}",
            f"lang={language}" if language else '',
            f"ext={filetype}" if filetype else '',
            ])
    )

    search_results_html = (requests
        .get(search_url)
        .content
        .decode('utf-8')
        .replace('<!--', '')
        .replace('-->', '')
    )

    soup = BeautifulSoup(search_results_html, features='html.parser')
    result_items = soup.find_all('div', class_='h-[125]')

    # TODO: extract this dumb function
    get_text = lambda elem: elem.getText() if elem else ''

    # TODO: add in link 
    # TODO: add in file extension
    result_items_data = []
    for y in result_items:
        x = y.find('a').find('div').findNextSibling()
        filename = x.find('div', class_='text-xs')
        publisher = x.find('div', class_='text-sm')
        author = x.find('div', class_='italic')
        title = x.find('h3')
        result_items_data.append({
            "title": get_text(title),
            "author": get_text(author),
            "filename": get_text(filename),
            "publisher": get_text(publisher),
        })

    data = pd.DataFrame(result_items_data)

    return data



    # for i, y in enumerate(result_items):
    #     x = y.find('a').find('div').findNextSibling()

    # for i, y in enumerate(result_items):
    #     print(i+1, y.getText().strip())
    #     a = y.find('a')
    #     if a:
    #         x = a.find('div').findNextSibling()
    #         filename = x.find('div', class_='text-xs')
    #         publisher = x.find('div', class_='text-sm')
    #         author = x.find('div', class_='italic')
    #         title = x.find('h3')
