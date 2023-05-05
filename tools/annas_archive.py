import requests
import re
import pandas as pd
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from typing import Literal
from tools.webscraping import get_text
#from webscraping import get_text
import tabulate
import argparse


CONTENT_TYPES = Literal[
    "book_any", 
    "book_fiction", 
    "book_unknown", 
    "journal_article", 
    "book_nonfiction", 
    "book_comic", 
    "magazine", 
    "standards_document"
]

FILETYPES = Literal[
    'epub',
    'pdf',
    'mobi',
    'azw3',
    'rtf',
    'fb2',
    'fb2.zip',
    'txt',
    'doc',
    'dbr',
    'lit',
    'html',
    'cbz',
    'rar',
    'zip',
    'htm',
    'djvu',
    'lrf',
    'mht',
    'docx'
]

SORT_OPTIONS = Literal[
    'relevant',
    'newest',
    'oldest',
    'largest',
    'smallest',
]

# TODO: finish filling these in later
VALID_LANGUAGES = Literal['_empty', 'en', 'fr', 'de', 'es']

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('query')
    parser.add_argument('--filetype', '-f')
    parser.add_argument('--language', '-l')
    args = parser.parse_args()
    main_display(args.query, args.filetype, args.language)


def main_display(query, filetype=None, language=None):
    search_url = compose_annas_archive_search_url(
        query=query, 
        filetype=filetype,
        language=language,
    )
    content = requests.get(search_url).content
    df_results = parse_annas_archive_results(content)
    print_results(df_results)


def print_results(df_results: pd.DataFrame) -> pd.DataFrame:
    df_formatted = (
        df_results
        .assign(
            title = lambda t: t['title'].str[:20],
            author = lambda t: t['author'].str[:30], 
            filesize_mb = lambda t: t['filesize_mb'].astype('int'),
            publisher = lambda t: t['publisher'].str[:50]
        )
        [['title', 'author', 'filetype', 'filesize_mb', 'language', 'publisher']]
        .rename(columns={
            'title': 'Title',
            'author': 'Author',
            'filetype': 'Type',
            'filesize_mb': 'Size (MB)',
            'language': 'Language',
            'publisher': 'Publisher',
        })
    )

    print(tabulate.tabulate(df_formatted, showindex=False, headers=df_formatted.columns))


def compose_annas_archive_search_url(
    query: str,
    filetype: FILETYPES=None,
    language: VALID_LANGUAGES=None,
    content_type: CONTENT_TYPES="book_any",
    sortby: SORT_OPTIONS=None,
) -> str:

    # note: sortby=None defaults to 'most relevant'
    root_url = "https://annas-archive.org/search?"

    arguments = {
        "q": quote_plus(query),
        "filetype": filetype,
        "content": content_type,
        "lang": language,
        "ext": filetype,
        "sort": sortby,
    }

    arguments_string = '&'.join(f'{k}={v}' for k, v in arguments.items() if v)
    search_url = root_url + arguments_string
    return search_url


def parse_annas_archive_results(results_html: bytes) -> pd.DataFrame:

    results_uncommented_html = (
        results_html
        .decode('utf-8')
        .replace('<!--', '')
        .replace('-->', '')
    )

    soup = BeautifulSoup(results_uncommented_html, features='html.parser')

    # remove all the "partial matches" from the soup
    if 'partial match' in soup.getText():
        try:
            partmatch = [x for x in soup.find_all('div', class_='italic') if 'partial match' in x.getText()][0]
            for div in partmatch.findNextSiblings():
                div.decompose()
        except:
            # TODO: this is bad, we should probably do another check instead with the result items first
            # TODO: make this dependent on the output columns somehow (also for all the other toolsw
            return pd.DataFrame()

    result_items = [x.find('a').find('div').findNextSibling() 
                    for x in soup.find_all('div', class_='h-[125]')]

    if not result_items:
        # TODO: remove this repeated thing
        return pd.DataFrame()

    # TODO: add in link 
    # TODO: add in file extension
    result_items_data = []
    for x in result_items:
        file_details = x.find('div', class_='text-xs')
        publisher = x.find('div', class_='text-sm')
        author = x.find('div', class_='italic')
        title = x.find('h3')

        file_details_text = get_text(file_details)

        try:
            filesize_mb = float(re.search(r'(\S+)MB', file_details_text).group(1).replace('<', ''))
        except:
            filesize_mb = 0

        try:
            item_language = re.search(r'^.*?\[(.*?)\].*MB', file_details_text).group(1)
        except:
            item_language = ''

        try:
            item_filetype = re.search(r'(\w+), \S+MB', file_details_text).group(1)
        except:
            item_filetype = ''

        try:
            filename = re.search(r'"(.+)"', file_details_text).group(1)
        except:
            filename = ''


        # TODO: add a second filter on the returned data based on the input arguments just
        # to make it extra sure

        result_items_data.append({
            "title": get_text(title).strip(),
            "author": get_text(author).strip(),
            "publisher": get_text(publisher).strip(),
            "filesize_mb": filesize_mb,
            "language": item_language,
            "filetype": item_filetype,
            "filename": filename,
        })

    data = pd.DataFrame(result_items_data)

    return data


if __name__ == '__main__':
    main()
