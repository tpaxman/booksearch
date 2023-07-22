import re
import pandas as pd
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from typing import Literal
import tabulate


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


def compose_search_url(
    keywords: str,
    filetype: FILETYPES=None,
    language: VALID_LANGUAGES=None,
    content_type: CONTENT_TYPES="book_any",
    sortby: SORT_OPTIONS=None,
) -> str:
    """
    Compose the search URL for Anna's Archive
    """
    # note: sortby=None defaults to 'most relevant'
    root_url = "https://annas-archive.org/search?"
    arguments = {
        "q": quote_plus(keywords),
        "filetype": filetype,
        "content": content_type,
        "lang": language,
        "ext": filetype,
        "sort": sortby,
    }
    arguments_string = '&'.join(f'{k}={v}' for k, v in arguments.items() if v)
    search_url = root_url + arguments_string
    return search_url


def parse_results(content: bytes) -> pd.DataFrame:
    # results come with some dirty html comments that need to be removed
    results_uncommented_html = (
        content
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

    result_items_data = []
    if result_items:
        # TODO: add in link 
        # TODO: add in file extension
        result_items_data = []
        for x in result_items:
            file_details = x.find('div', class_='text-xs')
            publisher = x.find('div', class_='text-sm')
            author = x.find('div', class_='italic')
            title = x.find('h3')

            file_details_text = _get_text(file_details)

            try:
                filesize_mb = float(
                    re.search(r'(\S+)MB', file_details_text).group(1).replace('<', '')
                )
            except:
                filesize_mb = 0

            try:
                language = re.search(r'^.*?\[(.*?)\].*MB', file_details_text).group(1)
            except:
                language = ''

            try:
                filetype = re.search(r'(\w+), \S+MB', file_details_text).group(1)
            except:
                filetype = ''

            try:
                filename = re.search(r'"(.+)"', file_details_text).group(1)
            except:
                filename = ''


            # TODO: add a second filter on the returned data based on the input arguments just
            # to make it extra sure

            result_items_data.append({
                "title": _get_text(title).strip(),
                "author": _get_text(author).strip(),
                "publisher": _get_text(publisher).strip(),
                "filesize_mb": filesize_mb,
                "language": language,
                "filetype": filetype,
                "filename": filename,
            })

    data = (
        pd.DataFrame(result_items_data)
        .reindex(['title', 'author', 'publisher','filesize_mb', 'language', 
        'filetype', 'filename'], axis=1)
    )

    return data


def _get_text(elem) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText() if elem else ''
