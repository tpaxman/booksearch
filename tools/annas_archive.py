import requests
import re
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
        '&'.join([x for x in [
            f"q={quoted_search_string}",
            f"lang={language}" if language else '',
            f"ext={filetype}" if filetype else '',
        ] if x])
    )
    print(f"ANNA'S ARCHIVE: {search_url}")

    search_results_html = (requests
        .get(search_url)
        .content
        .decode('utf-8')
        .replace('<!--', '')
        .replace('-->', '')
    )

    soup = BeautifulSoup(search_results_html, features='html.parser')

    # remove all the "partial matches" from the soup
    if 'partial match' in soup.getText():
        try:
            partmatch = [x for x in soup.find_all('div', class_='italic') if 'partial match' in x.getText()][0]
            for div in partmatch.findNextSiblings():
                div.decompose()
        except:
            # TODO: this is bad, we should probably do another check instead with the result items first
            # TODO: make this dependent on the output columns somehow (also for all the other toolsw
            return pd.DataFrame(columns=["title", "author", "publisher", "filesize_mb", "language" , "filetype", "filename"])

    try:
        result_items = [x.find('a').find('div').findNextSibling() 
                        for x in soup.find_all('div', class_='h-[125]')]
    except:
        # TODO: remove this repeated thing
        return pd.DataFrame(columns=["title", "author", "publisher", "filesize_mb", "language" , "filetype", "filename"])

    # TODO: extract this dumb function
    get_text = lambda elem: elem.getText() if elem else ''
    get_group = lambda result, group_id: result.group(group_id) if result else ''

    # TODO: add in link 
    # TODO: add in file extension
    result_items_data = []
    for x in result_items:
        file_details = x.find('div', class_='text-xs')
        publisher = x.find('div', class_='text-sm')
        author = x.find('div', class_='italic')
        title = x.find('h3')

        file_details_text = get_text(file_details)

        # filesize_mb = float(get_group(re.search(r'(\S+)MB', file_details_text), 1).replace('<', ''))
        # item_language = get_group(re.search(r'^.*?\[(.*?)\].*MB', file_details_text), 1)
        # item_filetype = get_group(re.search(r', (\w+), \S+MB', file_details_text), 1)

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
            "title": get_text(title),
            "author": get_text(author),
            "publisher": get_text(publisher),
            "filesize_mb": filesize_mb,
            "language": item_language,
            "filetype": item_filetype,
            "filename": filename,
        })

    data = pd.DataFrame(result_items_data)

    return data


