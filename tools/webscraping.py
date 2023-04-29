import re

def refilter(full_string, search_string):
    """ ensure all words in search_string appear in full_string """
    if search_string:
        search_words = re.findall(r'\w+', search_string)
        return all(w.lower() in full_string.lower() for w in search_words)
    else:
        return True

def get_text(elem) -> str:
    """ get text from a BeautifulSoup element if the element exists """
    return elem.getText() if elem else ''


def get_response_content(search_url: str) -> bytes:
    response = requests.get(search_url)
    results_html = response.content
    return results_html


