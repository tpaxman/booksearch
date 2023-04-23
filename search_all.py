from tools import abebooks, bibliocommons, goodreads, annas_archive

def lookup_everything(author: str=None, title: str=None, keywords: str=None):

    search_string = ' '.join(filter(bool, [author, title, keywords]))

    abebooks_results = abebooks.run_abebooks_search(author=author, title=title, keywords=keywords)
    epl_results = bibliocommons.search_epl(author=author, title=title, anywhere=keywords)
    cpl_results = bibliocommons.search_cpl(author=author, title=title, anywhere=keywords, formatcode=['EBOOK'])
    goodreads_results = goodreads.run_goodreads_search(search_string)
    annas_archive_results = annas_archive.run_annas_archive_search(search_string=search_string) [['title', 'author', 'filetype', 'filesize_mb']]

    return (abebooks_results, epl_results, cpl_results, goodreads_results, annas_archive_results)



