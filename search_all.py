from tools import abebooks, bibliocommons, goodreads, annas_archive
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--author", "-a")
    parser.add_argument("--title", "-t")
    parser.add_argument("--keywords", "-k")
    args = parser.parse_args()
    lookup_everything(author=args.author, title=args.title, keywords=args.keywords)



def lookup_everything(author: str=None, title: str=None, keywords: str=None):

    search_string = ' '.join(filter(bool, [author, title, keywords]))

    datasets = {
        "abebooks": abebooks.run_abebooks_search(author=author, title=title, keywords=keywords)[['title', 'author', 'binding', 'condition', 'seller', 'price_cad', 'total_price_cad']],
        "epl": bibliocommons.search_epl(author=author, title=title, anywhere=keywords)[['title', 'author', 'format_description', 'availability_status', 'hold_counts']],
        "cpl": bibliocommons.search_cpl(author=author, title=title, anywhere=keywords, formatcode='EBOOK')[['title', 'author', 'format_description', 'availability_status', 'hold_counts']],
        "goodreads": goodreads.run_goodreads_search(search_string)[['title', 'author', 'avg_rating', 'num_ratings']],
        "annas": annas_archive.run_annas_archive_search(search_string=search_string)[['title', 'author', 'filetype', 'filesize_mb']],
    }

    for k, v in datasets.items():
        print('-'*100)
        print(k.upper() + ':' + '\n')
        print(v.head(8) if not v.empty else "no results")
        print('\n\n')

# TODO: bug - CPL and ANNAS are getting no results for: lookup_everything(author='rowling', title='harry potter chamber secrets')
# TODO: bug - ABEBOOKS fails on lookup_everything(author='tartt', title='goldfinch')





if __name__ == '__main__':
    main()
