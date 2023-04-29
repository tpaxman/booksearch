from bs4 import BeautifulSoup
import pathlib
import tools.abebooks as abe
import tools.annas_archive as ann
import tools.bibliocommons as bib


# abe.parse_abebooks_results_html
# bib.parse_bibliocommons_search_results
# annas_archive.parse_annas_archive_results

def read_into_soup(filename: str) -> BeautifulSoup:
    with open(filename, encoding='utf-8') as f:
        return BeautifulSoup(f.read())


def read(filename: str) -> BeautifulSoup:
    with open(filename, encoding='utf-8') as f:
        return f.read()


source_folder = 'outputs/raw_html'

annas_paths = list(pathlib.Path(source_folder).glob('annas*.html'))
annas_soups = {x.stem: read_into_soup(x) for x in annas_paths}
annas_tables = {k: ann.parse_annas_archive_results(v) for k, v in annas_soups.items()}

df_annas = (pd
    .concat(annas_tables.values(), keys=annas_tables.keys())
    .reset_index()
    .drop(columns='level_1')
    .rename(columns={'level_0': 'filestem'})
    .assign(source = lambda t: t.filestem.str.replace(r'_\d+$', '', regex=True))
    .assign(goodreads_id = lambda t: t.filestem.str.replace(r'\D+', '', regex=True).astype('int'))
    .set_index(['goodreads_id', 'source'])
)

df_annas.to_csv('outputs/summaries/annas_archive.csv')


abebooks_edmonton_soups = {x.stem: read(x) for x in abebooks_edmonton_paths}
abebooks_edmonton_tables = {k: abe.parse_abebooks_results_html(v) for k, v in abebooks_edmonton_soups.items()}
abebooks_edmonton = (pd
    .concat(abebooks_edmonton_tables.values(), keys=abebooks_edmonton_tables.keys())
    .reset_index()
    .drop(columns='level_1')
    .rename(columns={'level_0': 'filestem'})
    .assign(source = lambda t: t.filestem.str.replace(r'_\d+$', '', regex=True))
    .assign(goodreads_id = lambda t: t.filestem.str.replace(r'\D+', '', regex=True).astype('int'))
    .set_index(['goodreads_id', 'source'])
)

abebooks_paths = [x for x in pathlib.Path(source_folder).glob('abebooks*.html') if 'edmonton' not in str(x)]


def combine_into_table(paths: list, reader, parser) -> pd.DataFrame:
    print('reading')
    htmls = {x.stem: reader(x) for x in paths}
    print('parsing')
    tables = {k: parser(v) for k, v in htmls.items()}
    df = (pd
        .concat(tables.values(), keys=tables.keys())
        .reset_index()
        .drop(columns='level_1')
        .rename(columns={'level_0': 'filestem'})
        .assign(source = lambda t: t.filestem.str.replace(r'_\d+$', '', regex=True))
        .assign(goodreads_id = lambda t: t.filestem.str.replace(r'\D+', '', regex=True).astype('int'))
        .set_index(['goodreads_id', 'source'])
    )
    return df

abebooks_edmonton_paths = list(pathlib.Path(source_folder).glob('abebooks_edmonton*.html'))
abebooks_edmonton = combine_into_table(abebooks_paths, read, abe.parse_abebooks_results_html)
abebooks_edmonton.to_csv('outputs/summaries/abebooks_edmonton.csv')

abebooks_paths = [x for x in pathlib.Path(source_folder).glob('abebooks*.html') if 'edmonton' not in str(x)]
abebooks = combine_into_table(abebooks_paths, read, abe.parse_abebooks_results_html)
abebooks.to_csv('outputs/summaries/abebooks.csv')

epl_paths = pathlib.Path(source_folder).glob('epl*.html')
epl = combine_into_table(epl_paths, read, bib.parse_bibliocommons_search_results)
epl.to_csv('outputs/summaries/epl.csv')

cpl_paths = pathlib.Path(source_folder).glob('cpl*.html')
cpl = combine_into_table(cpl_paths, read, bib.parse_bibliocommons_search_results)
cpl.to_csv('outputs/summaries/cpl.csv')


