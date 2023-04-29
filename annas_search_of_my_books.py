import pandas as pd
import time
from tools.annas_archive import compose_annas_archive_search_url, get_annas_archive_results_html, parse_annas_archive_results

def get_with_pause(search_url):
    time.sleep(1)
    print(search_url)
    return get_annas_archive_results_html(search_url)

input_filepath = 'exploration/all_of_it.csv'
df_inputs = (pd
    .read_csv(input_filepath)
    .fillna({'language': 'en', 'title': '', 'author': ''})
    .assign(search_url = lambda t: t.apply(lambda r: compose_annas_archive_search_url(query=r.title + ' ' + r.author, language=r.language), axis=1))
    #.head(25)
    .assign(results_html = lambda t: t.search_url.apply(get_with_pause))
)

df_parsed = (df_inputs
    .assign(parsed_results = lambda t: t.results_html.apply(parse_annas_archive_results))
    .set_index(['title', 'author', 'search_url'])
    #.parsed_results
    # TODO: fix this
    #.apply(lambda df: df.loc[df.filetype.isin(('epub', 'pdf'))].sort_values('filetype').iloc[0] if not df.empty else pd.Series())
)


summarized = (df_parsed
    .reset_index()
    .rename(columns={'title': 'search_title', 'author': 'search_author'})
    .set_index(['search_title', 'search_author', 'search_url'])
    .pipe(lambda t: pd.concat(list(t.parsed_results), keys=t.index))
    .reset_index()
    .drop(columns='level_3')
    .assign(actual_name = lambda t: t.title + ', ' + t.author)
    .loc[lambda t: t.filetype.isin(('epub', 'pdf'))]
    .groupby(['search_title', 'search_author', 'search_url', 'filetype']).first()
    .reset_index()
    .set_index(['search_title', 'search_author', 'search_url', 'filetype'])['actual_name'].unstack()
)

(df_inputs
    [['title', 'author']]
    .merge(summarized, how='left', left_on=['title', 'author'], right_on=['search_title', 'search_author'])
).to_clipboard()

