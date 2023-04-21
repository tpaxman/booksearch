import abebooks as abe
import pandas as pd

input_filename = 'other_inputs.txt'

with open(input_filename, encoding='utf-8') as f:
    raw_inputs = f.read()

inputs = [x.strip().split(';') for x in raw_inputs.strip().split('\n')]

outputs = [abe.run_abebooks_search(author=author, title=title, binding=binding).assign(author_search=author, title_search=title) for author, title, binding in inputs]

non_empties = pd.concat([x for x in outputs if not x.empty], ignore_index=True)

# non_empties.to_csv('pairs_outputs.csv')

g = non_empties.groupby(['title_search', 'author_search'])

g1 = g['title'].count().rename('item_count').to_frame()

g2 = (non_empties
    .loc[g['total_price_cad'].idxmin()]
    .sort_values('total_price_cad')
    .set_index(['title_search', 'author_search'])
    .join(g1)
    [['title', 'author', 'seller', 'binding', 'price_cad', 'total_price_cad', 'item_count']]
)

g = (
    non_empties
    .loc[
        non_empties.groupby(['title_search', 'author_search'])['total_price_cad'].idxmin(), 
        ['title', 'author', 'seller', 'binding', 'price_cad', 'total_price_cad']
    ]
    .sort_values('total_price_cad')
    .reset_index(drop=True)
)

localprices = non_empties.query('in_edmonton')[['title', 'author', 'seller', 'binding', 'total_price_cad']].sort_values('total_price_cad')
