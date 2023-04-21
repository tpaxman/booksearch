import abebooks as abe
import pandas as pd

input_filename = 'grandpa_meeks_collection.txt'

with open(input_filename, encoding='utf-8') as f:
    raw_inputs = f.read()

inputs = [x.strip().split(';') for x in raw_inputs.strip().split('\n')]

outputs_list = [abe.run_abebooks_search(author=author, title=title, binding=binding).assign(author_search=author, title_search=title) for author, title, binding in inputs]

outputs = pd.concat(outputs_list, ignore_index=True)

grouped = outputs.groupby(['title_search', 'author_search'])

aggregated = (outputs
    .loc[grouped['total_price_cad'].idxmin()]
    .sort_values('price_cad')
    .set_index(['title_search', 'author_search'])
    .join(grouped['title'].count().rename('item_count').to_frame())
    .reset_index(drop=True)
    [['title', 'author', 'seller', 'binding', 'price_cad', 'total_price_cad', 'item_count']]
)

localprices = (outputs
    .loc[lambda t: t.in_edmonton]
    [['title', 'author', 'seller', 'binding', 'price_cad']]
    .sort_values('price_cad')
    .assign(seller=lambda t: t.seller.str.replace(', Edmonton, Canada', ''))
    .reset_index(drop=True)
)
