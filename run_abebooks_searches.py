import tools.abebooks as abe
import pandas as pd

input_filename = 'all_books.txt'

with open(input_filename, encoding='utf-8') as f:
    raw_inputs = f.read()

inputs = [x.strip().split(';') for x in raw_inputs.strip().split('\n')]
inputs_table = pd.DataFrame(inputs, columns=['author_search', 'title_search', 'binding']).set_index(['author_search', 'title_search']).assign(dummy=1).drop(columns='binding')

outputs_list = [abe.run_abebooks_search(author=author, title=title, binding=binding).assign(author_search=author, title_search=title) for author, title, binding in inputs]

outputs = pd.concat(outputs_list, ignore_index=True)

grouped = outputs.groupby(['title_search', 'author_search'])

aggregated = (outputs
    .loc[grouped['total_price_cad'].idxmin()]
    .sort_values('price_cad')
    .set_index(['title_search', 'author_search'])
    .join(grouped['title'].count().rename('item_count').to_frame())
    [['title', 'author', 'seller', 'binding', 'price_cad', 'total_price_cad', 'item_count']]
)

aggregated_full = (inputs_table
    .join(aggregated)
    .drop(columns='dummy')
    #.reset_index(level='author_search', drop=True)
    .sort_values('price_cad')
)

localprices = (outputs
    .loc[lambda t: t.in_edmonton]
    [['title', 'author', 'seller', 'binding', 'price_cad']]
    .sort_values('price_cad')
    .assign(seller=lambda t: t.seller.str.replace(', Edmonton, Canada', ''))
    .reset_index(drop=True)
)
