import pathlib
import math
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import pandas as pd


# IMPORT AND CLEAN DATA
df_source = (
    pd.concat(
        [
            pd.read_pickle(x).assign(searched=x.stem)
            for x in pathlib.Path('output/flip').glob('abebooks*pkl')
        ],
        ignore_index=True
    )
    .assign(signed = lambda t: t.about.str.lower().str.contains('signed'))
    .assign(searched = lambda t: t.searched.str.replace('abebooks - ', ''))
)

#highest_price = df_source.price.max()

# START APP
app = Dash(__name__)

app.layout = html.Div([
    html.H1(children='Book Prices', style={'textAlign':'center'}),
    dcc.Dropdown(df_source.searched.unique(), '', id='searched-dropdown'),
    dcc.Graph(id='price_histogram'),
    dcc.Graph(id='price_bar'),
    dash_table.DataTable(
        id='summary_table',
        style_data={
            'whiteSpace': 'normal',
            'height': 'auto',
        },
        style_as_list_view=True,
        style_cell={'padding': '5px'},
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold'
        },
    ),
])

@callback(
    Output('price_bar', 'figure'),
    Input('searched-dropdown', 'value')
)
def update_histogram(value):
    dff = df_source.loc[lambda t: t.searched.eq(value)]
    upper_price = dff.price.max()
    num_results = dff.shape[0]
    dtick = math.ceil(upper_price / 50)
    nbins = math.ceil(num_results / 6)
    fig = px.bar(dff, y='price') #, range_x=[0, highest_price])
    fig.update_layout(
        bargap=0.2,
        xaxis = dict(
            tickmode = 'linear',
            tick0 = 0,
            dtick = dtick,
        )
    )
    return fig

@callback(
    Output('price_histogram', 'figure'),
    Input('searched-dropdown', 'value')
)
def update_histogram(value):
    dff = df_source.loc[lambda t: t.searched.eq(value)]
    upper_price = dff.price.max()
    num_results = dff.shape[0]
    dtick = math.ceil(upper_price / 50)
    nbins = math.ceil(num_results / 6)
    fig = px.histogram(dff, x='price', nbins=nbins) #, range_x=[0, highest_price])
    fig.update_layout(
        bargap=0.2,
        xaxis = dict(
            tickmode = 'linear',
            tick0 = 0,
            dtick = dtick,
        )
    )
    return fig


@callback(
    Output('summary_table', 'data'),
    Input('searched-dropdown', 'value')
)
def update_summary_table(value):
    data = (
        df_source
        .loc[lambda t: t.searched.eq(value)]
        [['title', 'author', 'price', 'currency', 'binding', 'about', 'seller_name']]
        .sort_values('price')
        .to_dict('records')
    )
    return data


if __name__ == '__main__':
    app.run(debug=True)

