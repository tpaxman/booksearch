import pathlib
import math
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import plotly.graph_objects as go
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
    .assign(searched = lambda t: t.searched.str.replace('abebooks - ', '').str.replace(' - ', '-'))
)

#highest_price = df_source.price.max()

# START APP
app = Dash(__name__)


static_fig = fig = px.scatter(
    df_source,
    x='price',
    y='searched',
    color='signed',
    hover_data=['title', 'author', 'seller_name', 'seller_city', 'seller_country', 'binding'],
    width=1200,
    height=2000,
) #, range_x=[0, highest_price])

static_fig.update_layout(
    xaxis = dict(
        tickmode = 'linear',
        tick0 = 0,
        dtick = 100,
    )
)


app.layout = html.Div([
    html.H1(children='Book Prices', style={'textAlign':'center'}),
    dcc.Dropdown(df_source.searched.unique(), df_source.searched.iloc[0], id='searched-dropdown'),
    #dcc.Graph(id='price_histogram'),
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
    dcc.Graph(id='static_graph', figure=static_fig)
])


@callback(
    Output('price_bar', 'figure'),
    Input('searched-dropdown', 'value')
)
def update_bar(value):
    dff = (
        df_source
        .loc[lambda t: t.searched.eq(value)]
        #.assign(x_label = lambda t: t.title + '(' + t.binding + '): ' + t.seller_name)
        .assign(details=lambda t: t.binding.fillna('?') + t.signed.map({True: '-signed', False: ''}))
        .sort_values('price')
        .reset_index(drop=True)
    )

    fig = px.bar(
        dff,
        y='price',
        color='signed',
        text='price',
        hover_data=['title', 'author', 'seller_name', 'seller_city', 'seller_country', 'binding'],
    ) #, range_x=[0, highest_price])

    fig.update_layout(
        bargap=0.2,
        xaxis = dict(
            tickmode = 'linear',
            #tick0 = 0,
            dtick = 1,
        )
    )

    return fig

#@callback(
#    Output('price_histogram', 'figure'),
#    Input('searched-dropdown', 'value')
#)
#def update_histogram(value):
#    dff = df_source.loc[lambda t: t.searched.eq(value)]
#    upper_price = dff.price.max()
#    num_results = dff.shape[0]
#    dtick = math.ceil(upper_price / 50)
#    nbins = math.ceil(num_results / 6)
#    fig = px.histogram(dff, x='price', nbins=nbins) #, range_x=[0, highest_price])
#    fig.update_layout(
#        bargap=0.2,
#        xaxis = dict(
#            tickmode = 'linear',
#            tick0 = 0,
#            dtick = dtick,
#        )
#    )
#    return fig


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

