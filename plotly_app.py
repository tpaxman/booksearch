import pathlib
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd


# IMPORT AND CLEAN DATA
df = (
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

highest_price = df.price.max()

# START APP
app = Dash(__name__)

app.layout = html.Div([
    html.H1(children='Title of Dash App', style={'textAlign':'center'}),
    dcc.Dropdown(df.searched.unique(), '', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
])

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = df[df.searched==value]
    fig = px.histogram(dff, x='price', nbins=25, range_x=[0, highest_price])
    fig.update_layout(
        bargap=0.2,
        xaxis = dict(
            tickmode = 'linear',
            tick0 = 0,
            dtick = 50,
        )
    )
    return fig

if __name__ == '__main__':
    app.run(debug=True)

