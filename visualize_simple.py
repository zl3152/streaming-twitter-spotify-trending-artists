import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from cassandra.cluster import Cluster
import pandas as pd

# Connect directly to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('spotify')

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1('TRENDING ARTISTS ON TWITTER', style={'textAlign': 'center'}),
    html.P('Real-time visualization of trending artists'),
    
    html.Div([
        html.Span('Total tweets: '),
        html.Span(id='total-tweets', style={'fontSize': '24px', 'fontWeight': 'bold'})
    ], style={'textAlign': 'center', 'margin': '20px'}),
    
    dcc.Graph(id='live-graph'),
    dcc.Interval(id='interval', interval=5000)
])

@app.callback(
    [Output('live-graph', 'figure'),
     Output('total-tweets', 'children')],
    [Input('interval', 'n_intervals')])
def update_dashboard(n):
    try:
        # Get all data from Cassandra
        query = "SELECT artist, count FROM artistshare"
        rows = session.execute(query)
        
        # Aggregate in Python
        artist_counts = {}
        total_tweets = 0
        
        for row in rows:
            artist = row.artist
            count = row.count
            total_tweets += count
            
            if artist in artist_counts:
                artist_counts[artist] += count
            else:
                artist_counts[artist] = count
        
        # Convert to DataFrame
        data = [{'artist': artist, 'count': count} 
                for artist, count in artist_counts.items()]
        df = pd.DataFrame(data)
        
        if not df.empty:
            # Sort and get top 20
            df = df.sort_values('count', ascending=False).head(20)
            
            # Create bar chart
            figure = {
                'data': [go.Bar(
                    x=df['count'],
                    y=df['artist'],
                    orientation='h',
                    marker={'color': 'rgb(26, 118, 255)'}
                )],
                'layout': go.Layout(
                    title='Top Artists by Tweet Count',
                    xaxis={'title': 'Number of Related Tweets'},
                    yaxis={'title': 'Artists', 'autorange': 'reversed'},
                    height=700,
                    margin={'l': 200}
                )
            }
        else:
            figure = {'data': [], 'layout': {'title': 'No data available'}}
        
        return figure, str(total_tweets)
        
    except Exception as e:
        print(f"Error: {e}")
        return {'data': [], 'layout': {'title': f'Error: {e}'}}, '0'

if __name__ == '__main__':
    app.run_server(debug=True, port=8051)