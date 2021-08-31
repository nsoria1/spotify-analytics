from datetime import datetime
import pandas as pd
import spotipy
import spotipy.util as util
import cred

##############################################################################
#### Handle connections functions
##############################################################################

def _get_token(scope):
    '''
    Function which will obtain the token for spotify api
    '''
    token = util.prompt_for_user_token(
        #username=cred.username,
        scope=scope,
        client_id=cred.client_id,
        client_secret=cred.client_secret,
        redirect_uri=cred.redirect_url
    )

    return token

def _get_connection(token):
    '''
    Create the connection to spotify API
    '''
    spotify = spotipy.Spotify(auth=token)
    return spotify

def _invoke_method(connection, query, limit=10, list=None):
    '''
    Function to invoke the method required to download data
    '''
    if query == 'current_user_top_artists':
        data = connection.current_user_top_artists(limit=limit)
        return parse_top_artists(data)
    elif query == 'current_user_recently_played':
        data = connection.current_user_recently_played(limit=limit)
        return parse_songplays(data, scope='user-read-recently-played')
    elif query == 'current_user_top_tracks':
        data = connection.current_user_top_tracks(limit=limit)
        return parse_top_tracks(data)
    elif query == 'audio_features':
        return connection.audio_features(list)
    elif query == 'artists':
        return connection.artists(list)

def _get_spotify_data(scope, query, limit=10):
    '''
    Function which will retrieve spotify data relying 
    on spotipy library
    '''
    token = _get_token(scope)
    spotify = _get_connection(token=token)
    data = _invoke_method(spotify, query, limit)
    return data

##############################################################################
#### Parse data functions
##############################################################################


def parse_json(data, columns, *args, **kwargs):
    '''
    Parses response data in JSON format
    '''
    if not (kwargs.get('result_key')==None):
        data = data[kwargs['result_key']]
    df = pd.json_normalize(data).reset_index()
    df['index'] = df['index'] + 1
    df = df[columns.keys()].rename(columns=columns)
    return df

def parse_top_artists(data):
    '''
    Parses top artists of user
    '''
    columns = {
        'index': 'artist_rank',
        'id': 'artist_id',
        'name': 'artist_name',
        'genres': 'artist_genre',
        'popularity': 'artist_popularity',
        'followers.total': 'artist_followers'
    }
    top_artists = parse_json(data=data, columns=columns, result_key='items')

    (top_artists['artist_genre'], 
        top_artists['artist_genre_others']) = zip(*top_artists['artist_genres'].apply(parse_primary_other))

    return top_artists

def parse_primary_other(parse_list=[]):
    '''
    Parses primary and other values for lists
    '''
    parse_list = parse_list.copy()
    try:
        primary = parse_list.pop(0)
    except IndexError:
        primary = None
    others = ", ".join(parse_list)
    return primary, others


def parse_songplays(data, scope, columns=None):
    '''
    Parses songplays data of user
    '''
    if columns is None:
        columns = {
            'index': 'songplays_id',
            'track.id': 'track_id',
            'track.name': 'track_name', 
            'track.artists': 'artists', 
            'track.duration_ms' : 'track_duration', 
            'track.explicit': 'track_is_explicit', 
            'track.popularity': 'track_popularity',
            'played_at': 'track_played_at',
            'track.album.id': 'album_id', 
            'track.album.name': 'album_name', 
            'track.album.release_date': 'album_release_year', 
            'track.album.type': 'album_type'}
    songplays = parse_json(data=data, columns=columns, result_key='items')

    # Parse artists
    def parse_artist(artists):
        # parse primary and other artists
        artist_name, artist_name_others = parse_primary_other([artist['name'] for artist in artists])
        artist_id, artist_id_others = parse_primary_other([artist['id'] for artist in artists])
        return artist_name, artist_name_others, artist_id, artist_id_others
    (songplays['artist_name'], songplays['artist_name_others'],
        songplays['artist_id'], songplays['artist_id_others']) = zip(*songplays['artists'].apply(parse_artist))
    
    # Get release year
    def parse_year(album_release_year):
        try:
            year = datetime.strptime(album_release_year, '%Y-%m-%d').year
        except (ValueError, NameError):
            year = datetime.strptime(album_release_year, '%Y').year
        return year
    songplays['album_release_year'] = songplays['album_release_year'].apply(lambda x: parse_year(x))
    
    # Convert timestamp
    try:
        songplays['track_played_at'] = songplays['track_played_at'] \
                                        .apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    except KeyError:
        pass
    
    #Convert track duration
    songplays['track_duration'] = songplays['track_duration'].apply(lambda x: x/60000)
    
    # Get features
    def get_features(key, method, df, columns, result_key=None):
        conn = _get_connection(token=_get_token(scope))
        features = _invoke_method(conn, method, list=(df[key].values.tolist()))
        features_df = parse_json(data=features, columns=columns, result_key=result_key)
        features_df.drop_duplicates(subset=key, inplace=True)
        df = df.merge(features_df, how='left', on=key)
        return df
    
    # Get track features
    track_features_columns = {
        'id': 'track_id',
        'danceability': 'track_danceability', 
        'energy': 'track_energy', 
        'key': 'track_key', 
        'loudness': 'track_loudness', 
        'mode': 'track_mode', 
        'speechiness': 'track_speechiness', 
        'acousticness': 'track_acousticness', 
        'instrumentalness': 'track_instrumentalness', 
        'liveness': 'track_liveness', 
        'valence': 'track_valence'}
    
    songplays = get_features(key='track_id', 
                            method='audio_features', 
                            df=songplays, 
                            columns=track_features_columns)
    # Get artist features
    artist_features_columns = {
        'id': 'artist_id',
        'genres': 'artist_genres',
        'popularity': 'artist_popularity',
        'followers.total': 'artist_followers'}
    
    songplays = get_features(key='artist_id', 
                            method='artists', 
                            df=songplays, 
                            columns=artist_features_columns,
                            result_key='artists')
    # Parse genres
    (songplays['artist_genre'], 
        songplays['artist_genre_others']) = zip(*songplays['artist_genres'].apply(parse_primary_other))
        
    songplays.drop(columns=['artist_genres', 'artists'], axis=1, inplace=True)
    return songplays

def parse_top_tracks(data):
    '''
    Parses top tracks of user
    '''
    columns = {
        'index': 'track_rank',
        'id': 'track_id',
        'name': 'track_name', 
        'artists': 'artists', 
        'duration_ms' : 'track_duration', 
        'explicit': 'track_is_explicit', 
        'popularity': 'track_popularity',
        'album.id': 'album_id', 
        'album.name': 'album_name', 
        'album.release_date': 'album_release_year', 
        'album.type': 'album_type'
    }
    top_tracks = parse_songplays(data=data, scope='user-read-recently-played', columns=columns)
    return top_tracks