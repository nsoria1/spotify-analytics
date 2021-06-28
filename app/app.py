import core

# configs
limit=50
dict = {
    'user_recent_songs': {'current_user_recently_played': 'user-read-recently-played'},
    'user_top_artists': {'current_user_top_artists': 'user-top-read'},
    'user_top_tracks': {'current_user_top_tracks': 'user-top-read'}
    }

def main():
    for data, scope_dict in dict.items():
        print(f'--- Getting the following data: {data} --')
        for query, scope in scope_dict.items():

            # invoking main function to get spotify data

            df = core._get_spotify_data(scope, query, limit)
            name = '../data/{fname}.csv'.format(fname=data)
            df.to_csv(name, index=False, encoding='utf-8')

if __name__ == '__main__':
    main()