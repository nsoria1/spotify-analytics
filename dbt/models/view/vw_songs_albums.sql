-- song albums table
with songs_albums as (
    select
        f.*,
        a.album_name,
        a.album_release_year,
        a.album_type,
        t.track_name,
        track_popularity,
        track_danceability,
        track_speechiness
    from {{ ref('fact_songs') }} f
    left join {{ ref('dimension_tracks') }} t using (track_id)
    left join {{ ref('dimension_album') }} a using (album_id)
)
select
    *
from songs_albums