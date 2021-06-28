-- songs table
with final as (
    select
        *
    from {{ ref('stg_user_recent_songs') }}
),
fact_songs as (
    select
        songplays_id,
        track_id,
        track_played_at,
        album_id,
        artist_id,
        artist_id_others,
        artist_name_others
    from final
)
select
    *
from fact_songs