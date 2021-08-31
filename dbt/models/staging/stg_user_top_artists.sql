-- staging user_top_artist
with final as (
    select
        *
    from {{ ref('user_top_artists') }}
),

stage_user_top_artist as (
    select
        artist_rank,
        artist_id,
        artist_name,
        artist_genre,
        artist_popularity,
        artist_followers,
        artist_genre_others
    from final
)
select
    *
from stage_user_top_artist