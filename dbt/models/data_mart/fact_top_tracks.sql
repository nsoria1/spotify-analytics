-- top tracks table
with final as (
    select
        *
    from {{ ref('stg_user_top_tracks') }}
),
fact_top_tracks as (
    select
        track_rank,
        track_id,
        album_id,
        artist_name_others,
        artist_id,
        artist_id_others
    from final
)
select
    *
from fact_top_tracks