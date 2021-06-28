-- top artists table
with final as (
    select
        *
    from {{ ref('stg_user_top_artists') }}
),
fact_top_artists as (
    select
        artist_rank,
        artist_id
    from final
)
select
    *
from fact_top_artists