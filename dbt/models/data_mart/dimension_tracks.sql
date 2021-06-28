-- tracks table
{% set tables = ['stg_user_recent_songs', 'stg_user_top_tracks'] %}

{% for table in tables %}
    select distinct
        track_id,
        track_name,
        track_duration,
        track_is_explicit,
        track_popularity,
        track_danceability,
        track_energy,
        track_key,
        track_loudness,
        track_mode,
        track_speechiness,
        track_acousticness,
        track_instrumentalness,
        track_liveness,
        track_valence
    from {{ ref(table) }}
    {% if not loop.last -%} union {%- endif %}
{% endfor %}