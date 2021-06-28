-- albums table
{% set tables = ['stg_user_recent_songs', 'stg_user_top_tracks'] %}

{% for table in tables %}
    select distinct
        album_id,
        album_name,
        album_release_year,
        album_type                                   
    from {{ ref(table) }}
    {% if not loop.last -%} union {%- endif %}
{% endfor %}