{{ config(
    materialized = 'incremental',
    unique_key = 'movie_id'
) }}

select
    movie_id,
    title,
    type,
    release_year,
    genre,
    duration_minutes,
    country,
    rating,
    imdb_score,
    added_date
from {{ source('RAW', 'NETFLIX') }}

{% if is_incremental() %}

-- Only load new rows based on latest added_date
where added_date > (select max(added_date) from {{ this }})

{% endif %}
