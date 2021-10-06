WITH rounds AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'rounds_raw') }}
)

SELECT
  CAST(avg_temp AS INT) AS avg_temp,
  CAST(avg_windspeed_mph AS INT) AS avg_windspeed_mph,
  CAST(course_id AS STRING) AS course_id,
  CAST(day AS DATE) AS day,
  CAST(full_round AS BOOL) AS full_round,
  CAST(round_id AS STRING) AS round_id
FROM rounds
