WITH holes AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'holes_raw') }}
)

SELECT
  CAST(course_id AS STRING) AS course_id,
  CAST(hole_id AS STRING) AS hole_id,
  CAST(par AS INT) AS par,
  CAST(satellite_yardage AS INT) AS satellite_yardage,
  CAST(scorecard_yardage AS INT) AS scorecard_yardage
FROM holes
