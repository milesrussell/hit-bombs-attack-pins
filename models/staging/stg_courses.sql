WITH courses AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'courses_raw') }}
)

SELECT
  CAST(course_id AS STRING) AS course_id,
  CAST(course_name AS STRING) AS course_name,
  CAST(satellite_yardage AS INT) AS satellite_yardage,
  CAST(scorecard_rating AS FLOAT64) AS scorecard_rating,
  CAST(scorecard_slope AS FLOAT64) AS scorecard_slope,
  CAST(scorecard_yardage AS INT) AS scorecard_yardage
FROM courses
