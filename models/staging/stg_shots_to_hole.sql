WITH shots_to_hole AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'shots_to_hole_raw') }}
)

SELECT
  CAST(distance AS INT) AS distance,
  ROUND(CAST(pro_strokes_to_hole AS FLOAT64), 2) AS pro_strokes_to_hole,
  ROUND(CAST(scratch_strokes_to_hole AS FLOAT64), 2) AS scratch_strokes_to_hole,
  CAST(shot_type AS STRING) AS shot_type
FROM shots_to_hole
