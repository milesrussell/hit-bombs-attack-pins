WITH shots_to_hole AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'shots_to_hole_raw') }}
)

SELECT
  distance AS distance,
  ROUND(pro_strokes_to_hole, 2) AS pro_strokes_to_hole,
  ROUND(scratch_strokes_to_hole, 2) AS scratch_strokes_to_hole,
  shot_type AS shot_type
FROM shots_to_hole
