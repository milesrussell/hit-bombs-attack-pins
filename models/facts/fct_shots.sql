WITH shots AS (
  SELECT
    *
  FROM {{ ref('stg_shots') }}
),

holes AS (
  SELECT
    *
  FROM {{ ref('stg_holes') }}
),

shot_types AS (
  SELECT
    *
  FROM {{ ref('stg_shot_types') }}
),

shots_joined AS (
  SELECT
    shots.shot_id,
    shots.hole_shot_seq,
    shot_types.shot_type_id,
    shot_types.name AS shot_type,
    shots.hole_id,
    shots.measured_distance,
    holes.scorecard_yardage AS hole_scorecard_yardage,
    holes.satellite_yardage AS hole_satellite_yardage,
    shots.round_id,
    CASE
      WHEN shots.measured_distance IS NOT NULL THEN shots.measured_distance
      WHEN shots.measured_distance IS NULL
        AND shot_types.name = 'Tee' THEN holes.satellite_yardage
      WHEN shots.measured_distance IS NULL
        AND holes.satellite_yardage IS NULL
        AND shot_types.name = 'Tee' THEN holes.scorecard_yardage
      ELSE NULL
    END AS distance
  FROM shots
  JOIN shot_types ON shots.shot_type = shot_types.shot_type_id
  LEFT JOIN holes ON shots.hole_id = holes.hole_id
)

SELECT
  *
FROM shots_joined
