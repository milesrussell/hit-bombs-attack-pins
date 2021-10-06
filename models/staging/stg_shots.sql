WITH shots AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'shots_raw') }}
)

SELECT
  CAST(hole_id AS STRING) AS hole_id,
  CAST(hole_shot_seq AS INT) AS hole_shot_seq,
  CAST(measured_distance AS INT) AS measured_distance,
  CAST(round_id AS STRING) AS round_id,
  CAST(shot_id AS STRING) AS shot_id,
  CAST(shot_type AS STRING) AS shot_type
FROM shots
