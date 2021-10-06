WITH shot_types AS (
  SELECT
    *
  FROM {{ source('hit_bombs_attack_pins', 'shot_types_raw') }}
)

SELECT
  CAST(id AS STRING) AS shot_type_id,
  CAST(name AS STRING) AS name
FROM shot_types
