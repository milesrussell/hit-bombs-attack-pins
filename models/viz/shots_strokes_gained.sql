WITH shots AS (
  SELECT
    *
  FROM {{ ref('fct_shots') }}
),

shots_to_hole AS (
  SELECT
    *
  FROM {{ ref('stg_shots_to_hole') }}
),

shots_with_avgs AS (
  SELECT
    shots.shot_id,
    shots.hole_shot_seq,
    shots.shot_type,
    shots.hole_id,
    shots.distance,
    shots.round_id,
    shots_to_hole.pro_strokes_to_hole,
    shots_to_hole.scratch_strokes_to_hole
  FROM shots
  LEFT JOIN shots_to_hole ON shots.shot_type_id = shots_to_hole.shot_type
    AND shots.distance = shots_to_hole.distance
)

SELECT
  shot_id,
  hole_shot_seq,
  shot_type,
  hole_id,
  distance,
  round_id,
  pro_strokes_to_hole,
  scratch_strokes_to_hole,

  {% set fields = ['distance', 'shot_type', 'pro_strokes_to_hole', 'scratch_strokes_to_hole'] %}

  {% for field in fields %}

  LEAD({{ field }}) OVER(
    PARTITION BY
      hole_id,
      round_id
    ORDER BY
      hole_shot_seq
  ) AS resulting_{{ field }}

  {% if not loop.last %} , {% endif %}

  {% endfor %}
FROM shots_with_avgs
