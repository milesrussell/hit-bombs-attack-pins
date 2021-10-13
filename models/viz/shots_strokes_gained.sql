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
),

shots_with_results AS (
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
    ) AS resulting_{{ field }},

    LEAD({{ field }}, 2) OVER(
      PARTITION BY
        hole_id,
        round_id
      ORDER BY
        hole_shot_seq
    ) AS two_strokes_later_{{ field }}

    {% if not loop.last %} , {% endif %}

    {% endfor %}
  FROM shots_with_avgs
)

SELECT
  *,
  {% set toggles = ['pro', 'scratch'] %}

  {% for toggle in toggles %}

  CASE
    WHEN resulting_shot_type IS NULL THEN {{ toggle }}_strokes_to_hole - 1 -- if the stroke is holed, return the average strokes to hole from the previous position minus the shot required to hole out.
    WHEN shot_type = 'Penalty' THEN 0 -- strokes lost due to penalty are applied to the shot preceding the penalty stroke.
    WHEN resulting_shot_type = 'Penalty' THEN {{ toggle }}_strokes_to_hole - two_strokes_later_{{ toggle }}_strokes_to_hole - 2 -- if a stroke results in a penalty, use the average strokes to hole from the shot following the penalty.
    ELSE {{ toggle }}_strokes_to_hole - resulting_{{ toggle }}_strokes_to_hole - 1
  END AS strokes_gained_{{ toggle }}

  {% if not loop.last %} , {% endif %}
  {% endfor %}
FROM shots_with_results
