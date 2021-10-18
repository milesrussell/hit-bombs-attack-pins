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

rounds AS (
  SELECT
    *
  FROM {{ ref('stg_rounds') }}
),

courses AS (
  SELECT
    *
  FROM {{ ref('stg_courses') }}
),

shots_with_avgs AS (
  SELECT
    shots.shot_id,
    shots.hole_shot_seq,
    shots.shot_type,
    shots.hole_id,
    shots.distance,
    shots.round_id,
    shots.sg_category,
    shots_to_hole.pro_strokes_to_hole,
    shots_to_hole.scratch_strokes_to_hole,
    rounds.day,
    rounds.avg_temp AS round_avg_temp,
    rounds.avg_windspeed_mph AS round_avg_windspeed_mph,
    rounds.full_round,
    courses.course_name,
    courses.satellite_yardage AS course_satellite_yardage,
    courses.scorecard_yardage AS course_scorecard_yardage
  FROM shots
  LEFT JOIN shots_to_hole ON shots.shot_type_id = shots_to_hole.shot_type
    AND shots.distance = shots_to_hole.distance
  LEFT JOIN rounds ON rounds.round_id = shots.round_id
  LEFT JOIN courses ON courses.course_id = rounds.course_id
),

shots_with_results AS (
  SELECT
    *,

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
  END AS strokes_gained_{{ toggle }},

  {% endfor %}

  COUNT(DISTINCT hole_id) OVER(
    PARTITION BY
      round_id
  ) AS holes_in_round,
  COUNT(shot_id) OVER(
    PARTITION BY
      round_id
  ) AS shots_in_round,
  SUM(CASE WHEN hole_shot_seq = 1 THEN distance ELSE 0 END) OVER(
    PARTITION BY
      round_id
  ) AS round_yardage
FROM shots_with_results
