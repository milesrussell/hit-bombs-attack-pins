WITH shots AS (
  SELECT
    *
  FROM {{ ref('shots_strokes_gained') }}
),

agg AS (
  SELECT
    sg_category,
    CASE
      WHEN shot_type = 'Tee' AND sg_category = 'Approach' THEN 'Fairway'
      ELSE shot_type
    END AS shot_type,
    CASE
      WHEN shot_type = 'Recovery' THEN shot_type
      WHEN sg_category IN ('Around The Green', 'Off The Tee') THEN sg_category
      WHEN shot_type = 'Green' AND distance <= 3 THEN '0-3 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 4 AND 10 THEN CONCAT(distance, ' feet')
      WHEN shot_type = 'Green' AND distance BETWEEN 11 AND 15 THEN '11-15 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 16 AND 20 THEN '16-20 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 21 AND 25 THEN '21-25 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 26 AND 30 THEN '26-30 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 31 AND 40 THEN '31-40 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 41 AND 50 THEN '41-50 feet'
      WHEN shot_type = 'Green' AND distance BETWEEN 51 AND 60 THEN '51-60 feet'
      WHEN shot_type = 'Green' AND distance >= 60 THEN '60+ feet'
      WHEN sg_category = 'Approach' AND distance <= 50 THEN 'less than 50 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 51 AND 75 THEN '51-75 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 76 AND 100 THEN '76-100 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 101 AND 125 THEN '101-125 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 126 AND 150 THEN '126-150 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 151 AND 175 THEN '151-175 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 176 AND 200 THEN '176-200 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 201 AND 225 THEN '201-225 yards'
      WHEN sg_category = 'Approach' AND distance BETWEEN 226 AND 250 THEN '226-250 yards'
      WHEN sg_category = 'Approach' AND distance > 250 THEN '250+ yards'
    END AS distance_bucket,
    COUNT(*) AS num_shots,
    ROUND(SUM(strokes_gained_pro), 2) AS sg_pro,
    ROUND(SUM(strokes_gained_scratch), 2) AS sg_scratch
  FROM shots
  WHERE sg_category IS NOT NULL
    AND shot_type != 'Penalty'
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3
)

SELECT
  *,
  ROUND(sg_pro / num_shots, 2) AS sg_pro_per_shot,
  ROUND(sg_scratch / num_shots, 2) AS sg_scratch_per_shot,
  {{ dbt_utils.surrogate_key(['sg_category', 'shot_type', 'distance_bucket']) }} AS key
FROM agg
