/*

This test checks that each shot's row number matches its hole_shot_seq value.
If the test fails, it means that a hole_shot_seq values probably got entered incorrectly.
I found one entry where I accidentally entered in the wrong hole for a shot, so
that may be worth checking as well if this test fails.
*/

WITH shots AS (
  SELECT
    *
  FROM {{ ref('stg_shots') }}
),

shots_with_row_number AS (
  SELECT
    ROW_NUMBER() OVER(
      PARTITION BY
        hole_id,
        round_id
      ORDER BY
        hole_shot_seq
    ) AS rn,
    hole_shot_seq,
    shot_id,
    hole_id,
    round_id
  FROM shots
)

SELECT
  *
FROM shots_with_row_number
WHERE rn != hole_shot_seq
