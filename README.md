# hit-bombs-attack-pins
Transforms raw shot-level data into strokes-gained analytics.

## Changelog
- Sept 29, 2021: Set up repository. Initiated dbt and connected to BigQuery.
- Oct 6, 2021: Added initial staging logic and testing to prepare raw data for analysis.
- Oct 13, 2021: Added transformation logic to calculate strokes gained for each shot, additional schema testing, and custom test to check for data entry errors. Strokes gained data relative to PGA TOUR average is ready to be visualized!
- Oct 23, 2021: A number of formatting adjustments for shot data. Created an aggregation file to make it easier to find areas for improvement.
