-- Target Table: `athletics-project.track_and_field.dim_competitions`
-- Layer: Bronze/Silver (Dimension)
-- Description: Stores metadata for scraped track and field competitions.

CREATE OR REPLACE TABLE `athletics-project.track_and_field.dim_competitions` (
    Competition_ID INT64,
    Name STRING,
    Venue STRING,
    Country STRING,
    Date STRING,
    Is_Indoor BOOL
);