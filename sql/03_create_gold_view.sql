-- Target View: `athletics-project.track_and_field.gold_results_dashboard`
-- Layer: Gold (Serving / Business Intelligence)
-- Description: Denormalized view serving the Looker Studio dashboard.
-- Transformations:
--   1. Dynamic Regex extraction of Competition End Date.
--   2. Historical age calculation based on exact days lived prior to race.
--   3. Splitting polymorphic 'Mark' column into clean metrics and status codes.


CREATE OR REPLACE VIEW `athletics-project.track_and_field.gold_results_dashboard` AS
SELECT 
    f.Competition_ID,
    c.Name AS Competition_Name,
    c.Venue,
    c.Country AS Host_Country,
    c.Is_Indoor,
    f.Event_Name,
    f.Athlete_Name,
    f.Nationality AS Athlete_Country,
    
    --Grab the raw competition date string
    c.Date AS Raw_Comp_Date,

    -- Extract the End Date using Regex and parse it into a real DATE type
    SAFE.PARSE_DATE('%d %b %Y', REGEXP_EXTRACT(c.Date, r'\d{1,2}\s[A-Za-z]{3}\s\d{4}$')) AS Parsed_Comp_Date,
    
    --Calculate accurate age on the specific day of the competition
    CAST(
        FLOOR(
            DATE_DIFF(
                SAFE.PARSE_DATE('%d %b %Y', REGEXP_EXTRACT(c.Date, r'\d{1,2}\s[A-Za-z]{3}\s\d{4}$')), 
                f.Birth_date, 
                DAY
            ) / 365.25
        ) 
    AS INT64) AS Age_At_Competition,
    
    -- Safely handle the Place column for mathematical aggregations
    SAFE_CAST(f.Place AS INT64) AS Finishing_Position,
    f.Mark AS Raw_Mark,
    
    --Isolate Status Codes (Disqualifications, Did Not Start, etc.)
    CASE 
        WHEN f.Mark IN ('DQ', 'DNS', 'DNF', 'NM', 'NH') THEN f.Mark
        WHEN f.Place IS NOT NULL THEN 'Finished'
        ELSE 'Unknown'
    END AS Result_Status,
    
    --Isolate clean athletic marks (Times and Distances)
    CASE 
        WHEN f.Mark NOT IN ('DQ', 'DNS', 'DNF', 'NM', 'NH') THEN f.Mark
        ELSE NULL 
    END AS Clean_Mark

FROM `athletics-project.track_and_field.fact_results` f
LEFT JOIN `athletics-project.track_and_field.dim_competitions` c
    ON f.Competition_ID = c.Competition_ID;