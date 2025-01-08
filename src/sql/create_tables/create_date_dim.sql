CREATE OR REPLACE TABLE DATE_DIM AS 
WITH generated_dates AS (
    SELECT
        DATEADD('day', SEQ4(), '2024-12-24')::DATE AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
)

SELECT
    REPLACE(date, '-', '') AS date_id,
    date,
    DATE_PART(year, date) AS year,
    DATE_PART(quarter, date) AS quarter,
    DATE_PART(month, date) AS month,
    DATE_PART(day, date) AS day,
    DATE_PART(week, date) AS week,
    DAYOFWEEKISO(date) AS day_of_week,
    CASE
        WHEN DAYOFWEEKISO(date) IN (1, 6, 7) THEN 1 ELSE 0
    END AS is_weekend
FROM generated_dates;
