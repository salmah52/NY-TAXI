

-- Answer the following questions: from the tables on bigquery
--What season has the highest number of pickup rides (Winter, Summer, Autumn and Spring)
SELECT
    CASE
        WHEN EXTRACT(MONTH FROM pickup_time) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM pickup_time) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM pickup_time) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM pickup_time) IN (9, 10, 11) THEN 'Autumn'
    END AS Season,
    COUNT(*) AS RideCount
FROM
    `celestial-gist-375110.MainTaxiData.MainTaxiData`
GROUP BY
    Season
ORDER BY
    RideCount DESC
LIMIT 1;

--What period of the day has the highest pickup number

SELECT
  pickup_period,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.MainTaxiData`
GROUP BY
  pickup_period
ORDER BY
  PickupCount DESC
LIMIT
  1;


WITH highest_pickup_counts AS (
  SELECT
    CASE
      WHEN UPPER(pickup_period) = 'AFTERNOON' THEN 'Afternoon'
      WHEN UPPER(pickup_period) = 'MIDNIGHT' THEN 'Late Night'
      WHEN UPPER(pickup_period) = 'EVENING' THEN 'Evening'
      WHEN UPPER(pickup_period) = 'MORNING' THEN 'Morning'
      ELSE NULL
    END AS pickup_period,
    COUNT(*) AS PickupCount
  FROM
     `celestial-gist-375110.MainTaxiData.MainTaxiData`
  WHERE
    pickup_period IS NOT NULL
  GROUP BY
    pickup_period
)
SELECT
  pickup_period,
  PickupCount
FROM
  highest_pickup_counts
WHERE
  PickupCount = (
    SELECT MAX(PickupCount)
    FROM highest_pickup_counts
  );


--What day of the week (Monday- Sunday) has the highest pickup number
SELECT
  EXTRACT(DAYOFWEEK FROM pickup_time) AS DayOfWeek,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.MainTaxiData`
GROUP BY
  DayOfWeek
ORDER BY
  PickupCount DESC
LIMIT 1;


SELECT
  FORMAT_DATETIME('%A', pickup_time) AS DayOfWeek,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.MainTaxiData`
GROUP BY
  DayOfWeek
ORDER BY
  PickupCount DESC
LIMIT 1;

--What zone has the highest
SELECT
  zone,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.Taxizone`
GROUP BY
  zone
ORDER BY
  PickupCount DESC
LIMIT 1;


SELECT
  service_zone,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.Taxizone`
GROUP BY
  service_zone
ORDER BY
  PickupCount DESC
LIMIT 1;


--



 
