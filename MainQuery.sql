--SELECT  FROM `celestial-gist-375110.MainTaxiData.MainTaxiData` LIMIT 1000

CREATE TABLE celestial-gist-375110.MainTaxiData.MainTaxiData_f AS
SELECT pickup_time, season, pickup_period, day_of_week
FROM `celestial-gist-375110.MainTaxiData.fhvv_tripdata`
UNION ALL
SELECT pickup_time, season, pickup_period, day_of_week
FROM `celestial-gist-375110.MainTaxiData.green_tripdata`
UNION ALL
SELECT pickup_time, season, pickup_period, day_of_week
FROM `celestial-gist-375110.MainTaxiData.yellow_tripdata`;

-- Answer the following questions: from the tables on bigquery
--What season has the highest number of pickup rides (Winter, Summer, Autumn and Spring)
SELECT season, COUNT(*) AS ride_count
FROM `celestial-gist-375110.MainTaxiData.MainTaxiData_f`
GROUP BY season
ORDER BY ride_count DESC
LIMIT 1;

--What period of the day has the highest pickup number

SELECT
  pickup_period,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.MainTaxiData_f`
GROUP BY
  pickup_period
ORDER BY
  PickupCount DESC
LIMIT
  1;


  --What day of the week (Monday- Sunday) has the highest pickup number
SELECT
  day_of_week,
  COUNT(*) AS PickupCount
FROM
  `celestial-gist-375110.MainTaxiData.MainTaxiData_f`
GROUP BY
  day_of_week
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


--What zone has the highest total amount of paid.
SELECT z.Zone, SUM(a.total_amount) AS total_amount_paid
FROM (
  SELECT pickup_location_id, total_amount
  FROM `celestial-gist-375110.MainTaxiData.yellow_tripdata`
  UNION ALL
  SELECT pickup_location_id, total_amount
  FROM `celestial-gist-375110.MainTaxiData.yellow_tripdata`
) AS a
JOIN `celestial-gist-375110.MainTaxiData.Taxizone` z ON a.pickup_location_id = z.LocationID
GROUP BY z.Zone
ORDER BY total_amount_paid DESC
LIMIT 1;









