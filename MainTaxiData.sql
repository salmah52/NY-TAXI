CREATE TABLE celestial-gist-375110.MainTaxiData.MainTaxiData AS
SELECT pickup_time, season, pickup_period, day_of_week
FROM `celestial-gist-375110.MainTaxiData.fhv_tripdata`
UNION ALL
SELECT pickup_time, season, pickup_period, day_of_week
FROM `celestial-gist-375110.MainTaxiData.green_tripdata`
UNION ALL
SELECT pickup_time, season, pickup_period, day_of_week
FROM `celestial-gist-375110.MainTaxiData.yellow_tripdata`;
