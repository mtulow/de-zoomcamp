```sql
-- -- QUESTION: How many taxi trips were totally made on September 18th 2019?
-- SELECT COUNT(*)
-- FROM dev.green_taxi_trips
-- 	WHERE lpep_pickup_datetime::date = '2019-09-18'::date
-- 	AND lpep_dropoff_datetime::date = '2019-09-18'::date;
-- -- SOLUTION: 15,612	
```


```sql
-- -- QUESTION: Which was the pick up day with the longest trip distance?
-- -- 			 Use the pick up time for your calculations.
-- SELECT lpep_pickup_datetime::date AS pickup_date,
-- 		SUM(trip_distance) AS total_distance
-- FROM dev.green_taxi_trips
-- GROUP BY pickup_date
-- ORDER BY total_distance DESC
-- LIMIT 10;
-- -- SOLUTION: (2019-09-26; 58,759.94)
```

```sql
-- -- QUESTION: Which were the 3 pick up Boroughs that had
-- -- 			 a sum of total_amount superior to 50,000?
-- SELECT z."Borough" as borough,
--        SUM(g.total_amount) as sum_total_amount
-- FROM dev.green_taxi_trips AS g
-- LEFT JOIN dev.taxi_zones AS z
-- 	ON z."LocationID" = g."PULocationID"
-- GROUP BY z."Borough"
-- ORDER BY sum_total_amount DESC;

-- SELECT z."Borough" AS borough,
--        SUM(total_amount) AS total_amount_sum
-- FROM dev.green_taxi_trips AS g,
-- 	 dev.taxi_zones AS z
-- 		ON g."PULocationID" = z."LocationID"
-- WHERE lpep_pickup_datetime::date = '2019-09-18'
-- 	  AND z."Borough" != 'Unknown'
-- GROUP BY z."Borough"
-- HAVING SUM(g.total_amount) > 50000
-- ORDER BY total_amount_sum DESC
-- LIMIT 3;
-- -- SOLUTION: Manhattan, Queens, Brooklyn
```

```sql
-- QUESTION: Return passengers picked up in September 2019
SELECT
	g.lpep_pickup_datetime AS pickup_datetime,
	g.lpep_dropoff_datetime AS dropoff_datetime,
	pickup_zones."Borough" AS pickup_borough,
	pickup_zones."Zone" AS pickup_zone,
	dropoff_zones."Borough" AS dropoff_borough,
	dropoff_zones."Zone" AS dropoff_zone,
	SUM(g.tip_amount) AS tip_amount_sum
FROM dev.green_taxi_trips AS g
JOIN dev.taxi_zones AS pickup_zones
	ON g."PULocationID" = pickup_zones."LocationID"
JOIN dev.taxi_zones AS dropoff_zones
	ON g."DOLocationID" = dropoff_zones."LocationID"
WHERE pickup_zones."Zone" LIKE '%Astoria%'
GROUP BY pickup_zone, 
;
-- -- SOLUTION: 
```

	
