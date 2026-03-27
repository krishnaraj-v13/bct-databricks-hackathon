CREATE OR REFRESH STREAMING TABLE primeins.gold.dim_cars AS
SELECT
  car_id,
  MAX(name) AS name,
  MAX(km_driven) AS km_driven,
  MAX(fuel) AS fuel,
  MAX(transmission) AS transmission,
  MAX(mileage) AS mileage,
  MAX(engine) AS engine,
  MAX(max_power) AS max_power,
  MAX(torque) AS torque,
  MAX(seats) AS seats,
  MAX(model) AS model
FROM STREAM(live.primeins.silver.cars)
GROUP BY car_id