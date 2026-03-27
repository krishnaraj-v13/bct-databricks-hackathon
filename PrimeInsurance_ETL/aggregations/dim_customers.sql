CREATE OR REFRESH STREAMING TABLE primeins.gold.dim_customers AS
SELECT
  customer_id,
  MAX(region) AS region,
  MAX(state) AS state,
  MAX(city) AS city,
  MAX(education) AS education,
  MAX(marital_status) AS marital_status,
  MAX(job) AS job,
  MAX(default_flag) AS default_flag,
  MAX(balance) AS balance,
  MAX(hh_insurance) AS hh_insurance,
  MAX(car_loan) AS car_loan
FROM STREAM(live.primeins.silver.customers)
-- WHERE customer_id = 148040
GROUP BY customer_id