CREATE OR REFRESH STREAMING TABLE primeins.gold.dim_policy AS
SELECT
  policy_number,
  MAX(policy_bind_date) AS policy_bind_date,
  MAX(policy_state) AS policy_state,
  MAX(policy_csl) AS policy_csl,
  MAX(policy_deductable) AS policy_deductable,
  MAX(policy_annual_premium) AS policy_annual_premium,
  MAX(umbrella_limit) AS umbrella_limit,
  MAX(car_id) AS car_id,
  MAX(customer_id) AS customer_id
FROM STREAM(live.primeins.silver.policy)
GROUP BY policy_number