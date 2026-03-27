CREATE OR REFRESH STREAMING TABLE primeins.silver.dq_issues AS
(
  (
SELECT * FROM STREAM(primeins.silver.dq_issues_claims)
LIMIT 5 -- To void resource utilization in free edition
)
UNION ALL 
SELECT * FROM STREAM(primeins.silver.dq_issues_cars)
UNION ALL 
(
SELECT * FROM STREAM(primeins.silver.dq_issues_customers)
LIMIT 5 -- To void resource utilization in free edition
)
UNION ALL 
SELECT * FROM STREAM(primeins.silver.dq_issues_policy)
UNION ALL 
SELECT * FROM STREAM(primeins.silver.dq_issues_sales)
)