CREATE OR REFRESH STREAMING TABLE primeins.silver.policy
(
  -- Primary key
  CONSTRAINT valid_policy_number EXPECT (policy_number IS NOT NULL) ON VIOLATION DROP ROW,
 -- Date validation
  CONSTRAINT valid_bind_date EXPECT (policy_bind_date IS NOT NULL AND policy_bind_date <= CURRENT_DATE())  ON VIOLATION DROP ROW,
  -- State validation
  CONSTRAINT valid_state EXPECT (policy_state IS NOT NULL OR LENGTH(policy_state) = 2),
 -- CSL validation
  CONSTRAINT valid_csl_not_null EXPECT (policy_csl IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_csl EXPECT (policy_csl IN ('100/300','250/500','500/1000')),
  -- Deductible validation
  CONSTRAINT valid_deductible EXPECT (policy_deductable IN (500,1000,2000)),
  -- Premium validation
  CONSTRAINT valid_premium EXPECT (policy_annual_premium IS NOT NULL OR policy_annual_premium > 0) ON VIOLATION DROP ROW,
  -- Umbrella validation
  CONSTRAINT valid_umbrella EXPECT (umbrella_limit IS NOT NULL AND umbrella_limit >= 0),
 -- Business rule
  CONSTRAINT valid_umbrella_csl EXPECT (NOT (umbrella_limit > 5000000 AND policy_csl = '100/300')),
 -- Referential integrity (soft check)
  CONSTRAINT valid_car_id EXPECT (car_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)  ON VIOLATION DROP ROW
)
AS
SELECT *
FROM STREAM(live.primeins.bronze.policy);

CREATE OR REFRESH STREAMING TABLE primeins.silver.policy_quarantine
AS

SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'DROP' AS validation_action
FROM STREAM(live.primeins.bronze.policy)
WHERE
  policy_number IS NULL
  OR policy_bind_date IS NULL
  OR policy_bind_date > CURRENT_DATE()
  OR policy_annual_premium IS NULL OR policy_annual_premium <= 0
  OR car_id IS NULL
  OR customer_id IS NULL

UNION ALL

SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'WARN' AS validation_action
FROM STREAM(live.primeins.bronze.policy)
WHERE
  umbrella_limit > 5000000 AND policy_csl = '100/300'
   OR umbrella_limit IS NULL OR umbrella_limit < 0
  OR policy_state IS NULL OR LENGTH(policy_state) != 2
  OR policy_csl NOT IN ('100/300','250/500','500/1000')
  OR policy_deductable NOT IN (500,1000,2000);

  CREATE OR REFRESH STREAMING TABLE primeins.silver.dq_issues_policy
AS
-- ── policy_number null ─────────────────────────────
SELECT
  uuid() as issue_id, 
  'primeins.bronze.policy' as table,
  'policy_number' as column,
  'Missing Primary Key' as rule_name,
  CAST(NULL AS STRING) as value,
  COALESCE(
    CAST(policy_number AS STRING),CONCAT('car=', car_id, '|customer=', customer_id))  AS record_id,
  1 as affected_ratio,
  'Missing policy number — cannot identify policy' as business_context,
  'Ensure policy_number is populated' as suggested_fix,
  file_path, 
  load_timestamp
FROM STREAM(live.primeins.bronze.policy)
WHERE policy_number IS NULL

UNION ALL

-- ── invalid bind date ──────────────────────────────
SELECT
  uuid(), 'primeins.bronze.policy', 'policy_bind_date', 'invalid_date',
  CAST(policy_bind_date AS STRING) ,
  CAST(policy_number AS STRING),
  1,
  'Future bind date not allowed',
  'Validate date <= current_date',
  file_path, load_timestamp
FROM STREAM(live.primeins.bronze.policy)
WHERE policy_bind_date > CURRENT_DATE()


UNION ALL

-- ── invalid premium ────────────────────────────────
SELECT
  uuid(), 'primeins.bronze.policy', 'policy_annual_premium', 'invalid_value',
  CAST(policy_annual_premium AS STRING),
  CAST(policy_number AS STRING),
  1,
  'Premium must be > 0',
  'Fix source pricing',
  file_path, 
  load_timestamp
FROM STREAM(live.primeins.bronze.policy)
WHERE policy_annual_premium <= 0

UNION ALL

-- ── Missing Policy CSL ────────────────────────────────
SELECT
  uuid(), 'primeins.bronze.policy', 'policy_CSL', 'Policy CSL missing ',
  CAST(policy_csl as STRING),
  CAST(policy_number AS STRING),
  1,
  'Policy CSL missing',
  'Fix CSL Value for Policy',
  file_path, 
  load_timestamp
FROM STREAM(live.primeins.bronze.policy)
WHERE policy_csl IS NULL;

-- UNION ALL

-- -- ── orphan car_id ──────────────────────────────────
-- SELECT
--   uuid(), 'primeins.bronze.policy', 'car_id', 'referential_integrity',
--   CAST(p.car_id AS STRING),
--   CAST(p.policy_number AS STRING),
--   1,
--   'Missing car reference',
--   'Ensure car exists in cars table',
--   p.file_path, p.load_timestamp
-- FROM STREAM(live.primeins.bronze.policy) p
-- LEFT JOIN STREAM(live.primeins.bronze.cars) c
--   ON p.car_id = c.car_id
-- WHERE p.car_id IS NULL

-- UNION ALL

-- -- ── orphan customer_id ─────────────────────────────
-- SELECT
--   uuid(), 'primeins.bronze.policy', 'customer_id', 'referential_integrity',
--   CAST(p.customer_id AS STRING),
--   CAST(p.policy_number AS STRING),
--   1,
--   'Missing customer reference',
--   'Ensure customer exists in customers table',
--   p.file_path, p.load_timestamp
-- FROM STREAM(live.primeins.bronze.policy) p
-- LEFT JOIN STREAM(live.primeins.bronze.customers) c
--   ON p.customer_id = c.customer_id
-- WHERE p.customer_id IS NULL;
  