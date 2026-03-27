CREATE OR REFRESH STREAMING TABLE primeins.silver.customers
(
  -- Primary Key Constraint with coalesced ID fields
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  
  -- Geographic Constraints with coalesced fields
  CONSTRAINT valid_region EXPECT (region IN ('East', 'West', 'Central', 'South'))  ON VIOLATION DROP ROW,
  CONSTRAINT valid_state EXPECT (state IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_city EXPECT (city IS NOT NULL),
  
  -- Categorical Field Constraints with coalesced fields and field swap handling
  CONSTRAINT valid_education EXPECT (education IN ('primary', 'secondary', 'tertiary')),
  CONSTRAINT valid_marital EXPECT (marital_status IN ('married', 'single', 'divorced')),
  
  -- Binary Flag Constraints
  CONSTRAINT valid_default_flag EXPECT (default_flag IN (0, 1)),
  CONSTRAINT valid_hhinsurance_flag EXPECT (hh_insurance IN (0, 1)),
  CONSTRAINT valid_carloan_flag EXPECT (car_loan IN (0, 1)),
  
  -- Numeric Range Constraints
  CONSTRAINT valid_balance EXPECT (balance IS NOT NULL AND balance >= 0)  ON VIOLATION DROP ROW
)
AS
SELECT 
  COALESCE(CustomerID, Customer_ID, cust_id) AS customer_id,
  CASE UPPER(TRIM(COALESCE(Region, Reg)))
            WHEN 'E'       THEN 'East'
            WHEN 'EAST'    THEN 'East'
            WHEN 'W'       THEN 'West'
            WHEN 'WEST'    THEN 'West'
            WHEN 'S'       THEN 'South'
            WHEN 'SOUTH'   THEN 'South'
            WHEN 'N'       THEN 'North'
            WHEN 'NORTH'   THEN 'North'
            WHEN 'C'       THEN 'Central'
            WHEN 'CENTRAL' THEN 'Central'
            ELSE COALESCE(Region, Reg)
        END  AS region,
  State AS state,
  COALESCE(City, City_in_state) AS city,
  -- Handle field swap: if Education contains marital values, swap them
  CASE 
    WHEN COALESCE(Education, Edu) IN ('married', 'single', 'divorced') 
    THEN COALESCE(Marital_status, Marital)
    ELSE COALESCE(Education, Edu)
  END AS education,
  CASE 
    WHEN COALESCE(Education, Edu) IN ('married', 'single', 'divorced') 
    THEN COALESCE(Education, Edu)
    ELSE COALESCE(Marital_status, Marital)
  END AS marital_status,
  Job AS job,
  Default AS default_flag,
  Balance AS balance,
  HHInsurance AS hh_insurance,
  CarLoan AS car_loan,
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.customers);

-- ________________________________________________________________
CREATE OR REFRESH STREAMING TABLE primeins.silver.customers_quarantine
AS
(
SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'DROP' AS validation_action
FROM STREAM(live.primeins.bronze.customers)
WHERE
    COALESCE(CustomerID, Customer_ID, cust_id) IS NULL
    OR COALESCE(Region, Reg) NOT IN ('East', 'West', 'Central', 'South', 'E', 'W', 'C', 'S')
    OR (State IS NULL)
    OR (balance IS NULL OR balance < 0)

  UNION ALL
SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'WARN' AS validation_action
FROM STREAM(live.primeins.bronze.customers)
WHERE
COALESCE(City, City_in_state) IS NULL
OR COALESCE(Education, Edu) NOT IN ('primary', 'secondary', 'tertiary')
OR COALESCE(Marital_status, Marital) NOT IN ('married', 'single', 'divorced')
OR (Default NOT IN (0, 1))
OR (HHInsurance NOT IN (0, 1))
OR (CarLoan NOT IN (0, 1))
);



-- ________________________________________________________________
CREATE OR REFRESH STREAMING TABLE primeins.silver.dq_issues_customers
AS
-- Constraint 1: Missing Customer ID (all three ID fields are NULL)
SELECT
  uuid() AS issue_id,
  'primeins.bronze.customers' AS table, 
  'customer_id' AS column,
  'Missing customer identifier' AS rule_name,
  CAST(NULL AS STRING) AS value,
  CONCAT(
    'State:', COALESCE(State, 'NULL'),
    '|City:', COALESCE(City, City_in_state, 'NULL'),
    '|File:', regexp_replace(file_path, '^.*autoinsurancedata/', '')
  ) AS record_id,
  1 AS affected_ratio,
  'Customers without any identifier cannot be tracked across systems, linked to policies or claims, preventing customer service, billing, and analytics operations' AS business_context,
  'Investigate source system to ensure customer ID generation. Check for data extraction errors. Implement mandatory ID assignment at customer onboarding. May require manual customer matching' AS suggested_fix,
  file_path,
  load_timestamp AS file_loaded_date
FROM STREAM(live.primeins.bronze.customers) 
WHERE COALESCE(CustomerID, Customer_ID, cust_id) IS NULL

UNION ALL

-- Constraint 2: Invalid Region value
SELECT
  uuid() AS issue_id,
  'primeins.bronze.customers',
  'region',
  'Invalid region code',
  COALESCE(Region, Reg),
  CONCAT('CustomerID:', COALESCE(CAST(CustomerID AS STRING), CAST(Customer_ID AS STRING), CAST(cust_id AS STRING), 'NULL')),
  1,
  'Invalid region codes prevent accurate regional sales analysis, territory assignment, and regional performance reporting critical for business planning',
  'Standardize region values to East/West/Central/South or E/W/C/S. Map state to correct region using reference table. Update source system validation rules',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.customers) 
WHERE COALESCE(Region, Reg) IS NOT NULL 
  AND COALESCE(Region, Reg) NOT IN ('East', 'West', 'Central', 'South', 'E', 'W', 'C', 'S')

UNION ALL
-- Constraint 3: Missing State
SELECT
  uuid(),
  'primeins.bronze.customers',
  'state',
  'Missing state information',
  CAST(NULL AS STRING) ,
  CONCAT('CustomerID:', COALESCE(CAST(CustomerID AS STRING), CAST(Customer_ID AS STRING), CAST(cust_id AS STRING), 'NULL'), '|Region:', COALESCE(Region, Reg, 'NULL')),
  1 AS affected_ratio,
  'Missing state prevents compliance with state-specific insurance regulations, accurate premium calculations, and state-mandated reporting requirements',
  'Obtain state from customer address records. Verify with customer during next contact. State is mandatory for insurance policy issuance and regulatory compliance',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.customers) 
WHERE State IS NULL

UNION ALL

-- Constraint 13: Invalid Balance range
SELECT
  uuid() AS issue_id,
  'primeins.bronze.customers',
  'balance',
  'Balance out of valid range',
  CAST(Balance AS STRING),
  CONCAT('CustomerID:', COALESCE(CAST(CustomerID AS STRING), CAST(Customer_ID AS STRING), CAST(cust_id AS STRING), 'NULL')),
  1,
  'Balance values outside reasonable range indicate data quality issues, potential fraud, or system errors affecting financial reporting and customer account management',
  'Investigate extreme balance values. Verify with accounting system. Balance should typically range from -$10,000 to $100,000. Flag accounts with unusual balances for manual review',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.customers) 
WHERE Balance IS NOT NULL AND (Balance < 0);


