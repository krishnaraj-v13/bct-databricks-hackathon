CREATE OR REFRESH STREAMING TABLE primeins.silver.sales
(
  CONSTRAINT valid_sales_id EXPECT (sales_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ad_placed_on EXPECT (ad_placed_on IS NOT NULL AND ad_placed_on RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$'),
  CONSTRAINT valid_sold_on_format EXPECT (sold_on <> 'NULL' OR sold_on RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$'),
  CONSTRAINT valid_selling_price EXPECT (original_selling_price IS NOT NULL),
  CONSTRAINT valid_region EXPECT ( Region IN ('Central', 'East', 'South', 'West')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_state EXPECT (State <> 'NULL'),
  CONSTRAINT valid_city EXPECT (City <> 'NULL'),
  CONSTRAINT valid_seller_type EXPECT (seller_type IN ('Dealer', 'Individual', 'Trustmark Dealer')),
  CONSTRAINT valid_owner EXPECT (owner IN ('First Owner', 'Second Owner', 'Third Owner', 'Fourth & Above Owner')),
  CONSTRAINT valid_car_id EXPECT (car_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT *
FROM STREAM(live.primeins.bronze.sales);

CREATE OR REFRESH STREAMING TABLE primeins.silver.sales_quarantine
AS

SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'DROP' AS validation_action
  FROM STREAM(live.primeins.bronze.sales)
  WHERE
  sales_id is NULL
  OR original_selling_price IS NULL
  OR original_selling_price <= 0
  OR Region NOT IN ('Central','East','South','West')
  OR car_id IS NULL

UNION ALL
SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'WARN' AS validation_action
  FROM STREAM(live.primeins.bronze.sales)
  WHERE
  sold_on = 'NULL'
  AND NOT (sold_on RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$')
  OR ad_placed_on = 'NULL' 
  OR NOT (ad_placed_on RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$')
  OR State ='NULL'
  OR City = 'NULL'
  OR seller_type NOT IN ('Dealer','Individual','Trustmark Dealer')
  OR owner NOT IN ('First Owner','Second Owner','Third Owner','Fourth & Above Owner');

  CREATE OR REFRESH STREAMING TABLE primeins.silver.dq_issues_sales
AS

select distinct * from 
(
-- Constraint 1: Missing sales_id
SELECT
  uuid() AS issue_id,
  'primeins.bronze.sales' AS table, 
  'sales_id' AS column,
  'Missing sales identifier' AS rule_name,
  'NULL' AS value,
  CONCAT('CarID:', COALESCE(CAST(car_id AS STRING), 'NULL'), '|Region:', COALESCE(Region, 'NULL'), '|State:', COALESCE(State, 'NULL')) AS record_id,
  1 AS affected_ratio,
  'Sales records without sales_id cannot be uniquely identified or tracked, preventing revenue reconciliation, sales performance analysis, and audit trail maintenance' AS business_context,
  'Investigate source system to ensure sales_id is auto-generated for all transactions. Check ETL process for data extraction errors. Sales ID is mandatory for transaction tracking and financial reporting' AS suggested_fix,
  file_path,
  load_timestamp AS file_loaded_date
FROM STREAM(live.primeins.bronze.sales) 
WHERE sales_id IS NULL

UNION ALL

-- Missing or invalid selling price
SELECT
  uuid(),
  'primeins.bronze.sales' ,
  'original_selling_price' ,
  'Missing or invalid selling price' ,
  CAST(original_selling_price AS STRING) ,
  CONCAT('SalesID:', COALESCE(CAST(sales_id AS STRING), 'NULL'), '|CarID:', COALESCE(CAST(car_id AS STRING), 'NULL')) ,
  1 AS affected_ratio,
  'Missing or invalid selling price prevents revenue recognition, commission calculations, and accurate financial reporting for sales performance' ,
  'Verify selling price from sales contract or invoice. Price must be positive and within reasonable range ($10,000 to $15,000,000). Investigate extreme values for data entry errors',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.sales)
WHERE original_selling_price IS NULL
   OR original_selling_price <= 0

UNION ALL

-- Invalid Region value
SELECT
  uuid(),
  'primeins.bronze.sales' ,
  'Region',
  'Invalid region code',
  Region,
  CONCAT('SalesID:', COALESCE(CAST(sales_id AS STRING), 'NULL'), '|CarID:', COALESCE(CAST(car_id AS STRING), 'NULL'), '|State:', COALESCE(State, 'NULL')),
  1,
  'Invalid region codes prevent accurate regional sales analysis, territory-based commission calculations, and regional inventory management critical for business operations',
  'Standardize region values to Central/East/South/West only. Map state to correct region using reference table. Update source system validation to enforce valid region codes',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.sales) 
WHERE Region IS NOT NULL AND Region NOT IN ('Central', 'East', 'South', 'West')


UNION ALL

-- Missing car_id
SELECT
  uuid(),
  'primeins.bronze.sales',
  'car_id',
  'Missing car identifier',
  'NULL',
  CONCAT('SalesID:', COALESCE(CAST(sales_id AS STRING), 'NULL'), '|Region:', COALESCE(Region, 'NULL'), '|State:', COALESCE(State, 'NULL'), '|City:', COALESCE(City, 'NULL')),
  1,
  'Sales records without car_id cannot be linked to vehicle inventory, preventing policy issuance, vehicle history tracking, and accurate sales-to-policy conversion analysis',
  'Verify car_id exists in vehicle inventory system. Cross-reference with VIN or registration number. Car ID is mandatory to link sales to insurance policies and vehicle records',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.sales) 
WHERE car_id IS NULL
);