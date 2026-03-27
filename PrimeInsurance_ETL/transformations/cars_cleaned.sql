CREATE OR REFRESH STREAMING TABLE primeins.silver.cars
(
  CONSTRAINT valid_car_id EXPECT (car_id IS NOT NULL) ON VIOLATION DROP ROW,
  -- CONSTRAINT unique_car_id EXPECT (count(1) OVER (PARTITION BY car_id) = 1) ON VIOLATION DROP ROW,
  CONSTRAINT valid_car_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_km EXPECT (km_driven IS NOT NULL AND km_driven > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_fuel EXPECT (fuel IN ('Petrol','Diesel','CNG','LPG')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_transmission EXPECT (transmission IS NOT NULL),
  CONSTRAINT valid_mileage_format EXPECT (mileage iLIKE '%kmpl%' OR mileage iLIKE '%km%kg%'),
  CONSTRAINT valid_engine_format EXPECT (engine iLIKE '%CC%'),
  CONSTRAINT valid_power_format EXPECT (max_power iLIKE '%bhp%')
)
AS
SELECT
  *
FROM STREAM(live.primeins.bronze.cars);


CREATE OR REFRESH STREAMING TABLE primeins.silver.cars_quarantine
AS
(
SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'DROP' AS validation_action
FROM STREAM(live.primeins.bronze.cars)
WHERE
  car_id IS NULL
  OR fuel NOT IN ('Petrol','Diesel','CNG','LPG')
  OR name IS NULL
  -- OR (count(1) OVER (PARTITION BY car_id) = 1)

  UNION ALL
  
  SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'WARN' AS validation_action
FROM STREAM(live.primeins.bronze.cars)
WHERE
  km_driven IS NULL OR km_driven < 0
  OR transmission IS NULL
  OR mileage IS NOT NULL AND mileage NOT iLIKE '%kmpl%' AND mileage NOT iLIKE '%km%kg%'
  OR engine IS NOT NULL AND engine NOT iLIKE '%CC%'
  OR max_power IS NOT NULL AND max_power NOT iLIKE '%bhp%'
)
  ;

CREATE OR REFRESH STREAMING TABLE PRIMEINS.SILVER.dq_issues_cars
AS

-- ── car_id: null ──────────────────────────────────────────────
SELECT
  uuid()                        AS issue_id,
  'primeins.bronze.cars'        AS table,
  'car_id'                      AS column,
  'Missing primary key'         AS rule_name,
  'NULL'          AS value,
  COALESCE(
    CAST(car_id AS STRING),
    CONCAT('name=', name, '|km=', CAST(km_driven AS STRING))
  )                             AS record_id,
  1          AS affected_ratio,
  'Car ID missing — record cannot be uniquely identified' AS business_context,
  'Ensure source system populates car_id before ingestion' AS suggested_fix,
  file_path,
  load_timestamp                AS file_loaded_date
FROM STREAM(live.primeins.bronze.cars)
WHERE car_id IS NULL

UNION ALL

-- Missing car name
SELECT
  uuid(),
  'primeins.bronze.cars',
  'name',
  'Missing car name',
  'NULL',
  CONCAT('CarID:', COALESCE(CAST(car_id AS STRING), 'NULL'), '|Model:', COALESCE(model, 'NULL'), '|Fuel:', COALESCE(fuel, 'NULL')),
  1 AS affected_ratio,
  'Cars without name cannot be properly identified in customer communications, sales listings, or policy documents, impacting customer service and sales operations',
  'Obtain vehicle name from registration documents or VIN lookup. Vehicle name is mandatory for customer-facing documents and sales listings',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.cars) 
WHERE name IS NULL

UNION ALL

-- ── Missing or invalid km_driven (NULL or <= 0)
SELECT
  uuid(),
  'primeins.bronze.cars',
  'km_driven',
  'Missing or invalid kilometers driven',
  CAST(km_driven AS STRING),
  CONCAT('CarID:', COALESCE(CAST(car_id AS STRING), 'NULL'), '|Name:', COALESCE(name, 'NULL')),
  1,
  'Missing or invalid km_driven prevents accurate vehicle valuation, wear-and-tear assessment, and insurance premium calculations based on usage',
  'Verify odometer reading from vehicle inspection or registration documents. Kilometers driven must be positive (> 0). Zero or negative values indicate data entry errors',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.cars) 
WHERE km_driven IS NULL OR km_driven < 0

UNION ALL

-- Invalid fuel type
SELECT
  uuid(),
  'primeins.bronze.cars',
  'fuel',
  'Invalid fuel type',
  fuel,
  CONCAT('CarID:', COALESCE(CAST(car_id AS STRING), 'NULL'), '|Name:', COALESCE(name, 'NULL')),
  1,
  'Invalid fuel type affects insurance premium calculations, environmental compliance reporting, and vehicle classification for policy coverage',
  'Standardize fuel type to Petrol/Diesel/CNG/LPG only. Verify fuel type from vehicle registration or manufacturer specifications. Update source system validation rules',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.cars) 
WHERE fuel IS NOT NULL AND fuel NOT IN ('Petrol', 'Diesel', 'CNG', 'LPG');