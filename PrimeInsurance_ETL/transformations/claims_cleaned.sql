CREATE OR REFRESH STREAMING TABLE primeins.silver.claims
(
  -- Primary Key Constraints
  CONSTRAINT valid_claim_id EXPECT (ClaimID IS NOT NULL OR ClaimID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_policy_id EXPECT (PolicyID IS NOT NULL) ON VIOLATION DROP ROW,
  
  -- Categorical Field Constraints
  CONSTRAINT valid_collision_type EXPECT (collision_type IN ('Front Collision', 'Rear Collision', 'Side Collision')),
  CONSTRAINT valid_incident_severity EXPECT (incident_severity IN ('Minor Damage', 'Major Damage', 'Trivial Damage')),
  CONSTRAINT valid_incident_type EXPECT (incident_type IN ('Multi-vehicle Collision', 'Single Vehicle Collision', 'Vehicle Theft', 'Parked Car')),
  CONSTRAINT valid_authorities_contacted EXPECT (authorities_contacted IN ('Police', 'Fire', 'Ambulance', 'Other', 'None')),
  CONSTRAINT valid_police_report EXPECT (police_report_available IN ('YES', 'NO')),
  CONSTRAINT valid_property_damage EXPECT (property_damage IN ('YES', 'NO')),
  CONSTRAINT valid_claim_rejected EXPECT (Claim_Rejected IN ('Y', 'N')) ON VIOLATION DROP ROW,
  
  -- Numeric Range Constraints
  CONSTRAINT valid_bodily_injuries EXPECT (bodily_injuries IS NOT NULL AND CAST(bodily_injuries AS INT) > 0),
  CONSTRAINT valid_witnesses EXPECT (witnesses IS NOT NULL AND CAST(witnesses AS INT) > 0),
  CONSTRAINT valid_vehicles_involved EXPECT (number_of_vehicles_involved IS NOT NULL AND CAST(number_of_vehicles_involved AS INT) >= 0),
  
  -- Required Fields
  CONSTRAINT valid_incident_date EXPECT (incident_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_claim_logged_date EXPECT (Claim_Logged_On IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT 
    cs.ClaimID::INT AS claimid,
    cs.PolicyID::INT AS policyid,
    cs.incident_state,
    cs.incident_city,
    cs.incident_type,
    CASE WHEN cs.collision_type = '?' THEN NULL ELSE cs.collision_type END AS collision_type,
    cs.incident_severity,
    cs.authorities_contacted,
    cs.number_of_vehicles_involved::INT AS number_of_vehicles_involved,
    CASE
      WHEN cs.Claim_Rejected = 'Y' THEN 'Y'
      WHEN cs.Claim_Rejected = 'N' THEN 'N'
      ELSE NULL
    END AS Claim_Rejected,

    -- -- Policy Date
    -- -- TO_DATE(py.policy_bind_date) AS policy_bind_date,

    -- Extract offsets
    TRY_CAST(regexp_extract(cs.incident_date, '(\\d+)', 1) AS INT) AS incident_happened_in_days,
    TRY_CAST(regexp_extract(cs.Claim_Logged_On, '(\\d+)', 1) AS INT) AS claim_logged_in_days,
    TRY_CAST(regexp_extract(cs.Claim_Processed_On, '(\\d+)', 1) AS INT) AS claim_processed_in_days,

    -- Derived Dates
    DATE_ADD(TO_DATE(py.policy_bind_date), 
        TRY_CAST(regexp_extract(cs.incident_date, '(\\d+)', 1) AS INT)
    ) AS incident_date,

    DATE_ADD(
        DATE_ADD(TO_DATE(py.policy_bind_date), 
            TRY_CAST(regexp_extract(cs.incident_date, '(\\d+)', 1) AS INT)
        ),
        TRY_CAST(regexp_extract(cs.Claim_Logged_On, '(\\d+)', 1) AS INT)
    ) AS claim_logged_on,

    DATE_ADD(
        DATE_ADD(
          DATE_ADD(TO_DATE(py.policy_bind_date), 
              TRY_CAST(regexp_extract(cs.incident_date, '(\\d+)', 1) AS INT)
          ),
          TRY_CAST(regexp_extract(cs.Claim_Logged_On, '(\\d+)', 1) AS INT)
          ),
        TRY_CAST(regexp_extract(cs.Claim_Processed_On, '(\\d+)', 1) AS INT)
    ) AS claim_processed_on,

    -- Processing Days
    DATEDIFF(
        DATE_ADD(
            DATE_ADD(TO_DATE(py.policy_bind_date), 
                TRY_CAST(regexp_extract(cs.incident_date, '(\\d+)', 1) AS INT)
            ),
            TRY_CAST(regexp_extract(cs.Claim_Processed_On, '(\\d+)', 1) AS INT)
        ),
        DATE_ADD(
              DATE_ADD(
                DATE_ADD(TO_DATE(py.policy_bind_date), 
                    TRY_CAST(regexp_extract(cs.incident_date, '(\\d+)', 1) AS INT)
                ),
                TRY_CAST(regexp_extract(cs.Claim_Logged_On, '(\\d+)', 1) AS INT)
              ),
            TRY_CAST(regexp_extract(cs.Claim_Logged_On, '(\\d+)', 1) AS INT)
        )
    ) AS days_to_process_claim,

    -- Numeric fields
    TRY_CAST(cs.injury AS DOUBLE) AS injury_amount,
    TRY_CAST(cs.property AS DOUBLE) AS property_damage_amount,
    TRY_CAST(cs.vehicle AS DOUBLE) AS vehicle_damage_amount,
    TRY_CAST(cs.bodily_injuries AS INT) AS bodily_injuries,
    TRY_CAST(cs.witnesses AS INT) AS witnesses,

    -- Clean categorical
    CASE 
        WHEN cs.police_report_available IN ('YES','NO') THEN cs.police_report_available
        ELSE NULL
    END AS police_report_available,

    CASE 
        WHEN cs.property_damage IN ('YES','NO') THEN cs.property_damage
        ELSE NULL
    END AS property_damage,

    -- Fraud flags

    CASE 
        WHEN TRY_CAST(cs.injury AS DOUBLE) > 20000 
             AND cs.police_report_available != 'YES'
        THEN 1 ELSE 0 
    END AS flag_high_amount_no_report,

    CASE 
        WHEN cs.incident_type = 'Vehicle Theft' 
             AND cs.authorities_contacted = 'None'
        THEN 1 ELSE 0 
    END AS flag_theft_no_authority,

    CASE 
        WHEN cs.incident_severity = 'Minor Damage' 
             AND TRY_CAST(cs.injury AS DOUBLE) > 20000
        THEN 1 ELSE 0 
    END AS flag_damage_severity_mismatch,

    CASE 
        WHEN TRY_CAST(cs.witnesses AS INT) = 0 
             AND TRY_CAST(cs.injury AS DOUBLE) > 15000
        THEN 1 ELSE 0 
    END AS flag_no_witness_high_claim,

    CASE 
        WHEN cs.Claim_Processed_On IS NULL OR cs.Claim_Processed_On = 'NULL'
        THEN 1 ELSE 0 
    END AS flag_unprocessed_claim

FROM STREAM(live.primeins.bronze.claims) cs
JOIN STREAM(live.primeins.bronze.policy) py
    ON cs.PolicyID = py.policy_number;
-- FROM STREAM(live.primeins.bronze.claims);


CREATE OR REFRESH STREAMING TABLE primeins.silver.claims_quarantine
AS
(
SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'DROP' AS validation_action
FROM STREAM(live.primeins.bronze.claims)
WHERE
    ClaimID = 'NULL'
    OR PolicyID = 'NULL'
    OR Claim_Rejected NOT IN ('Y', 'N')
    OR incident_date = 'NULL'
    OR Claim_Logged_On = 'NULL'

  UNION ALL
SELECT
  *,
  CURRENT_TIMESTAMP() AS quarantine_timestamp,
  FALSE AS invalid_data,
  'WARN' AS validation_action
FROM STREAM(live.primeins.bronze.claims)
WHERE
    collision_type NOT IN ('Front Collision', 'Rear Collision', 'Side Collision', '?')
    AND incident_severity NOT IN ('Minor Damage', 'Major Damage', 'Trivial Damage')
    AND incident_type NOT IN ('Multi-vehicle Collision', 'Single Vehicle Collision', 'Vehicle Theft', 'Parked Car')
    AND authorities_contacted NOT IN ('Police', 'Fire', 'Ambulance', 'Other', 'None')
    AND police_report_available NOT IN ('YES', 'NO', '?')
    AND property_damage NOT IN ('YES', 'NO', '?')
    AND (bodily_injuries = 'NULL' OR CAST(bodily_injuries AS INT) < 0)
    AND (witnesses = 'NULL' OR CAST(witnesses AS INT) < 0)
    AND number_of_vehicles_involved = 'NULL' AND CAST(number_of_vehicles_involved AS INT) <= 0

  );
-- _____________________________________________________________________________________________________________________
CREATE OR REFRESH STREAMING TABLE primeins.silver.dq_issues_claims
AS

-- 1. Missing ClaimID
SELECT
  uuid() AS issue_id,
  'primeins.bronze.claims' AS table,
  'ClaimID' AS column,
  'Missing Claim ID' AS rule_name,
  'NULL' AS value,
  CONCAT('ClaimID is ', PolicyID, ' and incident logged in ', split(incident_date, ':')[0]) AS record_id,
  1 AS affected_ratio,
  'Claim cannot be processed or tracked without a Claim ID' AS business_context,
  'Ensure source system always generates ClaimID before ingestion' AS suggested_fix,
  file_path,
  load_timestamp AS file_loaded_date
FROM STREAM(live.primeins.bronze.claims)
WHERE ClaimID = 'NULL'

UNION ALL

-- 2. Missing PolicyID
SELECT
  uuid(),
  'primeins.bronze.claims',
  'PolicyID',
  'Missing Policy ID',
  ClaimID,
  CONCAT('ClaimID is ', ClaimID, ' and incident logged in ', split(incident_date, ':')[0]),
  1,
  'Claim cannot be linked to a policy, impacting validation and payouts',
  'Validate PolicyID at source or enforce referential integrity',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.claims)
WHERE PolicyID = 'NULL'

UNION ALL

-- 3. Invalid Claim_Rejected
SELECT
  uuid(),
  'primeins.bronze.claims',
  'Claim_Rejected',
  'Invalid rejection flag',
  Claim_Rejected,
  CONCAT('ClaimID is ', ClaimID, ' and PolicyID is ', PolicyID),
  1,
  'Incorrect rejection flag leads to wrong claim status reporting',
  'Restrict values to Y or N at source system',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.claims)
WHERE Claim_Rejected NOT IN ('Y', 'N') OR Claim_Rejected = '?'

UNION ALL

-- 4. Missing Incident Date
SELECT
  uuid(),
  'primeins.bronze.claims',
  'incident_date',
  'Missing incident date',
  'NULL',
  CONCAT('ClaimID is ', ClaimID, ' and PolicyID is ', PolicyID),
  1,
  'Incident date is critical for claim validation and fraud checks',
  'Ensure incident_date is captured during claim registration',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.claims)
WHERE incident_date = 'NULL'

UNION ALL

-- 5. Missing Claim Logged Date
SELECT
  uuid(),
  'primeins.bronze.claims',
  'Claim_Logged_On',
  'Missing claim logged date',
  'NULL',
  CONCAT('ClaimID is ', ClaimID, ' and PolicyID is ', PolicyID),
  1,
  'Cannot measure claim processing time without logged date',
  'Ensure system logs claim creation timestamp',
  file_path,
  load_timestamp
FROM STREAM(live.primeins.bronze.claims)
WHERE Claim_Logged_On = 'NULL';