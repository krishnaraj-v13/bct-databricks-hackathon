CREATE OR REFRESH MATERIALIZED VIEW primeins.gold.claim_anomaly_explanations AS 
(
WITH percentiles AS (
  -- Step 1: Calculate 95th percentile thresholds (matches Python percentile_approx logic)
  SELECT
    percentile_approx(injury_amount, 0.95)         AS injury_p95,
    percentile_approx(vehicle_damage_amount, 0.95)  AS vehicle_p95,
    percentile_approx(property_damage_amount, 0.95) AS property_p95
  FROM primeins.silver.claims
),
claims_base AS (
  -- Step 2: Compute claim_amount and delay_days
  SELECT
    c.*,
    COALESCE(c.injury_amount, 0) + COALESCE(c.property_damage_amount, 0) + COALESCE(c.vehicle_damage_amount, 0) AS claim_amount,
    DATEDIFF(c.claim_logged_on, c.incident_date) AS delay_days
  FROM primeins.silver.claims c
),
rules_applied AS (
  -- Step 3: Apply business rules (BR_001–BR_006)
  SELECT
    b.*,
    CASE WHEN b.injury_amount > p.injury_p95 THEN 1 ELSE 0 END             AS BR_001,  -- High Injury Outlier
    CASE WHEN b.vehicle_damage_amount > p.vehicle_p95 THEN 1 ELSE 0 END    AS BR_002,  -- High Vehicle Damage
    CASE WHEN b.property_damage_amount > p.property_p95 THEN 1 ELSE 0 END  AS BR_003,  -- High Property Damage
    CASE WHEN b.incident_severity IN ('Trivial Damage', 'Minor Damage')
              AND b.injury_amount > 20000 THEN 1 ELSE 0 END                AS BR_004,  -- Severity Mismatch
    CASE WHEN b.bodily_injuries = 0
              AND b.injury_amount > 15000 THEN 1 ELSE 0 END                AS BR_005,  -- Zero Injury Conflict
    CASE WHEN b.delay_days > 10 THEN 1 ELSE 0 END                          AS BR_006   -- Delay Conflict
  FROM claims_base b
  CROSS JOIN percentiles p
),
scored AS (
  -- Step 4: Calculate anomaly score and priority
  SELECT
    r.*,
    (r.BR_001 * 25 + r.BR_002 * 20 + r.BR_003 * 15 + r.BR_004 * 15 + r.BR_005 * 15 + r.BR_006 * 15) AS anomaly_score,
    CASE
      WHEN (r.BR_001 * 25 + r.BR_002 * 20 + r.BR_003 * 15 + r.BR_004 * 15 + r.BR_005 * 15 + r.BR_006 * 15) >= 60 THEN 'HIGH'
      WHEN (r.BR_001 * 25 + r.BR_002 * 20 + r.BR_003 * 15 + r.BR_004 * 15 + r.BR_005 * 15 + r.BR_006 * 15) >= 30 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS priority,
    -- Step 5: Build triggered_rules array (matches Python filter+array logic)
    FILTER(
      ARRAY(
        CASE WHEN r.BR_001 = 1 THEN 'High Injury Outlier' END,
        CASE WHEN r.BR_002 = 1 THEN 'High Vehicle Damage' END,
        CASE WHEN r.BR_003 = 1 THEN 'High Property Damage' END,
        CASE WHEN r.BR_004 = 1 THEN 'Severity Mismatch' END,
        CASE WHEN r.BR_005 = 1 THEN 'Zero Injury Conflict' END,
        CASE WHEN r.BR_006 = 1 THEN 'Delay Conflict' END
      ),
      x -> x IS NOT NULL
    ) AS triggered_rules
  FROM rules_applied r
),
flagged AS (
  -- Step 6: Filter only claims with at least one triggered rule
  -- This is for demo: keep 2 HIGH, 2 MEDIUM, 1 LOW priority claims
  SELECT *
  FROM (
    (SELECT * FROM scored WHERE priority = 'HIGH'   LIMIT 2)
    UNION ALL
    (SELECT * FROM scored WHERE priority = 'MEDIUM' LIMIT 2)
    UNION ALL
    (SELECT * FROM scored WHERE priority = 'LOW'    LIMIT 1)
  ) 
  WHERE SIZE(triggered_rules) > 0
)
-- Step 7: Generate AI investigation brief + metadata columns
SELECT
  f.claimid,
  f.claim_amount,
  f.incident_severity,
  f.incident_type,
  f.anomaly_score,
  f.priority,
  f.triggered_rules,
  ai_query(
    'databricks-gpt-oss-20b',
    CONCAT(
      'You are an insurance fraud investigation assistant. Analyze the following insurance claim:\n',
      'Claim ID: ', f.claimid, '\n',
      'Total Amount: ', CAST(f.claim_amount AS STRING), '\n',
      'Severity: ', f.incident_severity, '\n',
      'Incident Type: ', f.incident_type, '\n',
      'Triggered Rules: ', ARRAY_JOIN(f.triggered_rules, ', '), '\n',
      'Anomaly Score: ', CAST(f.anomaly_score AS STRING), '\n',
      'Explain:\n',
      '1. Why this claim is suspicious (specific data points, not generic statements)\n',
      '2. What specific risks it presents\n',
      '3. What actions investigator should take (specific, actionable steps)\n',
      'Keep it concise and professional in plain english in maximum 2-3 lines without title.\n',
      'Keep it for non-technicals to understand and instead of trigger rule names, mention why.\n',
      'DO NOT MENTION rule NAMES.'
    ),
    modelParameters => named_struct('max_tokens', 400, 'temperature', 0.3)
  ) AS ai_investigation_brief,
  'databricks-gpt-oss-20b'            AS model_used,
  CURRENT_TIMESTAMP()                  AS generated_timestamp
FROM flagged f
);