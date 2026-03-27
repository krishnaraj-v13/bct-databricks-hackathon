CREATE OR REPLACE MATERIALIZED VIEW primeins.gold.ai_business_insights AS 
-- =====================================================
-- DYNAMIC KPI DASHBOARD WITH AI-GENERATED SUMMARIES
-- All values fetched dynamically from database
-- =====================================================

SELECT 
    'Policy Portfolio' AS domain_name,
    'Total Active Policies' AS summary_title,
    TO_JSON(STRUCT(
        COUNT(DISTINCT `policy_number`) AS total_policies,
        'Active policies in the portfolio' AS description
    )) AS kpi_data_json,
    COUNT(DISTINCT `policy_number`) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Total Active Policies = ', 
               CAST(COUNT(DISTINCT `policy_number`) AS STRING),
               '. Explain what this means for the business in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.dim_policy

UNION ALL

SELECT 
    'Policy Portfolio' AS domain_name,
    'Average Annual Premium' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(AVG(`policy_annual_premium`), 2) AS avg_premium,
        COUNT(DISTINCT `policy_number`) AS policy_count,
        'Average annual premium across all policies' AS description
    )) AS kpi_data_json,
    ROUND(AVG(`policy_annual_premium`), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Average Annual Premium = $', 
               CAST(ROUND(AVG(`policy_annual_premium`), 2) AS STRING),
               '. Explain what this means for revenue and pricing strategy  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.dim_policy

UNION ALL

SELECT 
    'Policy Portfolio' AS domain_name,
    'Total Premium Revenue' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(SUM(`policy_annual_premium`), 2) AS total_premium_revenue,
        COUNT(DISTINCT `policy_number`) AS policy_count,
        'Total annual premium revenue from all policies' AS description
    )) AS kpi_data_json,
    ROUND(SUM(`policy_annual_premium`), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Total Premium Revenue = $', 
               CAST(ROUND(SUM(`policy_annual_premium`), 2) AS STRING),
               '. Explain the business significance of this total revenue figure  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.dim_policy

UNION ALL

SELECT 
    'Claims Performance' AS domain_name,
    'Total Claims Filed' AS summary_title,
    TO_JSON(STRUCT(
        COUNT(DISTINCT `claimid`) AS total_claims,
        'Total number of claims filed' AS description
    )) AS kpi_data_json,
    COUNT(DISTINCT `claimid`) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Total Claims Filed = ', 
               CAST(COUNT(DISTINCT `claimid`) AS STRING),
               '. Explain what this claims volume means for the business  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.fact_claims

UNION ALL

SELECT 
    'Claims Performance' AS domain_name,
    'Claim Rejection Rate' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(SUM(CASE WHEN `Claim_Rejected` = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS rejection_rate_pct,
        SUM(CASE WHEN `Claim_Rejected` = 'Yes' THEN 1 ELSE 0 END) AS rejected_claims,
        COUNT(*) AS total_claims,
        'Percentage of claims that were rejected' AS description
    )) AS kpi_data_json,
    ROUND(SUM(CASE WHEN `Claim_Rejected` = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Claim Rejection Rate = ', 
               CAST(ROUND(SUM(CASE WHEN `Claim_Rejected` = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS STRING),
               '%. Explain what this rejection rate means for customer satisfaction and operations  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.fact_claims

UNION ALL

SELECT 
    'Claims Performance' AS domain_name,
    'Average Claim Processing Time' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(AVG(`days_to_process_claim`), 2) AS avg_processing_days,
        COUNT(DISTINCT `claimid`) AS claims_processed,
        'Average days to process a claim' AS description
    )) AS kpi_data_json,
    ROUND(AVG(`days_to_process_claim`), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Average Claim Processing Time = ', 
               CAST(ROUND(AVG(`days_to_process_claim`), 2) AS STRING),
               ' days. Explain what this processing time means for customer experience and efficiency  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.fact_claims
WHERE `days_to_process_claim` IS NOT NULL

UNION ALL

SELECT 
    'Claims Performance' AS domain_name,
    'Total Claim Payout Amount' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(SUM(`injury_amount` + `property_damage_amount` + `vehicle_damage_amount`), 2) AS total_payout,
        COUNT(DISTINCT `claimid`) AS claim_count,
        'Total amount paid out across all claims' AS description
    )) AS kpi_data_json,
    ROUND(SUM(`injury_amount` + `property_damage_amount` + `vehicle_damage_amount`), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Total Claim Payout Amount = $', 
               CAST(ROUND(SUM(`injury_amount` + `property_damage_amount` + `vehicle_damage_amount`), 2) AS STRING),
               '. Explain what this total payout means for financial performance  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.fact_claims

UNION ALL

SELECT 
    'Customer Profile' AS domain_name,
    'Total Active Customers' AS summary_title,
    TO_JSON(STRUCT(
        COUNT(DISTINCT `customer_id`) AS total_customers,
        'Total number of active customers' AS description
    )) AS kpi_data_json,
    COUNT(DISTINCT `customer_id`) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Total Active Customers = ', 
               CAST(COUNT(DISTINCT `customer_id`) AS STRING),
               '. Explain what this customer base size means for the business  in 3-4 lines in non-technical terms in plain English (title not required).'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.dim_customers

UNION ALL

SELECT 
    'Customer Profile' AS domain_name,
    'Average Customer Balance' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(AVG(`balance`), 2) AS avg_balance,
        COUNT(DISTINCT `customer_id`) AS customer_count,
        'Average account balance across all customers' AS description
    )) AS kpi_data_json,
    ROUND(AVG(`balance`), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Average Customer Balance = $', 
               CAST(ROUND(AVG(`balance`), 2) AS STRING),
               '. Explain what this average balance indicates about customer financial health  in 3-4 lines in non-technical terms.'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.dim_customers

UNION ALL

SELECT 
    'Customer Profile' AS domain_name,
    'Customer Default Rate' AS summary_title,
    TO_JSON(STRUCT(
        ROUND(SUM(`default_flag`) * 100.0 / COUNT(*), 2) AS default_rate_pct,
        SUM(`default_flag`) AS customers_in_default,
        COUNT(*) AS total_customers,
        'Percentage of customers with default flag' AS description
    )) AS kpi_data_json,
    ROUND(SUM(`default_flag`) * 100.0 / COUNT(*), 2) AS kpi_result,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT('Generate a brief executive summary for this KPI: Customer Default Rate = ', 
               CAST(ROUND(SUM(`default_flag`) * 100.0 / COUNT(*), 2) AS STRING),
               '%. Explain what this default rate means for credit risk and business health  in 3-4 lines in non-technical terms.'),
        modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
    ) AS ai_executive_summary,
    CURRENT_TIMESTAMP() AS generation_timestamp,
    'databricks-gpt-oss-20b' AS model_name,
    'completed' AS generation_status
FROM primeins.gold.dim_customers;