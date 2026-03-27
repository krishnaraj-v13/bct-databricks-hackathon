-- Gold layer: DQ Explanation Report
-- Reads DQ issues from silver and generates AI-powered plain-English explanations
-- Replicates the logic from the (Clone) DQ Explainer notebook

CREATE OR REFRESH STREAMING TABLE primeins.gold.dq_explanation_report
AS
SELECT
  issue_id,
  element_at(split(`table`, '\\.'), -1) AS table_name,
  `column` AS column_name,
  record_id AS affected_records,
  ai_query(
    'databricks-gpt-oss-20b',
    CONCAT(
      'You are a data quality analyst. Respond with plain English paragraphs only. No JSON. No markdown.\n\n',
      'The following data quality issue was found in the ',
      element_at(split(`table`, '\\.'), -1),
      ' dataset:\n',
      '- Field: ', `column`, '\n',
      '- Problem: ', rule_name, '\n',
      '- Value found: ', value, '\n',
      '- Record: ', record_id, '\n',
      '- Business context: ', business_context, '\n\n',
      'Write a 3-4 sentence plain-English explanation for the Regional Director.\n',
      'Explain what is wrong, why it likely happened, and what business decision or report is at risk if this is not corrected.\n',
      'Do not use technical jargon. Write in paragraph form only. No bullet points.'
    ),
    modelParameters => named_struct('max_tokens', 300, 'temperature', 0.3)
  ) AS issue_ai_explanation,
  current_timestamp() AS generated_timestamp,
  'databricks-gpt-oss-20b' AS model_name,
  'Success' AS generation_status,
  file_path as issue_file_path,
  file_loaded_date
FROM STREAM(live.primeins.silver.dq_issues)
