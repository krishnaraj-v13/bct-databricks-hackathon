# Databricks notebook source
# MAGIC %pip install openai -q

# COMMAND ----------

from pyspark.sql import functions as F
import seaborn as sns
import matplotlib.pyplot as plt
from openai import OpenAI
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
def initate_client():

    # Get Databricks workspace token and URL automatically
    DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

    # Create OpenAI-compatible client pointing to Databricks serving


    MODEL_NAME = "databricks-gpt-oss-20b"



    _ws_url = WORKSPACE_URL.replace("https://", "").replace("http://", "").strip("/")
    client = OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url=f"https://{_ws_url}/serving-endpoints"
    )

    print(f"LLM client ready. Model: {MODEL_NAME}")
    print("Client ready!")
    return client,MODEL_NAME
# Response parser — the Databricks model sometimes returns structured JSON.
# This function extracts just the readable text block.
def _extract_text(content):
    if content is None:
        return ""
    if isinstance(content, str):
        try:
            parsed = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return content.strip()
    else:
        parsed = content
    if isinstance(parsed, list):
        for block in parsed:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", "")).strip()
        texts = [str(b.get("text", "")) for b in parsed if isinstance(b, dict) and "text" in b]
        return "\n".join(texts).strip() if texts else str(parsed)
    if isinstance(parsed, dict) and "text" in parsed:
        return str(parsed["text"]).strip()
    return str(content).strip()

def generate_llm_response(prompt,client,model_name):
    
    response = client.chat.completions.create(
        model=model_name,
        messages=[
             {"role": "system", "content": "You are an insurance fraud investigation assistant."},
             {"role": "user", "content": prompt}
        ],
        max_tokens=300,
        temperature=0.3
    )
    #print(response.choices[0].message.content)
    return _extract_text(response.choices[0].message.content)
def build_prompt(row):
    return f"""
        Analyze the following insurance claim:
        Claim ID: {row.claimid}
        Total Amount: {row.claim_amount}
        Severity: {row.incident_severity}
        Incident Type: {row.incident_type}
        Triggered Rules: {row.triggered_rules}
        Anomaly Score: {row.anomaly_score}
        Explain:
        1. Why this claim is suspicious(specific data points, not generic statements)
        2. What specific risks it presents
        3. What actions investigator should take. (specific, actionable steps)

        Keep it concise and professional.
        """

def rules_and_scores():
    df = spark.table("primeins.silver.claims")
    # Calculate the 95th percentile (top 5%) for injury, vehicle, and property damage amounts.
    # This helps us identify unusually high claim values compared to most claims.
    percentiles = df.select(
        F.expr("percentile_approx(injury_amount, 0.95)").alias("injury_p95"),
        F.expr("percentile_approx(vehicle_damage_amount, 0.95)").alias("vehicle_p95"),
        F.expr("percentile_approx(property_damage_amount, 0.95)").alias("property_p95")
    ).collect()[0]

    injury_p95 = percentiles["injury_p95"]
    vehicle_p95 = percentiles["vehicle_p95"]
    property_p95 = percentiles["property_p95"]

    # Convert dates
    df = df.withColumn("incident_date", F.to_date("incident_date")) \
        .withColumn("claim_logged_on", F.to_date("claim_logged_on"))
    df = df.withColumn(
        "claim_amount",
        F.coalesce("injury_amount", F.lit(0)) +
        F.coalesce("property_damage_amount", F.lit(0)) +
        F.coalesce("vehicle_damage_amount", F.lit(0)))
    # Delay in reporting
    df = df.withColumn("delay_days", F.datediff("claim_logged_on", "incident_date"))
    df_rules = df \
    .withColumn("BR_001", F.when(F.col("injury_amount") > injury_p95, 1).otherwise(0)) \
    .withColumn("BR_002", F.when(F.col("vehicle_damage_amount") > vehicle_p95, 1).otherwise(0)) \
    .withColumn("BR_003", F.when(F.col("property_damage_amount") > property_p95, 1).otherwise(0)) \
    .withColumn("BR_004", F.when(
        (F.col("incident_severity").isin("Trivial Damage", "Minor Damage")) &
        (F.col("injury_amount") > 20000), 1).otherwise(0)) \
    .withColumn("BR_005", F.when(
        (F.col("bodily_injuries") == 0) &
        (F.col("injury_amount") > 15000), 1).otherwise(0)) \
    .withColumn(
        "BR_006",
        F.when(F.col("delay_days") > 10, 1).otherwise(0)
    )

    df_scored = df_rules.withColumn(
        "anomaly_score",
        F.col("BR_001") * 25 +
        F.col("BR_002") * 20 +
        F.col("BR_003") * 15 +
        F.col("BR_004") * 15 +
        F.col("BR_005") * 15 +
        F.col("BR_006") * 15 
    )

    df_scored = df_scored.withColumn(
        "priority",
        F.when(F.col("anomaly_score") >= 60, "HIGH")
        .when(F.col("anomaly_score") >= 30, "MEDIUM")
        .otherwise("LOW")
    )


    df_scored = df_scored.withColumn(
        "triggered_rules",
        F.expr("""
            filter( array(
                CASE WHEN BR_001 = 1 THEN 'High Injury Outlier' END,
                CASE WHEN BR_002 = 1 THEN 'High Vehicle Damage' END,
                CASE WHEN BR_003 = 1 THEN 'High Property Damage' END,
                CASE WHEN BR_004 = 1 THEN 'Severity Mismatch' END,
                CASE WHEN BR_005 = 1 THEN 'Zero Injury Conflict' END,
                CASE WHEN BR_006 = 1 THEN 'Delay Conflict' END
            ), x -> x IS NOT NULL)
        """))
    df_scored = df_scored.orderBy(F.rand()).limit(5)
    return df_scored
def ai_investigatiion_brief(rows):
    results = []
    client,model_name=initate_client()
    for row in rows:
        #print(row)
        if(len(row.triggered_rules)!=0):
            print(row.claimid)
            prompt = build_prompt(row)
            #print("going to call generate_llm_response")
            explanation = generate_llm_response(prompt,client,model_name)
            
            results.append((
                row.claimid,
                row.claim_amount,
                row.incident_severity,
                row.incident_type,
                row.anomaly_score,
                row.priority,
                row.triggered_rules,
                explanation
            ))
        
    return results
def execute_ai_investigate():
        df_scored=rules_and_scores()
        rows = df_scored.collect()
        #first_100_rows=rows[:100]
        #test the model
        results = ai_investigatiion_brief(
                rows
        )
        print("=== LLM Explanation  ===")
        for result in results:
                print(result)



# COMMAND ----------

execute_ai_investigate()

# COMMAND ----------

# DBTITLE 1,Claims Anomaly Detection with AI Investigation
# MAGIC %sql
# MAGIC WITH percentiles AS (
# MAGIC   -- Step 1: Calculate 95th percentile thresholds (matches Python percentile_approx logic)
# MAGIC   SELECT
# MAGIC     percentile_approx(injury_amount, 0.95)         AS injury_p95,
# MAGIC     percentile_approx(vehicle_damage_amount, 0.95)  AS vehicle_p95,
# MAGIC     percentile_approx(property_damage_amount, 0.95) AS property_p95
# MAGIC   FROM primeins.silver.claims
# MAGIC ),
# MAGIC claims_base AS (
# MAGIC   -- Step 2: Compute claim_amount and delay_days
# MAGIC   SELECT
# MAGIC     c.*,
# MAGIC     COALESCE(c.injury_amount, 0) + COALESCE(c.property_damage_amount, 0) + COALESCE(c.vehicle_damage_amount, 0) AS claim_amount,
# MAGIC     DATEDIFF(c.claim_logged_on, c.incident_date) AS delay_days
# MAGIC   FROM primeins.silver.claims c
# MAGIC ),
# MAGIC rules_applied AS (
# MAGIC   -- Step 3: Apply business rules (BR_001–BR_006)
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC     CASE WHEN b.injury_amount > p.injury_p95 THEN 1 ELSE 0 END             AS BR_001,  -- High Injury Outlier
# MAGIC     CASE WHEN b.vehicle_damage_amount > p.vehicle_p95 THEN 1 ELSE 0 END    AS BR_002,  -- High Vehicle Damage
# MAGIC     CASE WHEN b.property_damage_amount > p.property_p95 THEN 1 ELSE 0 END  AS BR_003,  -- High Property Damage
# MAGIC     CASE WHEN b.incident_severity IN ('Trivial Damage', 'Minor Damage')
# MAGIC               AND b.injury_amount > 20000 THEN 1 ELSE 0 END                AS BR_004,  -- Severity Mismatch
# MAGIC     CASE WHEN b.bodily_injuries = 0
# MAGIC               AND b.injury_amount > 15000 THEN 1 ELSE 0 END                AS BR_005,  -- Zero Injury Conflict
# MAGIC     CASE WHEN b.delay_days > 10 THEN 1 ELSE 0 END                          AS BR_006   -- Delay Conflict
# MAGIC   FROM claims_base b
# MAGIC   CROSS JOIN percentiles p
# MAGIC ),
# MAGIC scored AS (
# MAGIC   -- Step 4: Calculate anomaly score and priority
# MAGIC   SELECT
# MAGIC     r.*,
# MAGIC     (r.BR_001 * 25 + r.BR_002 * 20 + r.BR_003 * 15 + r.BR_004 * 15 + r.BR_005 * 15 + r.BR_006 * 15) AS anomaly_score,
# MAGIC     CASE
# MAGIC       WHEN (r.BR_001 * 25 + r.BR_002 * 20 + r.BR_003 * 15 + r.BR_004 * 15 + r.BR_005 * 15 + r.BR_006 * 15) >= 60 THEN 'HIGH'
# MAGIC       WHEN (r.BR_001 * 25 + r.BR_002 * 20 + r.BR_003 * 15 + r.BR_004 * 15 + r.BR_005 * 15 + r.BR_006 * 15) >= 30 THEN 'MEDIUM'
# MAGIC       ELSE 'LOW'
# MAGIC     END AS priority,
# MAGIC     -- Step 5: Build triggered_rules array (matches Python filter+array logic)
# MAGIC     FILTER(
# MAGIC       ARRAY(
# MAGIC         CASE WHEN r.BR_001 = 1 THEN 'High Injury Outlier' END,
# MAGIC         CASE WHEN r.BR_002 = 1 THEN 'High Vehicle Damage' END,
# MAGIC         CASE WHEN r.BR_003 = 1 THEN 'High Property Damage' END,
# MAGIC         CASE WHEN r.BR_004 = 1 THEN 'Severity Mismatch' END,
# MAGIC         CASE WHEN r.BR_005 = 1 THEN 'Zero Injury Conflict' END,
# MAGIC         CASE WHEN r.BR_006 = 1 THEN 'Delay Conflict' END
# MAGIC       ),
# MAGIC       x -> x IS NOT NULL
# MAGIC     ) AS triggered_rules
# MAGIC   FROM rules_applied r
# MAGIC ),
# MAGIC flagged AS (
# MAGIC   -- Step 6: Filter only claims with at least one triggered rule
# MAGIC   -- This is for demo: keep 2 HIGH, 2 MEDIUM, 1 LOW priority claims
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     (SELECT * FROM scored WHERE priority = 'HIGH'   LIMIT 2)
# MAGIC     UNION ALL
# MAGIC     (SELECT * FROM scored WHERE priority = 'MEDIUM' LIMIT 2)
# MAGIC     UNION ALL
# MAGIC     (SELECT * FROM scored WHERE priority = 'LOW'    LIMIT 1)
# MAGIC   ) 
# MAGIC   WHERE SIZE(triggered_rules) > 0
# MAGIC )
# MAGIC -- Step 7: Generate AI investigation brief + metadata columns
# MAGIC SELECT
# MAGIC   f.claimid,
# MAGIC   f.claim_amount,
# MAGIC   f.incident_severity,
# MAGIC   f.incident_type,
# MAGIC   f.anomaly_score,
# MAGIC   f.priority,
# MAGIC   f.triggered_rules,
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'You are an insurance fraud investigation assistant. Analyze the following insurance claim:\n',
# MAGIC       'Claim ID: ', f.claimid, '\n',
# MAGIC       'Total Amount: ', CAST(f.claim_amount AS STRING), '\n',
# MAGIC       'Severity: ', f.incident_severity, '\n',
# MAGIC       'Incident Type: ', f.incident_type, '\n',
# MAGIC       'Triggered Rules: ', ARRAY_JOIN(f.triggered_rules, ', '), '\n',
# MAGIC       'Anomaly Score: ', CAST(f.anomaly_score AS STRING), '\n',
# MAGIC       'Explain:\n',
# MAGIC       '1. Why this claim is suspicious (specific data points, not generic statements)\n',
# MAGIC       '2. What specific risks it presents\n',
# MAGIC       '3. What actions investigator should take (specific, actionable steps)\n',
# MAGIC       'Keep it concise and professional in plain english in maximum 2-3 lines without title.\n',
# MAGIC       'Keep it for non-technicals to understand and instead of trigger rule names, mention why.\n',
# MAGIC       'DO NOT MENTION rule NAMES.'
# MAGIC     ),
# MAGIC     modelParameters => named_struct('max_tokens', 400, 'temperature', 0.3)
# MAGIC   ) AS ai_investigation_brief,
# MAGIC   'databricks-gpt-oss-20b'            AS model_used,
# MAGIC   CURRENT_TIMESTAMP()                  AS generated_timestamp
# MAGIC FROM flagged f

# COMMAND ----------

