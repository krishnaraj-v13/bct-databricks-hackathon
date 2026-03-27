# Databricks notebook source
# DBTITLE 1,Cell 2
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from functools import reduce

# COMMAND ----------

#Setting up path and schema variables
CATALOG = 'primeins'
SCHEMA = 'bronze'
NAME_SPACE = CATALOG + "." + SCHEMA
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/"
RAW_PATH = f"{BASE_PATH}/autoinsurancedata/"
CHECKPOINT_BASE = f"{BASE_PATH}/raw_files/checkpoints"

# COMMAND ----------

# DBTITLE 1,Cell 4
@dp.table(
    name=f"{NAME_SPACE}.customers",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/**/*customers*.csv")
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("load_timestamp", current_timestamp())
    )

@dp.table(name=f"{NAME_SPACE}.claims",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def claims():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiline", "true")
        .load(f"{BASE_PATH}/**/*claims*.json")
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("load_timestamp", current_timestamp())
    )

# @dp.table(name=f"{NAME_SPACE}.sales",
#     table_properties={"delta.enableChangeDataFeed": "true"}
# )
# def sales():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("header", "true")
#         .load(f"{BASE_PATH}/**/*sales*.csv")
#         .withColumn("file_path", col("_metadata.file_path"))
#         .withColumn("load_timestamp", current_timestamp())
#         .filter(~(col("file_path").isNull() | col("load_timestamp").isNull()))
# )

@dlt.table(
    name=f"{NAME_SPACE}.sales",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def sales():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/**/*sales*.csv")
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("load_timestamp", current_timestamp())
    )

    # Exclude the two manually added columns from the null-check
    metadata_cols = {"file_path", "load_timestamp","_rescued_data"}
    original_cols = [c for c in df.columns if c not in metadata_cols]

    # Keep rows where at least one original column is non-null
    not_all_null = reduce(
        lambda a, b: a | b,
        [col(c).isNotNull() for c in original_cols]
    )

    return df.filter(not_all_null)


@dp.table(name=f"{NAME_SPACE}.cars",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def cars():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/**/*cars*.csv")
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("load_timestamp", current_timestamp())
    )

@dp.table(name=f"{NAME_SPACE}.policy",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def policy():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/**/*policy*.csv")
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("load_timestamp", current_timestamp())
    )

# COMMAND ----------

# dbutils.fs.rm(f"{CHECKPOINT_BASE}", True)
# /Volumes/primeins/bronze/raw_files/raw_files/checkpoints