# coding: utf-8
from datetime import datetime

import pyspark.sql.functions as sf
from pyspark.sql import Window
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark

# Start Spark Session
spark = SnessSpark()

# Loads the dataset
df_nps = spark.read.parquet(
    environment=Environment.PRODUCTION,
    zone=Zone.RAW,
    namespace="nps_solucx",
    dataset="satisfaction_survey",
)
# Create a window partition to remove duplicated IDs
last_value = Window.partitionBy("id").orderBy(
    sf.col("rating_lastUpdate").desc(), sf.col("datalog").desc()
)

# Create a column with a incremental value partitioned by ID
# The smallest value of the count, 1, it is always the most recent
df_create_id = df_nps.dropDuplicates().withColumn(
    "distinct", sf.row_number().over(last_value)
)

# Filter the most recently reg and drop column unnecessary at the calalog.
df_nps = df_create_id.filter("distinct == 1").drop("distinct")


# Loads the dataset
df_revisions = spark.read.parquet(
    environment=Environment.PRODUCTION,
    zone=Zone.RAW,
    namespace="nps_solucx",
    dataset="satisfaction_survey_revisions",
).repartition(1)

# deduplicando com inner join, e distinct

df_revisions_inner = df_revisions.join(
    df_nps,
    (df_revisions.satisfaction_survey_id == df_nps.id)
    & (df_revisions.satisfaction_survey_rating_lastUpdate == df_nps.rating_lastUpdate)
    & (df_revisions.satisfaction_survey_datalog == df_nps.datalog),
    how="inner",
).distinct()
df_revisions_inner = df_revisions_inner[df_revisions.columns]


# Stores in Parquet format
spark.write.parquet(
    df_nps,
    environment=Environment.PRODUCTION,
    zone=Zone.TRUSTED,
    namespace="nps_solucx",
    dataset="satisfaction_survey",
    mode="overwrite",
)


# Stores in Parquet format
spark.write.parquet(
    df_revisions_inner,
    environment=Environment.PRODUCTION,
    zone=Zone.TRUSTED,
    namespace="nps_solucx",
    dataset="satisfaction_survey_revisions",
    mode="overwrite",
)
