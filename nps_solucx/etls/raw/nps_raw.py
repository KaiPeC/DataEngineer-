# coding: utf-8
import datetime

import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark import SparkContext, SQLContext
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark

sc = SparkContext.getOrCreate()
ss = SnessSpark()

SCHEMA = st.StructType(
    [
        st.StructField("amount", st.LongType(), True),
        st.StructField(
            "customer",
            st.StructType(
                [
                    st.StructField("cpf", st.LongType(), True),
                    st.StructField("email", st.StringType(), True),
                    st.StructField("id", st.LongType(), True),
                    st.StructField("name", st.StringType(), True),
                    st.StructField("phone", st.StringType(), True),
                ]
            ),
            True,
        ),
        st.StructField("datalog", st.StringType(), True),
        st.StructField(
            "employee",
            st.StructType(
                [
                    st.StructField("externalId", st.StringType(), True),
                    st.StructField("id", st.LongType(), True),
                    st.StructField("name", st.StringType(), True),
                ]
            ),
            True,
        ),
        st.StructField("externalId", st.LongType(), True),
        st.StructField(
            "extras",
            st.StructType(
                [
                    st.StructField("description", st.StringType(), True),
                    st.StructField("mes", st.LongType(), True),
                ]
            ),
            True,
        ),
        st.StructField("id", st.LongType(), True),
        st.StructField("journey", st.StringType(), True),
        st.StructField(
            "rating",
            st.StructType(
                [
                    st.StructField("comment", st.StringType(), True),
                    st.StructField("dateTime", st.StringType(), True),
                    st.StructField("lastUpdate", st.StringType(), True),
                    st.StructField(
                        "motives",
                        st.StructType(
                            [
                                st.StructField(
                                    "negative",
                                    st.ArrayType(
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "description",
                                                    st.StringType(),
                                                    True,
                                                )
                                            ]
                                        ),
                                        True,
                                    ),
                                    True,
                                ),
                                st.StructField(
                                    "positive",
                                    st.ArrayType(
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "description",
                                                    st.StringType(),
                                                    True,
                                                )
                                            ]
                                        ),
                                        True,
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                    st.StructField("source", st.StringType(), True),
                    st.StructField("value", st.LongType(), True),
                ]
            ),
            True,
        ),
        st.StructField("reversed", st.BooleanType(), True),
        st.StructField(
            "revisions",
            st.ArrayType(
                st.StructType(
                    [
                        st.StructField("callOperator", st.StringType(), True),
                        st.StructField("dateTime", st.StringType(), True),
                        st.StructField(
                            "history",
                            st.StructType(
                                [
                                    st.StructField(
                                        "comment",
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "new",
                                                    st.StringType(),
                                                    True,
                                                ),
                                                st.StructField(
                                                    "old",
                                                    st.StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    st.StructField(
                                        "commentType",
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "new",
                                                    st.StringType(),
                                                    True,
                                                ),
                                                st.StructField(
                                                    "old",
                                                    st.StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    st.StructField(
                                        "motives",
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "negative",
                                                    st.ArrayType(
                                                        st.StructType(
                                                            [
                                                                st.StructField(
                                                                    "description",
                                                                    st.StringType(),
                                                                    True,
                                                                )
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                    True,
                                                ),
                                                st.StructField(
                                                    "positive",
                                                    st.ArrayType(st.StringType(), True),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    st.StructField(
                                        "rating",
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "new",
                                                    st.StringType(),
                                                    True,
                                                ),
                                                st.StructField(
                                                    "old",
                                                    st.StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    st.StructField(
                                        "reversed",
                                        st.StructType(
                                            [
                                                st.StructField(
                                                    "new",
                                                    st.StringType(),
                                                    True,
                                                ),
                                                st.StructField(
                                                    "old",
                                                    st.StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        st.StructField(
            "store",
            st.StructType(
                [
                    st.StructField("externalId", st.LongType(), True),
                    st.StructField("id", st.LongType(), True),
                    st.StructField("name", st.StringType(), True),
                ]
            ),
            True,
        ),
        st.StructField("timestamp", st.StringType(), True),
        st.StructField("transactionId", st.LongType(), True),
        st.StructField("type", st.StringType(), True),
    ]
)


df_read = ss.read.json(
    environment=Environment.PRODUCTION,
    zone=Zone.TRANSIENT,
    namespace="nps_solucx",
    dataset="satisfaction_survey",
    schema=SCHEMA,
)

df_nps = df_read.select(
    sf.col("id").alias("id"),
    sf.col("amount").alias("amount"),
    sf.col("datalog").cast("TimesTamp").alias("datalog"),
    sf.col("externalId").alias("externalId"),
    sf.col("journey").alias("journey"),
    sf.col("reversed").alias("reversed"),
    sf.col("timestamp").cast("TimesTamp").alias("timestamp"),
    sf.col("transactionId").alias("transactionId"),
    sf.col("type").alias("type"),
    sf.col("store.id").alias("store_id"),
    sf.col("store.externalId").alias("store_external_id"),
    sf.col("store.name").alias("store_name"),
    sf.col("customer.id").alias("customer_id"),
    sf.col("customer.cpf").alias("customer_cpf"),
    sf.col("customer.email").alias("customer_email"),
    sf.col("customer.name").alias("customer_name"),
    sf.col("customer.phone").alias("customer_phone"),
    sf.col("employee.id").alias("employee_id"),
    sf.col("employee.externalId").alias("employee_external_id"),
    sf.col("employee.name").alias("employee_name"),
    sf.col("extras.description").alias("extras_description"),
    sf.col("extras.mes").alias("extras_mes"),
    sf.col("rating.comment").alias("rating_comment"),
    sf.col("rating.dateTime").cast("TimesTamp").alias("rating_dateTime"),
    sf.col("rating.lastUpdate").cast("TimesTamp").alias("rating_lastUpdate"),
    sf.col("rating.source").alias("rating_source"),
    sf.col("rating.value").alias("rating_value"),
    sf.col("rating.motives.negative.description")
    .cast("string")
    .alias("rating_motives_negative_description"),
    sf.col("rating.motives.positive.description")
    .cast("string")
    .alias("rating_motives_positive_description"),
)

df_revisions = df_read.select(
    sf.col("id").alias("satisfaction_survey_id"),
    sf.col("datalog").cast("TimesTamp").alias("satisfaction_survey_datalog"),
    sf.col("rating.lastUpdate")
    .cast("TimesTamp")
    .alias("satisfaction_survey_rating_lastUpdate"),
    sf.explode("revisions").alias("revisions"),
)

df_revisions = df_revisions.select(
    sf.col("satisfaction_survey_id").alias("satisfaction_survey_id"),
    sf.col("satisfaction_survey_datalog").alias("satisfaction_survey_datalog"),
    sf.col("satisfaction_survey_rating_lastUpdate").alias(
        "satisfaction_survey_rating_lastUpdate"
    ),
    sf.col("revisions.callOperator").alias("revisions_callOperator"),
    sf.col("revisions.dateTime").alias("revisions_dateTime"),
    sf.col("revisions.history.comment.new").alias("revisions_history_comment_new"),
    sf.col("revisions.history.comment.old").alias("revisions_history_comment_old"),
    sf.col("revisions.history.commentType.new").alias(
        "revisions_history_commentType_new"
    ),
    sf.col("revisions.history.commentType.old").alias(
        "revisions_history_commentType_old"
    ),
    sf.col("revisions.history.rating.new").alias("revisions_history_rating_new"),
    sf.col("revisions.history.rating.old").alias("revisions_history_rating_old"),
    sf.col("revisions.history.reversed.new").alias("revisions_history_reversed_new"),
    sf.col("revisions.history.reversed.old").alias("revisions_history_reversed_old"),
    sf.col("revisions.history.motives.positive")
    .cast("string")
    .alias("revisions_history_motives_positive"),
    sf.col("revisions.history.motives.negative.description")
    .cast("string")
    .alias("revisions_history_motives_negative_description"),
)


ss.write.parquet(
    df_nps.repartition(1),
    environment=Environment.PRODUCTION,
    zone=Zone.RAW,
    namespace="nps_solucx",
    dataset="satisfaction_survey",
    mode="overwrite",
)

ss.write.parquet(
    df_revisions.repartition(1),
    environment=Environment.PRODUCTION,
    zone=Zone.RAW,
    namespace="nps_solucx",
    dataset="satisfaction_survey_revisions",
    mode="overwrite",
)
print("overwrite RAW")
