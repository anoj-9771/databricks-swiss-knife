# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import *

# COMMAND ----------

def get_update_df(source_df, target_table, primary_keys, timestamp):
    """for a given dataframe, compute an updated dataframe so it can be merged into target_table"""
    source_df = (source_df
                .withColumn('ETL_INSERT_DATETIME', F.lit(timestamp).cast(TimestampType()))
                .withColumn('ETL_UPDATE_DATETIME', F.lit(timestamp).cast(TimestampType()))
                .withColumn('ETL_ROW_EFFECTIVE_DATE', F.lit(timestamp).cast(TimestampType()))
                .withColumn('ETL_INSERT_JOB_NAME', F.lit('wf_amd_alm_equip_load'))
                .withColumn('ETL_UPDATE_JOB_NAME', F.lit('wf_amd_alm_equip_load'))
                )
    s_df = source_df.withColumn("merge_key", F.col(primary_keys[0]))
    t_df = source_df.withColumn("merge_key", F.lit(None).cast("string"))
    target_df = spark.table(target_table)

    # Updating column names to append '_target' in target_df
    renamed_columns = [F.col(c).alias(f"{c}_target") for c in target_df.columns]
    target_df = target_df.select(*renamed_columns)

    join_condition = [t_df[c] == target_df[f"{c}_target"] for c in primary_keys]
    filter_condition = F.expr(" OR ".join([f"!(s.{c} <=> t.{c}_target)" for c in source_df.columns if ('ETL' not in c) and ('__operation' not in c) and (c not in primary_keys)]))

    joined_df = t_df.alias("s").join(target_df.alias("t"), on=join_condition, how="inner").filter(filter_condition).filter(F.col("t.ETL_ROW_CURRENT_FLAG_target") == 1)

    # selecting columns not having _target as prefix
    columns_to_keep = [c for c in joined_df.columns if '_target' not in c]

    # Select columns to keep
    joined_df = joined_df.select(*columns_to_keep)
    update_df = s_df.unionByName(joined_df)

    return update_df

# COMMAND ----------

def scd2_merge(update_df, target_table, primary_keys, timestamp):
    """Merge given dataset into target table using the set of primary_keys given.
       Check replication flow metadata for correct operations to form an SCD target."""

    target_tbl = DeltaTable.forName(spark, target_table)
    target_table_df = spark.table(target_table)

    change_columns_conditions = F.expr(" OR ".join([f"NOT (s.{c} <=> t.{c})" for c in target_table_df.columns if ('ETL' not in c) and (c not in primary_keys)]))
    print(change_columns_conditions)
    join_condition = " AND ".join([f"s.{c} = t.{c}" for c in primary_keys])
    join_condition_expr = f"({join_condition}) AND (s.merge_key = t.{primary_keys[0]})"
    timestamp = timestamp - timedelta(seconds=1)

    # Define the merge operation
    target_tbl.alias("t").merge(
        update_df.alias("s"),
        join_condition_expr
    ).whenMatchedUpdate(
        condition=(F.col("s.__operation_type") == 'X'),
        set={
            "ETL_ROW_EXPIRY_DATE": F.lit(timestamp).cast(TimestampType()),
            "ETL_ROW_CURRENT_FLAG": F.lit(0),
            "ETL_ROW_DELETED_FLAG": F.lit(1)
        }
    ).whenMatchedUpdate(
        condition=(
            (F.col("t.ETL_ROW_CURRENT_FLAG") == 1) &
            (F.col("s.__operation_type") != 'X') &
            change_columns_conditions
        ),
        set={
            "ETL_ROW_EXPIRY_DATE": F.lit(timestamp).cast(TimestampType()),
            "ETL_ROW_CURRENT_FLAG": F.lit(0)
        }
    ).whenNotMatchedInsert(
        condition=(F.col("s.__operation_type") != 'X'),
        values={c: F.col(f"s.{c}") for c in target_table_df.columns}
    ).execute()

