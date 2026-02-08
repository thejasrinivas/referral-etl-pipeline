#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_timestamp, from_utc_timestamp,
    initcap, upper, month, year, lit, row_number, month, year, current_timestamp, datediff
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
import pandas as pd


DATA_PATH = "data/"
OUTPUT_PATH = "output/final_output.csv"

# -----------------------------
# 1. Spark Session
# -----------------------------
def create_spark_session():
    return (
        SparkSession.builder
        .appName("Referral Fraud Detection")
        .getOrCreate()
    )


# -----------------------------
# 2. Load Data
# -----------------------------
def load_data(spark):
    tables = {
        "lead_logs": spark.read.option("header", True).csv(DATA_PATH + "lead_log.csv"),
        "user_referrals": spark.read.option("header", True).csv(DATA_PATH + "user_referrals.csv"),
        "user_referral_logs": spark.read.option("header", True).csv(DATA_PATH + "user_referral_logs.csv"),
        "user_logs": spark.read.option("header", True).csv(DATA_PATH + "user_logs.csv"),
        "user_referral_statuses": spark.read.option("header", True).csv(DATA_PATH + "user_referral_statuses.csv"),
        "referral_rewards": spark.read.option("header", True).csv(DATA_PATH + "referral_rewards.csv"),
        "paid_transactions": spark.read.option("header", True).csv(DATA_PATH + "paid_transactions.csv"),
    }

    return tables

# -----------------------------
# 3. Clean & Cast
# -----------------------------
def clean_and_cast(tables):

    lead_logs = tables["lead_logs"]
    user_referrals = tables["user_referrals"]
    user_referral_logs = tables["user_referral_logs"]
    user_logs = tables["user_logs"]
    user_referral_statuses = tables["user_referral_statuses"]
    referral_rewards = tables["referral_rewards"]
    paid_transactions = tables["paid_transactions"]

    # -------------------------
    # Basic type casting
    # -------------------------

    lead_logs = lead_logs.withColumn(
        "id", col("id").cast("int")
    )

    user_referrals = user_referrals         .withColumn("referral_reward_id", col("referral_reward_id").cast("int"))         .withColumn("user_referral_status_id", col("user_referral_status_id").cast("int"))

    user_referral_logs = user_referral_logs         .withColumn("id", col("id").cast("int"))         .withColumn("is_reward_granted", col("is_reward_granted").cast("boolean"))

    user_logs = user_logs         .withColumn("id", col("id").cast("int"))         .withColumn("is_deleted", col("is_deleted").cast("boolean"))

    user_referral_statuses = user_referral_statuses.withColumn(
        "id", col("id").cast("int")
    )

    referral_rewards = referral_rewards         .withColumn("id", col("id").cast("int"))         .withColumn("reward_value", col("reward_value").cast("int"))         .withColumn("reward_type", col("reward_type").cast("int"))

    # -------------------------
    # Timezone conversions
    # -------------------------

    lead_logs = lead_logs.withColumn(
        "created_at",
        from_utc_timestamp(
            to_timestamp(col("created_at")),
            col("timezone_location")
        )
    )

    user_logs = user_logs.withColumn(
        "membership_expired_date",
        from_utc_timestamp(
            to_timestamp(col("membership_expired_date")),
            col("timezone_homeclub")
        )
    )

    paid_transactions = paid_transactions.withColumn(
        "transaction_at",
        from_utc_timestamp(
            to_timestamp(col("transaction_at")),
            col("timezone_transaction")
        )
    )


    # -------------------------
    # Column renaming / selection
    # -------------------------

    lead_logs = lead_logs.select(
        col("id").alias("lead_log_id"),
        col("lead_id"),
        col("source_category").alias("lead_source_category"),
        col("created_at").alias("lead_created_at"),
        col("preferred_location"),
        col("timezone_location"),
        col("current_status").alias("lead_status")
    )

    user_referral_logs = user_referral_logs.select(
        col("id").alias("referral_log_id"),
        col("user_referral_id"),
        col("source_transaction_id"),
        col("created_at").alias("reward_granted_at"),
        col("is_reward_granted")
    )

    user_referral_statuses = user_referral_statuses.select(
        col("id").alias("status_id"),
        col("description").alias("status_description"),
        col("created_at").alias("status_created_at")
    )

    referral_rewards = referral_rewards.select(
        col("id").alias("reward_id"),
        col("reward_value"),
        col("created_at").alias("reward_created_at"),
        col("reward_type")
    )

    user_logs = user_logs.select(
        col("id").alias("user_log_id"),
        col("user_id"),
        col("name"),
        col("phone_number"),
        col("homeclub"),
        col("timezone_homeclub"),
        col("membership_expired_date"),
        col("is_deleted")
    )

    # -------------------------
    # Return cleaned tables
    # -------------------------

    return {
        "lead_logs": lead_logs,
        "user_referrals": user_referrals,
        "user_referral_logs": user_referral_logs,
        "user_logs": user_logs,
        "user_referral_statuses": user_referral_statuses,
        "referral_rewards": referral_rewards,
        "paid_transactions": paid_transactions,
    }

# -----------------------------
# 4. Transform & Join
# -----------------------------
def transform_data(tables):

    # Extract tables
    user_referrals = tables["user_referrals"]
    user_referral_logs = tables["user_referral_logs"]
    user_referral_statuses = tables["user_referral_statuses"]
    referral_rewards = tables["referral_rewards"]
    paid_transactions = tables["paid_transactions"]
    lead_logs = tables["lead_logs"]
    user_logs = tables["user_logs"]

    # -------------------------
    # Deduplicate referral logs
    # -------------------------

    window_spec = Window.partitionBy("user_referral_id")                         .orderBy(col("reward_granted_at").desc())

    logs_clean = user_referral_logs         .withColumn("rn", row_number().over(window_spec))         .filter(col("rn") == 1)         .drop("rn")

    # -------------------------
    # Core joins
    # -------------------------

    df = user_referrals.join(
        logs_clean,
        user_referrals.referral_id == logs_clean.user_referral_id,
        "left"
    )

    df = df.join(
        user_referral_statuses,
        df.user_referral_status_id == user_referral_statuses.status_id,
        "left"
    )

    df = df.join(
        referral_rewards,
        df.referral_reward_id == referral_rewards.reward_id,
        "left"
    )

    df = df.join(
        paid_transactions,
        "transaction_id",
        "left"
    )

    # -------------------------
    # Deduplicate lead logs
    # -------------------------

    window_leads = Window.partitionBy("lead_id")                          .orderBy(col("lead_created_at").desc())

    lead_clean = lead_logs         .withColumn("rn", row_number().over(window_leads))         .filter(col("rn") == 1)         .drop("rn")

    df = df.join(
        lead_clean,
        (df.referral_source == "Lead") &
        (df.referee_id == lead_clean.lead_id),
        "left"
    )

    # -------------------------
    # Deduplicate user logs
    # -------------------------

    window_users = Window.partitionBy("user_id")                          .orderBy(col("membership_expired_date").desc())

    users_clean = user_logs         .withColumn("rn", row_number().over(window_users))         .filter(col("rn") == 1)         .drop("rn")

    df = df.join(
        users_clean,
        df.referrer_id == users_clean.user_id,
        "left"
    )

    # -------------------------
    # Referral source category
    # -------------------------

    df = df.withColumn(
        "referral_source_category",
        when(col("referral_source") == "User Sign Up", "Online")
        .when(col("referral_source") == "Draft Transaction", "Offline")
        .when(col("referral_source") == "Lead", col("lead_source_category"))
    )

    # -------------------------
    # Capitalize string columns
    # -------------------------

    string_cols_to_capitalize = [
        "transaction_status",
        "transaction_location",
        "transaction_type",
        "status_description",
        "referee_name",
        "referral_source",
        "referral_source_category"
    ]

    for c in string_cols_to_capitalize:
        df = df.withColumn(c, initcap(col(c)))

    # -------------------------
    # Timezone conversions
    # -------------------------

    df = df         .withColumn(
            "referral_at",
            from_utc_timestamp(
                to_timestamp("referral_at"),
                col("timezone_homeclub")
            )
        ) \
        .withColumn(
            "updated_at",
            from_utc_timestamp(
                to_timestamp("updated_at"),
                col("timezone_homeclub")
            )
        ) \
        .withColumn(
            "reward_granted_at",
            from_utc_timestamp(
                to_timestamp("reward_granted_at"),
                col("timezone_homeclub")
            )
        ) \
        .withColumn(
            "status_created_at",
            from_utc_timestamp(
                to_timestamp("status_created_at"),
                col("timezone_homeclub")
            )
        ) \
        .withColumn(
            "reward_created_at",
            from_utc_timestamp(
                to_timestamp("reward_created_at"),
                col("timezone_homeclub")
            )
        )

    return df

# -----------------------------
# 5. Apply Business Logic
# -----------------------------
def apply_business_logic(df):

    # ------------------------------
    # VALID CONDITIONS
    # ------------------------------

    valid_condition_1 = (
        (col("reward_value") > 0) &
        (col("status_description") == "Berhasil") &
        col("transaction_id").isNotNull() &
        (col("transaction_status") == "Paid") &
        (col("transaction_type") == "New") &
        (col("transaction_at") > col("referral_at")) &
        (month("transaction_at") == month("referral_at")) &
        (year("transaction_at") == year("referral_at")) &
        (col("membership_expired_date") > current_timestamp()) &
        (col("is_deleted") == False) &
        (col("is_reward_granted") == True)
    )

    valid_condition_2 = (
        col("status_description").isin("Menunggu", "Tidak Berhasil") &
        col("reward_value").isNull()
    )

    # ------------------------------
    # INVALID CONDITIONS
    # ------------------------------

    invalid_1 = (
        (col("reward_value") > 0) &
        (col("status_description") != "Berhasil")
    )

    invalid_2 = (
        (col("reward_value") > 0) &
        col("transaction_id").isNull()
    )

    invalid_3 = (
        col("reward_value").isNull() &
        col("transaction_id").isNotNull() &
        (col("transaction_status") == "Paid") &
        (col("transaction_at") > col("referral_at"))
    )

    invalid_4 = (
        (col("status_description") == "Berhasil") &
        ((col("reward_value").isNull()) | (col("reward_value") == 0))
    )

    invalid_5 = (
        col("transaction_at") < col("referral_at")
    )

    # ------------------------------
    # Combine conditions
    # ------------------------------

    invalid_any = invalid_1 | invalid_2 | invalid_3 | invalid_4 | invalid_5
    valid_any = valid_condition_1 | valid_condition_2

    # ------------------------------
    # Final validation flag
    # ------------------------------

    df = df.withColumn(
        "is_business_logic_valid",
        when(invalid_any, False)
        .when(valid_any, True)
        .otherwise(False)
        .cast(BooleanType())
    )

    # ------------------------------
    # Reward duration metric
    # ------------------------------

    df = df.withColumn(
        "num_reward_days",
        when(
            col("reward_granted_at").isNotNull() &
            col("referral_at").isNotNull(),
            datediff(col("reward_granted_at"), col("referral_at"))
        ).cast("int")
    )

    # ------------------------------
    # Final output dataset
    # ------------------------------

    final_df = df.select(
        col("referral_log_id").alias("referral_details_id"),
        col("referral_id"),
        col("referral_source"),
        col("referral_source_category"),
        col("referral_at"),
        col("referrer_id"),
        col("name").alias("referrer_name"),
        col("phone_number").alias("referrer_phone_number"),
        col("homeclub").alias("referrer_homeclub"),
        col("referee_id"),
        col("referee_name"),
        col("referee_phone"),
        col("status_description").alias("referral_status"),
        col("num_reward_days"),
        col("transaction_id"),
        col("transaction_status"),
        col("transaction_at"),
        col("transaction_location"),
        col("transaction_type"),
        col("updated_at"),
        col("reward_granted_at"),
        col("is_business_logic_valid")
    )

    return final_df

def save_output(df):

    os.makedirs("output", exist_ok=True)

    pdf = df.toPandas()
    pdf.to_csv(OUTPUT_PATH, index=False)

    print(f"\nFinal output saved to: {OUTPUT_PATH}")
    
def main():

    spark = None

    try:
        print("Starting Referral ETL Pipeline")

        # 1. Create Spark session
        spark = create_spark_session()
        print("Spark session created")

        # 2. Load raw data
        tables = load_data(spark)
        print("Data loaded")

        # 3. Clean & cast
        tables = clean_and_cast(tables)
        print("Data cleaned and cast")

        # 4. Transform & join
        df = transform_data(tables)
        print("Transformations complete")

        # 5. Apply business logic
        final_df = apply_business_logic(df)
        print("Business logic applied")

        # 6. Save output
        save_output(final_df)
        print("Output saved")

        print("Pipeline completed successfully")

    except Exception as e:
        print("Pipeline failed")
        print(f"Error: {e}")

    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    main()


# In[ ]:




