#!/usr/bin/env python
# coding: utf-8

# In[14]:


"""
profiling.py

Data profiling script for referral program.

Steps:
1. Load CSV tables
2. Cast columns to appropriate data types
3. Profile each column (null count, distinct count, min/max)
4. Save profiling report to CSV
"""

import pandas as pd
import os

# -----------------------------
# CONFIG
# -----------------------------
DATA_PATH = "data/"
OUTPUT_PATH = "output/profiling_report.csv"


# -----------------------------
# TYPE CASTING
# -----------------------------
def cast_table_types(df, table_name):

    if table_name == "lead_logs":
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    elif table_name == "user_referrals":
        df["referral_reward_id"] = pd.to_numeric(df["referral_reward_id"], errors="coerce")
        df["user_referral_status_id"] = pd.to_numeric(df["user_referral_status_id"], errors="coerce")
        df["referral_at"] = pd.to_datetime(df["referral_at"], errors="coerce")
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    elif table_name == "user_referral_logs":
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["is_reward_granted"] = df["is_reward_granted"].astype("boolean")

    elif table_name == "user_logs":
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        df["membership_expired_date"] = pd.to_datetime(
            df["membership_expired_date"], errors="coerce"
        )
        df["is_deleted"] = df["is_deleted"].astype("boolean")

    elif table_name == "user_referral_statuses":
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    elif table_name == "referral_rewards":
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        df["reward_value"] = pd.to_numeric(df["reward_value"], errors="coerce")
        df["reward_type"] = pd.to_numeric(df["reward_type"], errors="coerce")
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    elif table_name == "paid_transactions":
        df["transaction_at"] = pd.to_datetime(
            df["transaction_at"], errors="coerce"
        )
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype("string")

    return df


# -----------------------------
# LOAD TABLES
# -----------------------------
def load_tables():

    raw_tables = {
        "lead_logs": pd.read_csv(DATA_PATH + "lead_log.csv"),
        "user_referrals": pd.read_csv(DATA_PATH + "user_referrals.csv"),
        "user_referral_logs": pd.read_csv(DATA_PATH + "user_referral_logs.csv"),
        "user_logs": pd.read_csv(DATA_PATH + "user_logs.csv"),
        "user_referral_statuses": pd.read_csv(DATA_PATH + "user_referral_statuses.csv"),
        "referral_rewards": pd.read_csv(DATA_PATH + "referral_rewards.csv"),
        "paid_transactions": pd.read_csv(DATA_PATH + "paid_transactions.csv"),
    }

    tables = {}

    for name, df in raw_tables.items():
        print(f"Casting data types for: {name}")
        tables[name] = cast_table_types(df, name)

    return tables


# -----------------------------
# PROFILE TABLE
# -----------------------------
def profile_table(df, table_name):

    results = []
    total_rows = len(df)

    for column in df.columns:

        null_count = df[column].isnull().sum()
        distinct_count = df[column].nunique()

        min_value = None
        max_value = None

        if pd.api.types.is_numeric_dtype(df[column]) or            pd.api.types.is_datetime64_any_dtype(df[column]):

            min_value = str(df[column].min())
            max_value = str(df[column].max())

        results.append([
            table_name,
            column,
            str(df[column].dtype),
            total_rows,
            int(null_count),
            int(distinct_count),
            min_value,
            max_value
        ])

    return results


#def print_schema(df, table_name):
#    print(f"\nSchema for table: {table_name}")
#   print("-" * 40)
#   for column, dtype in df.dtypes.items():
#       print(f"{column}: {dtype}")


# -----------------------------
# MAIN
# -----------------------------
def main():

    os.makedirs("output", exist_ok=True)

    tables = load_tables()

    all_results = []

    for name, df in tables.items():
   #     print_schema(df, name)
        print(f"Profiling table: {name}")
        all_results.extend(profile_table(df, name))

    columns = [
        "table_name",
        "column_name",
        "data_type",
        "total_rows",
        "null_count",
        "distinct_count",
        "min_value",
        "max_value"
    ]

    profiling_df = pd.DataFrame(all_results, columns=columns)

    profiling_df.to_csv(OUTPUT_PATH, index=False, mode="w")

    print(f"\nProfiling report saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()


# In[ ]:




