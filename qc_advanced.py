"""
QC Advanced Script for ETL Forecasting Tables
Performs deep-dive analytics for ALL projects, including row-level outputs and anomaly checks.
"""

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

# List of tables
TABLES = [
    "ACTUALVALUE",
    "PLANNEDVALUE",
    "FORECASTVALUE",
    "COMBINED_CURVES"
]

ADVANCED_KEYS = ["PROJECT_NUMBER", "STAGE", "SUB_STAGE"]


def analyze_all_projects(session):
    print("\n---\nQC Advanced: Deep-Dive Analytics for ALL Projects\n---")
    # Get all unique projects from COMBINED_CURVES
    combined_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.COMBINED_CURVES")
    all_projects = combined_df.select("PROJECT_NUMBER").distinct().collect()
    for proj_row in all_projects:
        pn = proj_row[0]
        print(f"\nProject: {pn}")
        # ACTUALVALUE details
        actual_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").filter(col("PROJECT_NUMBER") == pn)
        actual_rows = actual_df.limit(5).collect()
        print("ACTUALVALUE (first 5 rows):")
        for row in actual_rows:
            print(row)
        # PLANNEDVALUE details
        planned_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").filter(col("PROJECT_NUMBER") == pn)
        planned_rows = planned_df.limit(5).collect()
        print("PLANNEDVALUE (first 5 rows):")
        for row in planned_rows:
            print(row)
        # FORECASTVALUE details
        forecast_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.FORECASTVALUE").filter(col("PROJECT_NUMBER") == pn)
        forecast_rows = forecast_df.limit(5).collect()
        print("FORECASTVALUE (first 5 rows):")
        for row in forecast_rows:
            print(row)
        # COMBINED_CURVES details
        combined_proj_df = combined_df.filter(col("PROJECT_NUMBER") == pn)
        combined_rows = combined_proj_df.limit(5).collect()
        print("COMBINED_CURVES (first 5 rows):")
        for row in combined_rows:
            print(row)
        # Anomaly checks: missing planned/actual/forecast
        if not planned_rows:
            print("Warning: No planned values found for this project.")
        if not actual_rows:
            print("Warning: No actual values found for this project.")
        if not forecast_rows:
            print("Warning: No forecast values found for this project.")
        # Nulls/outliers with column existence check
        def safe_null_count(df, colname):
            try:
                if colname in [c.name for c in df.schema.fields]:
                    return df.filter(col(colname).is_null()).count()
                else:
                    print(f"Column {colname} not found in table for project {pn}.")
                    return "N/A"
            except Exception as e:
                print(f"Error checking nulls for {colname}: {e}")
                return "Error"
        null_actuals = safe_null_count(actual_df, "ACTUAL_COST") if actual_rows else 0
        null_planned = safe_null_count(planned_df, "PLANNED_COST") if planned_rows else 0
        null_forecast = safe_null_count(forecast_df, "FORECAST_COST") if forecast_rows else 0
        print(f"Null ACTUAL_COST rows: {null_actuals}")
        print(f"Null PLANNED_COST rows: {null_planned}")
        print(f"Null FORECAST_COST rows: {null_forecast}")
        # Outlier detection (example: very high planned/actual/forecast)
        def safe_max(df, colname):
            try:
                if colname in [c.name for c in df.schema.fields]:
                    return df.agg({colname: "max"}).collect()[0][0]
                else:
                    print(f"Column {colname} not found in table for project {pn}.")
                    return "N/A"
            except Exception as e:
                print(f"Error checking max for {colname}: {e}")
                return "Error"
        max_actual = safe_max(actual_df, "ACTUAL_COST") if actual_rows else None
        max_planned = safe_max(planned_df, "PLANNED_COST") if planned_rows else None
        max_forecast = safe_max(forecast_df, "FORECAST_COST") if forecast_rows else None
        print(f"Max ACTUAL_COST: {max_actual}")
        print(f"Max PLANNED_COST: {max_planned}")
        print(f"Max FORECAST_COST: {max_forecast}")
        print("---")


def main(session: snowpark.Session):
    import pandas as pd
    # Ensure current database is set for Snowflake session
    session.sql("USE DATABASE FCD_090_DB_DEV").collect()
    output_lines = []
    def log(line):
        print(line)
        output_lines.append(str(line))

    def analyze_all_projects_export(session):
        log("\n---\nQC Advanced: Deep-Dive Analytics for ALL Projects\n---")
        combined_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.COMBINED_CURVES")
        all_projects = combined_df.select("PROJECT_NUMBER").distinct().collect()
        for proj_row in all_projects:
            pn = proj_row[0]
            log(f"\nProject: {pn}")
            actual_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").filter(col("PROJECT_NUMBER") == pn)
            actual_rows = actual_df.limit(5).collect()
            log("ACTUALVALUE (first 5 rows):")
            for row in actual_rows:
                log(row)
            planned_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").filter(col("PROJECT_NUMBER") == pn)
            planned_rows = planned_df.limit(5).collect()
            log("PLANNEDVALUE (first 5 rows):")
            for row in planned_rows:
                log(row)
            forecast_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.FORECASTVALUE").filter(col("PROJECT_NUMBER") == pn)
            forecast_rows = forecast_df.limit(5).collect()
            log("FORECASTVALUE (first 5 rows):")
            for row in forecast_rows:
                log(row)
            combined_proj_df = combined_df.filter(col("PROJECT_NUMBER") == pn)
            combined_rows = combined_proj_df.limit(5).collect()
            log("COMBINED_CURVES (first 5 rows):")
            for row in combined_rows:
                log(row)
            if not planned_rows:
                log("Warning: No planned values found for this project.")
            if not actual_rows:
                log("Warning: No actual values found for this project.")
            if not forecast_rows:
                log("Warning: No forecast values found for this project.")
            def safe_null_count(df, colname):
                try:
                    if colname in [c.name for c in df.schema.fields]:
                        return df.filter(col(colname).is_null()).count()
                    else:
                        log(f"Column {colname} not found in table for project {pn}.")
                        return "N/A"
                except Exception as e:
                    log(f"Error checking nulls for {colname}: {e}")
                    return "Error"
            null_actuals = safe_null_count(actual_df, "ACTUAL_COST") if actual_rows else 0
            null_planned = safe_null_count(planned_df, "PLANNED_COST") if planned_rows else 0
            null_forecast = safe_null_count(forecast_df, "FORECAST_COST") if forecast_rows else 0
            log(f"Null ACTUAL_COST rows: {null_actuals}")
            log(f"Null PLANNED_COST rows: {null_planned}")
            log(f"Null FORECAST_COST rows: {null_forecast}")
            def safe_max(df, colname):
                try:
                    if colname in [c.name for c in df.schema.fields]:
                        return df.agg({colname: "max"}).collect()[0][0]
                    else:
                        log(f"Column {colname} not found in table for project {pn}.")
                        return "N/A"
                except Exception as e:
                    log(f"Error checking max for {colname}: {e}")
                    return "Error"
            max_actual = safe_max(actual_df, "ACTUAL_COST") if actual_rows else None
            max_planned = safe_max(planned_df, "PLANNED_COST") if planned_rows else None
            max_forecast = safe_max(forecast_df, "FORECAST_COST") if forecast_rows else None
            log(f"Max ACTUAL_COST: {max_actual}")
            log(f"Max PLANNED_COST: {max_planned}")
            log(f"Max FORECAST_COST: {max_forecast}")
            log("---")

    analyze_all_projects_export(session)
    full_output = "\n".join(output_lines)
    print("\n--- QC ADVANCED FULL OUTPUT ---\n")
    print(full_output)
    return session.create_dataframe(pd.DataFrame({"QC_ADVANCED_OUTPUT": [full_output]}))
