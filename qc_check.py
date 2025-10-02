"""
QC Script for ETL Forecasting Tables
Checks row counts and key consistency across ACTUALVALUE, PLANNEDVALUE, FORECASTVALUE, and COMBINED_CURVES.
"""

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col


# List of source tables and output tables
SOURCE_TABLES = [
    "UNIFIER__Z_BP_INVOICE",
    "UNIFIER__Z_BP_PAYAPP",
    "UNIFIER__Z_BP_PAYREQ",
    "UNIFIER__Z_CBS_PRJ_DETAIL",
    "P6_SCHEDULE",
    "NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1"
]
OUTPUT_TABLES = [
    "ACTUALVALUE",
    "PLANNEDVALUE",
    "FORECASTVALUE",
    "COMBINED_CURVES"
]


def get_row_count(session, table):
    return session.table(f"FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.{table}").count()

def get_counts_by_project(session, table):
    df = session.table(f"FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.{table}")
    return df.group_by("PROJECT_NUMBER").count().to_pandas()

def get_unique_keys(session, table, keys):
    df = session.table(f"FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.{table}")
    return df.select(keys).distinct().count()

def main(session: snowpark.Session):
    import pandas as pd
    try:
        keys = ["PROJECT_NUMBER", "STAGE", "SUB_STAGE"]
        print("\n---\nQC Next Steps & Example Problem Outputs")
        # 1. Review projects with actuals but no planned values
        # Collect all outputs in a list for export
        output_lines = []
        def log(line):
            print(line)
            output_lines.append(str(line))

        def analyze_problem_projects_export(session):
            problem_projects = [
                {"PROJECT_NUMBER": "K124-00-00-X041", "STAGE": None, "SUB_STAGE": None},
                {"PROJECT_NUMBER": "P118-25-00-E001", "STAGE": None, "SUB_STAGE": None},
                {"PROJECT_NUMBER": "A120-00-00-C003", "STAGE": None, "SUB_STAGE": None},
            ]
            keys = ["PROJECT_NUMBER", "STAGE", "SUB_STAGE"]
            log("\n---\nAdvanced Project Analytics & Example Outputs")
            actual_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").select(keys).distinct()
            planned_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").select(keys).distinct()
            forecast_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.FORECASTVALUE").select(keys).distinct()
            for proj in problem_projects:
                pn = proj["PROJECT_NUMBER"]
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
                combined_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.COMBINED_CURVES").filter(col("PROJECT_NUMBER") == pn)
                combined_rows = combined_df.limit(5).collect()
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
            log("\n---\nEnd of advanced analytics for problem projects.")

        analyze_problem_projects_export(session)
        actual_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").select(keys).distinct()
        planned_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").select(keys).distinct()
        missing_in_planned = actual_keys.subtract(planned_keys)
        log("\nProjects with actuals but no planned values (examples):")
        for row in missing_in_planned.limit(5).collect():
            log(row)
        # ...existing code for other gap checks, replace print with log...
        summary = []
        # ...existing code for summary table...
        # At the end, print all output_lines as text and return as a single string column
        full_output = "\n".join(output_lines)
        print("\n--- QC FULL OUTPUT ---\n")
        print(full_output)
        summary_df = pd.DataFrame(summary)
        if 'Unique Keys' in summary_df.columns:
            summary_df['Unique Keys'] = summary_df['Unique Keys'].astype(str)
        log(summary_df)
        if not summary:
            return session.create_dataframe(pd.DataFrame({"QC_OUTPUT": [full_output]}))
        return session.create_dataframe(summary_df)
    except Exception as e:
        # Always return a DataFrame with error message if something fails
        return session.create_dataframe(pd.DataFrame({"QC_ERROR": [str(e)]}))
def analyze_problem_projects(session):
    """
    Advanced analytics for three problem projects, with example outputs.
    Projects: K124-00-00-X041, P118-25-00-E001, A120-00-00-C003
    """
    problem_projects = [
        {"PROJECT_NUMBER": "K124-00-00-X041", "STAGE": None, "SUB_STAGE": None},
        {"PROJECT_NUMBER": "P118-25-00-E001", "STAGE": None, "SUB_STAGE": None},
        {"PROJECT_NUMBER": "A120-00-00-C003", "STAGE": None, "SUB_STAGE": None},
    ]
    keys = ["PROJECT_NUMBER", "STAGE", "SUB_STAGE"]
    print("\n---\nAdvanced Project Analytics & Example Outputs")
    # Prepare keys for gap checks
    actual_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").select(keys).distinct()
    planned_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").select(keys).distinct()
    forecast_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.FORECASTVALUE").select(keys).distinct()

    for proj in problem_projects:
        pn = proj["PROJECT_NUMBER"]
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
        combined_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.COMBINED_CURVES").filter(col("PROJECT_NUMBER") == pn)
        combined_rows = combined_df.limit(5).collect()
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

    print("\n---\nEnd of advanced analytics for problem projects.")

    # 2. Projects with planned values but no actuals
    print("\nProjects with planned values but no actuals (examples):")
    missing_in_actual = planned_keys.subtract(actual_keys)
    for row in missing_in_actual.limit(5).collect():
        print(row)

    # 3. Confirm low forecast coverage
    print("\nProjects with planned values but no forecast (examples):")
    forecast_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.FORECASTVALUE").select(keys).distinct()
    missing_in_forecast = planned_keys.subtract(forecast_keys)
    for row in missing_in_forecast.limit(5).collect():
        print(row)

    print("\nUse these outputs to further investigate gaps and ensure data integrity across all ETL stages.")

    print("\nQC Report: Gap Checks Between Stages")
    # 1. Source tables to ACTUALVALUE
    print("\nProjects in source tables but missing in ACTUALVALUE:")
    # Get all project numbers from source tables
    invoice_projects = session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_BP_INVOICE").select(col("PROJECTID").cast("string").alias("PROJECT_NUMBER")).distinct()
    payapp_projects = session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_BP_PAYAPP").select(col("UNIFIER_PROJECT_ID").cast("string").alias("PROJECT_NUMBER")).distinct()
    payreq_projects = session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_BP_PAYREQ").select(col("PROJECT_ID").cast("string").alias("PROJECT_NUMBER")).distinct()
    all_source_projects = invoice_projects.union_all(payapp_projects).union_all(payreq_projects).distinct()
    actual_projects = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").select(col("PROJECT_NUMBER").cast("string").alias("PROJECT_NUMBER")).distinct()
    missing_in_actual = all_source_projects.subtract(actual_projects)
    print(f"Projects missing in ACTUALVALUE: {missing_in_actual.count()}")
    # Optionally print missing project numbers
    for row in missing_in_actual.limit(10).collect():
        print(row)

    # 2. P6_SCHEDULE to PLANNEDVALUE
    print("\nProjects in P6_SCHEDULE but missing in PLANNEDVALUE:")
    schedule_projects = session.table("FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE").select("PROJECTID").distinct()
    planned_projects = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").select("PROJECT_NUMBER").distinct()
    missing_in_planned = schedule_projects.subtract(planned_projects)
    print(f"Projects missing in PLANNEDVALUE: {missing_in_planned.count()}")
    for row in missing_in_planned.limit(10).collect():
        print(row)

    # 3. PLANNEDVALUE to FORECASTVALUE
    print("\nProjects in PLANNEDVALUE but missing in FORECASTVALUE:")
    forecast_projects = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.FORECASTVALUE").select("PROJECT_NUMBER").distinct()
    missing_in_forecast = planned_projects.subtract(forecast_projects)
    print(f"Projects missing in FORECASTVALUE: {missing_in_forecast.count()}")
    for row in missing_in_forecast.limit(10).collect():
        print(row)

    # 4. Combined table check
    print("\nProjects in input tables but missing in COMBINED_CURVES:")
    combined_projects = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.COMBINED_CURVES").select("PROJECT_NUMBER").distinct()
    all_input_projects = actual_projects.union_all(planned_projects).union_all(forecast_projects).distinct()
    missing_in_combined = all_input_projects.subtract(combined_projects)
    print(f"Projects missing in COMBINED_CURVES: {missing_in_combined.count()}")
    for row in missing_in_combined.limit(10).collect():
        print(row)
    # Ensure current database is set for Snowflake session
    session.sql("USE DATABASE FCD_090_DB_DEV").collect()
    keys = ["PROJECT_NUMBER", "STAGE", "SUB_STAGE"]

    summary = []

    print("QC Report: Source Table Row Counts")
    for table in SOURCE_TABLES:
        # Use LANDING_DB.FCD_UNIFIER for source tables except schedule and curve
        if table == "P6_SCHEDULE":
            full_table = "FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE"
        elif table == "NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1":
            full_table = "FCD_090_DB.CDBGPROGRAM.NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1"
        else:
            full_table = f"LANDING_DB.FCD_UNIFIER.{table}"
        try:
            count = session.table(full_table).count()
        except Exception as e:
            count = f"Error: {e}"
        summary.append({"Table": table, "Total Rows": count, "Unique Keys": "N/A (source)"})
        print(f"{table}: {count} rows")

    print("\nQC Report: Output Table Row Counts (README Order)")
    for table in OUTPUT_TABLES:
        count = get_row_count(session, table)
        summary.append({"Table": table, "Total Rows": count})
        print(f"{table}: {count} rows")

    print("\nQC Report: Counts by Project Number")
    for table in OUTPUT_TABLES:
        print(f"\n{table} counts by PROJECT_NUMBER:")
        project_counts = get_counts_by_project(session, table)
        print(project_counts)


    print("\nQC Report: Unique Key Counts")
    for table in OUTPUT_TABLES:
        if table == "COMBINED_CURVES":
            print(f"{table} unique keys: N/A (combined table, may have duplicate types)")
            summary_entry = next(item for item in summary if item["Table"] == table)
            summary_entry["Unique Keys"] = "N/A (combined table)"
        else:
            unique_count = get_unique_keys(session, table, keys)
            print(f"{table} unique keys: {unique_count}")
            summary_entry = next(item for item in summary if item["Table"] == table)
            summary_entry["Unique Keys"] = unique_count

    print("\nQC Report: Key Consistency")
    actual_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE").select(keys).distinct()
    planned_keys = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE").select(keys).distinct()
    missing_in_planned = actual_keys.subtract(planned_keys)
    print(f"Keys in ACTUALVALUE but not in PLANNEDVALUE: {missing_in_planned.count()}")
    missing_in_actual = planned_keys.subtract(actual_keys)
    print(f"Keys in PLANNEDVALUE but not in ACTUALVALUE: {missing_in_actual.count()}")

    print("\nQC Summary Table (Inputs vs Final Output)")
    import pandas as pd
    summary_df = pd.DataFrame(summary)
    # Ensure all 'Unique Keys' are strings for Snowpark compatibility
    if 'Unique Keys' in summary_df.columns:
        summary_df['Unique Keys'] = summary_df['Unique Keys'].astype(str)
    print(summary_df)
    # Always return a Snowpark DataFrame, even if summary is empty
    if not summary:
        # Return a simple DataFrame with a message
        return session.create_dataframe(pd.DataFrame({"QC_STATUS": ["QC checks completed. See console output for details."]}))
    return session.create_dataframe(summary_df)

if __name__ == "__main__":
    # Example usage: pass a valid Snowpark session
    pass
