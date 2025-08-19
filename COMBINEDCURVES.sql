import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
import pandas as pd

def main(session: snowpark.Session):

    # Load Actual data
    actual_df = session.table("ACTUALVALUE").select(
        col("PROJECT_NUMBER"),
        col("STAGE"),
        col("SUB_STAGE"),
        col("INVOICE_DATE").alias("DATE"),
        col("CUMULATIVE_ACTUAL_COST").alias("VALUE")
    ).with_column("TYPE", lit("ACTUAL"))

    # Load Planned data
    planned_df = session.table("PLANNEDVALUE").select(
        col("PROJECT_NUMBER"),
        col("STAGE"),
        col("SUB_STAGE"),
        col("DATE"),
        col("PLANNED_CURVE_VALUE").alias("VALUE")
    ).with_column("TYPE", lit("PLANNED"))

    # Load Forecast data
    forecast_df = session.table("FORECASTVALUE").select(
        col("PROJECT_NUMBER"),
        col("STAGE"),
        col("SUB_STAGE"),
        col("FORECAST_DATE").alias("DATE"),
        col("FORECAST_FINAL_VALUE").alias("VALUE")
    ).with_column("TYPE", lit("FORECAST"))

    # Convert each Snowpark DataFrame to Pandas for processing and concatenation
    actual_pdf = pd.DataFrame([r.as_dict() for r in actual_df.to_local_iterator()])
    planned_pdf = pd.DataFrame([r.as_dict() for r in planned_df.to_local_iterator()])
    forecast_pdf = pd.DataFrame([r.as_dict() for r in forecast_df.to_local_iterator()])

    # Ensure consistent data types across all three DataFrames
    for df in [actual_pdf, planned_pdf, forecast_pdf]:
        df["PROJECT_NUMBER"] = df["PROJECT_NUMBER"].astype(str)
        df["STAGE"] = df["STAGE"].astype(str)
        df["SUB_STAGE"] = df["SUB_STAGE"].astype(str)
        df["DATE"] = pd.to_datetime(df["DATE"])
        df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce").fillna(0)

    # Combine all three dataframes into a single table with TYPE to differentiate them
    combined_df = pd.concat([actual_pdf, planned_pdf, forecast_pdf], ignore_index=True)

    # Convert back to Snowpark DataFrame and upload to Snowflake
    combined_snowpark_df = session.create_dataframe(combined_df)
    combined_snowpark_df.write.mode("overwrite").save_as_table("COMBINED_CURVES")

    print("COMBINED_CURVES table created successfully with ACTUAL, PLANNED, and FORECAST curves.")

    return combined_snowpark_df.limit(10)

