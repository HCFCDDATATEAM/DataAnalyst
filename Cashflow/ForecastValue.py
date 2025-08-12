import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import (
    col, sum as sf_sum, max as sf_max, when, current_date, greatest, lit, row_number
)
from snowflake.snowpark.window import Window

def create_forecast_curve(session: snowpark.Session):
    # Load actual and planned tables
    planned_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PLANNEDVALUE")
    actual_df = session.table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE")

    # Step 1: Aggregate total actual cost and get the approved budget for each group
    actuals_summary = (
        actual_df.group_by("PROJECT_NUMBER", "STAGE", "SUB_STAGE")
        .agg(
            sf_sum("ACTUALS_COST").alias("TOTAL_ACTUAL_COST"),
            sf_max("BUDGET_CURRENT_APPROVED").alias("BUDGET_CURRENT_APPROVED")
        )
    )

    # Step 2: Get the latest cumulative actual per group (the point from which forecast starts)
    last_cumulative_df = (
        actual_df
        .select("PROJECT_NUMBER", "STAGE", "SUB_STAGE", "INVOICE_DATE", "CUMULATIVE_ACTUAL_COST")
        .with_column("row_num", row_number().over(
            Window.partition_by("PROJECT_NUMBER", "STAGE", "SUB_STAGE")
            .order_by(col("INVOICE_DATE").desc())
        ))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Step 3: Join actual summary and latest cumulative to planned values
    joined_df = (
        planned_df.join(actuals_summary, on=["PROJECT_NUMBER", "STAGE", "SUB_STAGE"])
                  .join(last_cumulative_df, on=["PROJECT_NUMBER", "STAGE", "SUB_STAGE"])
                  .filter(col("DATE") > current_date())
    )

    # Step 4: Calculate total planned value remaining (from today to project end)
    dist_df = (
        joined_df.group_by("PROJECT_NUMBER", "STAGE", "SUB_STAGE")
        .agg(sf_sum("PLANNED_CURVE_VALUE").alias("FUTURE_PLANNED_TOTAL"))
    )

    # Step 5: Forecast calculation based on planned distribution
    forecast_df = (
        joined_df.join(dist_df, on=["PROJECT_NUMBER", "STAGE", "SUB_STAGE"])
        .with_column(
            # Planned percentage for the current date's share
            "PLANNED_PERCENT",
            when(col("FUTURE_PLANNED_TOTAL") == 0, 0)
            .otherwise(col("PLANNED_CURVE_VALUE") / col("FUTURE_PLANNED_TOTAL"))
        )
        .with_column(
            # Budget remaining to be spent
            "REMAINING_AMOUNT",
            greatest(lit(0), col("BUDGET_CURRENT_APPROVED") - col("TOTAL_ACTUAL_COST"))
        )
        .with_column(
            # Distribute the remaining budget according to planned percentage
            "FORECAST_VALUE", col("PLANNED_PERCENT") * col("REMAINING_AMOUNT"))
    )

    # Step 6: Generate cumulative forecast and add to the last actuals
    window_spec = Window.partition_by("PROJECT_NUMBER", "STAGE", "SUB_STAGE").order_by("DATE")

    forecast_output = (
        forecast_df
        .with_column("CUM_FORECAST", sf_sum("FORECAST_VALUE").over(window_spec))
        .with_column(
            # Final forecast = cumulative forecast added to latest cumulative actual cost
            "FORECAST_FINAL_VALUE", col("CUM_FORECAST") + col("CUMULATIVE_ACTUAL_COST"))
        .select(
            col("PROJECT_NUMBER"),
            col("STAGE"),
            col("SUB_STAGE"),
            col("DATE").alias("FORECAST_DATE"),
            col("FORECAST_VALUE"),
            col("FORECAST_FINAL_VALUE")
        )
    )

    # Save output to table
    forecast_output.write.mode("overwrite").save_as_table("FORECASTVALUE")

    return forecast_output

# Entry point for execution
def main(session: snowpark.Session):
    return create_forecast_curve(session)