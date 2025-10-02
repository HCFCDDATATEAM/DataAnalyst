import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
import pandas as pd

# Load and Prepare Actuals

def load_actuals_with_invoice_date(session: snowpark.Session):
    
    # Function to select from INVOICE table
    def select_invoice():
        return session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_BP_INVOICE").select(
            col("PROJECTID").cast("string").alias("PROJECT_NUMBER"),
            col("STAGE").alias("STAGE"),
            col("SUBSTAGE").alias("SUB_STAGE"),
            col("INVOICEDATE").alias("INVOICE_DATE"),
            col("INVOICELINEAMOUNT").alias("ACTUALS_COST") # to actuals
        )
    
    # Function to select from PAY_APP table
    def select_payapp():
        return session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_BP_PAYAPP").select(
            col("UNIFIER_PROJECT_ID").cast("string").alias("PROJECT_NUMBER"),
            col("STAGE").alias("STAGE"),
            col("SUBSTAGE").alias("SUB_STAGE"),
            col("PAYMENTDATE").alias("INVOICE_DATE"),
            col("PAYMENTAMOUNT").alias("ACTUALS_COST")
        )
    
    # Function to select from PAY_REQ table  
    def select_payreq():
        return session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_BP_PAYREQ").select(
            col("PROJECT_ID").alias("PROJECT_NUMBER"),
            lit(None).alias("STAGE"),
            lit(None).alias("SUB_STAGE"),
            col("PYMT_DATE").alias("INVOICE_DATE"),
            col("PYMT_AMT").alias("ACTUALS_COST")
        )

    # Load data from all actuals-related sources with updated mappings from CSV
    invoice_df = select_invoice()
    pay_app_df = select_payapp()
    pay_req_df = select_payreq()

    # Union all invoice-related datasets into one dataframe
    return invoice_df.union_all(pay_app_df).union_all(pay_req_df)

# Load Budget Table
    
def load_budget_data(session: snowpark.Session):
    df = session.table("LANDING_DB.FCD_UNIFIER.UNIFIER__Z_CBS_PRJ_DETAIL").select(
        col("PROJECT_NUMBER"),
        col("STAGE"),
        col("SUB_STAGE"),
        col("BUDGET_CURRENT_APPROVED")
    )
    # Aggregate by PROJECT_NUMBER, STAGE, SUB_STAGE (remove fund dependency)
    return df.group_by("PROJECT_NUMBER", "STAGE", "SUB_STAGE").agg({"BUDGET_CURRENT_APPROVED": "sum"}).select(
        col("PROJECT_NUMBER"),
        col("STAGE"),
        col("SUB_STAGE"),
        col("SUM(BUDGET_CURRENT_APPROVED)").alias("BUDGET_CURRENT_APPROVED")
    )

# Load Schedule Table

# def load_schedule_data(session: snowpark.Session):
#     return session.table("FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE").select(
#         col("PROJECTID").alias("PROJECT_NUMBER"),
#         col("STAGECODE").alias("STAGE"),
#         col("STAGESUBSTAGECODE").alias("SUB_STAGE"),
#         col("CURRENTSTART"),
#         col("CURRENTEND")
#     )
# # select * from FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE where PROJECT_NUMBER = 'Z100-00-00-H065';
# # -- filter out for just substage ignore stage
def load_schedule_data(session: snowpark.Session):
    return session.table("FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE").filter(
        col("STAGEORSUBSTAGE") == "Substage"
    ).select(
        col("PROJECTID").alias("PROJECT_NUMBER"),
        col("STAGECODE").alias("STAGE"),
        col("STAGESUBSTAGECODE").alias("SUB_STAGE"),
        col("CURRENTSTART"),
        col("CURRENTEND")
    )
    
# Join All Tables

def join_all_data(actuals_df, budget_df, schedule_df):
    
    # Join actuals with budget by PROJECT_NUMBER only
    joined_with_budget = actuals_df.join(
        budget_df,
        actuals_df["PROJECT_NUMBER"] == budget_df["PROJECT_NUMBER"],
        how="left"
    )
    
    # Then join with schedule by PROJECT_NUMBER only  
    final_joined = joined_with_budget.join(
        schedule_df,
        actuals_df["PROJECT_NUMBER"] == schedule_df["PROJECT_NUMBER"],
        how="left"
    ).select(
        actuals_df["PROJECT_NUMBER"],
        actuals_df["STAGE"],
        actuals_df["SUB_STAGE"], 
        actuals_df["INVOICE_DATE"],
        actuals_df["ACTUALS_COST"],
        budget_df["BUDGET_CURRENT_APPROVED"],
        schedule_df["CURRENTSTART"],
        schedule_df["CURRENTEND"]
    )
    
    return final_joined

# Compute Cumulative Actual Cost

def add_cumulative_per_group(session, df):
    # Set current database for temp stage operations
    session.sql("USE DATABASE FCD_090_DB_DEV").collect()
    
    # Convert Snowpark DataFrame to Pandas for custom logic
    records = [row.as_dict() for row in df.to_local_iterator()]
    pdf = pd.DataFrame(records)

    # Rename columns to consistent format for easier handling
    col_map = {}
    for col_name in pdf.columns:
        if "PROJECT_NUMBER" in col_name: col_map[col_name] = "PROJECT_NUMBER"
        elif "STAGE" in col_name and "SUB" not in col_name: col_map[col_name] = "STAGE"
        elif "SUB_STAGE" in col_name: col_map[col_name] = "SUB_STAGE"
        elif "INVOICE_DATE" in col_name: col_map[col_name] = "INVOICE_DATE"
        elif "ACTUALS_COST" in col_name: col_map[col_name] = "ACTUALS_COST"
        elif "BUDGET_CURRENT_APPROVED" in col_name: col_map[col_name] = "BUDGET_CURRENT_APPROVED"
        elif "CURRENTSTART" in col_name: col_map[col_name] = "CURRENTSTART"
        elif "CURRENTEND" in col_name: col_map[col_name] = "CURRENTEND"
    pdf.rename(columns=col_map, inplace=True)

    # Type conversion
    pdf["INVOICE_DATE"] = pd.to_datetime(pdf["INVOICE_DATE"])
    pdf["ACTUALS_COST"] = pd.to_numeric(pdf["ACTUALS_COST"], errors='coerce').fillna(0)

    # Group and sum duplicate rows by same Project/Stage/SubStage/Invoice Date
    pdf = pdf.groupby(["PROJECT_NUMBER", "STAGE", "SUB_STAGE", "INVOICE_DATE"], as_index=False).agg({
        "ACTUALS_COST": "sum",
        "BUDGET_CURRENT_APPROVED": "first",
        "CURRENTSTART": "first",
        "CURRENTEND": "first"
    })

    # Drop rows where ACTUALS_COST == 0 after summing (offsetting +X and -X removed)
    pdf = pdf[pdf["ACTUALS_COST"] != 0]

    # Sort and calculate cumulative cost
    pdf = pdf.sort_values(by=["PROJECT_NUMBER", "STAGE", "SUB_STAGE", "INVOICE_DATE"])
    pdf["CUMULATIVE_ACTUAL_COST"] = (
        pdf.groupby(["PROJECT_NUMBER", "STAGE", "SUB_STAGE"])["ACTUALS_COST"].cumsum()
    )

    # Convert back to Snowpark DataFrame
    return session.create_dataframe(pdf)

# Main Entry Point

def main(session: snowpark.Session): 
    # Load all three data sources
    actuals_df = load_actuals_with_invoice_date(session)
    budget_df = load_budget_data(session)
    schedule_df = load_schedule_data(session)

    # Join and calculate cumulative actual cost
    joined_df = join_all_data(actuals_df, budget_df, schedule_df)
    final_df = add_cumulative_per_group(session, joined_df)

    # Save result for use in Power BI
    final_df.write.mode("overwrite").save_as_table("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.ACTUALVALUE")

    return final_df
