import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, sum as sf_sum
import pandas as pd
from datetime import timedelta

# Load Actuals
def load_actuals_with_invoice_date(session: snowpark.Session):
    def select_actuals(table_name):
        return session.table(table_name).select(
            col("PROJECTID").alias("PROJECT_NUMBER"),
            col("STAGEPHASE_CODE").alias("STAGE"),
            col("SUBSTAGEACTIVITY_CODE").alias("SUB_STAGE"),
            col("INVOICE_DATE"),
            col("ACTUALS_COST")
        )
    invoice_df = select_actuals("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE")
    pay_app_df = select_actuals("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_APP")
    pay_req_df = select_actuals("FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_REQ")
    return invoice_df.union_all(pay_app_df).union_all(pay_req_df)

# Load Budget and Aggregate

def load_budget_data(session: snowpark.Session):
    df = session.table("FCD_090_DB.UNIFIER.UNIFIER_COSTSHEET_SUBSTAGE_FUND").select(
        col("PROJECT_NUMBER"), col("STAGE"), col("SUB_STAGE"), col("BUDGET_CURRENT_APPROVED")
    )
    return df.group_by("PROJECT_NUMBER", "STAGE", "SUB_STAGE").agg(
        sf_sum("BUDGET_CURRENT_APPROVED").alias("BUDGET_CURRENT_APPROVED")
    )

# Load project schedule data
def load_schedule_data(session: snowpark.Session):
    return session.table("FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE").select(
        col("PROJECTID").alias("PROJECT_NUMBER"),
        col("STAGECODE").alias("STAGE"),
        col("STAGESUBSTAGECODE").alias("SUB_STAGE"),
        col("CURRENTSTART"),
        col("CURRENTEND")
    )

# Load Normalized Curve Table
def load_normalized_curve(session: snowpark.Session):
    df = session.table("FCD_090_DB.CDBGPROGRAM.NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1") \
        .select(
            col("SUBSTAGES"),
            col("PERCENT_COMPLETIONTIME"),
            col("NORM_PERCENT_CUMSPENT")
        )
    records = [row.as_dict() for row in df.to_local_iterator()]
    curve_df = pd.DataFrame(records)

    # Extract substage code from SUBSTAGES column
    curve_df["SUB_STAGE"] = curve_df["SUBSTAGES"].str.extract(r'(^[A-Z0-9]+)')
    curve_df["PERCENT_COMPLETIONTIME"] = curve_df["PERCENT_COMPLETIONTIME"].round(2)
    return curve_df[["SUB_STAGE", "PERCENT_COMPLETIONTIME", "NORM_PERCENT_CUMSPENT"]]

# Join Budget and Schedule
def join_all_data(budget_df, schedule_df):
    return budget_df.join(
        schedule_df,
        (budget_df["PROJECT_NUMBER"] == schedule_df["PROJECT_NUMBER"]) &
        (budget_df["STAGE"] == schedule_df["STAGE"]) &
        (budget_df["SUB_STAGE"] == schedule_df["SUB_STAGE"]),
        how="inner"
    ).select(
        budget_df["PROJECT_NUMBER"],
        budget_df["STAGE"],
        budget_df["SUB_STAGE"],
        budget_df["BUDGET_CURRENT_APPROVED"],
        schedule_df["CURRENTSTART"],
        schedule_df["CURRENTEND"]
    )

# Compute Planned Curve
def compute_curves(session, df, norm_curve_df):
    rows = [r.as_dict() for r in df.to_local_iterator()]
    pdf = pd.DataFrame(rows)

    # Normalize column names
    col_map = {}
    for c in pdf.columns:
        if "PROJECT_NUMBER" in c: col_map[c] = "PROJECT_NUMBER"
        elif "STAGE" in c and "SUB" not in c: col_map[c] = "STAGE"
        elif "SUB_STAGE" in c: col_map[c] = "SUB_STAGE"
        elif "BUDGET_CURRENT_APPROVED" in c: col_map[c] = "BUDGET_CURRENT_APPROVED"
        elif "CURRENTSTART" in c: col_map[c] = "CURRENTSTART"
        elif "CURRENTEND" in c: col_map[c] = "CURRENTEND"

    pdf.rename(columns=col_map, inplace=True)

    # Clean and typecast
    pdf = pdf[pdf["PROJECT_NUMBER"].notnull()]
    pdf["CURRENTSTART"] = pd.to_datetime(pdf["CURRENTSTART"])
    pdf["CURRENTEND"] = pd.to_datetime(pdf["CURRENTEND"])
    pdf["BUDGET_CURRENT_APPROVED"] = pd.to_numeric(pdf["BUDGET_CURRENT_APPROVED"], errors="coerce").fillna(0)

    result_rows = []

    # Loop through each unique substage to generate daily planned values
    for key, group in pdf.groupby(["PROJECT_NUMBER", "STAGE", "SUB_STAGE"]):
        first = group.iloc[0]
        sub = first["SUB_STAGE"]
        start = first["CURRENTSTART"]
        end = first["CURRENTEND"]
        budget = first["BUDGET_CURRENT_APPROVED"]
        # Skip invalid or missing dates
        if pd.isnull(start) or pd.isnull(end) or start >= end:
            continue

        duration = (end - start).days + 1

        # Loop over each day of the schedule to apply the normalized % value
        for i in range(duration):
            current_date = start + timedelta(days=i)
            pct = round(i / duration, 2)
            norm = norm_curve_df[
                (norm_curve_df["SUB_STAGE"] == sub) &
                (norm_curve_df["PERCENT_COMPLETIONTIME"] == pct)
            ]
            norm_pct = norm["NORM_PERCENT_CUMSPENT"].iloc[0] if not norm.empty else 0
            planned_val = norm_pct * float(budget)
            result_rows.append({
                "PROJECT_NUMBER": first["PROJECT_NUMBER"],
                "STAGE": first["STAGE"],
                "SUB_STAGE": sub,
                "DATE": current_date,
                "PLANNED_CURVE_VALUE": planned_val,
                "NORMALIZED_PERCENT": norm_pct
            })

    planned_df = pd.DataFrame(result_rows)

     # Deduplicate overlapping percent entries and sort
    planned_df = (
        planned_df.sort_values(by=["PROJECT_NUMBER", "STAGE", "SUB_STAGE", "DATE"])
                  .groupby(["PROJECT_NUMBER", "STAGE", "SUB_STAGE", "NORMALIZED_PERCENT"], as_index=False)
                  .first()
                  .sort_values(by=["PROJECT_NUMBER", "STAGE", "SUB_STAGE", "DATE"])
    )

    # Compute cumulative planned value to build the full curve
    planned_df["CUMULATIVE_PLANNED_CURVE"] = (
        planned_df.groupby(["PROJECT_NUMBER", "STAGE", "SUB_STAGE"])["PLANNED_CURVE_VALUE"].cumsum()
    )

    return session.create_dataframe(planned_df)

# Main function to orchestrate the planned curve process
def main(session: snowpark.Session):
    actuals_df = load_actuals_with_invoice_date(session)  # Not used here, but kept for structure
    budget_df = load_budget_data(session)
    schedule_df = load_schedule_data(session)
    joined_df = join_all_data(budget_df, schedule_df)
    norm_curve_df = load_normalized_curve(session)
    output_df = compute_curves(session, joined_df, norm_curve_df)

    output_df.write.mode("overwrite").save_as_table("PLANNEDVALUE")
    return output_df



