# ETL Forecasting Project

This project contains a set of Python scripts designed to work with Snowflake data for project cost forecasting and analysis. The scripts process actual costs, create planned spending curves, generate forecasts, and combine all data for comprehensive reporting.

## Overview

The ETL forecasting system processes project financial data through four main components:

1. **ACTUALVALUE.txt** - Processes historical actual costs
2. **PLANNEDVALUE.txt** - Generates planned spending curves
3. **FORECASTVALUE.txt** - Creates future cost forecasts
4. **COMBINEDCURVES.txt** - Combines all data for unified reporting

## File Descriptions

### 1. ACTUALVALUE.txt

**Purpose**: Processes and aggregates actual project costs from multiple invoice sources to create cumulative spending curves.

**Key Functionality**:
- **Data Sources**: Combines data from three invoice tables (INVOICE, PAY_APP, PAY_REQ)
- **Data Integration**: Joins actual costs with budget and schedule data
- **Deduplication**: Groups and sums duplicate entries by Project/Stage/SubStage/Invoice Date
- **Cumulative Calculation**: Computes running cumulative actual costs over time

**Critical Logic**:
```python
# Removes offsetting transactions (where +X and -X cancel out)
pdf = pdf[pdf["ACTUALS_COST"] != 0]

# Calculates cumulative spending per project stage
pdf["CUMULATIVE_ACTUAL_COST"] = (
    pdf.groupby(["PROJECT_NUMBER", "STAGE", "SUB_STAGE"])["ACTUALS_COST"].cumsum()
)
```

**Output Table**: `ACTUALVALUE` - Contains historical spending data with cumulative costs

---

### 2. PLANNEDVALUE.txt

**Purpose**: Generates planned spending curves using normalized spending patterns and project schedules.

**Key Functionality**:
- **Normalized Curves**: Uses historical spending patterns from `NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1`
- **Schedule Mapping**: Maps spending curves to actual project start/end dates
- **Daily Distribution**: Creates daily planned spending values across project duration
- **Curve Generation**: Applies normalized percentage curves to total budget amounts

**Critical Logic**:
```python
# Maps normalized spending percentages to actual project timeline
for i in range(duration):
    current_date = start + timedelta(days=i)
    pct = round(i / duration, 2)  # Percentage through project
    norm_pct = norm_curve_df[...]["NORM_PERCENT_CUMSPENT"].iloc[0]
    planned_val = norm_pct * float(budget)
```

**Output Table**: `PLANNEDVALUE` - Contains planned spending curves with daily values

---

### 3. FORECASTVALUE.txt

**Purpose**: Creates future cost forecasts by distributing remaining budget based on planned spending patterns.

**Key Functionality**:
- **Current State Analysis**: Determines total actual costs and remaining budget
- **Future Planning**: Filters planned values to dates after today
- **Proportional Distribution**: Distributes remaining budget according to planned curve percentages
- **Cumulative Forecasting**: Builds cumulative forecast starting from last actual cost

**Critical Logic**:
```python
# Calculates remaining budget to be spent
"REMAINING_AMOUNT", greatest(lit(0), col("BUDGET_CURRENT_APPROVED") - col("TOTAL_ACTUAL_COST"))

# Distributes remaining budget proportionally based on planned curve
"FORECAST_VALUE", col("PLANNED_PERCENT") * col("REMAINING_AMOUNT")

# Adds forecast to last actual cumulative cost
"FORECAST_FINAL_VALUE", col("CUM_FORECAST") + col("CUMULATIVE_ACTUAL_COST")
```

**Output Table**: `FORECASTVALUE` - Contains future spending projections

---

### 4. COMBINEDCURVES.txt

**Purpose**: Unifies actual, planned, and forecast data into a single table for comprehensive analysis and reporting.

**Key Functionality**:
- **Data Standardization**: Normalizes column names and data types across all three sources
- **Type Identification**: Adds TYPE column to distinguish ACTUAL, PLANNED, and FORECAST records
- **Data Concatenation**: Combines all three datasets into unified structure
- **Consistency Enforcement**: Ensures consistent data types and handles missing values

**Critical Logic**:
```python
# Standardizes data types across all dataframes
for df in [actual_pdf, planned_pdf, forecast_pdf]:
    df["PROJECT_NUMBER"] = df["PROJECT_NUMBER"].astype(str)
    df["DATE"] = pd.to_datetime(df["DATE"])
    df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce").fillna(0)

# Combines with type identification
combined_df = pd.concat([actual_pdf, planned_pdf, forecast_pdf], ignore_index=True)
```

**Output Table**: `COMBINED_CURVES` - Unified dataset for Power BI reporting

## Data Flow

```
1. ACTUALVALUE.txt    → Process historical costs → ACTUALVALUE table
2. PLANNEDVALUE.txt   → Generate planned curves → PLANNEDVALUE table  
3. FORECASTVALUE.txt  → Create forecasts        → FORECASTVALUE table
4. COMBINEDCURVES.txt → Combine all data        → COMBINED_CURVES table
```

## Key Tables Used

**Source Tables**:
- `FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE`
- `FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_APP`
- `FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_REQ`
- `FCD_090_DB.UNIFIER.UNIFIER_COSTSHEET_SUBSTAGE_FUND`
- `FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE`
- `FCD_090_DB.CDBGPROGRAM.NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1`

**Output Tables**:
- `ACTUALVALUE`
- `PLANNEDVALUE`
- `FORECASTVALUE`
- `COMBINED_CURVES`

## Execution Order

The scripts should be executed in the following order:

1. **ACTUALVALUE.txt** - Establishes baseline actual costs
2. **PLANNEDVALUE.txt** - Creates planned spending baselines
3. **FORECASTVALUE.txt** - Generates forecasts (depends on ACTUALVALUE and PLANNEDVALUE)
4. **COMBINEDCURVES.txt** - Combines all outputs (depends on all previous outputs)

## Critical Business Logic

- **Cumulative Cost Tracking**: All curves show cumulative spending over time
- **Zero-Sum Elimination**: Offsetting transactions are removed to show net spending
- **Budget Distribution**: Remaining budget is distributed proportionally based on historical spending patterns
- **Timeline Alignment**: All data is aligned to actual project schedules
- **Forecast Continuity**: Forecasts start from the last actual cumulative cost point

## Dependencies

- Snowflake Snowpark Python library
- Pandas for data manipulation
- Access to HCFCD project and financial databases
