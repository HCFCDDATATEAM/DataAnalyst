# ETL Forecasting Project: Quality Control Summary

This document summarizes the results of the ETL pipeline quality control (QC) checks, providing a high-level overview of data flow, integrity, and potential gaps. It is designed to help analysts and stakeholders quickly assess the health and completeness of the ETL outputs.

---

## Data Flow Overview

The ETL system processes project financial data through the following stages:

1. **Source Tables**: Raw data from invoices, pay applications, pay requests, budgets, schedules, and normalized curves.
2. **ACTUALVALUE**: Aggregates historical actual costs by project/stage/substage.
3. **PLANNEDVALUE**: Generates daily planned spending curves for each project/stage/substage.
4. **FORECASTVALUE**: Creates future cost forecasts for eligible projects.
5. **COMBINED_CURVES**: Combines actual, planned, and forecast data for unified reporting.

---

## QC Summary Table

| Table                                   | Total Rows | Unique Keys                |
|-----------------------------------------|------------|----------------------------|
| UNIFIER__Z_BP_INVOICE                   | 11,255     | N/A (source)               |
| UNIFIER__Z_BP_PAYAPP                    | 1,173      | N/A (source)               |
| UNIFIER__Z_BP_PAYREQ                    | 152        | N/A (source)               |
| UNIFIER__Z_CBS_PRJ_DETAIL               | 8,175      | N/A (source)               |
| P6_SCHEDULE                             | 5,688      | N/A (source)               |
| NORM_BRGSPENDINGCURVES_SUBSTAGE_0_TO_1  | 1,414      | N/A (source)               |
| ACTUALVALUE                             | 59         | 41                         |
| PLANNEDVALUE                            | 175,670    | 1,877                      |
| FORECASTVALUE                           | 656        | 20                         |
| COMBINED_CURVES                         | 176,385    | N/A (combined table)       |

---

## Key Consistency Checks

- **ACTUALVALUE unique keys:** 41
- **PLANNEDVALUE unique keys:** 1,877
- **FORECASTVALUE unique keys:** 20
- **COMBINED_CURVES unique keys:** N/A (combined table, may have duplicate types)

- **Keys in ACTUALVALUE but not in PLANNEDVALUE:** 16
- **Keys in PLANNEDVALUE but not in ACTUALVALUE:** 1,852

These numbers indicate:
- Some actuals are not mapped to planned values (possible missing schedule or mismatched keys).
- Most planned values do not have corresponding actuals (expected for future or not-yet-started projects).
- Only a small subset of planned projects are eligible for forecasting.

---

## COMBINED_CURVES: Project Row Counts

Sample of row counts by project:

| PROJECT_NUMBER      | COUNT |
|---------------------|-------|
| P118-09-00-E001     | 303   |
| P118-09-00-X014     | 101   |
| P118-25-00-E001     | 674   |
| P118-25-00-P001     | 101   |
| P130-00-00-X026     | 403   |
| ...                 | ...   |
| O100-00-00-E003     | 404   |
| P100-00-00-X156     | 598   |
| P100-00-00-X157     | 598   |
| P103-00-00-H001     | 101   |
| P118-00-00-R004     | 202   |
| K700-02-00-Y001     | 19    |

Total unique projects in COMBINED_CURVES: **679**

---

## QC Next Steps & Example Problem Outputs

### 1. Review projects with actuals but no planned values
These are projects that have actual cost data but do not have corresponding planned values. This may indicate missing schedule data or mismatched keys.

**Examples:**
Row(PROJECT_NUMBER='K124-00-00-X041', STAGE='4', SUB_STAGE='C001')
Row(PROJECT_NUMBER='P118-25-00-E001', STAGE='3', SUB_STAGE='I002')
Row(PROJECT_NUMBER='P118-25-00-E001', STAGE='6', SUB_STAGE='I001')
Row(PROJECT_NUMBER='P118-25-00-E001', STAGE='6', SUB_STAGE='I004')
Row(PROJECT_NUMBER='K124-00-00-X041', STAGE='6', SUB_STAGE='I003')

### 2. Review projects with planned values but no actuals
These are projects that have planned spending curves but no actual cost data. This is expected for future or not-yet-started projects, but may also indicate missing actuals.

**Examples:**
Row(PROJECT_NUMBER='A120-00-00-C003', STAGE='3', SUB_STAGE='I001')
Row(PROJECT_NUMBER='A135-01-00-E001', STAGE='6', SUB_STAGE='G001')
Row(PROJECT_NUMBER='A500-04-00-E001', STAGE='4', SUB_STAGE='D001')
Row(PROJECT_NUMBER='A500-08-00-E002', STAGE='4', SUB_STAGE='C001')
Row(PROJECT_NUMBER='A500-08-00-E002', STAGE='7', SUB_STAGE='H001')

### 3. Confirm that the low forecast coverage matches business requirements
These are projects that have planned values but do not have corresponding forecasts. This may be due to strict eligibility criteria or missing data.

**Examples:**
Row(PROJECT_NUMBER='A120-00-00-C003', STAGE='3', SUB_STAGE='I001')
Row(PROJECT_NUMBER='A135-01-00-E001', STAGE='6', SUB_STAGE='G001')
Row(PROJECT_NUMBER='A500-04-00-E001', STAGE='4', SUB_STAGE='D001')
Row(PROJECT_NUMBER='A500-08-00-E002', STAGE='4', SUB_STAGE='C001')
Row(PROJECT_NUMBER='A500-08-00-E002', STAGE='7', SUB_STAGE='H001')

---

## Additional Gap Checks Between Stages

### Projects in source tables but missing in ACTUALVALUE
Projects missing in ACTUALVALUE: 412
Row(PROJECT_NUMBER='1026')
Row(PROJECT_NUMBER='1791')
Row(PROJECT_NUMBER='1872')
Row(PROJECT_NUMBER='1773')
Row(PROJECT_NUMBER='2077')
Row(PROJECT_NUMBER='1808')
Row(PROJECT_NUMBER='3264')
Row(PROJECT_NUMBER='Z100-00-00-H067')
Row(PROJECT_NUMBER='1818')
Row(PROJECT_NUMBER='3316')

### Projects in P6_SCHEDULE but missing in PLANNEDVALUE
Projects missing in PLANNEDVALUE: 110
Row(PROJECTID='Z100-00-00-P023')
Row(PROJECTID='P138-00-00-X003')
Row(PROJECTID='P118-00-00-X068')
Row(PROJECTID='M102-00-00-X004')
Row(PROJECTID='B100-00-00-X014')
Row(PROJECTID='C106-03-00-C002')
Row(PROJECTID='L112-01-00-E001')
Row(PROJECTID='D140-04-00-X007')
Row(PROJECTID='K140-00-00-X025')
Row(PROJECTID='B104-03-02.1-X008')

### Projects in PLANNEDVALUE but missing in FORECASTVALUE
Projects missing in FORECASTVALUE: 671
Row(PROJECT_NUMBER='P118-26-00-R001')
Row(PROJECT_NUMBER='P118-32-01-X003')
Row(PROJECT_NUMBER='P500-01-00-X024')
Row(PROJECT_NUMBER='P500-02-00-E011')
Row(PROJECT_NUMBER='P500-02-00-E012')
Row(PROJECT_NUMBER='P500-02-00-E013')
Row(PROJECT_NUMBER='P500-06-00-E003')
Row(PROJECT_NUMBER='Q128-00-00-E001')
Row(PROJECT_NUMBER='Q700-01-00-Y002')
Row(PROJECT_NUMBER='T100-00-00-X002')

### Projects in input tables but missing in COMBINED_CURVES
Projects missing in COMBINED_CURVES: 0

---

## Interpretation & Recommendations

- The reduction in row counts from source to output tables is expected due to aggregation, filtering, and joining.
- The large number of planned keys vs. actual keys is normal for a schedule-driven process.
- The low number of forecast keys may indicate strict eligibility or missing data; review business logic if more forecasts are expected.
- Key consistency checks help identify missing or mismatched records. Investigate missing keys for potential data quality improvements.

---

## Next Steps

- Review projects with actuals but no planned values, and vice versa, for completeness.
- Confirm that the low forecast coverage matches business requirements.
- Use the QC script to further investigate gaps and ensure data integrity across all ETL stages.
---


