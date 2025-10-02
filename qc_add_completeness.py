import pandas as pd

# Load the QC summary CSV
qc_df = pd.read_csv('qc_advanced_table_full.csv')

def is_complete(row):
    checks = [
        row['Missing Actuals'] == 'No',
        row['Missing Forecast'] == 'No',
        row['Missing Planned Cost Column'] == 'No',
        row['Missing Actual Cost Column'] == 'No',
        row['Missing Forecast Cost Column'] == 'No',
        str(row['Null Actual Cost Rows']) == '0',
        str(row['Null Planned Cost Rows']) == '0',
        str(row['Null Forecast Cost Rows']) == '0',
        row['Max Actual Cost'] not in ['None', 'N/A', ''],
        row['Max Planned Cost'] not in ['None', 'N/A', ''],
        row['Max Forecast Cost'] not in ['None', 'N/A', ''],
    ]
    return 'Yes' if all(checks) else 'No'

qc_df['Complete'] = qc_df.apply(is_complete, axis=1)

qc_df.to_csv('qc_advanced_table_full.csv', index=False)
qc_df.to_excel('qc_advanced_table_full.xlsx', index=False)

print('Added Complete column to QC summary.')
