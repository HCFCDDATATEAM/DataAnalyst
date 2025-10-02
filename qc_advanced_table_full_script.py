import re
import pandas as pd

# Path to the advanced QC output file
qc_file = 'outputs_advanced.txt'

# Regex to extract project blocks
project_block_re = re.compile(r'Project: (.*?)\n(.*?)(?=---\n|\Z)', re.DOTALL)

# Regex patterns for QC issues
qc_patterns = {
    'Missing Actuals': r'Warning: No actual values found',
    'Missing Forecast': r'Warning: No forecast values found',
    'Missing Planned Cost Column': r'Column PLANNED_COST not found',
    'Missing Actual Cost Column': r'Column ACTUAL_COST not found',
    'Missing Forecast Cost Column': r'Column FORECAST_COST not found',
    'Null Actual Cost Rows': r'Null ACTUAL_COST rows: (\d+|N/A)',
    'Null Planned Cost Rows': r'Null PLANNED_COST rows: (\d+|N/A)',
    'Null Forecast Cost Rows': r'Null FORECAST_COST rows: (\d+|N/A)',
    'Max Actual Cost': r'Max ACTUAL_COST: (None|N/A|[\d\.]+)',
    'Max Planned Cost': r'Max PLANNED_COST: (None|N/A|[\d\.]+)',
    'Max Forecast Cost': r'Max FORECAST_COST: (None|N/A|[\d\.]+)',
}

# Read file
with open(qc_file, 'r', encoding='utf-8') as f:
    qc_text = f.read()

# Extract all project blocks
projects = []
for match in project_block_re.finditer(qc_text):
    code = match.group(1).strip()
    block = match.group(2)
    qc_info = {'Project': code}
    for col, pat in qc_patterns.items():
        found = re.search(pat, block)
        if found:
            if 'Null' in col or 'Max' in col:
                qc_info[col] = found.group(1)
            else:
                qc_info[col] = 'Yes'
        else:
            qc_info[col] = 'No' if 'Missing' in col else ''
    projects.append(qc_info)

# Create DataFrame
qc_df = pd.DataFrame(projects)

# Save to CSV and Excel
qc_df.to_csv('qc_advanced_table_full.csv', index=False)
qc_df.to_excel('qc_advanced_table_full.xlsx', index=False)

print('QC summary table generated: qc_advanced_table_full.csv and qc_advanced_table_full.xlsx')
