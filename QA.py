import os
from datetime import datetime
import pandas as pd
from QA_tests import get_tests
from zoneinfo import ZoneInfo
import pandas_gbq
tests = get_tests()

csv_file = "tests.csv"

if os.path.exists(csv_file):
    # Load existing data from the CSV file
    df = pd.read_csv(csv_file)
else:
    # Create a new DataFrame if the file does not exist
    df = pd.DataFrame(columns=["date","source","test_name"])

project_id='cdphe-lakehouse-infra-prod'

def get_mismatched_dtypes(df1, df2):
    df1_types = set(df1.dtypes.items())
    df2_types = set(df2.dtypes.items())
    for column_name, df1_type in df1_types - df2_types:
        yield column_name, (df1_type, df2.dtypes[column_name])
        
def qa_compare(source_df, target_df):
    source_cnt = source_df.shape[0]
    target_cnt = target_df.shape[0]
    diff_cnt = source_cnt - target_cnt
    completion_pct = 0
    if target_cnt != 0:
        completion_pct =  100 * target_cnt/source_cnt
    source_col_cnt = source_df.shape[1]    
    mismachted_cols = dict(get_mismatched_dtypes(source_df, target_df))
    
    rows_diff_values_cnt = 0
    diff_df = source_df[target_df.ne(source_df).any(axis=1)]
    rows_diff_values_cnt = diff_df.shape[0]
    
    return {
        "source_cnt" : source_cnt,
        "target_cnt" : target_cnt,
        "diff_cnt": diff_cnt,
        "completion_pct": completion_pct,
        "source_col_cnt": source_col_cnt,
        "mismachted_col_cnt": len(mismachted_cols),
        "mismachted_cols": mismachted_cols,
        "rows_diff_values_cnt": rows_diff_values_cnt
    }
new_tests = []
for t in tests:
    # print(t["source_query"])
    source_df = pandas_gbq.read_gbq(t["source_query"], project_id=project_id)
    target_df = pandas_gbq.read_gbq(t["target_query"], project_id=project_id)
    comparison = qa_compare(source_df, target_df)
    comparison["date"]= datetime.now(ZoneInfo('America/Denver')).strftime("%Y-%m-%d %H:%M:%S")
    comparison["source"]= t["source"]
    comparison["test_name"]= t["test_name"]
    comparison["notes"]= t["notes"]
    print(comparison)
    new_tests.append(comparison)
new_data_df = pd.DataFrame(new_tests)
df = pd.concat([df, new_data_df], ignore_index=True)

# Save the updated DataFrame to the CSV file
df.to_csv(csv_file, index=False)

print(f"Data appended and saved to {csv_file}")