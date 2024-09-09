import os
from datetime import datetime
import pandas as pd
from utils import get_table_info
from google.cloud import bigquery
from zoneinfo import ZoneInfo
import pandas_gbq
import json
import logging
import csv

# configure logging
logging.basicConfig(filename='datecheck_logs.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# loading the config file
def load_config(path):
    with open(path,'r') as f:
        return json.load(f)
# Initialize BigQuery client
def get_bigquery_client(project_id):
    try:
        logging.info('Connecting to Bigquery with project_id : %s',project_id)
        client = bigquery.Client(project=project_id)
        return client
    except Exception as e :
        logging.error('Error connecting to Bigquery: %s', e)
        raise
csv_file = "datechecks.csv"
if os.path.exists(csv_file):
    # Load existing data from the CSV file
    df = pd.read_csv(csv_file)
else:
    # Create a new DataFrame if the file does not exist
    df = pd.DataFrame(columns=["date","table_name"])

if __name__ == '__main__':
    
    config=load_config('config.json')
    project_id=config.get('project_id')
    client = get_bigquery_client(project_id)
    table_defs = get_table_info(client)
    query1 = """SELECT count(*) cnt FROM(SELECT effective_start_dt, effective_end_dt from `raw_vault.{table}` 
    WHERE effective_end_dt IS NOT NULL AND effective_start_dt >= effective_end_dt) """
    query2 = """
    SELECT count(*) cnt FROM(
    SELECT effective_start_dt, effective_end_dt from `raw_vault.{table}` 
    WHERE effective_end_dt IS NOT NULL AND effective_start_dt < DATE_ADD(effective_end_dt, INTERVAL 60 MINUTE)
    )
    """
    query3 = """
    SELECT count(*) cnt FROM(
    SELECT effective_start_dt, effective_end_dt, CURRENT_DATETIME() from `raw_vault.{table}`
    WHERE effective_end_dt IS NOT NULL AND effective_end_dt > CURRENT_DATETIME()
    )
    """
    query4 = """
    SELECT 
    count(*) cnt
    FROM(
    SELECT 
        effective_start_dt, effective_end_dt, load_dt, 
        LAG(effective_end_dt) OVER (PARTITION BY {pk_str} ORDER BY record_owner, effective_start_dt ASC, effective_start_dt ASC, load_dt) AS prv_end,
        DATETIME_DIFF(effective_start_dt, LAG(effective_end_dt) OVER (PARTITION BY {pk_str} ORDER BY effective_start_dt ASC, effective_start_dt ASC, load_dt), MICROSECOND) diff,
    from `raw_vault.{table}`
    ORDER BY effective_start_dt,effective_end_dt, load_dt
    )
    WHERE diff > 0
    """
    new_tests = []
    for td in table_defs:
        table = td['table_name']
        pk_cols = td['primary_keys']
        cols = td['columns']
        if 'effective_start_dt' in cols and len(pk_cols) > 1:
            print(table)
            pk_str = ','.join(pk_cols)
            print(pk_str)
            result1_df = pandas_gbq.read_gbq(query1.format(table=table), project_id=project_id)
            result2_df = pandas_gbq.read_gbq(query2.format(table=table), project_id=project_id)
            result3_df = pandas_gbq.read_gbq(query3.format(table=table), project_id=project_id)
            result4_df = pandas_gbq.read_gbq(query4.format(table=table, pk_str=pk_str), project_id=project_id)
            if not result1_df.empty: 

                test_result = {
                    "date": datetime.now(ZoneInfo('America/Denver')).strftime("%Y-%m-%d %H:%M:%S"),
                    "table_name": table,
                    "start_after_or_equals_end": result1_df['cnt'].iloc[0],
                    "start_after_end_plus_1h": result2_df['cnt'].iloc[0],
                    "end_in_future": result3_df['cnt'].iloc[0],
                    "gap_btw_start_and_prevend": result4_df['cnt'].iloc[0]
                }
                new_tests.append(test_result)
                #print(test_result)
                #header=["date","table_name","start_after_or_equals_end","start_after_end_plus_1h","end_in_future", "gap_btw_start_and_prevend"]
                #new_tests.to_csv('date_check.csv', index=False)
                #with open('date_check.csv', 'w', newline='') as file:
                   # writer = csv.DictWriter(file, fieldnames=header)
                    #writer.writeheader()
                    #writer.writerows(new_tests) 
new_data_df = pd.DataFrame(new_tests)
df = pd.concat([df, new_data_df], ignore_index=True)

# Save the updated DataFrame to the CSV file
df.to_csv(csv_file, index=False)

print(f"Data appended and saved to {csv_file}")