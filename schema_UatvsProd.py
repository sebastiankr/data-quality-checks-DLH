import os
import pandas as pd
from google.cloud import bigquery
import logging
import json
from datetime import datetime
from utils import get_table_info
from UATvsPROD import compare_schemas
import csv

# Configure logging
#logging.basicConfig(filename='bigquery_logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)
   
# Initialize BigQuery client
def get_bigquery_client(project_id):
    try:
        logging.info('Connecting to BigQuery with project_id: %s', project_id)
        client = bigquery.Client(project=project_id)
        return client
    except Exception as e:
        logging.error('Error connecting to BigQuery: %s', e)
        raise

if __name__ == '__main__':
    config = load_config('config.json')
    project_id = config.get('project_prod_id')
   
    client = get_bigquery_client(project_id)
   
    # Fetch table information
    df_table = pd.DataFrame(get_table_info(client))
    df_table.to_csv('table_details.csv', index=False)
   
    project_id_uat = config.get('project_uat_id')
    project_id_prod = config.get('project_prod_id')
    dataset_id = 'raw_vault'

    # Fetch schema details for UAT and Prod
    schema_uat = get_table_info(get_bigquery_client(project_id_uat))
    schema_prod = get_table_info(get_bigquery_client(project_id_prod))

    # Compare schemas
    comparison_result = compare_schemas(schema_uat, schema_prod)
   
    # Write 'only_in_uat' and 'only_in_prod' to CSV
    with open('table_differences_UatVsProd.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Table Name', 'Present in UAT', 'Present in Prod'])
        for table in comparison_result['only_in_uat']:
            writer.writerow([table, 'Yes', 'No'])
        for table in comparison_result['only_in_prod']:
            writer.writerow([table, 'No', 'Yes'])
   
    # Write 'column_diffs' to CSV
    with open('column_differences_UatVsProd.csv', 'w', newline='') as f:
        fieldnames = ['table_name', 'cols_only_in_uat', 'cols_only_in_prod', 'uat_primary_keys', 'prod_primary_keys']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(comparison_result['column_diffs'])
   
    print("Comparison and table details have been written to CSV files.")