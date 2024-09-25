import os
from datetime import datetime
import pandas as pd
from QA_tests import get_tests
from zoneinfo import ZoneInfo
import pandas_gbq 
import json
import logging

#logging.basicConfig(filename='hash_id_checks.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

with open('config.json', 'r') as f:
        config = json.load(f)
project_ids= {'uat':config.get('project_uat_id'),
              'prod':config.get('project_prod_id')}

output_folder='results'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

for env,project_id in project_ids.items():
     env_folder=os.path.join(output_folder,env)
     if not os.path.exists(env_folder):
         os.makedirs(env_folder)
     csv_file=os.path.join(env_folder,f"hash_id_checks_{env}.csv")

     if os.path.exists(csv_file):
        # Load existing data from the CSV file
        df = pd.read_csv(csv_file)
     else:
        # Create a new DataFrame if the file does not exist
        df = pd.DataFrame(columns=["date","table_name"])

     cols_df = pandas_gbq.read_gbq('SELECT * FROM raw_vault.INFORMATION_SCHEMA.COLUMNS', project_id=project_id)
     hash_ids_tables = cols_df.loc[(cols_df['column_name'] == 'hash_id')]
     query = """
    SELECT count(*) as cnt FROM(
        SELECT cnt_hash FROM (
        SELECT hash_id, count(*) cnt_hash from `raw_vault.{table}`
        GROUP BY hash_id
        )
        WHERE cnt_hash > 1
        ORDER by cnt_hash DESC
    )
    """

     new_tests = []
     for i,c in hash_ids_tables.iterrows():
        table = c['table_name']
        print(table)
        result_df = pandas_gbq.read_gbq(query.format(table=table), project_id=project_id)
        if not result_df.empty: 

            test_result = {
                "date": datetime.now(ZoneInfo('America/Denver')).strftime("%Y-%m-%d %H:%M:%S"),
                "table_name": table,
                "hash_id_duplications": result_df['cnt'].iloc[0]
            }
            new_tests.append(test_result)
            #print(test_result)
     new_data_df = pd.DataFrame(new_tests)
     df = pd.concat([df, new_data_df], ignore_index=True)
     # Save the updated DataFrame to the CSV file
     df.to_csv(csv_file, index=False)
     print(f"Data appended and saved to {csv_file}")