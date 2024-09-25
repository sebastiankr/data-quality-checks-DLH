import os
from datetime import datetime
import pandas as pd
from QA_tests import get_tests
from zoneinfo import ZoneInfo
import pandas_gbq
import logging
import json
#logging.basicConfig(filename='model_checks.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

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
     csv_file=os.path.join(env_folder,f"model_checks_{env}.csv")
     if os.path.exists(csv_file):
        # Load existing data from the CSV file
        df = pd.read_csv(csv_file)
     else:
        # Create a new DataFrame if the file does not exist
        df = pd.DataFrame(columns=["date","table_name"])

     cols_df = pandas_gbq.read_gbq('SELECT * FROM raw_vault.INFORMATION_SCHEMA.COLUMNS ORDER BY table_name', project_id=project_id)
     all_tables= cols_df['table_name'].drop_duplicates().tolist()

     key_cols_df = pandas_gbq.read_gbq(
    """
    SELECT
        --*
       kcu.table_name,kcu.column_name, constraint_type 
    FROM raw_vault.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    JOIN raw_vault.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON 
        kcu.constraint_catalog = tc.constraint_catalog AND 
        kcu.constraint_schema = tc.constraint_schema AND 
        kcu.constraint_name = tc.constraint_name AND 
        kcu.table_catalog = tc.table_catalog AND 
        kcu.table_schema = tc.table_schema AND 
        kcu.table_name = tc.table_name
    WHERE constraint_type = 'PRIMARY KEY'
    ORDER BY tc.table_name, kcu.ordinal_position
    """, project_id=project_id)
     table_defs = []
     for t in all_tables:
         pk_cols_df = key_cols_df.loc[(key_cols_df['table_name'] == t)]
         pk_cols= pk_cols_df['column_name'].tolist()
         table_defs.append(
        {
            "table_name": t,
            "pk_cols": pk_cols
        }
    )
     query1 = """
SELECT count(*) as cnt FROM(
  SELECT {pk_str}, count(*) as cnt from `raw_vault.{table}`
  GROUP BY {pk_str}
)
WHERE cnt > 1
"""

     new_tests = []
     for td in table_defs:
         table = td['table_name']
         pk_str = ','.join(td['pk_cols'])
         has_pks = len(td['pk_cols'])>1
         test_result = {
        "date": datetime.now(ZoneInfo('America/Denver')).strftime("%Y-%m-%d %H:%M:%S"),
        "table_name":table,
        "has_pks": has_pks,
        "pk_duplicates": 'N/A'
            }
         if has_pks:
          result1_df = pandas_gbq.read_gbq(query1.format(table=table, pk_str=pk_str), project_id=project_id)
          test_result['pk_duplicates'] = result1_df['cnt'].iloc[0]
        
          new_tests.append(test_result)
     #print(test_result)
     new_data_df = pd.DataFrame(new_tests)
     df = pd.concat([df, new_data_df], ignore_index=True)

# Save the updated DataFrame to the CSV file
     df.to_csv(csv_file, index=False)

     print(f"Data appended and saved to {csv_file}")