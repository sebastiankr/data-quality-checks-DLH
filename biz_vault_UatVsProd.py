from google.cloud import bigquery
import pandas as pd
import json

# to get list of views from biz_vault
def get_views_from_dataset(project_id):
    query=f"""SELECT table_name FROM `{project_id}.biz_vault.INFORMATION_SCHEMA.VIEWS` """
    client=bigquery.Client(project_id)
    query_job=client.query(query)
    results=query_job.result()
    views=[row.table_name for row in results]
    return set(views)
# to compare the views and return the differences
def compare_views(uat_views,prod_views):
    views_only_in_uat=uat_views-prod_views
    views_only_in_prod=prod_views-uat_views
    return views_only_in_uat,views_only_in_prod
# save comaprision result to csv
def results_tocsv(views_only_in_uat,views_only_in_prod,filename):
    df=pd.DataFrame({'Views present only in UAT' : list(views_only_in_uat),
          'Views present only in Prod' : list(views_only_in_uat)})
    df.to_csv(filename,index=False)
    print(f"comparision results saved to {filename}")
#loading config file
with open('config.json', 'r') as f:
        config = json.load(f)
#fetch views from uat and prod
uat=config.get('project_uat_id')
prod=config.get('project_prod_id')
#print (uat,prod)
uat_views=get_views_from_dataset(uat)
#print(uat_views)
prod_views=get_views_from_dataset(prod)
only_in_uat,only_in_prod=compare_views(uat_views,prod_views)
results_tocsv(only_in_uat,only_in_prod,"Biz_Vault_comparision.csv")




    

