from google.cloud import bigquery
import pandas as pd
import json

# to get list of views from biz_vault
def get_views_from_dataset(project_id):
    query=f"""SELECT table_name,view_definition FROM `{project_id}.biz_vault.INFORMATION_SCHEMA.VIEWS` """
    client=bigquery.Client(project_id)
    query_job=client.query(query)
    results=query_job.result()
    views={row.table_name:row.view_definition.strip() for row in results}
    return views
# to compare the views and return the differences
def compare_view_definition(uat_views,prod_views):
    differences=[]
    for view in uat_views:
         uat_definition=uat_views.get(view)
         prod_definition=prod_views.get(view)
         if view in prod_views:
              if uat_definition.strip() != prod_definition.strip():
                   differences.append({'View': view,
                                       'UAT Definition': uat_definition,
                                       'Prod Definition' : prod_definition})
         else:
              differences.append({'View': view,
                                       'UAT Definition': uat_definition,
                                       'Prod Definition' : 'View missing in prod'})
    for view in prod_views:
         if view not in uat_views:
              differences.append({'View': view,
                                       'UAT Definition': 'View missing in UAT',
                                       'Prod Definition' : prod.views.get(view)})
    return differences
# save comaprision result to csv
def results_tocsv(differences,filename):
    df=pd.DataFrame(differences)
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
differences= compare_view_definition(uat_views,prod_views)
results_tocsv(differences,"Biz_Vault__View_comparision.csv")




    

