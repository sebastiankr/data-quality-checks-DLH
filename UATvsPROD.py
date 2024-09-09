# Compare UAT vs Prod schemas
#from utils import get_table_info
def compare_schemas(schema_uat, schema_prod):
    uat_tables = {entry['table_name'] for entry in schema_uat}
    prod_tables = {entry['table_name'] for entry in schema_prod}

    # Find differences in table names
    only_in_uat = uat_tables - prod_tables
    only_in_prod = prod_tables - uat_tables

    # Compare columns and primary keys for common tables
    common_tables = uat_tables & prod_tables
    column_diffs = []

    uat_dict = {entry['table_name']: entry for entry in schema_uat}
    prod_dict = {entry['table_name']: entry for entry in schema_prod}
   
    for table in common_tables:
        uat_columns = set(uat_dict[table]['columns'])
        prod_columns = set(prod_dict[table]['columns'])
       
       # Compare column names
        cols_only_in_uat = uat_columns - prod_columns
        cols_only_in_prod = prod_columns - uat_columns
       
        # Compare primary keys
        uat_pk = set(uat_dict[table]['primary_keys'])
        prod_pk = set(prod_dict[table]['primary_keys'])
       
        if cols_only_in_uat or cols_only_in_prod or uat_pk != prod_pk:
            column_diffs.append({
                'table_name': table,
                'cols_only_in_uat': list(cols_only_in_uat),
                'cols_only_in_prod': list(cols_only_in_prod),
                'uat_primary_keys': list(uat_pk),
                'prod_primary_keys': list(prod_pk)
            })
   
    return {
        'only_in_uat': only_in_uat,
        'only_in_prod': only_in_prod,
        'column_diffs': column_diffs
    }


   