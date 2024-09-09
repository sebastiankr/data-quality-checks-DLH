# Extract table schema and primary keys using INFORMATION_SCHEMA and load it into a dataframe and create a list out of it
def get_table_info(client):
    query= f"""
    SELECT 
        COL.table_name,
        COL.column_name,
        IF(TC.constraint_type= 'PRIMARY KEY', TRUE, FALSE) AS is_primary_key
    FROM raw_vault.INFORMATION_SCHEMA.COLUMNS COL 
    LEFT JOIN raw_vault.INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCOL
    ON COL.column_name=KCOL.column_name
    AND KCOL.table_name=COL.table_name
    LEFT JOIN raw_vault.INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
    ON KCOL.constraint_name=TC.constraint_name
    AND TC.constraint_type='PRIMARY KEY'
    ORDER BY COL.table_name,COL.ordinal_position 
    """
    df=client.query(query).to_dataframe()
    result=[]
    grouped=df.groupby('table_name')
    for table_name,group in grouped:
        primary_keys=group[group['is_primary_key']].column_name.tolist()
        columns=group.column_name.tolist()
        result.append({'table_name': table_name,
                       'primary_keys':primary_keys,
                       'columns': columns})

    return result
