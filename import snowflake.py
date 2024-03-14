import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
import json

connection_sf = "connexion/sf_hr.json"
with open(connection_sf) as f:
    connection_params = json.load(f)
session_sf = Session.builder.configs(connection_params).create()

def main():
    try:
        src_tables = session_sf.sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEAM_TAILOR' AND TABLE_NAME LIKE 'vw_api%' ORDER BY 1").collect()
        tgt_tables = session_sf.sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEAM_TAILOR' AND TABLE_NAME LIKE 'tt_%' ORDER BY 1").collect()

        iCountInsert = 0
        iCountUpdate = 0
        iCountInsertFinal = 0
        iCountUpdateFinal = 0
        if len(src_tables) == len(tgt_tables) and len(src_tables)>0:
            for idx in range(len(src_tables)):
                iCountInsert,iCountUpdate = format_filter_condition(session_sf,str(src_tables[idx][0]),str(tgt_tables[idx][0]),'id,source','id,source' )
                iCountInsertFinal = iCountInsertFinal + iCountInsert
                iCountUpdateFinal = iCountUpdateFinal + iCountUpdate
        else:
            return 'Error - Count of source tables doesnt match the target tables count'
        
        return "Number fo rows inserted: {0} / Updated: {1}".format(str(iCountInsertFinal), str(iCountUpdateFinal))
    except Exception as e:
        return str(e)


def format_filter_condition(snowpark_session, src_table, tgt_table, src_filter,tgt_filter):
    filter_cond = list()
    split_src = src_filter.split(',')
    split_tgt = tgt_filter.split(',')

    #--Check both source and target filter condition are the same length
    #--Note : Filter condition order matters here:
    if len(split_src) == len(split_tgt):
        for i in range(len(split_src)):
            filter_cond.append('src."' + split_src[i]  + '" = tgt."' + split_tgt[i]+ '"')
    else:
        return "Error"
    
    s_filter_cond = " AND ".join(filter_cond)

    #--Call the function to generate the merge statement
    s_merge_statment = format_insert_update(snowpark_session,src_table,tgt_table,s_filter_cond)

    #--Execute the merge statement
    iInsertedRows = 0
    iUpdatedRows = 0
    if s_merge_statment.upper() != 'ERROR':
        src_table_col = session_sf.sql(s_merge_statment).collect()
        iInsertedRows = int(src_table_col[0][0])
        iUpdatedRows  = int(src_table_col[0][1])

    return iInsertedRows,iUpdatedRows

def format_insert_update(snowpark_session,src_table,tgt_table,s_filter_cond):
    """
        Function query the snowflake metadata and generate the Merge
    """
    sel_colum = list()
    update_col = list()
    insert_sel= list()
    insert_val = list()
    
    src_table_col = snowpark_session.sql("SELECT COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' ORDER BY 1".format(src_table)).collect()
    tgt_table_col = snowpark_session.sql("SELECT COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' ORDER BY 1".format(tgt_table)).collect()

    if len(src_table_col) != 0:
        for idx_value in range (len(src_table_col)):
            sel_colum.append('"' + src_table_col[idx_value][0] + '"')
            insert_val.append('src."' + str(src_table_col[idx_value][0]) + '"')
            insert_sel.append('tgt."' + str(tgt_table_col[idx_value][0]) + '"')
            update_col.append('tgt."' + str(tgt_table_col[idx_value][0]) + '" = src."' + str(src_table_col[idx_value][0]) + '"')
        
        s_merge_stmnt =  """MERGE INTO "{0}" tgt USING (SELECT DISTINCT  {1} FROM "{2}") src ON {3} WHEN MATCHED THEN UPDATE SET {4} WHEN NOT MATCHED THEN INSERT({5}) VALUES ({6})""".format(tgt_table,",".join(sel_colum),src_table,s_filter_cond,
                                                                                ','.join(update_col),','.join(insert_sel),','.join(insert_val))
    else : 
        return "Error"
    return s_merge_stmnt

main()
