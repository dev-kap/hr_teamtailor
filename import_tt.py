import requests
import json
import pandas
#import _snowflake
import snowflake.snowpark as snowpark
from datetime import datetime
import time,pytz
from snowflake.snowpark import Session, DataFrame

#Global Variables
#Endpoint URLs
endpoint_url = "https://api.teamtailor.com/v1/"

def main(loadType):#session: snowpark.Session):
    start_dtm = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    connection_sf = "connexion/sf_hr.json"
    with open(connection_sf) as f:
        connection_params = json.load(f)
    session_sf = Session.builder.configs(connection_params).create()

    #Params      
    endpoint_params_job_app = dict()
    endpoint_params_job = dict()
    endpoint_params_candidate = dict()
    endpoint_params_department = dict()
    endpoint_params_location = dict()
    endpoint_params_roles = dict()
    endpoint_params_users = dict()
    endpoint_params_reject_reason = dict()
    endpoint_params_company = dict()

    endpoint_params_job_app["include"]  = "candidate,job,stage,reject-reason"
    endpoint_params_job_app["sort"] = "updated-at"
    endpoint_params_job["include"]      = "department,location,role,user" 
    endpoint_params_job["filter[status"] = "all"
    endpoint_params_job["filter[feed]"] = "public,internal"
    endpoint_params_job["sort"] = "updated-at"
    endpoint_params_candidate["include"]= "activities"
    endpoint_params_candidate["sort"] = "updated-at"
    endpoint_params_users["include"] = "department,location"
    try: 
          #--Truncate Api tables
          truncate_api_tables(session_sf)
          #--Retrieve list of source & country
          src_countries = session_sf.sql('SELECT "source","country" FROM HR.TEAM_TAILOR."cfg_sources" WHERE "isactive"=\'Y\' ORDER BY "sort"').collect()

          #--
          if len(src_countries) > 0 :
               for idx in range(len(src_countries)):
                    source = src_countries[idx]
                    
                    if loadType in ('FULL','DIM', None):
                         #--COMPANY
                         create_dataframe(f'{endpoint_url}company','company',source,endpoint_params_company,session_sf)
                         print(str(source['source']) + ' - Company Done')    
                         #--DEPARTMENT
                         create_dataframe(f'{endpoint_url}departments','department', source, endpoint_params_department, session_sf)
                         print(str(source['source']) + ' - Job Department Done')
                         #--LOCATION
                         create_dataframe(f'{endpoint_url}locations', 'location',source,endpoint_params_location,session_sf)   
                         print(str(source['source']) + ' - Job Location Done')      
                         #--ROLE
                         create_dataframe(f'{endpoint_url}roles','roles',source,endpoint_params_roles, session_sf)
                         print(str(source['source']) + ' - Job Role  Done')
                         #--USERS
                         create_dataframe(f'{endpoint_url}users','users',source,endpoint_params_users,session_sf)   
                         print(str(source['source']) + ' - Job User Done')   
                         #--REJECT REASON                         
                         create_dataframe(f'{endpoint_url}reject-reasons','reject_reasons',source,endpoint_params_reject_reason, session_sf)   
                         print(str(source['source']) +  ' - Job Reject Reason Done')    

                    if loadType in ('FULL','FACT', None) :                         
                         #--JOB
                         create_dataframe(f'{endpoint_url}jobs','job',source,endpoint_params_job,session_sf)
                         print(str(source['source']) + ' - Job Done')
                         #--JOB APPLICATION
                         create_dataframe(f'{endpoint_url}job-applications', 'job_application',source,endpoint_params_job_app,session_sf)
                         print(str(source['source']) + ' - Job Application Done')
                         #--CANDIDATE
                         create_dataframe(f'{endpoint_url}candidates','candidate',source,endpoint_params_candidate,session_sf)
                         print(str(source['source']) + ' - Job Candidate Done')                    

               # CALL MERGE 
               res = session_sf.call('HR.TEAM_TAILOR.TT_DYNAMIC_REFRESH') 
               end_dtm = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
               create_log(end_dtm, start_dtm,end_dtm,
                          'SUCCESS',res,session_sf)
               print('Refresh Done : ' + res)    

    except Exception as e:
          print(e)
          res = session_sf.call('HR.TEAM_TAILOR.TT_DYNAMIC_REFRESH') 
          end_dtm =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
          create_log(end_dtm, start_dtm,end_dtm,
                          'ERROR',str(e),session_sf)
          print('Refresh Done : ' + res)    

def create_log(log_dtm, start_dtm, end_dtm, log_type, log_message, sf_session):
      sf_session.sql(f'INSERT INTO HR.TEAM_TAILOR."cfg_log" VALUES (\'{log_dtm}\',\'{log_type}\',\'{log_message}\',\'{start_dtm}\',\'{end_dtm}\')').collect()
      
def create_dataframe(endpoint_url, table_name, source, params, session):
     df = pandas.DataFrame()
     df_stages = pandas.DataFrame()
     df_activities = pandas.DataFrame()     
     creation_dtm = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
     
     source_name =source['source']
     country_name = source['country']
     
     if table_name not in ('department','location','roles','users','reject_reasons','company'):
        last_update_dtm = get_last_updated_dt('tt_' + table_name,session,source_name) 
        if last_update_dtm != None:
            params["filter[updated-at][from]"] = last_update_dtm
        else:
            params["filter[updated-at][from]"] = '2020-01-01T00:00:00.000+00:00'

     pg_count = 0
     if table_name == 'company':
          pg_count = 1
     else:
          pg_count = get_endpoint_response(endpoint_url, source_name, params)["meta"]["page-count"] 

     for current_page in range(1, pg_count + 1):
        params["page[number]"] = current_page
        print(source_name + ' - ' + table_name + ' - ' + str(current_page )+ '/' + str(pg_count))
        endpoint_response = None
        if table_name== 'company':
             endpoint_response = get_endpoint_response(endpoint_url, source_name, None)
        else:
             endpoint_response = get_endpoint_response(endpoint_url, source_name, params)

        #Get Stages if exist
        if table_name ==  'job_application':
            for ln in range(0, len(endpoint_response["data"]) ):
                if "stage" in  endpoint_response["data"][ln]["relationships"]:
                    df_stages = pandas.concat([df_stages, pandas.json_normalize(get_endpoint_response(endpoint_response["data"][ln]["relationships"]["stage"]["links"]["related"],
                                                                    source_name,None)["data"])],ignore_index=True)
                    df_stages["country"] = country_name
                    df_stages["source"] = source_name
                
        # Get Activities if exist
        if table_name in ('candidate'):
             for rowindex, candidate_data in enumerate(endpoint_response["data"]):
                  paramsActivities = dict()
                  activity_ok = []
                  pgCount_activities = get_endpoint_response(candidate_data["relationships"]["activities"]["links"]["related"],source_name,None)["meta"]["page-count"]

                  for current_page_act in range(1, pgCount_activities + 1):
                       paramsActivities["page[number]"] = current_page_act
                       paramsActivities["include"] = "user"
                       activities = get_endpoint_response(candidate_data["relationships"]["activities"]["links"]["related"],source_name,paramsActivities)
                       
                       for rowindex1, activity in enumerate(activities["data"]):
                            if activity["attributes"]["code"] == "stage":
                                 activity_ok.append(activity)
                  df_activities_ok = pandas.json_normalize(activity_ok)
                  df_activities_ok["candidate_id"] = candidate_data["id"] 
                  df_activities = pandas.concat([df_activities, df_activities_ok],ignore_index=True)

        df =pandas.concat([df, pandas.json_normalize(endpoint_response["data"])],ignore_index=True)

        #Commit in Snowflake after 10 pages
        #if current_page % 20 == 0:
        #     commit_dataframe(table_name, country_name, source_name, df, df_stages, df_activities, session,creation_dtm)    
        #     df = df.truncate(after=-1)    
        #     df_stages = df_stages.truncate(after=-1)
        #     df_activities = df_activities.truncate(after=-1)            
     
     commit_dataframe(table_name, country_name, source_name, df, df_stages, df_activities, session,creation_dtm)      
     df = df.truncate(after=-1)    
     df_stages = df_stages.truncate(after=-1)
     df_activities = df_activities.truncate(after=-1)            
     

def get_last_updated_dt(table_name,session, country):
    
    last_updated_dtm = None
    try:
         last_updated_dtm_result = session.sql(f'SELECT max("attributes.updated-at") LAST_UPDATED_DTM FROM HR.TEAM_TAILOR."{table_name}" WHERE "source" = \'{country}\'').collect()
         if last_updated_dtm_result[0]['LAST_UPDATED_DTM'] != None:
            last_updated_dtm = last_updated_dtm_result[0]['LAST_UPDATED_DTM']
    except Exception as e:
         pass

    return last_updated_dtm

def get_credentials_details(tt_country):
    #credentials = json.loads(_snowflake.get_generic_secret_string(tt_country), strict=False)    
    credentials = None
    with open(f"connexion/{tt_country}.json") as cred:
            credentials = json.load(cred)
    return credentials

def get_endpoint_response(endpoint_url , country, params):        
        endpoint_request_header =get_credentials_details(country)
        status_code = 0
        while status_code != 200:
                if params != None:
                    params["page[size]"] = "30"
                    endpoint_request_response = requests.get(endpoint_url, headers=endpoint_request_header
                                                         ,params=params)
                else :
                     endpoint_request_response = requests.get(endpoint_url, headers=endpoint_request_header)
                     
                status_code = endpoint_request_response.status_code
        return endpoint_request_response.json()

def truncate_api_tables(session):
     api_tables = session.sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEAM_TAILOR' AND TABLE_NAME LIKE 'api_%' AND ROW_COUNT > 0 ORDER BY 1").collect()     
     if len(api_tables) != 0:
          for idx in range(len(api_tables)):
               session.sql('TRUNCATE TABLE HR.TEAM_TAILOR."' + str(api_tables[idx][0]) + '"').collect()
               print(str(api_tables[idx][0]) +  ' truncated')  

     return str(len(api_tables))

def commit_dataframe(table_name, country_name, country,df, df_stages, df_activities, session,creation_dtm):
     if len(df)> 0:
          df["country"] = country_name
          df["source"] = country
          df["creation_dtm"] = creation_dtm
          if table_name == 'candidate':
               if "relationships.user.data" not in df.columns:
                    df["relationships.user.data"] = ""
          if table_name == 'job_application':
               if "relationships.reject-reason.data.id" not in df.columns:
                    df["relationships.reject-reason.data.id"] = ""
               if "relationships.reject-reason.data.type"  in df.columns:
                    df["relationships.reject-reason.data.type"] = ""
          if table_name == 'job':
               if "relationships.location.data.id" not in df.columns:
                    df["relationships.location.data.id"] = ""
               if "relationships.role.data.id" not in df.columns:
                    df["relationships.role.data.id"] = ""
               if "links.careersite-job-internal-url" not in df.columns:
                    df["links.careersite-job-internal-url"] = ""
               if "relationships.department.data.id" not in df.columns:
                    df["relationships.department.data.id"] = ""
               if "relationships.role.data" not in df.columns:
                    df["relationships.role.data"] = ""
               if "relationships.role.data.type" not in df.columns:
                    df["relationships.role.data.type"] = ""
          if table_name == 'users':
               if "relationships.department.data.id" not in df.columns:
                    df["relationships.department.data.id"] = ""
          df = df.astype(str)
          df = df.drop_duplicates()
          session.write_pandas(df, f'api_{table_name}_{country}',auto_create_table=False,overwrite=False)
     if len(df_stages) > 0 :
          df_stages["creation_dtm"] = creation_dtm
          df_stages = df_stages.astype(str)
          df_stages = df_stages.drop_duplicates()
          df_stages = df_stages.reset_index(drop=True)
          session.write_pandas(df_stages,f'api_stages_{country}',auto_create_table=False,overwrite=False)
     if len(df_activities) > 0:
          df_activities["country"] = country_name
          df_activities["source"] = country
          df_activities = df_activities.astype({'id':'int32', 'candidate_id':'int32'})
          df_activities = df_activities.loc[df_activities.groupby('candidate_id')['id'].idxmax()]
          df_activities["creation_dtm"] = creation_dtm
          df_activities = df_activities.astype(str)
          df_activities = df_activities.drop_duplicates()
          df_activities = df_activities.reset_index(drop=True)
          session.write_pandas(df_activities, f'api_activities_{country}', auto_create_table=False,overwrite=True)

#--FULL,FACT,DIM
main('FULL')


