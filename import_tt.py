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
endpoint_job_app_url    = "https://api.teamtailor.com/v1/job-applications" 
endpoint_job_url        = "https://api.teamtailor.com/v1/jobs"
endpoint_candidate_url  = "https://api.teamtailor.com/v1/candidates"
endpoint_department_url = "https://api.teamtailor.com/v1/departments"
endpoint_location_url   = "https://api.teamtailor.com/v1/locations"
endpoint_company_url    = "https://api.teamtailor.com/v1/company"

def main():#session: snowpark.Session):
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
    endpoint_params_stages = dict()
    endpoint_params_reject_reason = dict()
    endpoint_params_activities = dict()
    endpoint_params_company = dict()

    endpoint_params_job_app["include"]  = "candidate,job,stage,reject-reason"
    endpoint_params_job["include"]      = "department,location,role,user" 
    endpoint_params_candidate["include"]= "activities"
    endpoint_params_users["include"] = "department,location"

    for country in ['belgium','brazil','canada_en','canada_fr','colombia','epm','france','group','kls_belgium','kls_france','kls_group','kls_north_america','spain','portugal','uk','usa']: 
    #for country in ['kls_group','kls_north_america','spain','portugal','uk','usa']:    
        try:   
          # COMPANY
          create_dataframe(endpoint_company_url,'company',country,endpoint_params_company,session_sf)
          print(country + ' - Company Done')        
          # JOB APPLICATION
          create_dataframe(endpoint_job_app_url, 'job_application',country,endpoint_params_job_app,session_sf)
          print(country + ' - Job Application Done')
          # JOB
          create_dataframe(endpoint_job_url,'job',country,endpoint_params_job,session_sf)
          print(country + ' - Job Done')
          # CANDIDATE
          create_dataframe(endpoint_candidate_url,'candidate',country,endpoint_params_candidate,session_sf)
          print(country + ' - Job Candidate Done')
          # DEPARTMENT
          create_dataframe(endpoint_department_url,'department', country, endpoint_params_department, session_sf)
          print(country + ' - Job Department Done')
          # LOCATION
          create_dataframe(endpoint_location_url, 'location',country,endpoint_params_location,session_sf)   
          print(country + ' - Job Location Done')      
          # ROLE
          create_dataframe(f'{endpoint_url}roles','roles',country,endpoint_params_roles, session_sf)
          print(country + ' - Job Role  Done')
          # USERS
          create_dataframe(f'{endpoint_url}users','users',country,endpoint_params_users,session_sf)   
          print(country + ' - Job User Done')   
          # REJECT REASON
          create_dataframe(f'{endpoint_url}reject-reasons','reject_reasons',country,endpoint_params_reject_reason, session_sf)   
          print(country + ' - Job Reject Reason Done')  

          # CALL MERFE FUNCTION
          
        except Exception as e:
             print(e)
             continue
    session_sf.call('HR.TEAM_TAILOR.TT_UPDATE_TABLES') 
  
def create_dataframe(endpoint_url, table_name, country, params, session):
     df = pandas.DataFrame()
     df_stages = pandas.DataFrame()
     df_activities = pandas.DataFrame()

     country_name = ""
     if country == "belgium":
          country_name = "Belgium"
     elif country == "brazil":
          country_name = "Brazil"
     elif country in  ("canada_en","canada_fr"):
          country_name = "Canada"
     elif country == "colombia":
          country_name = "Colombia"
     elif country == "epm":
          country_name = "EPM"
     elif country == "france":
          country_name = "France"
     elif country == "group":
          country_name = "Group"
     elif country == "kls_belgium":
          country_name = "KLS Belgium"
     elif country == "kls_france":
          country_name = "KLS France"
     elif country == "kls_group":
          country_name = "KLS Group"
     elif country == "kls_north_america":
          country_name = "KLS North America"
     elif country == "portugal":
          country_name = "Portugal"
     elif country == "spain":
          country_name = "Spain"
     elif country == "uk":
          country_name = "UK"
     elif country == "usa":
          country_name = "USA"


     if table_name not in ('department','location','roles','users','reject_reasons','company'):
        last_update_dtm = get_last_updated_dt('tt_' + table_name,session,country) 
        if last_update_dtm != None:
            params["filter[updated-at][from]"] = last_update_dtm
        else:
            params["filter[created-at][from]"] = '2024-01-01T00:00:00.000+00:00'

     pg_count = 0
     if table_name == 'company':
          pg_count = 1
     else:
          pg_count = get_endpoint_response(endpoint_url, country, params)["meta"]["page-count"] 

     for current_page in range(1, pg_count + 1):
        params["page[number]"] = current_page
        print(country + ' - ' + table_name + ' - ' + str(current_page )+ '/' + str(pg_count))
        endpoint_response = None
        if table_name== 'company':
             endpoint_response = get_endpoint_response(endpoint_url, country, None)
        else:
             endpoint_response = get_endpoint_response(endpoint_url, country, params)

        #Get Stages if exist
        if table_name ==  'job_application':
            for ln in range(0, len(endpoint_response["data"]) ):
                if "stage" in  endpoint_response["data"][ln]["relationships"]:
                    df_stages = pandas.concat([df_stages, pandas.json_normalize(get_endpoint_response(endpoint_response["data"][ln]["relationships"]["stage"]["links"]["related"],
                                                                    country,None)["data"])],ignore_index=True)
                    df_stages["country"] = country_name
                    df_stages["source"] = country
                
        # Get Activities if exist
        if table_name in ('candidate'):
             for rowindex, candidate_data in enumerate(endpoint_response["data"]):
                  paramsActivities = dict()
                  activity_ok = []
                  pgCount_activities = get_endpoint_response(candidate_data["relationships"]["activities"]["links"]["related"],country,None)["meta"]["page-count"]

                  for current_page in range(1, pgCount_activities + 1):
                       paramsActivities["page[number]"] = current_page
                       paramsActivities["include"] = "user"
                       activities = get_endpoint_response(candidate_data["relationships"]["activities"]["links"]["related"],country,paramsActivities)
                       
                       for rowindex1, activity in enumerate(activities["data"]):
                            if activity["attributes"]["code"] == "stage":
                                 activity_ok.append(activity)
                  df_activities_ok = pandas.json_normalize(activity_ok)
                  df_activities_ok["candidate_id"] = candidate_data["id"] 
                  df_activities = pandas.concat([df_activities, df_activities_ok],ignore_index=True)

        df =pandas.concat([df, pandas.json_normalize(endpoint_response["data"])],ignore_index=True)
     
     creation_dtm = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
     if len(df)> 0:
          df["country"] = country_name
          df["source"] = country
          df["creation_dtm"] = creation_dtm
          if table_name == 'job_application':
               if "relationships.reject-reason.data.id" not in df.columns:
                    df["relationships.reject-reason.data.id"] = ""
          if table_name == 'job':
               if "relationships.location.data.id" not in df.columns:
                    df["relationships.location.data.id"] = ""
               if "relationships.role.data.id" not in df.columns:
                    df["relationships.role.data.id"] = ""
          if table_name == 'users':
               if "relationships.department.data.id" not in df.columns:
                    df["relationships.department.data.id"] = ""
          df = df.astype(str)
          session.write_pandas(df, f'api_{table_name}_{country}',auto_create_table=False,overwrite=True)
     if len(df_stages) > 0 :
          df_stages["creation_dtm"] = creation_dtm
          df_stages = df_stages.astype(str)
          df_stages = df_stages.drop_duplicates()
          session.write_pandas(df_stages,f'api_stages_{country}',auto_create_table=False,overwrite=True)
     if len(df_activities) > 0:
          df_activities["country"] = country_name
          df_activities["source"] = country
          df_activities = df_activities.astype({'id':'int32', 'candidate_id':'int32'})
          df_activities = df_activities.loc[df_activities.groupby('candidate_id')['id'].idxmax()]
          df_activities["creation_dtm"] = creation_dtm
          df_activities = df_activities.astype(str)
          session.write_pandas(df_activities, f'api_activities_{country}', auto_create_table=False,overwrite=True)

     

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

main()

