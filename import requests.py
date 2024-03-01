import requests
import json
import pandas
import pytz
#import _snowflake
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
from datetime import datetime

def main():#session: snowpark.Session):
    connection_sf = "connexion/sf_hr.json"
    with open(connection_sf) as f:
        connection_params = json.load(f)
    session_sf = Session.builder.configs(connection_params).create()
       
    #Endpoint URLs
    endpoint_job_app_url    = "https://api.teamtailor.com/v1/job-applications" 
    endpoint_job_url        = "https://api.teamtailor.com/v1/jobs"
    endpoint_candidate_url  = "https://api.teamtailor.com/v1/candidates"
    #endpoint_stages_url     = "https://api.teamtailor.com/v1/stages"
    #Params
    endpoint_request_params_job_app = dict()
    endpoint_request_params_job = dict()
    endpoint_request_params_candidate = dict()
    #endpoint_request_params_stages = dict()

    endpoint_request_params_job_app["filter[updated-at][from]"]     = get_last_updated_dt('tt_job_application',session_sf)
    endpoint_request_params_job["filter[updated-at][from]"]         = get_last_updated_dt('tt_job',session_sf)
    endpoint_request_params_candidate["filter[updated-at][from]"]   = get_last_updated_dt('tt_candidate',session_sf)
    
    endpoint_request_params_job_app["include"]  = "candidate,job,stage,reject-reason"
    endpoint_request_params_job["include"]      = "department,location,role,user" 
    
    for country in ['belgium','brazil','colombia','france','group','kls_belgium','kls_france','portugal','uk','usa']:        
        #Pandas dataframe
        df_job_application = None
        df_job = None
        df_candidate = None
        #df_stages = None;t
        #Number of page returns by the API
        page_count_job_application = 0
        page_count_job = 0
        page_count_candidate = 0
        #page_count_stages = 0
        
        page_count_job_application = get_endpoint_response(endpoint_job_app_url, country, endpoint_request_params_job_app)["meta"]["page-count"]  
        page_count_job = get_endpoint_response(endpoint_job_url, country, endpoint_request_params_job)["meta"]["page-count"]  
        page_count_candidate = get_endpoint_response(endpoint_candidate_url, country, endpoint_request_params_candidate)["meta"]["page-count"] 
        #page_count_stages = get_endpoint_response(endpoint_stages_url, country, endpoint_request_params_stages)["meta"]["page-count"]  
        
        df_job_application, df_job, df_candidate = set_dataframe_template(df_job_application, df_job,df_candidate) #, df_stages = set_dataframe_template(df_job_application, df_job,df_candidate, df_stages)
        print(country)
        for current_page_job_app in range(1, page_count_job_application + 1):
            endpoint_request_params_job_app["page[number]"] = current_page_job_app
            
            endpoint_request_response_job_application = None
            endpoint_request_response_job_application = get_endpoint_response(endpoint_job_app_url, country, endpoint_request_params_job_app)
            
            for row_index, job_application_data in enumerate(endpoint_request_response_job_application["data"],1):
                set_dataframe_data(df_job_application,country, job_application_data = job_application_data)

        print('Job Application Done')
        for current_page_job in range(1, page_count_job + 1):
            endpoint_request_params_job["page[number]"] = current_page_job

            endpoint_request_response_job = None
            endpoint_request_response_job = get_endpoint_response(endpoint_job_url, country, endpoint_request_params_job)

            for row_index, job_data in enumerate(endpoint_request_response_job["data"],1):
                set_dataframe_data(df_job,country, job_data = job_data)
        print('Job Done')

        for current_page_candidate in range(1, page_count_candidate + 1):
            endpoint_request_params_candidate["page[number]"] = current_page_candidate

            endpoint_request_response_candidate = None
            endpoint_request_response_candidate = get_endpoint_response(endpoint_candidate_url,country, endpoint_request_params_candidate)

            for row_index, candidate_data in enumerate(endpoint_request_response_candidate["data"],1):
                set_dataframe_data(df_candidate, country, candidate_data = candidate_data)
        print('Candidate Done')

        #for current_page_stages in range(1, page_count_stages + 1):
        #    endpoint_request_params_stages["page[number]"] = current_page_stages

        #    endpoint_request_response_stages = None
        #    endpoint_request_response_stages = get_endpoint_response(endpoint_stages_url,country, endpoint_request_params_stages)

        #    for row_index, stage_data in enumerate(endpoint_request_response_stages["data"],1):
        #        set_dataframe_data(df_stages,country,stages_data = stage_data)
        #print('Stages Done')
        dfp_job_app, dfp_job, dfp_candidate = create_dataframe(country,df_job_application, df_job, df_candidate) #, dfp_stages = create_dataframe(country,df_job_application, df_job, df_candidate, df_stages)
        print('Create df Done')
        dfp_job_app = dfp_job_app.astype(str)        
        dfp_job = dfp_job.astype(str)
        dfp_candidate = dfp_candidate.astype(str)

        #Add Creation datetime
        creation_dtm = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        dfp_job_app['creation_dtm'] = creation_dtm
        dfp_job['creation_dtm'] = creation_dtm
        dfp_candidate['creation_dtm'] = creation_dtm

        #dfp_stages = dfp_stages.astype(str)

        session_sf.write_pandas(dfp_job_app, "api_job_application_" +country,auto_create_table = True, overwrite=True)        
        print('SF Job Application Load')
        session_sf.write_pandas(dfp_job, "api_job_" +country,auto_create_table = True, overwrite=True)         
        print('SF Job Load')
        session_sf.write_pandas(dfp_candidate, "api_candidate_" +country,auto_create_table = True, overwrite=True)     
        print('SF Candidate Load')
        #session_sf.write_pandas(dfp_stages, "api_stages_" +country,auto_create_table = True, overwrite=True)
        #print('SF Stages Load')    

    session_sf.call("TT_UPDATE_JOB_APPLICATION")  
    session_sf.call("TT_UPDATE_JOB")     
    session_sf.call("TT_UPDATE_CANDIDATE")  
    #session_sf.call("TT_UPDATE_STAGES")   
    return "Success"

def get_last_updated_dt(table_name,session):
    last_updated_dtm_result = session.sql(f'SELECT max("updated-at") LAST_UPDATED_DTM FROM HR.TEAM_TAILOR."{table_name}"').collect()
    last_updated_dtm = None
    if last_updated_dtm_result[0]['LAST_UPDATED_DTM'] != None:
        last_updated_dtm = last_updated_dtm_result[0]['LAST_UPDATED_DTM']
    else : 
        last_updated_dtm = '2000-01-01T00:00:00.000+00:00'
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

def set_dataframe_data(df,country,**kwargs):

    ################JOB APPLICATION######################################################################    
    def set_id_in_datadrame_data(df,job_application_id):
        df["id"].append(job_application_id)

    def set_job_id_in_dataframe_data(df, job_application_data):
        if job_application_data["relationships"]["job"]["data"]:
            df["job_id"].append(job_application_data["relationships"]["job"]["data"]["id"])
        else:
            df["job_id"].append("-1")
    
    def set_candidate_id_in_dataframe_data(df,job_application_data):
        if job_application_data["relationships"]["candidate"]["data"]:
            df["candidate_id"].append(job_application_data["relationships"]["candidate"]["data"]["id"])
        else:
            df["candidate_id"].append("-1")

    def set_stage_id_in_dataframe_data(df,job_application_data):
        if job_application_data["relationships"]["stage"]["data"]:
            df["stage_id"].append(job_application_data["relationships"]["stage"]["data"]["id"])
        else:
            df["stage_id"].append("-1")
    
    def set_reject_reason_id_in_dataframe_data(df,job_application_data):
        if job_application_data["relationships"]["reject-reason"]["data"]:
            df["reject_reason_id"].append(job_application_data["relationships"]["reject-reason"]["data"]["id"])
        else:
            df["reject_reason_id"].append("-1")

    def set_attributes_in_dataframe_data(df, job_application_data):
        for attribute in df["attributes"].keys():
            if job_application_data["attributes"][attribute]:
                df["attributes"][attribute].append(job_application_data["attributes"][attribute])
            else: 
                df["attributes"][attribute].append("")
    
    ############# JOB #############################################################################
    def set_job_id_in_datadrame_data(df, job_id):
        df["id"].append(job_id)

    def set_job_relationship_id_in_dataframe(df, job_data):
        #Department
        if job_data["relationships"]["department"]["data"]:
            df["department_id"].append(job_data["relationships"]["department"]["data"]["id"])
        else:
            df["department_id"].append("-1")
        
        #Location
        if job_data["relationships"]["location"]["data"]:
            df["location_id"].append(job_data["relationships"]["location"]["data"]["id"])
        else:
            df["location_id"].append("-1")
        
        #Role
        if job_data["relationships"]["role"]["data"]:
            df["role_id"].append(job_data["relationships"]["role"]["data"]["id"])
        else:
            df["role_id"].append("-1")
        
        #User
        if job_data["relationships"]["user"]["data"]:
            df["user_id"].append(job_data["relationships"]["user"]["data"]["id"])
        else:
            df["user_id"].append("-1")
        
    def set_job_attributes_in_dataframe_data(df, job_data):
        for attribute in df["attributes"].keys():
            if job_data["attributes"][attribute]:
                df["attributes"][attribute].append(job_data["attributes"][attribute])
            else:
                df["attributes"][attribute].append("")

    ############# CANDIDATE ########################################################################  
    def set_candidate_id_in_datadrame_data(df,candidate_id):
        df["id"].append(candidate_id)
    
    def set_candidate_attributes_in_dataframe_data(df, candidate_job):
        for attribute in df["attributes"].keys():
            if candidate_job["attributes"][attribute]:
                df["attributes"][attribute].append(candidate_job["attributes"][attribute])
            else:
                df["attributes"][attribute].append("")

    ############# STAGES ##########################################################################
    def set_stage_type_and_name_in_dataframe_data(df,stage_url):
        stage_api_response = get_endpoint_response(stage_url,country,None)["data"]
        if stage_api_response["attributes"]["stage-type"]:
            df["stage_type"].append(stage_api_response["attributes"]["stage-type"])
            df["stage_name"].append(stage_api_response["attributes"]["name"])
        else:
            df["stage_type"].append("")
            df["stage_name"].append("")


    def set_stage_id_in_df_data(df, stage_id):
        df["id"].append(stage_id)            
    
    def set_stage_attributes_in_df_data(df, stage_data):
        for attribute in df["attributes"].keys():
            if stage_data["attributes"][attribute]:
                df["attributes"][attribute].append(stage_data["attributes"][attribute])
            else:
                df["attributes"][attribute].append("") 

    def set_department_location_role_and_user_names_in_dataframe_data(df,job_data):
         for element in ["department", "location", "role", "user"]:
              element_api_response = get_endpoint_response(job_data["data"]["relationships"][element]["links"]["related"],country)
              if element == "location":
                   column_name = element
              else:
                   column_name = f"{element}_name"
              if element_api_response["data"]:
                   df[column_name].append(element_api_response["data"]["attributes"]["name"])
              else:
                   df[column_name].append("")  
    def set_fk_candidate_and_candidate_name_in_dataframe_data(df, candidate_data):
        if candidate_data["data"]:
            df["fk_candidate"].append(candidate_data["data"]["id"])
            if candidate_data["data"]["attributes"]["first-name"]:
                first_name = candidate_data["data"]["attributes"]["first-name"]
            else:
                first_name = None
            if candidate_data["data"]["attributes"]["last-name"]:
                last_name = candidate_data["data"]["attributes"]["last-name"]
            else:
                last_name = None
            if first_name and last_name:
                name  = first_name + " " + last_name
            else:
                name = last_name
            if name:
                df["candidate-name"].append(name.replace("  "," ").title())
            else :
                df["candidate-name"].append("")
        else : 
            df["candidate-name"].append("")
    
    def set_job_title_in_dataframe_data(df, job_api_data):
        df["job-title"].append(job_api_data["data"]["attributes"]["title"])


    
    def set_reject_reason_in_dataframe_data(df, endpointurl):
        reject_reason_api_respone = get_endpoint_response(endpointurl,country)
        if reject_reason_api_respone["data"]:
            df["reject-reason"].append(reject_reason_api_respone["data"]["attributes"]["reason"])
        else:
            df["reject-reason"].append("")

    def set_job_created_at_in_dataframe_data(df,job_api_data):
        df["job-created-at"].append(job_api_data["data"]["attributes"]["created-at"])
    
    def set_recruiter_in_dataframe_data(df,candidate_api_data):
        count_page_activities = get_endpoint_response(candidate_api_data["data"]["relationships"]["activities"]["links"]["related"],country)["meta"]["page-count"]
        stage_change_user = ""
        api_params = dict()

        for current_page_count in range(1, count_page_activities + 1):
            api_params["page[number]"] = current_page_count
            activities_api_response = get_endpoint_response(candidate_api_data["data"]["relationships"]["activities"]["links"]["related"],country,
                                                            endpoint_params =api_params)
            for activity in activities_api_response["data"]:
                if activity["attributes"]["code"] == "stage":
                    user_api_response = get_endpoint_response(activity["relationships"]["user"]["links"]["related"],country)
                    if user_api_response["data"]:
                        stage_change_user = user_api_response["data"]["attributes"]["name"]
                    else:
                        stage_change_user = ""
        df["stage-change-user"] = stage_change_user


    if "job_application_data" in kwargs:
        set_id_in_datadrame_data(df,kwargs["job_application_data"]["id"])
        set_job_id_in_dataframe_data(df,kwargs["job_application_data"])
        set_candidate_id_in_dataframe_data(df,kwargs["job_application_data"])        
        set_stage_id_in_dataframe_data(df,kwargs["job_application_data"])   
        set_reject_reason_id_in_dataframe_data(df,kwargs["job_application_data"])
        set_stage_type_and_name_in_dataframe_data(df,kwargs["job_application_data"]["relationships"]["stage"]["links"]["related"])    
        set_attributes_in_dataframe_data(df, kwargs["job_application_data"] )

    if "candidate_data" in kwargs:
        set_candidate_id_in_datadrame_data(df,kwargs["candidate_data"]["id"])
        set_candidate_attributes_in_dataframe_data(df,kwargs["candidate_data"])

    if "job_data" in kwargs:
        set_job_id_in_datadrame_data(df,kwargs["job_data"]["id"])
        set_job_relationship_id_in_dataframe(df,kwargs["job_data"])
        #set_stage_type_and_name_in_dataframe_data(df,kwargs["job_data"]["relationships"]["stages"]["links"]["related"])  
        set_job_attributes_in_dataframe_data(df,kwargs["job_data"])

    #if "stages_data" in kwargs:
    #    set_stage_id_in_df_data(df,kwargs["stages_data"]["id"])
    #    set_stage_attributes_in_df_data(df,kwargs["stages_data"])

    #set_candidate_tags_in_dataframe_data(df, kwargs["candidate_api_data"])
    #set_department_location_role_and_user_names_in_dataframe_data(df,kwargs["job_api_data"])
    #set_fk_job_in_dataframe_data(df, kwargs["job_api_data"])
    #set_job_title_in_dataframe_data(df,kwargs["job_api_data"])
    #set_reject_reason_in_dataframe_data(df,kwargs["job_application_date"]["relationships"]["reject-reason"]["links"]["related"])
    #set_job_created_at_in_dataframe_data(df,kwargs["job_api_data"])
    #set_recruiter_in_dataframe_data(df,kwargs["candidate_api_data"])

    return df   
                 
def set_dataframe_template(dfjobapp, dfjob, dfcandidate):#, dfStages):
    dfjobapp = {
            "id": [], "job_id":[],"candidate_id":[],"stage_id":[],"stage_type":[],"stage_name":[], "reject_reason_id":[],
            "attributes": {"cover-letter":[],"referring-site": [], "created-at": [], "rejected-at": [], "updated-at": [], "changed-stage-at": []}
    }
    dfjob = {
            "id":[], "department_id":[],"location_id":[],"role_id":[],"user_id":[],#"stage_type":[],"stage_name":[],
            "attributes":{"title": [],"created-at":[], "updated-at":[], "human-status":[], "status":[], "tags":[]}
    }
    dfcandidate = {
            "id":[], "type":[],
            "attributes":{"created-at":[],  "updated-at":[], "first-name":[], "last-name":[],"tags":[] }
    }
    #dfStages = {
    #        "id":[],
    #        "attributes":{"name":[], "stage-type":[], "created-at":[], "updated-at":[]}
    #}
    return dfjobapp,dfjob,dfcandidate#,dfStages

def create_dataframe(country, df_job_app, df_job, df_candidate):#, df_stages):
     dfp_job_app = pandas.DataFrame(
          {
            "country": country, 
            "job_application_id": df_job_app["id"], 
            "job_id": df_job_app["job_id"],
            "candidate_id": df_job_app["candidate_id"], 
            "stage_id": df_job_app["stage_id"],
            "stage_type": df_job_app["stage_type"],
            "stage_name": df_job_app["stage_name"],
            "reject_reason_id": df_job_app["reject_reason_id"],
            **df_job_app["attributes"]#,
            #"candidate-name": df["candidate-name"],
            #"department_name": df["department_name"],
            #"role_name": df["role_name"],
            #"user_name": df["user_name"], 
            #"stage-type": df["stage-type"], 
            #"stage-name": df["stage-name"], 
            #"reject-reason": df["reject-reason"], 
            #"job-created-at": df["job-created-at"], 
            #"job-human-status": df["job-human-status"],
            #"candidate-tags": df["candidate-tags"], 
            #"location": df["location"], 
            #"stage-change-user": df["stage-change-user"]  
         }
     )

     dfp_job = pandas.DataFrame(
         {
             "country":country,
             "job_id": df_job["id"],
             "department_id": df_job["department_id"],
             "location_id" : df_job["location_id"],
             "role_id" : df_job["role_id"],
             "user_id": df_job["user_id"],
             #"stage_type": df_job["stage_type"],
             #"stage_name": df_job["stage_name"],
             **df_job["attributes"]
         }
     )

     dfp_candidate = pandas.DataFrame(
         {
             "country":country,
             "candidate_id" : df_candidate["id"],
             **df_candidate["attributes"]
         }
     )

     #dfp_stages = pandas.DataFrame(
     #    {
     #        "country":country,
     #        "stage_id" : df_stages["id"],
     #        **df_stages["attributes"]
     #    }
     #)
     return dfp_job_app, dfp_job, dfp_candidate#, dfp_stages

main()