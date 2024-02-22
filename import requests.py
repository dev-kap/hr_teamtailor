import requests
import json
import pandas
#import _snowflake
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame

def main():#session: snowpark.Session):
    #Endpoint URLs
    endpoint_url="https://api.teamtailor.com/v1/job-applications" 
    endpoint_job_url = "https://api.teamtailor.com/v1/jobs"
    endpoint_candidate_url = "https://api.teamtailor.com/v1/candidates"
    #Params
    params = dict(created_at_from='2023-01-11T14:41:26.446+01:00')
    params["includes"]="candidate,job,stage,reject-reason"
    params["page_size]"]="30"
    endpoint_request_parameters = None
    endpoint_request_parameters = {"page[size]" : "30"}
    endpoint_request_parameters["filter[created-at][from]"] = params["created_at_from"]
    endpoint_request_parameters["include"] = params["includes"]

    endpoint_request_params_job = None
    endpoint_request_params_job = {"page[size]" : "30"}
    endpoint_request_params_job["filter[created-at][from]"] = params["created_at_from"]
    endpoint_request_params_job = {"include" : "department"}

    endpoint_request_params_candidate = None
    endpoint_request_params_candidate = {"page[size]" : "30"}
    endpoint_request_params_candidate["filter[created-at][from]"] = params["created_at_from"]

    
    for country in ['belgium']:#,'brazil','colombia','france','group','kls_belgium','kls_france','portugal','uk','usa']:        
        #job application               
        pdf = None
        df_job_application = None
        df_job = None
        df_candidate = None
        page_count_job_application = 0
        page_count_job = 0
        page_count_candidate = 0
        
        page_count_job_application = get_endpoint_response(endpoint_url, country, endpoint_request_parameters)["meta"]["page-count"]  
        #page_count_job = get_endpoint_response(endpoint_job_url, country, endpoint_params = endpoint_request_parameters)["meta"]["page-count"]  
        #page_count_candidate = get_endpoint_response(endpoint_candidate_url, country, endpoint_params = endpoint_request_parameters)["meta"]["page-count"]  
        
        df_job_application, df_job, df_candidate = set_dataframe_template(df_job_application, df_job,df_candidate)

        for current_page_nb in range(1, 2):
            endpoint_request_parameters["page[number]"] = current_page_nb
            
            endpoint_request_response_job_application = None
            endpoint_request_response_job_application = get_endpoint_response(endpoint_url, country, endpoint_request_parameters)
            
            for row_index, job_application_data in enumerate(endpoint_request_response_job_application["data"],1):
                #job_application_api_response = None
                #candidate_api_response = None
                #job_application_api_response = get_endpoint_response(job_application_data["relationships"]["job"]["links"]["related"], country)
                #candidate_api_response = get_endpoint_response(job_application_data["relationships"]["candidate"]["links"]["related"],country)

                set_dataframe_data(df_job_application,country, job_application_data = job_application_data)
                                        #, job_api_data= job_application_api_response, candidate_api_data = candidate_api_response)
        
        for current_page_job in range(1,2):
            endpoint_request_params_job["page[number]"] = current_page_job

            endpoint_request_response_job = None
            endpoint_request_response_job = get_endpoint_response(endpoint_job_url, country, endpoint_request_params_job)

            for row_index, job_data in enumerate(endpoint_request_response_job["data"],1):
                set_dataframe_data(df_job,country, job_data = job_data)
        
        for current_page_candidate in range(1,2):
            endpoint_request_params_candidate["page[number]"] = current_page_candidate

            endpoint_request_response_candidate = None
            endpoint_request_response_candidate = get_endpoint_response(endpoint_candidate_url,country, endpoint_request_params_candidate)

            for row_index, candidate_data in enumerate(endpoint_request_response_candidate["data"],1):
                set_dataframe_data(df_candidate, country, candidate_data = candidate_data)
        


        dfp_job_app, dfp_job, dfp_candidate = create_dataframe(country,df_job_application, df_job, df_candidate)

        dfp_job_app = dfp_job_app.astype(str)
        dfp_job = dfp_job.astype(str)
        dfp_candidate = dfp_candidate.astype(str)
        
        #Commit dataframe in SF
        #session.write_pandas(pdf, "Job_application_" + country,auto_create_table = True, overwrite=True)
        connection_sf = "connexion/sf_hr.json"
        with open(connection_sf) as f:
            connection_params = json.load(f)
        session_sf = Session.builder.configs(connection_params).create()

        session_sf.write_pandas(dfp_job_app, "job_application_" +country,auto_create_table = True, overwrite=True)
        session_sf.write_pandas(dfp_job, "job_" +country,auto_create_table = True, overwrite=True)
        session_sf.write_pandas(dfp_candidate, "candidate_" +country,auto_create_table = True, overwrite=True)

    return "Success"

def get_credentials_details(tt_country):
    #credentials = json.loads(_snowflake.get_generic_secret_string(tt_country), strict=False)    
    credentials = None
    with open("connexion/belgium.json") as cred:
            credentials = json.load(cred)
    return credentials

def get_endpoint_response(endpoint_url , country, params):        
        endpoint_request_header =get_credentials_details(country)
        status_code = 0
        while status_code != 200:
                if params:
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

    def set_department_id_in_dataframe(df, job_data):
        if job_data["relationships"]["department"]["data"]:
            df["department_id"].append(job_data["relationships"]["department"]["data"]["id"])
        else:
            df["department_id"].append("-1")
    
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

    def set_candidate_tags_in_dataframe_data(df, candidate_data):
         df["candidate-tags"].append(candidate_data["data"]["attributes"]["tags"])

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
    
    def set_fk_job_in_dataframe_data(df,job_api_data ):
        df["fk_job"].append(job_api_data["data"]["id"])

    def set_job_title_in_dataframe_data(df, job_api_data):
        df["job-title"].append(job_api_data["data"]["attributes"]["title"])

    def set_stage_type_and_name_in_dataframe_data(df,endpoint_url):
        stage_api_response = get_endpoint_response(endpoint_url,country)
        df["stage-type"].append(stage_api_response["data"]["attributes"]["stage-type"])
        df["stage-name"].append(stage_api_response["data"]["attributes"]["name"])
    
    def set_reject_reason_in_dataframe_data(df, endpointurl):
        reject_reason_api_respone = get_endpoint_response(endpointurl,country)
        if reject_reason_api_respone["data"]:
            df["reject-reason"].append(reject_reason_api_respone["data"]["attributes"]["reason"])
        else:
            df["reject-reason"].append("")

    def set_job_created_at_in_dataframe_data(df,job_api_data):
        df["job-created-at"].append(job_api_data["data"]["attributes"]["created-at"])

    def set_job_human_status_in_dataframe_data(df,job_api_data):
        df["job-human-status"].append(job_api_data["data"]["attributes"]["human-status"])
    
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
        set_attributes_in_dataframe_data(df, kwargs["job_application_data"] )

    if "candidate_data" in kwargs:
        set_candidate_id_in_datadrame_data(df,kwargs["candidate_data"]["id"])
        set_candidate_attributes_in_dataframe_data(df,kwargs["candidate_data"])

    if "job_data" in kwargs:
        set_job_id_in_datadrame_data(df,kwargs["job_data"]["id"])
        set_department_id_in_dataframe(df,kwargs["job_data"])
        set_job_attributes_in_dataframe_data(df,kwargs["job_data"])
    #set_candidate_tags_in_dataframe_data(df, kwargs["candidate_api_data"])
    #set_department_location_role_and_user_names_in_dataframe_data(df,kwargs["job_api_data"])
    #set_fk_job_in_dataframe_data(df, kwargs["job_api_data"])
    #set_job_title_in_dataframe_data(df,kwargs["job_api_data"])
    #set_stage_type_and_name_in_dataframe_data(df,kwargs["job_application_date"]["relationships"]["stage"]["links"]["related"])
    #set_reject_reason_in_dataframe_data(df,kwargs["job_application_date"]["relationships"]["reject-reason"]["links"]["related"])
    #set_job_created_at_in_dataframe_data(df,kwargs["job_api_data"])
    #set_job_human_status_in_dataframe_data(df,kwargs["job_api_data"])
    #set_recruiter_in_dataframe_data(df,kwargs["candidate_api_data"])

    return df   
                 
def set_dataframe_template(dfjobapp, dfjob, dfcandidate):
    dfjobapp = {
            "id": [], "job_id":[],"candidate_id":[],"stage_id":[],"reject_reason_id":[],
            "attributes": {"cover-letter":[],"referring-site": [], "created-at": [], "rejected-at": [], "updated-at": [], "changed-stage-at": []}
    }
    dfjob = {
            "id":[], "department_id":[],
            "attributes":{"title": [],"created-at":[], "updated-at":[], "human-status":[], "status":[], "tags":[]}
    }
    dfcandidate = {
            "id":[], "type":[],
            "attributes":{"created-at":[], "first-name":[], "last-name":[],"tags":[] }
    }
    return dfjobapp,dfjob,dfcandidate


def create_dataframe(country, df_job_app, df_job, df_candidate):
     dfp_job_app = pandas.DataFrame(
          {
            "country": country, 
            "job_application_id": df_job_app["id"], 
            "job_id": df_job_app["job_id"],
            "candidate_id": df_job_app["candidate_id"], 
            "stage_id": df_job_app["stage_id"],
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
     return dfp_job_app, dfp_job, dfp_candidate

main()