import requests
import json
import pandas
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame

def main():
    endpoint_url="https://api.teamtailor.com/v1/job-applications" 
    params = dict(created_at_from='2024-01-01')
    endpoint_request_parameters = None
    endpoint_request_parameters = {"page[size]" : "30"}
    endpoint_request_parameters["filter[created-at][from]"] = params["created_at_from"]


    df_job_application = None
    
    page_count_job_application = get_endpoint_response(endpoint_url,endpoint_params = endpoint_request_parameters)["meta"]["page-count"]  
     
    df_job_application = set_job_application_dataframe_template(df_job_application)

    for current_page_nb in range(1, 2):
        endpoint_request_parameters["page[number]"] = current_page_nb
        
        endpoint_request_response_job_application = get_endpoint_response(endpoint_url,endpoint_params = endpoint_request_parameters)
        for row_index, job_application_data in enumerate(endpoint_request_response_job_application["data"],1):
            job_api_response = get_endpoint_response(endpoint_url=job_application_data["relationships"]["job"]["links"]["related"])
            candidate_api_response = get_endpoint_response(endpoint_url=job_application_data["relationships"]["candidate"]["links"]["related"])

            set_dataframe_data_job_app(df_job_application, job_application_date = job_application_data
                                       , job_api_data= job_api_response, candidate_api_data = candidate_api_response)
    
    pdf = create_dataframe(df_job_application)

    #Commit dataframe in SF
    connection_sf = "connexion/sf_hr.json"
    with open(connection_sf) as f:
         connection_params = json.load(f)
    session_sf = Session.builder.configs(connection_params).create()

    pdf = pdf.astype(str)

    session_sf.write_pandas(pdf, "Job_application",auto_create_table = True, overwrite=True)

    print(endpoint_request_response_job_application)

def get_credentials_details():
    credentials = None
    with open("connexion/belgium.json") as cred:
            credentials = json.load(cred)
    return credentials

#def get_endpoint_response(endpoint_url , endpoint_headers, endpoint_params):
def get_endpoint_response(endpoint_url , **kwargs):        
        endpoint_request_header =get_credentials_details()
        status_code = 0
        while status_code != 200:
                if "endpoint_params" in kwargs:
                    endpoint_request_response = requests.get(endpoint_url, headers=endpoint_request_header
                                                         ,params=kwargs["endpoint_params"])
                else :
                     endpoint_request_response = requests.get(endpoint_url, headers=endpoint_request_header)
                     
                status_code = endpoint_request_response.status_code
        return endpoint_request_response.json()

def set_dataframe_data_job_app(df, **kwargs):

    def set_attributes_in_dataframe_data(df, job_application_data):
        for attribute in df["attributes"].keys():
            if job_application_data["attributes"][attribute]:
                df["attributes"][attribute].append(job_application_data["attributes"][attribute])
            else: 
                df["attributes"][attribute].append("")
    
    def set_candidate_tags_in_dataframe_data(df, candidate_data):
         df["candidate-tags"].append(candidate_data["data"]["attributes"]["tags"])

    def set_department_location_role_and_user_names_in_dataframe_data(df,job_data):
         for element in ["department", "location", "role", "user"]:
              element_api_response = get_endpoint_response(endpoint_url=job_data["data"]["relationships"][element]["links"]["related"])
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
    
    def set_id_in_datadrame_data(df,job_application_id):
        df["id"].append(job_application_id)

    def set_job_title_in_dataframe_data(df, job_api_data):
        df["job-title"].append(job_api_data["data"]["attributes"]["title"])

    def set_stage_type_and_name_in_dataframe_data(df,endpoint_url):
        stage_api_response = get_endpoint_response(endpoint_url=endpoint_url)
        df["stage-type"].append(stage_api_response["data"]["attributes"]["stage-type"])
        df["stage-name"].append(stage_api_response["data"]["attributes"]["name"])
    
    def set_reject_reason_in_dataframe_data(df, endpointurl):
        reject_reason_api_respone = get_endpoint_response(endpoint_url=endpointurl)
        if reject_reason_api_respone["data"]:
            df["reject-reason"].append(reject_reason_api_respone["data"]["attributes"]["reason"])
        else:
            df["reject-reason"].append("")

    def set_job_created_at_in_dataframe_data(df,job_api_data):
        df["job-created-at"].append(job_api_data["data"]["attributes"]["created-at"])

    def set_job_human_status_in_dataframe_data(df,job_api_data):
        df["job-human-status"].append(job_api_data["data"]["attributes"]["human-status"])
    
    def set_recruiter_in_dataframe_data(df,candidate_api_data):
        count_page_activities = get_endpoint_response(endpoint_url=candidate_api_data["data"]["relationships"]["activities"]["links"]["related"])["meta"]["page-count"]
        stage_change_user = ""
        api_params = dict()

        for current_page_count in range(1, count_page_activities + 1):
            api_params["page[number]"] = current_page_count
            activities_api_response = get_endpoint_response(endpoint_url=candidate_api_data["data"]["relationships"]["activities"]["links"]["related"],
                                                            endpoint_params =api_params)
            for activity in activities_api_response["data"]:
                if activity["attributes"]["code"] == "stage":
                    user_api_response = get_endpoint_response(endpoint_url=activity["relationships"]["user"]["links"]["related"])
                    if user_api_response["data"]:
                        stage_change_user = user_api_response["data"]["attributes"]["name"]
                    else:
                        stage_change_user = ""
        df["stage-change-user"] = stage_change_user


    set_attributes_in_dataframe_data(df, kwargs["job_application_date"] )
    set_candidate_tags_in_dataframe_data(df, kwargs["candidate_api_data"])
    set_department_location_role_and_user_names_in_dataframe_data(df,kwargs["job_api_data"])
    set_fk_candidate_and_candidate_name_in_dataframe_data(df, kwargs["candidate_api_data"])
    set_fk_job_in_dataframe_data(df, kwargs["job_api_data"])
    set_id_in_datadrame_data(df,kwargs["job_application_date"]["id"])
    set_job_title_in_dataframe_data(df,kwargs["job_api_data"])
    set_stage_type_and_name_in_dataframe_data(df,kwargs["job_application_date"]["relationships"]["stage"]["links"]["related"])
    set_reject_reason_in_dataframe_data(df,kwargs["job_application_date"]["relationships"]["reject-reason"]["links"]["related"])
    set_job_created_at_in_dataframe_data(df,kwargs["job_api_data"])
    set_job_human_status_in_dataframe_data(df,kwargs["job_api_data"])
    set_recruiter_in_dataframe_data(df,kwargs["candidate_api_data"])

    return df   
                 
def set_job_application_dataframe_template(df):
    df = {
            "country": [], "id": [], "fk_candidate": [], "fk_job": [],
            "attributes": {"created-at": [], "referring-site": [], "rejected-at": [], "updated-at": [], "changed-stage-at": []},
            "candidate-name": [], "job-title": [], "department_name": [], "role_name": [], "user_name": [], "stage-type": [],
            "stage-name": [], "reject-reason": [], "job-created-at": [], "job-human-status": [], "candidate-tags": [],
            "location": [], "stage-change-user": []}
    return df

def create_dataframe(df):
     pdf = pandas.DataFrame(
          {
            "country": "Belgium", 
            "job_application_id": df["id"], 
            "candidate_id": df["fk_candidate"], 
            "job_id": df["fk_job"],
            "job-title": df["job-title"],
            **df["attributes"],
            "candidate-name": df["candidate-name"],
            "department_name": df["department_name"],
            "role_name": df["role_name"],
            "user_name": df["user_name"], 
            "stage-type": df["stage-type"], 
            "stage-name": df["stage-name"], 
            "reject-reason": df["reject-reason"], 
            "job-created-at": df["job-created-at"], 
            "job-human-status": df["job-human-status"],
            "candidate-tags": df["candidate-tags"], 
            "location": df["location"], 
            "stage-change-user": df["stage-change-user"]  
         }
     )
     return pdf
     
main()